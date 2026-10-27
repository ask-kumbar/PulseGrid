import asyncio
import base64
import json
import os
from datetime import timezone
from typing import Any
from uuid import uuid4

import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from dotenv import load_dotenv
from google.protobuf.message import DecodeError
from redis import asyncio as redis

from generated.telemetry_pb2 import TelemetryEvent
from services.telemetry_validation import TelemetryValidationError, validate_telemetry_event
from services.telemetry_processor.alert_rules import (
    AlertDecision,
    AlertRulesFile,
    evaluate_metric,
    load_alert_rules,
)

load_dotenv()

TELEMETRY_TOPIC = "telemetry.raw"
DEAD_LETTER_TOPIC = os.getenv("TELEMETRY_DLQ_TOPIC", "telemetry.dlq")
CONSUMER_GROUP = "telemetry-processor"
DATABASE_URL = "postgresql://pulsegrid:change-me-for-local-only@localhost:5432/pulsegrid"
DEFAULT_ALERT_RULES_PATH = "config/alert_rules.yaml"


def dead_letter_envelope(record: Any, reason: str) -> bytes:
    """Encode an invalid Kafka record for the telemetry DLQ.

    The JSON envelope preserves the source location and raw payload so an
    operator can inspect or replay the record later.  Binary Kafka values and
    keys are represented as base64 strings; a null Kafka value remains null.
    """
    raw_value = getattr(record, "value", None)
    raw_key = getattr(record, "key", None)
    source: dict[str, Any] = {
        "topic": getattr(record, "topic", TELEMETRY_TOPIC),
        "partition": getattr(record, "partition", None),
        "offset": getattr(record, "offset", None),
        "timestamp_ms": getattr(record, "timestamp", None),
        "key_base64": (
            base64.b64encode(raw_key).decode("ascii") if raw_key is not None else None
        ),
    }
    envelope = {
        "schema_version": 1,
        "reason": reason,
        "source": source,
        "raw_payload_base64": (
            base64.b64encode(raw_value).decode("ascii") if raw_value is not None else None
        ),
    }
    return json.dumps(envelope, separators=(",", ":"), sort_keys=True).encode("utf-8")


async def send_to_dead_letter(
    producer: AIOKafkaProducer,
    record: Any,
    reason: str,
) -> None:
    """Publish an invalid record before allowing the source offset to commit."""
    await producer.send_and_wait(
        DEAD_LETTER_TOPIC,
        dead_letter_envelope(record, reason),
        key=getattr(record, "key", None),
    )


def latest_state_key(event: TelemetryEvent) -> str:
    """Return the Redis key that represents one device's current state."""
    return f"telemetry:latest:{event.tenant_id}:{event.device_id}"


def latest_state_value(event: TelemetryEvent) -> str:
    """Turn the Protobuf event into JSON that a future query API can return."""
    state = {
        "event_id": event.event_id,
        "device_id": event.device_id,
        "tenant_id": event.tenant_id,
        "observed_at": event.observed_at.ToJsonString(),
        "sequence_no": event.sequence_no,
        "metrics": [
            {"name": metric.name, "value": metric.value, "unit": metric.unit}
            for metric in event.metrics
        ],
    }
    return json.dumps(state)


async def insert_alert(
    database: asyncpg.Connection,
    event: TelemetryEvent,
    decision: AlertDecision,
) -> str | None:
    """Persist one threshold breach, unless this event/metric already alerted."""
    alert_id = await database.fetchval(
        """
        INSERT INTO alerts
          (alert_id, event_id, tenant_id, device_id, metric, unit, value, threshold,
           condition, severity, message, observed_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (event_id, metric) DO NOTHING
        RETURNING alert_id
        """,
        uuid4(),
        event.event_id,
        event.tenant_id,
        event.device_id,
        decision.metric,
        decision.unit,
        decision.value,
        decision.threshold,
        decision.condition,
        decision.severity,
        decision.message,
        event.observed_at.ToDatetime(tzinfo=timezone.utc),
    )
    return str(alert_id) if alert_id is not None else None


async def store_event(
    database: asyncpg.Connection,
    event: TelemetryEvent,
    alert_rules: AlertRulesFile,
) -> list[str]:
    """Store all metric history rows and any threshold alerts as one DB transaction."""
    created_alert_ids: list[str] = []
    observed_at = event.observed_at.ToDatetime(tzinfo=timezone.utc)

    # One event can contain several metrics, so each metric is a history row.
    async with database.transaction():
        for metric in event.metrics:
            await database.execute(
                """
                INSERT INTO telemetry_events
                  (event_id, device_id, tenant_id, observed_at, sequence_no, metric, value, unit)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (event_id, metric, observed_at) DO NOTHING
                """,
                event.event_id,
                event.device_id,
                event.tenant_id,
                observed_at,
                event.sequence_no,
                metric.name,
                metric.value,
                metric.unit,
            )

            try:
                decision = evaluate_metric(
                    metric.name,
                    metric.value,
                    metric.unit,
                    alert_rules,
                )
            except ValueError as error:
                # Retain valid telemetry even if its configured alert rule expects another unit.
                print(f"Skipping alert evaluation for {metric.name}: {error}")
                continue

            if decision is None:
                continue

            alert_id = await insert_alert(database, event, decision)
            if alert_id is not None:
                created_alert_ids.append(alert_id)

    return created_alert_ids


async def main() -> None:
    alert_rules_path = os.getenv("ALERT_RULES_PATH", DEFAULT_ALERT_RULES_PATH)
    alert_rules = load_alert_rules(alert_rules_path)
    redis_client = redis.from_url(
        os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        decode_responses=True,
    )
    consumer = AIOKafkaConsumer(
        TELEMETRY_TOPIC,
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        group_id=CONSUMER_GROUP,
        enable_auto_commit=False,
        # A new group must process events accepted before the processor started.
        # Existing groups resume from their committed offsets as usual.
        auto_offset_reset="earliest",
    )
    dead_letter_producer = AIOKafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        acks="all",
    )

    database: asyncpg.Connection | None = None
    dead_letter_producer_started = False
    try:
        await redis_client.ping()
        database = await asyncpg.connect(os.getenv("DATABASE_URL", DATABASE_URL))
        await dead_letter_producer.start()
        dead_letter_producer_started = True
        await consumer.start()
        print(
            f"Processor started: group={CONSUMER_GROUP}, topic={TELEMETRY_TOPIC}, "
            f"dlq={DEAD_LETTER_TOPIC}"
        )
        print(f"Loaded alert rules: path={alert_rules_path}, metrics={len(alert_rules.rules)}")

        async for record in consumer:
            event = TelemetryEvent()
            try:
                event.ParseFromString(record.value)
            except (DecodeError, TypeError) as error:
                reason = f"protobuf_decode_error: {error}"
                await send_to_dead_letter(dead_letter_producer, record, reason)
                print(
                    f"Dead-lettered invalid Protobuf at partition={record.partition}, "
                    f"offset={record.offset}"
                )
                await consumer.commit()
                continue

            try:
                validate_telemetry_event(event)
            except TelemetryValidationError as error:
                reason = f"semantic_validation_error: {error}"
                await send_to_dead_letter(dead_letter_producer, record, reason)
                print(
                    f"Dead-lettered invalid telemetry at partition={record.partition}, "
                    f"offset={record.offset}: {error}"
                )
                await consumer.commit()
                continue

            print(f"Received partition={record.partition}, offset={record.offset}")
            print(event)

            created_alert_ids = await store_event(database, event, alert_rules)

            key = latest_state_key(event)
            await redis_client.set(key, latest_state_value(event))
            print(f"Stored history in TimescaleDB and latest state in Redis: {key}")
            if created_alert_ids:
                print(f"Created alerts: {', '.join(created_alert_ids)}")

            # Both stores have the event, so it is safe to record this Kafka offset.
            await consumer.commit()
    finally:
        await consumer.stop()
        if dead_letter_producer_started:
            await dead_letter_producer.stop()
        if database is not None:
            await database.close()
        await redis_client.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # asyncio.run cancels main first, so its finally block closes all clients.
        print("Processor stopped.")
