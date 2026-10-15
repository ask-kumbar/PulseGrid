import asyncio
import json
import os
from datetime import timezone
from uuid import uuid4

import asyncpg
from aiokafka import AIOKafkaConsumer
from dotenv import load_dotenv
from google.protobuf.message import DecodeError
from redis import asyncio as redis

from generated.telemetry_pb2 import TelemetryEvent
from services.telemetry_processor.alert_rules import (
    AlertDecision,
    AlertRulesFile,
    evaluate_metric,
    load_alert_rules,
)

load_dotenv()

TELEMETRY_TOPIC = "telemetry.raw"
CONSUMER_GROUP = "telemetry-processor"
DATABASE_URL = "postgresql://pulsegrid:change-me-for-local-only@localhost:5432/pulsegrid"
DEFAULT_ALERT_RULES_PATH = "config/alert_rules.yaml"


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
        auto_offset_reset="latest",
    )

    database: asyncpg.Connection | None = None
    try:
        await redis_client.ping()
        database = await asyncpg.connect(os.getenv("DATABASE_URL", DATABASE_URL))
        await consumer.start()
        print(f"Processor started: group={CONSUMER_GROUP}, topic={TELEMETRY_TOPIC}")
        print(f"Loaded alert rules: path={alert_rules_path}, metrics={len(alert_rules.rules)}")

        async for record in consumer:
            event = TelemetryEvent()
            try:
                event.ParseFromString(record.value)
            except DecodeError:
                print(f"Skipping invalid Protobuf at partition={record.partition}, offset={record.offset}")
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
        if database is not None:
            await database.close()
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
