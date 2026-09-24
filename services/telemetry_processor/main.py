import asyncio
import json
import os
from datetime import timezone

import asyncpg
from aiokafka import AIOKafkaConsumer
from dotenv import load_dotenv
from google.protobuf.message import DecodeError
from redis import asyncio as redis

from generated.telemetry_pb2 import TelemetryEvent

load_dotenv()

TELEMETRY_TOPIC = "telemetry.raw"
CONSUMER_GROUP = "telemetry-processor"
DATABASE_URL = "postgresql://pulsegrid:change-me-for-local-only@localhost:5432/pulsegrid"


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


async def main() -> None:
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
                        event.observed_at.ToDatetime(tzinfo=timezone.utc),
                        event.sequence_no,
                        metric.name,
                        metric.value,
                        metric.unit,
                    )

            key = latest_state_key(event)
            await redis_client.set(key, latest_state_value(event))
            print(f"Stored history in TimescaleDB and latest state in Redis: {key}")

            # Both stores have the event, so it is safe to record this Kafka offset.
            await consumer.commit()
    finally:
        await consumer.stop()
        if database is not None:
            await database.close()
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
