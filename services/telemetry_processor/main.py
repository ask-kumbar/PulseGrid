import asyncio
import json
import os

import asyncpg
from aiokafka import AIOKafkaConsumer
from redis.asyncio import Redis
from dotenv import load_dotenv

load_dotenv()


async def main() -> None:
    consumer = AIOKafkaConsumer(
        "telemetry.raw",
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        group_id="telemetry-processor",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    redis = Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)
    database = await asyncpg.connect(os.getenv(
        "DATABASE_URL", "postgresql://pulsegrid:change-me-for-local-only@localhost:5432/pulsegrid"
    ))
    await consumer.start()
    try:
        async for record in consumer:
            event = json.loads(record.value)
            await redis.hset(f"device:{event['deviceId']}:latest", mapping={
                event["metric"]: event["value"],
                "updated_at": event["timestamp"],
            })
            await database.execute(
                """INSERT INTO telemetry_events
                   (event_id, device_id, tenant_id, observed_at, sequence_no, metric, value)
                   VALUES ($1, $2, $3, $4, $5, $6, $7)
                   ON CONFLICT (event_id) DO NOTHING""",
                event["eventId"], event["deviceId"], event["tenantId"], event["timestamp"],
                event["sequenceNo"], event["metric"], event["value"],
            )
            await consumer.commit()
    finally:
        await consumer.stop()
        await redis.aclose()
        await database.close()


if __name__ == "__main__":
    asyncio.run(main())
