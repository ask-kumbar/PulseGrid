# Build plan

## Milestone 1 — Infrastructure

Run the Compose stack. Create the Kafka topics with three partitions:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic telemetry.raw --partitions 3 --replication-factor 1
```

Also create `telemetry.dlq` with one partition locally.

## Milestone 2 — Ingestion API

Build a FastAPI `POST /v1/telemetry` endpoint. It validates the event contract and produces an event to `telemetry.raw`, keyed by `deviceId`.

## Milestone 3 — Processor

Build a consumer group named `telemetry-processor`. For each valid record:

1. Deduplicate on `eventId`.
2. Upsert latest metric state in Redis under `device:{deviceId}:latest`.
3. Insert the event into `telemetry_events` with `ON CONFLICT (event_id) DO NOTHING`.
4. Put parse/processing failures in `telemetry.dlq`.

## Milestone 4 — Query API and dashboard

Expose latest device state from Redis and time-range queries from TimescaleDB. Add WebSocket or Server-Sent Events after the processor is reliable.

## Demo targets

- Simulate 1,000 devices at 1 message per second.
- Show consumer lag while intentionally slowing the processor.
- Kill and restart the processor; demonstrate Kafka replay and idempotent writes.
- Stop Redis, restore it, and rebuild latest state from Kafka/history.
