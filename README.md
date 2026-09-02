# PulseGrid

PulseGrid is a hands-on distributed IoT telemetry pipeline. Devices send telemetry to an API; the API puts it on Kafka; a processor stores current state in Redis and history in TimescaleDB.

```mermaid
flowchart LR
  D[Device simulator] --> A[Ingestion API]
  A --> K[(Kafka: telemetry.raw)]
  K --> P[Processor consumer]
  P --> R[(Redis: latest state)]
  P --> T[(TimescaleDB: history)]
```

## First milestone

Run Kafka, Redis, and TimescaleDB with Docker, then start the ingestion API. The processor and simulator are included as the next small services to run.

## Local setup

1. Install Docker Desktop and Python 3.12+.
2. Copy environment values and start infrastructure:

   ```bash
   cp .env.example .env
   docker compose up -d
   ```

3. Create the Kafka topics:

   ```bash
   docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic telemetry.raw --partitions 3 --replication-factor 1
   docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic telemetry.dlq --partitions 1 --replication-factor 1
   ```

4. Create a virtual environment, install dependencies, and run the API:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -e .
   uvicorn services.ingestion_api.main:app --reload
   ```

5. In a second terminal, run the processor; in a third, run the simulator:

   ```bash
   python -m services.telemetry_processor.main
   python -m services.device_simulator.main
   ```

Open `http://127.0.0.1:8000/docs` to send a test event.

## Event contract

```json
{
  "eventId": "9c03fa7b-7069-4c47-94ef-4c0d7203120c",
  "deviceId": "sensor-204",
  "tenantId": "acme",
  "timestamp": "2026-09-02T10:15:31.245Z",
  "sequenceNo": 18420,
  "metric": "temperature_c",
  "value": 27.4
}
```

Kafka uses `deviceId` as the message key, which preserves ordering for each device.

## What you will learn

- Kafka producers, consumers, partitions, consumer groups, and offsets
- Redis as a fast latest-state store
- TimescaleDB for durable time-series history
- Idempotent processing with event IDs

## License

MIT
