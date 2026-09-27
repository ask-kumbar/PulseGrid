# ⚡ PulseGrid Command Guide

> A practical reference for running and understanding the PulseGrid telemetry pipeline.

```text
Simulator → FastAPI → Kafka → Processor → Redis + TimescaleDB
```

## Quick start

### Start Kafka

```bash
docker compose up -d kafka
```

### Start Redis

```bash
docker compose up -d redis
```

### Start TimescaleDB

```bash
docker compose up -d timescaledb
```

### Start the ingestion API

```bash
uvicorn services.ingestion_api.main:app --reload
```

### Start the query API

```bash
uvicorn services.query_api.main:app --reload --port 8001
```

### Start the processor

```bash
python -m services.telemetry_processor.main
```

### Send one telemetry event

```bash
python -m services.device_simulator.main
```

### Check API health

```bash
curl http://127.0.0.1:8000/health
```

> [!TIP]
> Run commands from the project root unless a section says otherwise:

```bash
cd /Users/apple/Documents/ChatGPT/PulseGrid
```

---

## 1 · 🐳 Docker and Kafka

Docker Desktop runs Kafka locally in a container.

### Make Docker available in the current terminal

```bash
export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
```

> [!NOTE]
> This is needed on this Mac because Docker Desktop's command-line tools are not automatically on the shell `PATH`. It applies only to the current terminal window.

### Verify Docker Desktop

```bash
docker version
docker compose version
```

The first command checks the Docker client and engine. The second checks Docker Compose.

### Start Kafka only

```bash
docker compose up -d kafka
```

Downloads the Kafka image if necessary and starts only the `kafka` service in the background. It maps the Mac's port `9092` to Kafka's container port `9092`.

### Inspect running containers

```bash
docker compose ps
```

Shows the service name, container name, status, and exposed ports. The correct project container name begins with `pulsegrid-kafka-1`.

### Stop local containers

```bash
docker compose down
```

> [!WARNING]
> This stops and removes containers created by this Compose project. It does not remove downloaded images. Do not add `-v` unless you intentionally want to delete Docker-managed data volumes.

---

## 2 · 📬 Kafka topics

A topic is a named event stream. PulseGrid uses `telemetry.raw` for incoming, unprocessed telemetry.

### Create the telemetry topic

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic telemetry.raw \
  --partitions 3 \
  --replication-factor 1
```

`3` partitions let us later run up to three active workers in one consumer group. `replication-factor 1` is required locally because we have one Kafka broker.

### Describe the topic

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic telemetry.raw
```

Shows the topic's partitions, leader broker, replicas, and in-sync replicas.

### Manual console producer

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic telemetry.raw \
  --property parse.key=true \
  --property key.separator=":"
```

Then type a key and value, for example:

```text
sensor-204:{"temperature_c":27.4}
```

The text before the first `:` is the Kafka key. In PulseGrid, the key is `device_id`, which determines the partition.

### Manual console consumer

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic telemetry.raw \
  --from-beginning \
  --property print.partition=true \
  --property print.key=true \
  --property key.separator=" | "
```

Reads topic events and prints their partition, key, and value. Press `Ctrl + C` to stop the console consumer or producer; Kafka continues running.

---

## 3 · 🐍 Python environment

PulseGrid uses Python 3.12 or newer.

### Check Python version and path

```bash
python3.12 --version
which python3.12
python3.12 -c "import sys; print(sys.executable)"
```

### Create and activate the virtual environment

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python --version
```

The `(.venv)` prefix in the terminal means the environment is active. Project libraries are installed there instead of globally on the Mac.

### Install the project in editable development mode

```bash
pip install --upgrade -e ".[dev]"
```

`-e` means editable: Python uses the source files in this project folder directly. The `[dev]` extra also installs development tools such as `grpcio-tools`, `pytest`, and `pytest-asyncio`.

---

## 4 · 📦 Protobuf schema and generated Python code

The schema is in `proto/telemetry.proto`. Protobuf code is generated; do not edit generated files manually.

### Install the Protobuf runtime and compiler

```bash
pip install protobuf grpcio-tools
```

`protobuf` reads and writes Protobuf messages. `grpcio-tools` provides the `protoc` compiler command.

### Generate Python classes

```bash
mkdir -p generated

python -m grpc_tools.protoc \
  -I proto \
  --python_out=generated \
  proto/telemetry.proto
```

This converts `proto/telemetry.proto` into `generated/telemetry_pb2.py`.

### Test serialization and deserialization

```bash
python -m scripts.protobuf_demo
```

Creates one `TelemetryEvent`, serializes it to Protobuf bytes, deserializes it, and prints the restored event.

---

## 5 · 🚀 FastAPI ingestion service

The ingestion API accepts Protobuf HTTP requests at `POST /v1/telemetry` and publishes validated event bytes to Kafka.

### Start the API

```bash
source .venv/bin/activate
uvicorn services.ingestion_api.main:app --reload
```

Uvicorn runs the API on `http://127.0.0.1:8000` by default. `--reload` restarts it after source-code changes. Kafka must be running because the API creates a Kafka producer during startup.

### Check API health

```bash
curl http://127.0.0.1:8000/health
```

Expected response:

```json
{"status":"ok"}
```

---

## 6 · 🔄 Device simulator and processor

### Send one Protobuf telemetry event

```bash
source .venv/bin/activate
python -m services.device_simulator.main
```

The simulator creates a `TelemetryEvent`, serializes it to Protobuf bytes, sends it to FastAPI, and deserializes the Protobuf acknowledgment. Expected output includes `accepted=True`, the device ID, sequence number, and event ID.

### Run a finite multi-device load test

```bash
python -m services.device_simulator.main --devices 3 --count 20 --interval 0.5
```

This runs three device tasks concurrently. Each device sends 20 ordered events, one every half second. Each event contains temperature and humidity metrics.

### Run continuous telemetry

```bash
python -m services.device_simulator.main --devices 3 --count 0 --interval 1
```

`--count 0` continues until `Ctrl+C`. Use `--start-sequence` when continuing a simulated device from a known sequence number.

### Run one processor worker

```bash
source .venv/bin/activate
python -m services.telemetry_processor.main
```

The processor joins consumer group `telemetry-processor`, consumes newly published records from `telemetry.raw`, deserializes their Protobuf bytes, writes history to TimescaleDB, updates the latest state in Redis, then commits its Kafka offset.

---

## 7 · ⚡ Redis latest device state

Redis stores the latest telemetry state for each tenant and device. It is not the event history; TimescaleDB handles history.

### Start Redis

```bash
docker compose up -d redis
```

### Verify Redis is running

```bash
docker compose exec redis redis-cli ping
```

Expected response:

```text
PONG
```

### Read the latest state for `sensor-204`

```bash
docker compose exec redis redis-cli --raw GET telemetry:latest:acme:sensor-204 | jq
```

The key format is `telemetry:latest:<tenant_id>:<device_id>`. The value is the most recent event as formatted JSON.

---

## 8 · 🕒 TimescaleDB telemetry history

TimescaleDB is PostgreSQL with time-series capabilities. It stores every processed metric as a durable history row.

### Start TimescaleDB

```bash
docker compose up -d timescaledb
```

### Inspect the telemetry table

```bash
docker compose exec timescaledb psql -U pulsegrid -d pulsegrid -c "\d telemetry_events"
```

### Upgrade an existing local table

Run this once if the TimescaleDB volume was created before metric units and the tenant-aware index were added:

```bash
docker compose exec -T timescaledb psql -U pulsegrid -d pulsegrid < infra/timescaledb/migrations/001_history_query.sql
```

### Read recent telemetry history

```bash
docker compose exec timescaledb psql -U pulsegrid -d pulsegrid -c "SELECT event_id, device_id, metric, value, observed_at FROM telemetry_events ORDER BY observed_at DESC;"
```

Each event metric becomes one row. The table's primary key is `(event_id, metric, observed_at)`, so Kafka redelivery does not create duplicate history rows.

---

## 9 · 🔎 Query API

The query API serves dashboard-friendly JSON. It reads current state from Redis and historical metrics from TimescaleDB.

### Start the query API

```bash
source .venv/bin/activate
uvicorn services.query_api.main:app --reload --port 8001
```

### Check query API health

```bash
curl http://127.0.0.1:8001/health
```

### Read latest device state

```bash
curl http://127.0.0.1:8001/v1/tenants/acme/devices/sensor-204/latest | jq
```

### Read device history

```bash
curl "http://127.0.0.1:8001/v1/tenants/acme/devices/sensor-204/history?hours=24&limit=100" | jq
```

Add `&metric=temperature_c` to filter the history to one metric. `hours` accepts `1` through `720`; `limit` accepts `1` through `1000`.

---

## 10 · ✅ Current end-to-end test

Use four terminals. Start them in this order:

```text
Terminal 1: Kafka + Redis + TimescaleDB + API
        ↓
Terminal 2: Processor
        ↓
Terminal 3: Query API
        ↓
Terminal 4: Simulator and query checks
```

### Terminal 1: infrastructure and API

Ensure Kafka, Redis, and TimescaleDB are running, then start FastAPI:

```bash
export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
docker compose up -d kafka redis timescaledb

source .venv/bin/activate
uvicorn services.ingestion_api.main:app --reload
```

### Terminal 2: processor worker

```bash
source .venv/bin/activate
python -m services.telemetry_processor.main
```

### Terminal 3: query API

```bash
source .venv/bin/activate
uvicorn services.query_api.main:app --reload --port 8001
```

### Terminal 4: simulator and queries

```bash
source .venv/bin/activate
python -m services.device_simulator.main
curl http://127.0.0.1:8001/v1/tenants/acme/devices/sensor-204/latest | jq
curl "http://127.0.0.1:8001/v1/tenants/acme/devices/sensor-204/history?hours=24&limit=100" | jq
```

Expected flow:

```text
Simulator → FastAPI → Kafka → processor → Redis + TimescaleDB → query API
```

The API returns `202 Accepted`, and the processor prints the decoded event with its Kafka partition and offset. Verify Redis with `GET` and TimescaleDB with the history query above.

---

## 11 · 🧪 Automated tests

Run the test suite from the project root:

```bash
python -m pytest -q
```

The tests verify event construction, Protobuf request validation, Kafka publishing, Redis latest-state responses, and TimescaleDB history-query behavior. They replace external services with in-memory fakes, so Docker does not need to be running.

Expected result:

```text
9 passed
```

---

## 12 · 🧹 Clean local topic reset

> [!CAUTION]
> This permanently deletes every event in `telemetry.raw`. Stop the processor first with `Ctrl + C`.

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic telemetry.raw
```

Recreate the topic afterward:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic telemetry.raw \
  --partitions 3 \
  --replication-factor 1
```

New events will begin at offset `0` in each partition.
