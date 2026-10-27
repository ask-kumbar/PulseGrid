# ⚡ PulseGrid Command Guide

> A practical reference for running and understanding the PulseGrid telemetry pipeline.

```text
Simulator → Ingestion API → Kafka: telemetry.raw → Processor → Redis + TimescaleDB → Query API → Dashboard
                                                   └→ Kafka: telemetry.dlq (invalid records)
```

## Choose a workflow

- **Full Docker demo**: the quickest way to demonstrate the complete application. Docker runs the APIs, processor, stores, and built dashboard together.
- **Development stack**: run the infrastructure in Docker and the Python/React services on your machine while you edit and learn the code.

Do not combine `compose.demo.yaml` with the development `compose.yaml`; they are intentionally separate environments.

## 0 · 🎬 Full Docker demo

Use this self-contained workflow when you want to show the project without starting five local terminals. Docker Desktop must be running.

```bash
cd /Users/apple/Documents/ChatGPT/PulseGrid
docker compose -f compose.demo.yaml up --build -d --wait
```

Open [http://localhost:8080](http://localhost:8080). The dashboard starts empty until telemetry arrives. Send sample traffic from a one-shot simulator container:

```bash
docker compose -f compose.demo.yaml run --rm simulator --devices 3 --count 30 --interval 1
```

Then switch between `sensor-204`, `sensor-205`, and `sensor-206` in the device selector. To create a predictable critical alert:

```bash
docker compose -f compose.demo.yaml run --rm simulator --devices 1 --count 1 --scenario temperature-critical
```

For a real-stack behavior check, run the smoke script. It uses a new tenant on every run and verifies ingestion, validation, device discovery, storage, alerting, and an identical-event retry:

```bash
docker compose -f compose.demo.yaml run --rm --entrypoint python simulator -m scripts.smoke_demo
```

Inspect or stop the demo without deleting its data volumes:

```bash
docker compose -f compose.demo.yaml ps
docker compose -f compose.demo.yaml down
```

The demo binds only the dashboard (`127.0.0.1:8080`) and ingestion API (`127.0.0.1:8081`) to the host. Kafka, Redis, TimescaleDB, and the Query API remain internal to the Docker network. Do not expose this unauthenticated demo publicly.

Optional demo overrides live in `.env.demo.example`. Copy it to `.env.demo`, then use `docker compose --env-file .env.demo -f compose.demo.yaml ...`. The default values are local-demo credentials only.

---

## Quick start: development stack

Run backend commands from the project root. Activate the virtual environment in each terminal that runs a Python service:

```bash
cd /Users/apple/Documents/ChatGPT/PulseGrid
source .venv/bin/activate
```

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

### Start the dashboard in a separate terminal

```bash
cd /Users/apple/Documents/ChatGPT/PulseGrid/dashboard
npm run dev
```

Open [http://localhost:5173](http://localhost:5173). Keep the Query API running on port `8001`. On the first run, install dashboard dependencies with `npm ci` before starting Vite; see section 10 below.

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

## 1 · 🐳 Development Docker infrastructure

The development `compose.yaml` runs Kafka, Redis, and TimescaleDB only. You start the APIs, processor, simulator, and dashboard from your local source tree.

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

Shows the service name, container name, status, and exposed ports. Container names usually begin with `pulsegrid-`; rely on the `SERVICE` column rather than copying a container name.

### Stop local containers

```bash
docker compose down
```

> [!WARNING]
> This stops and removes containers created by this Compose project. It does not remove downloaded images. Do not add `-v` unless you intentionally want to delete Docker-managed data volumes.

---

## 2 · 📬 Kafka topics

A topic is a named event stream. PulseGrid uses `telemetry.raw` for incoming, unprocessed telemetry and `telemetry.dlq` for malformed or semantically invalid records that the processor cannot safely store.

### Create the development topics

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic telemetry.raw \
  --partitions 3 \
  --replication-factor 1

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic telemetry.dlq \
  --partitions 3 \
  --replication-factor 1
```

`3` partitions let us later run up to three active workers in one consumer group. `replication-factor 1` is required locally because we have one Kafka broker. The full Docker demo creates both topics automatically; this step is only for the development stack.

### Describe the topic

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic telemetry.raw
```

Shows the topic's partitions, leader broker, replicas, and in-sync replicas.

### Inspect the live telemetry topic

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic telemetry.raw \
  --from-beginning \
  --property print.partition=true \
  --property print.key=true \
  --property key.separator=" | "
```

The key and partition are readable, but the value is binary Protobuf and may look like unreadable characters. That is expected. Use the simulator and processor logs for a readable end-to-end event flow.

### Inspect dead-letter records

`telemetry.dlq` values are JSON envelopes, so they are readable in Kafka's console consumer. Each envelope includes a failure reason, source topic/partition/offset, and Base64-encoded original key and payload bytes.

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic telemetry.dlq \
  --from-beginning
```

### Optional text-only Kafka exercise

The console producer writes text, whereas PulseGrid's processor expects binary Protobuf. Do **not** send JSON text to `telemetry.raw` during normal work: the processor will dead-letter it to `telemetry.dlq` rather than store it. If you want to learn Kafka keys and partitions manually, create a scratch topic instead:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic telemetry.scratch --partitions 1 --replication-factor 1

docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic telemetry.scratch \
  --property parse.key=true \
  --property key.separator=":"
```

Then enter a test record such as:

```text
sensor-204:{"temperature_c":27.4}
```

The text before the first `:` is the Kafka key. Press `Ctrl+C` to stop the console producer; Kafka itself continues running.

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

### Use the project Protobuf tooling

The development install from section 3 includes the Protobuf runtime and `grpcio-tools` compiler. Keep generation inside `.venv` so the generated code is compatible with the installed runtime:

```bash
source .venv/bin/activate
python -m pip install -e ".[dev]"
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

### Generate a threshold breach

Use a named scenario to create a predictable alert for an end-to-end test:

```bash
python -m services.device_simulator.main --count 1 --scenario temperature-critical
```

Available scenarios are `normal`, `temperature-warning`, `temperature-critical`, `humidity-low`, and `humidity-critical`.

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

The processor joins consumer group `telemetry-processor`, decodes and validates records from `telemetry.raw`, then writes history to TimescaleDB and the latest state to Redis before committing its Kafka offset. If decoding or semantic validation fails, it first publishes a JSON envelope to `telemetry.dlq`; the source offset is committed only after Kafka acknowledges that dead-letter publish.

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

Create the alerts table in an existing local database:

```bash
docker compose exec -T timescaledb psql -U pulsegrid -d pulsegrid < infra/timescaledb/migrations/002_alerts.sql
```

Verify its columns, constraints, and indexes:

```bash
docker compose exec timescaledb psql -U pulsegrid -d pulsegrid -c "\d alerts"
```

### Read recent alerts

```bash
docker compose exec timescaledb psql -U pulsegrid -d pulsegrid -c "SELECT alert_id, device_id, metric, severity, value, threshold, status, observed_at FROM alerts ORDER BY observed_at DESC LIMIT 10;"
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

### Discover tenant devices

```bash
curl "http://127.0.0.1:8001/v1/tenants/acme/devices?limit=100" | jq
```

This reads device IDs from persisted history, ordered by most recent observation. It returns an empty list for a tenant with no stored telemetry.

### Read latest device state

```bash
curl http://127.0.0.1:8001/v1/tenants/acme/devices/sensor-204/latest | jq
```

### Read device history

```bash
curl "http://127.0.0.1:8001/v1/tenants/acme/devices/sensor-204/history?hours=24&limit=100" | jq
```

Add `&metric=temperature_c` to filter the history to one metric. `hours` accepts `1` through `720`; `limit` accepts `1` through `1000`.

### Read active alerts

```bash
curl "http://127.0.0.1:8001/v1/tenants/acme/alerts?severity=critical&limit=20" | jq
```

Alerts are returned newest first. `status` defaults to `active`; it can also be `acknowledged` or `resolved`. Optionally add `&device_id=sensor-204` to filter one device.

---

## 10 · 🖥️ React dashboard

The dashboard initially targets tenant `acme` and `sensor-204`, then discovers available devices and lets you switch between them. It displays latest readings, history charts, and active alerts, refreshing every five seconds.

### Install dashboard dependencies

Node.js and npm must be installed. The dashboard's Vite version requires a current supported Node release; Node `20.19+` or `22.12+` is suitable.

```bash
node --version
npm --version
cd /Users/apple/Documents/ChatGPT/PulseGrid/dashboard
npm ci
```

`npm ci` installs the dependency versions recorded in `package-lock.json`. Run it for the initial setup or after dependency changes; it is not needed every time you start the dashboard.

### Start the development server

```bash
cd /Users/apple/Documents/ChatGPT/PulseGrid/dashboard
npm run dev
```

Open [http://localhost:5173](http://localhost:5173). If that port is occupied, Vite may choose another one; use the `Local` URL printed in the terminal. Leave this terminal running. Vite updates the page when frontend source files change.

No Python virtual environment is needed for this terminal. For reading existing data, the dashboard needs the Query API, Redis, and TimescaleDB. To generate new readings, also run Kafka, the ingestion API, the processor, and the simulator as shown in section 11.

### Check the dashboard-to-API connection

With the Query API and Vite running, use another terminal:

```bash
curl http://127.0.0.1:8001/health
curl http://localhost:5173/api/health
```

Both should return `{"status":"ok"}`. The second command goes through Vite, which forwards `/api/health` to `http://127.0.0.1:8001/health`. Adjust `5173` if Vite printed a different port.

No dashboard `.env` file is required for the default local setup. If you create one, use `VITE_QUERY_API_URL=/api` or omit the setting; a blank value also falls back to `/api`. Restart Vite after changing environment values.

### Check a production build

```bash
cd /Users/apple/Documents/ChatGPT/PulseGrid/dashboard
npm run build
```

Checks TypeScript and generates browser files in `dashboard/dist/`. This does not deploy the dashboard or start its backend services.

### Stop the dashboard

Press `Ctrl+C` in the Vite terminal. The backend services running in other terminals continue running.

---

## 11 · ✅ Run the full dashboard pipeline

Use five terminals. These commands assume the initial Python setup, Kafka topic creation, and database setup in the earlier sections are complete, and dashboard dependencies are installed. Docker Desktop must be running. Start the services in this order, waiting for each service to finish starting before sending events:

```text
Terminal 1: Kafka + Redis + TimescaleDB + ingestion API
        ↓
Terminal 2: Processor
        ↓
Terminal 3: Query API
        ↓
Terminal 4: Dashboard
        ↓
Terminal 5: Simulator and query checks
```

### Terminal 1: infrastructure and API

Ensure Kafka, Redis, and TimescaleDB are running, then start FastAPI:

```bash
cd /Users/apple/Documents/ChatGPT/PulseGrid
export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
docker compose up -d kafka redis timescaledb

source .venv/bin/activate
uvicorn services.ingestion_api.main:app --reload
```

### Terminal 2: processor worker

```bash
cd /Users/apple/Documents/ChatGPT/PulseGrid
source .venv/bin/activate
python -m services.telemetry_processor.main
```

### Terminal 3: query API

```bash
cd /Users/apple/Documents/ChatGPT/PulseGrid
source .venv/bin/activate
uvicorn services.query_api.main:app --reload --port 8001
```

### Terminal 4: dashboard

```bash
cd /Users/apple/Documents/ChatGPT/PulseGrid/dashboard
npm run dev
```

Open [http://localhost:5173](http://localhost:5173), or the URL printed by Vite.

### Terminal 5: continuous device readings

```bash
cd /Users/apple/Documents/ChatGPT/PulseGrid
source .venv/bin/activate
python -m services.device_simulator.main --devices 1 --count 0 --interval 1
```

Sends readings for `sensor-204` repeatedly, with a one-second pause between sends. The metric cards and history charts refresh every five seconds. Press `Ctrl+C` in this simulator terminal when you want to stop sending.

Then send one critical temperature event to populate the alerts panel:

```bash
python -m services.device_simulator.main --count 1 --scenario temperature-critical
```

Wait for the processor to print that it stored the event, then verify the Query API:

```bash
curl http://127.0.0.1:8001/v1/tenants/acme/devices/sensor-204/latest | jq
curl "http://127.0.0.1:8001/v1/tenants/acme/devices/sensor-204/history?hours=24&limit=100" | jq
curl "http://127.0.0.1:8001/v1/tenants/acme/alerts?device_id=sensor-204&status=active&limit=20" | jq
```

Expected flow:

```text
Simulator → Ingestion API → Kafka → processor → Redis + TimescaleDB → Query API → Dashboard
```

The ingestion API returns `202 Accepted`. The processor stores the event, and the dashboard picks it up on its next refresh. An empty latest-state message means no Redis state exists yet for the selected device; start the processor and send a reading. Empty charts can also mean there are no history readings within the last 24 hours.

---

## 12 · 🧪 Automated tests

Run the test suite from the project root:

```bash
python -m pytest -q
```

The tests verify event construction, Protobuf request validation, Kafka publishing, Redis latest-state responses, and TimescaleDB history-query behavior. They replace external services with in-memory fakes, so Docker does not need to be running.

Expected result:

```text
64 passed
```

The test count can grow as coverage is added; the important result is a passing suite with no failures.

---

## 13 · 🧹 Clean local topic reset

> [!CAUTION]
> This permanently deletes every event in `telemetry.raw`. Stop the processor first with `Ctrl + C`.
> It does not delete `telemetry.dlq`, Redis latest-state keys, TimescaleDB history, alerts, or Docker volumes.

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

On this newly created local topic, new records begin at offset `0` in each partition.
