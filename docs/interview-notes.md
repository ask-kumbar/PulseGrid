# PulseGrid interview notes

## Thirty-second explanation

PulseGrid is a local fleet-signal monitoring project. A simulator sends typed Protobuf telemetry to a FastAPI ingestion service. Kafka decouples the request path from processing. A consumer writes current state to Redis, durable metric history and alerts to TimescaleDB, and a separate JSON Query API feeds a React dashboard.

```text
Simulator → Ingestion API → Kafka → Processor → Redis + TimescaleDB → Query API → Dashboard
```

## Design decisions to explain

| Decision | Reason |
| --- | --- |
| Protobuf for ingestion | A compact, typed contract for device-to-service telemetry. |
| FastAPI at the edge | Validates the request and responds after Kafka acknowledges it. |
| Kafka between ingestion and storage | Decouples producers from storage work, supports replay, and provides partition-based consumer scaling. |
| `device_id` as the Kafka key | Events for one device go to one partition, preserving their append order within that partition. |
| Dead-letter topic | Malformed or semantically invalid Kafka records are captured with source metadata instead of blocking the source partition. |
| Redis for latest state | A dashboard can read the most recently processed device event without scanning history. |
| TimescaleDB for history | PostgreSQL SQL queries plus time-series storage/indexing around `observed_at`. |
| JSON Query API | Browser-friendly responses that are easy to inspect and chart. |
| React polling | Simple and reliable for the MVP; the dashboard refreshes every five seconds. |

## Delivery and replay answer

The ingestion endpoint returns `202 Accepted` after Kafka acknowledges an event. That confirms the event entered Kafka; it does **not** mean the database writes have already completed.

The processor disables automatic Kafka commits. It stores history and alerts in a PostgreSQL transaction, writes the latest JSON state to Redis, and then commits the Kafka offset. If the process fails before the commit, Kafka can deliver the event again.

That is **at-least-once** processing. Duplicate database writes are safe because history uses the unique key `(event_id, metric, observed_at)` and alerts use `(event_id, metric)`, both with `ON CONFLICT DO NOTHING`. Redis `SET` simply replaces the prior latest-state value. This is not exactly-once processing across Kafka, Redis, and PostgreSQL because those operations do not share one distributed transaction.

If the processor cannot decode a record or it fails semantic validation, it publishes a JSON envelope to `telemetry.dlq` before committing the source offset. The envelope preserves the source metadata and Base64-encoded original bytes. If the dead-letter publish fails, the source offset stays uncommitted for retry.

## Alert answer

The processor evaluates YAML-configured threshold rules for each metric. A breach creates a durable warning or critical alert in TimescaleDB; the dashboard reads active alerts through the Query API. The thresholds are demonstration values, not equipment safety limits.

## Honest current scope

- The data comes from a simulator, not physical robots or deployed devices.
- The demo uses one Kafka broker, one processor, replication factor one, and no throughput/latency benchmark claim.
- There is no authentication, tenant authorization, alert acknowledgement workflow, or automatic alert resolution yet.
- The processor captures invalid records in a dead-letter topic, but automated triage, alerting, and controlled replay tooling are future work.
- Redis holds the last processed event; late-event timestamp ordering is a future improvement.
- Docker packaging is local-only until a secure hosted deployment is configured with HTTPS, secrets, and access controls.

## Good next-step answer

For a production version, I would first add authentication and tenant authorization, dead-letter triage/replay workflows, late-event handling for latest state, monitoring/lag metrics, and replicated storage. I would add WebSockets or Server-Sent Events only if five-second polling became insufficient for the product requirement.
