# PulseGrid implementation notes

The original scaffold plan is now implemented as a local fleet-monitoring demo. This document records what the code does today and which reliability improvements remain.

## Event path

1. The concurrent simulator builds `TelemetryEvent` messages containing an event UUID, tenant/device IDs, observation time, sequence number, and metrics.
2. `POST /v1/telemetry` validates the binary Protobuf request before publishing it: required IDs, UUID format, timestamp, sequence number, metric names, and finite metric values. It then publishes the unchanged bytes to `telemetry.raw`, using `device_id` as the Kafka key.
3. The ingestion API waits for Kafka acknowledgement before returning a Protobuf `202 Accepted` response. Database storage happens asynchronously afterward.
4. The `telemetry-processor` consumer group reads the topic. A new group starts from the earliest available messages; an existing group resumes from its committed offsets.
5. The processor decodes and applies the same semantic validation. If a record is malformed or invalid, it publishes a JSON envelope containing the source metadata and Base64-encoded bytes to `telemetry.dlq`, then commits the source offset only after that publish succeeds.
6. For a valid event, the processor stores metric history and any threshold alerts in one PostgreSQL transaction.
7. It writes JSON latest state to `telemetry:latest:<tenant_id>:<device_id>` in Redis, then commits its Kafka offset.
8. The Query API serves JSON device discovery, latest state, history, and alerts. The React dashboard polls it every five seconds.

## Data model and replay behavior

| Store | Contents | Duplicate handling |
| --- | --- | --- |
| `telemetry_events` hypertable | One row per event metric, partitioned by observation time | Primary key `(event_id, metric, observed_at)` with `ON CONFLICT DO NOTHING` |
| `alerts` table | Threshold breach, severity, observed value, rule threshold, and status | Unique `(event_id, metric)` with `ON CONFLICT DO NOTHING` |
| Redis latest-state key | One JSON event for a tenant/device | `SET` replaces the prior value |
| `telemetry.dlq` Kafka topic | JSON envelope for malformed or semantically invalid source records | Source offset is committed only after Kafka acknowledges the dead-letter publish |

An identical replay does not add duplicate history or alert rows. This relies on an event keeping its UUID, metric names, and observation time during retries. A reused UUID with changed data is not a supported update mechanism.

The database transaction covers history and alerts together. Redis and the Kafka offset commit are separate operations. If the process stops after storing an event but before committing its offset, Kafka may deliver it again. This is why database inserts must tolerate replays. The same at-least-once boundary applies to dead-letter envelopes if the process stops after a DLQ publish but before the source offset commit.

Redis currently represents the last processed event, which can differ from the event with the greatest observation timestamp if devices send late data or a replay occurs. Timestamp/sequence guards and cache rebuilding are future improvements.

## Partitions and scale

The demo's `telemetry.raw` and `telemetry.dlq` topics each have three partitions on one broker with replication factor one. Events for the same key go to the same partition while the partitioning configuration is unchanged. Kafka preserves partition order; it does not impose a single order across all devices.

One processor currently handles the demo. Additional processors with the same group ID can share the partitions, up to three active consumers for this topic. That is an architectural scaling mechanism, not a benchmark or a high-availability claim.

## Dashboard scope

The dashboard discovers tenant devices from persisted history and queries the selected device's latest state, recent readings, and active alerts. A missing Redis key is shown as an empty latest-state result rather than a failure of the entire API.

Chart values use observation timestamps and retain subsecond detail for tooltips. History requests are bounded by both time window and row limit, so a 24-hour window does not guarantee that every reading from that period is displayed under heavy traffic.

## Known limits

- The APIs have no authentication or tenant authorization. Tenant IDs currently scope data queries, not user permissions.
- The processor has a dead-letter queue, but no automated triage, alerting, or controlled replay workflow for those envelopes yet.
- Storage failures can stop the processor. Supervision can restart it, but application-level retry/backoff policies remain future work.
- Alerts stay active until a future lifecycle feature updates them. Returning a metric to normal does not automatically resolve previous alerts.
- The simulator resets its sequence counter between runs unless `--start-sequence` is supplied.
- `/health` is an API liveness check, not an end-to-end pipeline readiness or lag measurement.
- No throughput, p95 latency, large-fleet capacity, or uptime targets have been measured.

## Release order

Finish and verify the reproducible local Docker demo, including its isolated `scripts.smoke_demo` check, then record a short demonstration and prepare the repository for review. Next select hosting, add access controls and HTTPS, deploy, and repeat the end-to-end checks. Use the [MVP checklist](mvp-timeline.md) for release progress and the [portfolio guide](portfolio.md) for an honest demonstration and resume description.
