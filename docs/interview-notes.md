# Interview narrative

**Problem:** ingest high-volume sensor telemetry, serve current device state quickly, and retain data for historical analysis.

**Flow:** device → ingestion API → Kafka (`telemetry.raw`) → consumer processor → Redis + TimescaleDB.

**Why Kafka:** it decouples producers and consumers, supports replay, and scales through partitions and consumer groups.

**Why Redis:** dashboards need the latest device state quickly; querying an entire history table for every refresh is unnecessary.

**Why TimescaleDB:** SQL plus time-series indexing, retention, compression, and time-bucket aggregation.

**Delivery semantics:** at-least-once. The processor is idempotent through the event UUID and database unique constraint.

**Ordering:** Kafka preserves ordering only within a partition, so events are keyed by device ID.
