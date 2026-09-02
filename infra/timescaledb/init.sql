CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE telemetry_events (
  event_id UUID PRIMARY KEY,
  device_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  sequence_no BIGINT NOT NULL,
  metric TEXT NOT NULL,
  value DOUBLE PRECISION NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable('telemetry_events', by_range('observed_at'), if_not_exists => TRUE);
CREATE INDEX telemetry_events_device_time_idx ON telemetry_events (device_id, observed_at DESC);
