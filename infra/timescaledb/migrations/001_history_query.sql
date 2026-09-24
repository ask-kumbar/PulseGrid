ALTER TABLE telemetry_events
  ADD COLUMN IF NOT EXISTS unit TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS telemetry_events_tenant_device_time_idx
  ON telemetry_events (tenant_id, device_id, observed_at DESC);
