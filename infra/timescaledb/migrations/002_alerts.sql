CREATE TABLE IF NOT EXISTS alerts (
  alert_id UUID PRIMARY KEY,
  event_id UUID NOT NULL,
  tenant_id TEXT NOT NULL,
  device_id TEXT NOT NULL,
  metric TEXT NOT NULL,
  unit TEXT NOT NULL,
  value DOUBLE PRECISION NOT NULL,
  threshold DOUBLE PRECISION NOT NULL,
  condition TEXT NOT NULL CHECK (condition IN ('high', 'low')),
  severity TEXT NOT NULL CHECK (severity IN ('warning', 'critical')),
  message TEXT NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  status TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'acknowledged', 'resolved')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (event_id, metric)
);

CREATE INDEX IF NOT EXISTS alerts_tenant_status_time_idx
  ON alerts (tenant_id, status, observed_at DESC);

CREATE INDEX IF NOT EXISTS alerts_tenant_device_time_idx
  ON alerts (tenant_id, device_id, observed_at DESC);
