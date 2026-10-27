export type Metric = { name: string; value: number; unit: string }

export type LatestState = {
  event_id: string
  device_id: string
  tenant_id: string
  observed_at: string
  sequence_no: number
  metrics: Metric[]
}

export type HistoryReading = {
  event_id: string
  device_id: string
  tenant_id: string
  observed_at: string
  sequence_no: number
  metric: string
  value: number
  unit: string
  ingested_at: string
}

export type HistoryResponse = { readings: HistoryReading[] }

export type DeviceSummary = { device_id: string; last_observed_at: string }
export type DevicesResponse = { tenant_id: string; count: number; devices: DeviceSummary[] }

export type Alert = {
  alert_id: string
  event_id: string
  tenant_id: string
  device_id: string
  metric: string
  unit: string
  value: number
  threshold: number
  condition: 'high' | 'low'
  severity: 'warning' | 'critical'
  message: string
  observed_at: string
  status: 'active' | 'acknowledged' | 'resolved'
  created_at: string
}

export type AlertsResponse = { alerts: Alert[] }
