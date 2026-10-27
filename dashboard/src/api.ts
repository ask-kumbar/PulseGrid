import type { AlertsResponse, DevicesResponse, HistoryResponse, LatestState } from './types'

export type DashboardData = {
  latest: LatestState | null
  history: HistoryResponse
  alerts: AlertsResponse
  devices: DevicesResponse
}

const queryApiUrl = (import.meta.env.VITE_QUERY_API_URL?.trim() || '/api').replace(/\/$/, '')
export const tenantId = import.meta.env.VITE_TENANT_ID?.trim() || 'acme'
export const defaultDeviceId = import.meta.env.VITE_DEVICE_ID?.trim() || 'sensor-204'

async function requestJson<T>(path: string, signal: AbortSignal): Promise<T> {
  const response = await fetch(`${queryApiUrl}${path}`, { signal })
  if (!response.ok) throw new Error(`${path} returned HTTP ${response.status}`)
  return response.json() as Promise<T>
}

async function requestOptionalJson<T>(path: string, signal: AbortSignal): Promise<T | null> {
  const response = await fetch(`${queryApiUrl}${path}`, { signal })
  // The latest-state endpoint intentionally returns 404 until Redis has seen
  // telemetry for this device. That is an empty dashboard state, not an API failure.
  if (response.status === 404) return null
  if (!response.ok) throw new Error(`${path} returned HTTP ${response.status}`)
  return response.json() as Promise<T>
}

export async function getDashboardData(
  deviceId: string = defaultDeviceId,
  signal: AbortSignal = new AbortController().signal,
): Promise<DashboardData> {
  const encodedTenantId = encodeURIComponent(tenantId)
  const encodedDeviceId = encodeURIComponent(deviceId)
  const [latest, history, alerts, devices] = await Promise.all([
    requestOptionalJson<LatestState>(`/v1/tenants/${encodedTenantId}/devices/${encodedDeviceId}/latest`, signal),
    requestJson<HistoryResponse>(`/v1/tenants/${encodedTenantId}/devices/${encodedDeviceId}/history?hours=24&limit=500`, signal),
    requestJson<AlertsResponse>(`/v1/tenants/${encodedTenantId}/alerts?status=active&device_id=${encodedDeviceId}&limit=20`, signal),
    requestJson<DevicesResponse>(`/v1/tenants/${encodedTenantId}/devices?limit=1000`, signal),
  ])
  return { latest, history, alerts, devices }
}
