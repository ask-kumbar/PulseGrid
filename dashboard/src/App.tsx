import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  Activity,
  BellRing,
  Box,
  ChevronRight,
  Cpu,
  Gauge,
  RefreshCw,
  Thermometer,
  Waves,
  Wifi,
  Zap,
} from 'lucide-react'
import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { defaultDeviceId, getDashboardData, tenantId, type DashboardData } from './api'
import type { Alert, DeviceSummary, HistoryReading, LatestState } from './types'
import './App.css'

const REFRESH_INTERVAL_MS = 5_000

function formatTimestamp(value: string | undefined): string {
  if (!value) return '—'
  return new Intl.DateTimeFormat(undefined, {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).format(new Date(value))
}

function formatChartTimestamp(value: string | undefined): string {
  if (!value) return '—'
  return new Intl.DateTimeFormat(undefined, {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    fractionalSecondDigits: 3,
  }).format(new Date(value))
}

function formatAge(value: string | undefined): string {
  if (!value) return 'No recent event'
  const seconds = Math.max(0, Math.round((Date.now() - new Date(value).getTime()) / 1_000))
  if (seconds < 60) return `${seconds}s ago`
  if (seconds < 3_600) return `${Math.floor(seconds / 60)}m ago`
  return `${Math.floor(seconds / 3_600)}h ago`
}

function latestMetric(latest: LatestState | null, name: string) {
  return latest?.metrics.find((metric) => metric.name === name)
}

function chartPoints(readings: HistoryReading[], metric: string) {
  return readings
    .filter((reading) => reading.metric === metric)
    .map((reading) => ({ value: reading.value, observedAt: reading.observed_at }))
    .reverse()
}

function chartDomain(points: { value: number }[]): [number, number] {
  if (points.length === 0) return [0, 1]

  const values = points.map((point) => point.value)
  const minimum = Math.min(...values)
  const maximum = Math.max(...values)
  const range = maximum - minimum
  const padding = Math.max(range * 0.25, Math.abs(maximum) * 0.05, 1)

  // Round to a tenth so the axis is readable (for example, 23.6–25.0 °C).
  return [
    Math.floor((minimum - padding) * 10) / 10,
    Math.ceil((maximum + padding) * 10) / 10,
  ]
}

function MetricCard({
  label,
  value,
  unit,
  trend,
  icon,
  tone,
}: {
  label: string
  value: number | undefined
  unit: string
  trend: string
  icon: React.ReactNode
  tone: 'cyan' | 'violet'
}) {
  return (
    <article className={`metric-card metric-card--${tone}`}>
      <div className="metric-card__topline">
        <span className="metric-card__icon">{icon}</span>
        <span className="metric-card__trend">{trend}</span>
      </div>
      <p className="metric-card__label">{label}</p>
      <p className="metric-card__value">
        {value === undefined ? '—' : value.toFixed(1)}
        <span>{unit}</span>
      </p>
    </article>
  )
}

function ChartCard({
  title,
  unit,
  points,
  color,
}: {
  title: string
  unit: string
  points: { observedAt: string; value: number }[]
  color: string
}) {
  const gradientId = `gradient-${title.toLowerCase()}`
  const domain = chartDomain(points)
  return (
    <article className="chart-card">
      <div className="chart-card__header">
        <div>
          <p className="eyebrow">24-hour window</p>
          <h3>{title}</h3>
        </div>
        <span className="chart-card__unit">{unit}</span>
      </div>
      <div className="chart-card__plot">
        {points.length === 0 ? (
          <p className="empty-chart">Waiting for telemetry history</p>
        ) : (
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={points} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id={gradientId} x1="0" x2="0" y1="0" y2="1">
                  <stop offset="0%" stopColor={color} stopOpacity={0.8} />
                  <stop offset="100%" stopColor={color} stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid stroke="#202832" strokeDasharray="3 7" vertical={false} />
              <XAxis
                dataKey="observedAt"
                axisLine={false}
                tickLine={false}
                minTickGap={54}
                tick={{ fill: '#75808e', fontSize: 11 }}
                tickFormatter={(value) => formatChartTimestamp(String(value))}
              />
              <YAxis domain={domain} axisLine={false} tickLine={false} tick={{ fill: '#75808e', fontSize: 11 }} width={46} tickMargin={6} />
              <Tooltip
                contentStyle={{ background: '#121820', border: '1px solid #2a3542', borderRadius: 10, color: '#edf2f7' }}
                cursor={{ stroke: '#64748b', strokeDasharray: '3 5' }}
                labelFormatter={(value) => formatChartTimestamp(String(value))}
                formatter={(value) => [`${Number(value).toFixed(1)} ${unit}`, title]}
              />
              <Area
                type="monotone"
                dataKey="value"
                stroke={color}
                strokeWidth={2.5}
                fill={`url(#${gradientId})`}
                activeDot={{ r: 4, fill: color, stroke: '#0d1117', strokeWidth: 2 }}
              />
            </AreaChart>
          </ResponsiveContainer>
        )}
      </div>
    </article>
  )
}

function AlertRow({ alert }: { alert: Alert }) {
  return (
    <article className={`alert-row alert-row--${alert.severity}`}>
      <div className="alert-row__indicator" aria-hidden="true" />
      <div className="alert-row__body">
        <div className="alert-row__heading">
          <span className="alert-row__severity">{alert.severity}</span>
          <span className="alert-row__device">{alert.device_id}</span>
        </div>
        <p>{alert.message}</p>
      </div>
      <time dateTime={alert.observed_at}>{formatTimestamp(alert.observed_at)}</time>
    </article>
  )
}

function App() {
  const [deviceId, setDeviceId] = useState(defaultDeviceId)
  const [devices, setDevices] = useState<DeviceSummary[]>([])
  const [data, setData] = useState<DashboardData | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [isRefreshing, setIsRefreshing] = useState(false)
  const activeRequest = useRef<AbortController | null>(null)

  const cancelActiveRequest = useCallback(() => {
    activeRequest.current?.abort()
    activeRequest.current = null
  }, [])

  const refresh = useCallback(async () => {
    cancelActiveRequest()
    const controller = new AbortController()
    activeRequest.current = controller
    setIsRefreshing(true)
    try {
      const nextData = await getDashboardData(deviceId, controller.signal)
      if (activeRequest.current !== controller) return
      setData(nextData)
      setDevices(nextData.devices.devices)
      setError(null)
    } catch (requestError) {
      if (controller.signal.aborted || activeRequest.current !== controller) return
      setError(requestError instanceof Error ? requestError.message : 'Unable to load telemetry')
    } finally {
      if (activeRequest.current === controller) {
        activeRequest.current = null
        setIsLoading(false)
        setIsRefreshing(false)
      }
    }
  }, [cancelActiveRequest, deviceId])

  useEffect(() => {
    void refresh()
    const intervalId = window.setInterval(() => void refresh(), REFRESH_INTERVAL_MS)
    return () => {
      window.clearInterval(intervalId)
      cancelActiveRequest()
    }
  }, [cancelActiveRequest, refresh])

  const temperature = latestMetric(data?.latest ?? null, 'temperature_c')
  const humidity = latestMetric(data?.latest ?? null, 'humidity_pct')
  const temperaturePoints = useMemo(() => chartPoints(data?.history.readings ?? [], 'temperature_c'), [data])
  const humidityPoints = useMemo(() => chartPoints(data?.history.readings ?? [], 'humidity_pct'), [data])
  const alerts = data?.alerts.alerts ?? []
  const deviceOptions = Array.from(new Set([deviceId, ...devices.map((device) => device.device_id)])).sort()

  function selectDevice(nextDeviceId: string) {
    if (nextDeviceId === deviceId) return
    cancelActiveRequest()
    setData(null)
    setError(null)
    setIsLoading(true)
    setDeviceId(nextDeviceId)
  }

  return (
    <main className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <span className="brand__mark"><Zap size={17} strokeWidth={2.8} /></span>
          <span>PulseGrid</span>
        </div>
        <nav className="sidebar__nav" aria-label="Dashboard navigation">
          <a className="nav-item nav-item--active" href="#overview"><Gauge size={18} />Overview</a>
          <a className="nav-item" href="#telemetry"><Activity size={18} />Telemetry</a>
          <a className="nav-item" href="#alerts"><BellRing size={18} />Alerts <span>{alerts.length}</span></a>
        </nav>
        <div className="sidebar__footer">
          <p className="eyebrow">Environment</p>
          <div className="environment"><span className="pulse-dot" />Local development</div>
        </div>
      </aside>

      <section className="workspace">
        <header className="topbar">
          <div className="breadcrumb"><span>Operations</span><ChevronRight size={15} /><strong>Live monitor</strong></div>
          <div className="topbar__actions">
            <span className="sync-state"><Wifi size={15} />{error ? 'Connection issue' : 'Query API connected'}</span>
            <button className="refresh-button" type="button" onClick={() => void refresh()} disabled={isRefreshing}>
              <RefreshCw size={16} className={isRefreshing ? 'spin' : ''} />Refresh
            </button>
          </div>
        </header>

        <div className="content">
          <section className="page-heading" id="overview">
            <div>
              <p className="eyebrow">Real-time fleet observability</p>
              <h1>Fleet Signal Monitor</h1>
              <p className="page-heading__description">A real-time telemetry control panel for your device pipeline, with the latest signals and threshold breaches in one place.</p>
            </div>
            <div className="device-filter">
              <label className="eyebrow" htmlFor="device-select">Device · {tenantId}</label>
              <div className="device-chip">
                <Cpu size={17} />
                <select id="device-select" value={deviceId} onChange={(event) => selectDevice(event.target.value)}>
                  {deviceOptions.map((id) => <option key={id} value={id}>{id}</option>)}
                </select>
              </div>
            </div>
          </section>

          {isLoading ? (
            <section className="loading-panel" aria-live="polite"><span className="loading-orbit" />Loading live telemetry…</section>
          ) : error ? (
            <section className="error-panel" aria-live="polite">
              <Box size={24} />
              <div><h2>Dashboard data could not be loaded</h2><p>{error}</p></div>
              <button type="button" onClick={() => void refresh()}>Retry</button>
            </section>
          ) : (
            <>
              {data?.latest === null && (
                <section className="telemetry-empty" aria-live="polite">
                  <Activity size={19} />
                  <p><strong>No latest telemetry yet.</strong> Start the processor, then send an event with the device simulator. The dashboard will update automatically.</p>
                </section>
              )}
              <section className="status-grid" aria-label="Current device summary">
                <MetricCard label="Temperature" value={temperature?.value} unit="°C" trend="Latest reading" icon={<Thermometer size={18} />} tone="cyan" />
                <MetricCard label="Humidity" value={humidity?.value} unit="%" trend="Latest reading" icon={<Waves size={18} />} tone="violet" />
                <article className="summary-card">
                  <div className="summary-card__icon"><Activity size={18} /></div>
                  <p className="metric-card__label">Last telemetry</p>
                  <p className="summary-card__value">{formatAge(data?.latest?.observed_at)}</p>
                  <span>{formatTimestamp(data?.latest?.observed_at)}</span>
                </article>
                <article className="summary-card summary-card--alerts">
                  <div className="summary-card__icon"><BellRing size={18} /></div>
                  <p className="metric-card__label">Active alerts</p>
                  <p className="summary-card__value">{alerts.length}</p>
                  <span>{alerts.length === 0 ? 'All clear' : 'Needs attention'}</span>
                </article>
              </section>

              <section className="charts-section" id="telemetry">
                <div className="section-heading">
                  <div><p className="eyebrow">Device timeline</p><h2>Signal history</h2></div>
                  <span>Auto-refreshes every 5 seconds</span>
                </div>
                <div className="charts-grid">
                  <ChartCard title="Temperature" unit="°C" points={temperaturePoints} color="#48d6ff" />
                  <ChartCard title="Humidity" unit="%" points={humidityPoints} color="#a78bfa" />
                </div>
              </section>

              <section className="alerts-section" id="alerts">
                <div className="section-heading">
                  <div><p className="eyebrow">Attention queue</p><h2>Active alerts</h2></div>
                  <span className="alert-count"><BellRing size={15} />{alerts.length} active</span>
                </div>
                <div className="alerts-panel">
                  {alerts.length === 0 ? (
                    <div className="empty-alerts"><BellRing size={21} /><p>No active alerts. All observed metrics are within their configured thresholds.</p></div>
                  ) : alerts.map((alert) => <AlertRow alert={alert} key={alert.alert_id} />)}
                </div>
              </section>
            </>
          )}
        </div>
      </section>
    </main>
  )
}

export default App
