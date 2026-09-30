# PulseGrid MVP timeline

**Target:** a deployed, portfolio-ready MVP by **October 1, 2026**.

## Already complete

- [x] Docker Compose infrastructure for Kafka, Redis, and TimescaleDB
- [x] Protobuf telemetry contract
- [x] FastAPI ingestion API and Kafka producer
- [x] Kafka processor with manual offset commits
- [x] Redis latest-device state
- [x] TimescaleDB metric history and idempotent inserts
- [x] Latest-state and history query APIs
- [x] Concurrent continuous device simulator
- [x] Automated unit/API tests
- [x] YAML alert-threshold configuration

## September 28 — Alert pipeline

- [ ] Load and validate alert rules when the processor starts
- [x] Evaluate temperature and humidity thresholds
- [x] Create the `alerts` database table
- [ ] Insert alerts idempotently with `UNIQUE (event_id, metric)`
- [ ] Add an API endpoint for recent/active alerts
- [ ] Add evaluator and API tests
- [ ] Verify a threshold breach end to end

## September 29 — Web dashboard

- [ ] Create a React and TypeScript dashboard with Vite
- [ ] Show service/pipeline status and latest device readings
- [ ] Plot recent temperature and humidity history
- [ ] Show warning and critical alerts clearly
- [ ] Poll the query API every few seconds
- [ ] Add loading, empty, and error states

## September 30 — Deployment

- [ ] Add production container configuration
- [ ] Configure cloud secrets, database URLs, Kafka, Redis, and CORS
- [ ] Deploy backend services and dashboard
- [ ] Run the complete simulator-to-dashboard flow in the hosted environment
- [ ] Check persistence, health endpoints, logs, and restart behavior

## October 1 — Portfolio polish and buffer

- [ ] Fix deployment or UI issues
- [ ] Update the architecture diagram and README
- [ ] Add dashboard screenshots and clear setup instructions
- [ ] Record a short end-to-end demonstration
- [ ] Finalize resume bullets and the founder follow-up message

## MVP definition of done

The MVP is complete when a simulated device can send Protobuf telemetry to the public ingestion API, Kafka delivers it to the processor, Redis and TimescaleDB store it, threshold breaches become durable alerts, and the deployed web dashboard displays latest readings, history, and alerts.

## Deferred until after October 1

- Authentication and tenant administration
- Kubernetes
- WebSocket streaming; the MVP uses polling
- UI-based rule editing and complex per-device rules
- Alert acknowledgement and automatic resolution workflows
- Large-scale load testing and advanced observability
