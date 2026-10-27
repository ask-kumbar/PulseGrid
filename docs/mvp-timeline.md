# PulseGrid MVP checklist

Updated October 26, 2026. The immediate goal is a reproducible local portfolio demo; public hosting is a separate release step because no hosting account has been selected yet.

## Implemented

- [x] Docker infrastructure for Kafka, Redis, and TimescaleDB
- [x] Protobuf telemetry contract and binary HTTP ingestion
- [x] Kafka producer and processor with manual offset commits
- [x] Processor-side semantic validation and a durable Kafka dead-letter path
- [x] Redis latest-device state and TimescaleDB metric history
- [x] Duplicate-safe history and alert inserts
- [x] Validated YAML threshold rules and durable warning/critical alerts
- [x] Query endpoints for devices, latest readings, history, and alerts
- [x] Concurrent simulator with finite/continuous runs and alert scenarios
- [x] React/TypeScript dashboard with device selection, readings, charts, and alerts
- [x] Five-second dashboard polling with loading, empty, and error states
- [x] Python unit/API tests
- [x] Docker demo definition with isolated data volumes and an end-to-end smoke script
- [x] README, command guide, and architecture explanation

## Local release checks

- [x] Validate the Docker demo Compose definition
- [x] Run the Python suite (64 passed) and dashboard lint/build on the final code pass
- [x] Verify a complete Docker demo startup from the documented command
- [x] Run `scripts.smoke_demo` against the started Docker demo
- [x] Send test events for three devices and verify device switching in the browser
- [x] Trigger a critical alert and verify both its table row and dashboard display
- [x] Restart the demo without deleting volumes and verify persisted history
- [x] Capture a dashboard screenshot and prepare the short demo outline in [portfolio.md](portfolio.md)
- [ ] Review the changes, commit them, and push when the project owner is ready

The local MVP is ready to demonstrate when these checks pass. A resume can describe the implemented project accurately without claiming a public deployment or unmeasured performance.

## Hosted release — pending

- [ ] Select a hosting provider and budget
- [ ] Configure persistent storage, secrets, and a stable domain or URL
- [ ] Add HTTPS and access controls before exposing the APIs or dashboard publicly
- [ ] Deploy the services and run the simulator-to-dashboard checks remotely
- [ ] Verify restart behavior and storage persistence in that environment
- [ ] Add the verified live demo URL to the README and portfolio

The local Docker configuration is a starting point for packaging. It does not establish production security, availability, or cloud reliability.

## After the MVP

- Alert acknowledgement, automatic resolution, and alert lifecycle UI
- Late-event handling for Redis latest state
- Dead-letter triage, controlled replay, and operational alerting
- Authentication, tenant authorization, and administration
- Measured throughput/latency benchmarks and operational monitoring
- Replicated Kafka and database availability planning
- WebSocket/SSE delivery if polling becomes insufficient
- Real device integration and equipment-specific alert rules
- Kubernetes only if the deployment needs justify it
