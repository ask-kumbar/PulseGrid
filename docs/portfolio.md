# PulseGrid portfolio guide

## Position the project accurately

**PulseGrid — Fleet Signal Monitor** is a portfolio project that demonstrates a complete device-signal ingestion, processing, storage, and monitoring flow. The signals come from a simulator; no physical robotics integration or public cloud deployment is claimed.

Use the repository link when applying: [github.com/ask-kumbar/PulseGrid](https://github.com/ask-kumbar/PulseGrid). Add a hosted demo link only after it has been deployed and verified. A short recording can demonstrate the local version in the meantime.

## Resume entry

**PulseGrid — Fleet Signal Monitor** | Python, FastAPI, Kafka, Protobuf, Redis, TimescaleDB, React, TypeScript, Docker

- Built a device-telemetry pipeline using Protobuf over HTTP and Kafka, with asynchronous processing into Redis latest state and TimescaleDB history.
- Implemented configurable threshold alerts, a Kafka dead-letter path for invalid records, and duplicate-safe database writes; committed offsets after persistence to support at-least-once processing.
- Developed a React/TypeScript dashboard for device selection, time-series charts, and active alerts, supported by a concurrent simulator and automated Python tests.

Do not add a throughput, uptime, latency, fleet-size, or production-deployment claim until it has been measured or demonstrated. Describe Docker deployment as local containerization until there is a verified hosted environment.

## 90-second demonstration

Prepare the full Docker demo before recording. Open the dashboard and have a terminal ready with the simulator commands from the README.

| Time | Show | Explain |
| --- | --- | --- |
| 0–15s | Architecture diagram | Devices publish Protobuf telemetry; Kafka decouples ingestion from processing. |
| 15–40s | Send events for three devices and switch devices in the dashboard | Latest readings come from Redis; history comes from TimescaleDB. The UI polls every five seconds. |
| 40–60s | Send a `temperature-critical` event | The processor applies a configured rule, persists the alert, and the dashboard displays it. |
| 60–75s | Query an event's stored history or show processor logs | Trace the event through the actual services instead of presenting static mock data. |
| 75–90s | Code/architecture and next steps | Explain manual offset commits, replay-safe inserts, and the remaining security/hosting work. |

Capture one screenshot with visible readings, chart data, and an alert. Keep the demo focused on a working flow; detailed design discussion can follow in an interview.

## Concepts to be ready to explain

- Why Kafka sits between the HTTP endpoint and the processor.
- Why `202 Accepted` confirms Kafka acknowledgement, not completed database storage.
- How device keys and partitions affect ordering and worker assignments.
- Why a crash before an offset commit can cause a replay, and how the unique keys handle it.
- Why latest state and historical storage use different databases.
- Why the database transaction does not make Redis and Kafka writes exactly once.
- Why the demo uses polling and what would change with WebSockets.
- Which behaviors remain incomplete: authorization, late-event handling, dead-letter review/replay workflow, alert resolution, and hosting.

## Founder follow-up draft

This is a draft to personalize and send yourself after the repository and demo are ready. It does not assume any unverified details about Jaipur Robotics' product.

**Subject:** Follow-up: PulseGrid device-monitoring project

Hi [Founder name],

Following up on my earlier message, I built PulseGrid, a project that receives device signals through a Protobuf/FastAPI endpoint, processes them through Kafka, and displays readings and threshold alerts in a React dashboard. I wanted hands-on experience with the backend systems used to monitor connected equipment.

Repository: https://github.com/ask-kumbar/PulseGrid

Short demo: [add the recording link]

I would welcome a conversation about engineering opportunities at Jaipur Robotics. My background is [one specific sentence about your relevant experience], and I can walk through the design decisions and the parts I would improve for a real deployment.

Thanks,
[Your name]
