"""Verify the real HTTP → Kafka → stores → Query API path without deleting data.

Each run writes six events under a unique test tenant, retries one event, and
checks that a malformed-but-decodable Kafka record reaches the DLQ. Run with
the complete stack already started. No dependencies are mocked.
"""

import argparse
import asyncio
import base64
import json
import os
from uuid import uuid4

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from generated.telemetry_pb2 import IngestTelemetryResponse
from services.device_simulator.main import PROTOBUF_MEDIA_TYPE, build_event


TELEMETRY_TOPIC = "telemetry.raw"
DEFAULT_DLQ_TOPIC = "telemetry.dlq"


def matching_dead_letter_envelope(raw_value: bytes, raw_payload: bytes) -> dict | None:
    """Return a DLQ envelope for this raw payload, ignoring unrelated records."""
    try:
        envelope = json.loads(raw_value)
    except (json.JSONDecodeError, TypeError, UnicodeDecodeError):
        return None
    if envelope.get("raw_payload_base64") != base64.b64encode(raw_payload).decode("ascii"):
        return None
    return envelope


async def wait_for_dlq_assignment(consumer: AIOKafkaConsumer, timeout: float) -> None:
    """Wait until a new consumer group is at the DLQ end offset before publishing."""
    deadline = asyncio.get_running_loop().time() + timeout
    while not consumer.assignment():
        await consumer.getmany(timeout_ms=100)
        if asyncio.get_running_loop().time() >= deadline:
            raise RuntimeError("Timed out while preparing the DLQ consumer")


async def verify_direct_kafka_dead_letter(
    kafka_bootstrap_servers: str,
    dlq_topic: str,
    tenant: str,
    timeout: float,
) -> None:
    """Inject an invalid decoded event into raw Kafka and verify its DLQ envelope."""
    invalid_event = build_event("sensor-dlq", 1, 0)
    invalid_event.tenant_id = tenant
    invalid_event.event_id = "not-a-uuid"
    raw_payload = invalid_event.SerializeToString()
    raw_key = invalid_event.device_id.encode("utf-8")

    consumer = AIOKafkaConsumer(
        dlq_topic,
        bootstrap_servers=kafka_bootstrap_servers,
        group_id=f"smoke-dlq-{uuid4()}",
        enable_auto_commit=False,
        auto_offset_reset="latest",
    )
    try:
        await consumer.start()
        # Establish an assignment first, so `latest` is before this test's record.
        await wait_for_dlq_assignment(consumer, timeout=min(timeout, 10))

        # Do not create the producer until the consumer is ready.  Its context
        # manager also closes the network client immediately after the send.
        async with AIOKafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            acks="all",
        ) as producer:
            await producer.send_and_wait(TELEMETRY_TOPIC, raw_payload, key=raw_key)

        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            batches = await consumer.getmany(timeout_ms=250, max_records=50)
            for records in batches.values():
                for record in records:
                    envelope = matching_dead_letter_envelope(record.value, raw_payload)
                    if envelope is None:
                        continue
                    assert envelope["reason"].startswith("semantic_validation_error:")
                    assert envelope["source"]["topic"] == TELEMETRY_TOPIC
                    assert isinstance(envelope["source"]["partition"], int)
                    assert isinstance(envelope["source"]["offset"], int)
                    assert envelope["source"]["key_base64"] == base64.b64encode(raw_key).decode(
                        "ascii"
                    )
                    return
            await asyncio.sleep(0.1)
        raise RuntimeError(f"No matching dead-letter record appeared within {timeout}s")
    finally:
        await consumer.stop()


async def check_pipeline(
    ingestion_url: str,
    query_url: str,
    kafka_bootstrap_servers: str,
    dlq_topic: str,
    timeout: float,
) -> None:
    tenant = f"smoke-{uuid4().hex[:12]}"
    events = []
    for index in range(3):
        device_id = f"sensor-{204 + index}"
        for sequence, scenario in [(1, "normal"), (2, "temperature-critical")]:
            event = build_event(device_id, sequence, index, scenario)
            event.tenant_id = tenant
            events.append(event)

    headers = {"Content-Type": PROTOBUF_MEDIA_TYPE, "Accept": PROTOBUF_MEDIA_TYPE}
    async with httpx.AsyncClient(timeout=10) as client:
        # Retry the exact bytes, including the UUID and observed timestamp.
        for event in [*events, events[-1]]:
            response = await client.post(ingestion_url, content=event.SerializeToString(), headers=headers)
            assert response.status_code == 202, response.text
            acknowledgment = IngestTelemetryResponse.FromString(response.content)
            assert acknowledgment.accepted and acknowledgment.event_id == event.event_id
        print(f"PASS: six events accepted across three devices; one identical event retried ({tenant}).")

        invalid_event = build_event("sensor-invalid", 1, 0)
        invalid_event.event_id = "not-a-uuid"
        invalid_event.tenant_id = tenant
        response = await client.post(ingestion_url, content=invalid_event.SerializeToString(), headers=headers)
        assert response.status_code == 422, f"Invalid UUID accepted: {response.status_code}"
        print("PASS: invalid event rejected before Kafka publishing.")

        await verify_direct_kafka_dead_letter(
            kafka_bootstrap_servers,
            dlq_topic,
            tenant,
            timeout,
        )
        print("PASS: directly injected invalid telemetry was preserved in telemetry.dlq.")

        deadline = asyncio.get_running_loop().time() + timeout
        last_failure = "Waiting for the processor."
        while asyncio.get_running_loop().time() < deadline:
            try:
                response = await client.get(f"{query_url}/v1/tenants/{tenant}/devices")
                response.raise_for_status()
                devices = response.json()["devices"]
                assert {device["device_id"] for device in devices} == {event.device_id for event in events}

                for index in range(3):
                    normal, critical = events[index * 2:index * 2 + 2]
                    base = f"{query_url}/v1/tenants/{tenant}/devices/{critical.device_id}"
                    response = await client.get(f"{base}/latest")
                    response.raise_for_status()
                    latest = response.json()
                    assert latest["event_id"] == critical.event_id, "Latest state is not yet current."
                    assert latest["tenant_id"] == tenant
                    assert next(metric["value"] for metric in latest["metrics"] if metric["name"] == "temperature_c") == 85.0

                    response = await client.get(f"{base}/history", params={"hours": 1, "limit": 100})
                    response.raise_for_status()
                    readings = response.json()["readings"]
                    assert len(readings) == 4, f"Expected four metric rows, found {len(readings)}."
                    assert {row["event_id"] for row in readings} == {normal.event_id, critical.event_id}
                    assert len({(row["event_id"], row["metric"]) for row in readings}) == 4

                response = await client.get(f"{query_url}/v1/tenants/{tenant}/alerts", params={"status": "active"})
                response.raise_for_status()
                alerts = response.json()["alerts"]
                assert len(alerts) == 3, f"Expected three alerts, found {len(alerts)}."
                assert {alert["event_id"] for alert in alerts} == {event.event_id for event in events[1::2]}
                assert all(alert["severity"] == "critical" and alert["metric"] == "temperature_c" for alert in alerts)
                break
            except (AssertionError, httpx.HTTPStatusError) as error:
                last_failure = str(error) or "The processor has not stored all events yet."
                await asyncio.sleep(0.25)
        else:
            raise RuntimeError(f"Pipeline did not complete within {timeout}s: {last_failure}")

        print("PASS: device discovery, latest readings, 12 history rows, and three critical alerts.")
        print("PASS: retry created no duplicate history rows or alerts.")

        other_tenant = f"{tenant}-empty"
        response = await client.get(f"{query_url}/v1/tenants/{other_tenant}/devices")
        response.raise_for_status()
        assert response.json()["devices"] == []
        for path, collection in [("devices/sensor-204/history", "readings"), ("alerts", "alerts")]:
            response = await client.get(f"{query_url}/v1/tenants/{other_tenant}/{path}")
            response.raise_for_status()
            assert response.json()[collection] == []
        response = await client.get(f"{query_url}/v1/tenants/{other_tenant}/devices/sensor-204/latest")
        assert response.status_code == 404
        print("PASS: reads for a different tenant return no test data.")
        print("End-to-end smoke check passed. This checks behavior, not production throughput.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ingestion-url", default=os.getenv("INGESTION_API_URL", "http://127.0.0.1:8000/v1/telemetry"))
    parser.add_argument("--query-url", default=os.getenv("QUERY_API_URL", "http://127.0.0.1:8001"))
    parser.add_argument(
        "--kafka-bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument("--dlq-topic", default=os.getenv("TELEMETRY_DLQ_TOPIC", DEFAULT_DLQ_TOPIC))
    parser.add_argument("--timeout", type=float, default=30)
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    asyncio.run(
        check_pipeline(
            args.ingestion_url,
            args.query_url.rstrip("/"),
            args.kafka_bootstrap_servers,
            args.dlq_topic,
            args.timeout,
        )
    )


if __name__ == "__main__":
    main()
