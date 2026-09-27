from uuid import uuid4

import httpx
import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from generated.telemetry_pb2 import IngestTelemetryResponse, TelemetryEvent
from services.ingestion_api.main import PROTOBUF_MEDIA_TYPE, TELEMETRY_TOPIC, app


class FakeKafkaProducer:
    def __init__(self) -> None:
        self.messages: list[tuple[str, bytes, bytes]] = []

    async def send_and_wait(self, topic: str, payload: bytes, key: bytes) -> None:
        self.messages.append((topic, payload, key))


def build_valid_payload() -> tuple[TelemetryEvent, bytes]:
    observed_at = Timestamp()
    observed_at.GetCurrentTime()
    event = TelemetryEvent(
        event_id=str(uuid4()),
        device_id="sensor-204",
        tenant_id="acme",
        observed_at=observed_at,
        sequence_no=1,
    )
    metric = event.metrics.add()
    metric.name = "temperature_c"
    metric.value = 27.4
    metric.unit = "celsius"
    return event, event.SerializeToString()


@pytest.fixture
async def api_client() -> tuple[httpx.AsyncClient, FakeKafkaProducer]:
    producer = FakeKafkaProducer()
    app.state.kafka_producer = producer
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, producer


async def test_valid_protobuf_is_published_and_acknowledged(api_client) -> None:
    client, producer = api_client
    event, payload = build_valid_payload()

    response = await client.post(
        "/v1/telemetry",
        content=payload,
        headers={"Content-Type": PROTOBUF_MEDIA_TYPE},
    )

    assert response.status_code == 202
    acknowledgment = IngestTelemetryResponse()
    acknowledgment.ParseFromString(response.content)
    assert acknowledgment.accepted is True
    assert acknowledgment.event_id == event.event_id
    assert producer.messages == [(TELEMETRY_TOPIC, payload, b"sensor-204")]


async def test_wrong_content_type_returns_415(api_client) -> None:
    client, producer = api_client

    response = await client.post("/v1/telemetry", content=b"{}", headers={"Content-Type": "application/json"})

    assert response.status_code == 415
    assert producer.messages == []


async def test_invalid_protobuf_returns_400(api_client) -> None:
    client, producer = api_client

    response = await client.post(
        "/v1/telemetry",
        content=b"\xff",
        headers={"Content-Type": PROTOBUF_MEDIA_TYPE},
    )

    assert response.status_code == 400
    assert producer.messages == []


async def test_missing_required_fields_returns_422(api_client) -> None:
    client, producer = api_client
    incomplete_event = TelemetryEvent(device_id="sensor-204", sequence_no=1)
    metric = incomplete_event.metrics.add()
    metric.name = "temperature_c"
    metric.value = 27.4

    response = await client.post(
        "/v1/telemetry",
        content=incomplete_event.SerializeToString(),
        headers={"Content-Type": PROTOBUF_MEDIA_TYPE},
    )

    assert response.status_code == 422
    assert producer.messages == []
