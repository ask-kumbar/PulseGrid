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


@pytest.mark.parametrize(
    ("field", "value", "detail"),
    [
        ("event_id", "not-a-uuid", "valid UUID"),
        ("device_id", "   ", "required"),
        ("tenant_id", "   ", "required"),
        ("device_id", "sensor\x00invalid", "NUL"),
        ("tenant_id", "acme\x00invalid", "NUL"),
        ("sequence_no", 0, "sequence_no"),
        ("sequence_no", 2**63, "sequence_no"),
    ],
)
async def test_invalid_event_fields_are_rejected_before_publish(
    api_client, field, value, detail
) -> None:
    client, producer = api_client
    event, _ = build_valid_payload()
    setattr(event, field, value)

    response = await client.post(
        "/v1/telemetry",
        content=event.SerializeToString(),
        headers={"Content-Type": PROTOBUF_MEDIA_TYPE},
    )

    assert response.status_code == 422
    assert detail in response.json()["detail"]
    assert producer.messages == []


@pytest.mark.parametrize("field", ["observed_at", "metrics"])
async def test_missing_timestamp_or_metrics_is_rejected_before_publish(
    api_client, field
) -> None:
    client, producer = api_client
    event, _ = build_valid_payload()
    event.ClearField(field)

    response = await client.post(
        "/v1/telemetry",
        content=event.SerializeToString(),
        headers={"Content-Type": PROTOBUF_MEDIA_TYPE},
    )

    assert response.status_code == 422
    assert producer.messages == []


@pytest.mark.parametrize(
    ("seconds", "nanos"),
    [(253402300800, 0), (-62135596801, 0), (0, -1), (0, 1_000_000_000)],
)
async def test_invalid_timestamp_is_rejected_before_publish(
    api_client, seconds, nanos
) -> None:
    client, producer = api_client
    event, _ = build_valid_payload()
    event.observed_at.seconds = seconds
    event.observed_at.nanos = nanos

    response = await client.post(
        "/v1/telemetry",
        content=event.SerializeToString(),
        headers={"Content-Type": PROTOBUF_MEDIA_TYPE},
    )

    assert response.status_code == 422
    assert "valid timestamp" in response.json()["detail"]
    assert producer.messages == []


@pytest.mark.parametrize(
    ("field", "value", "detail"),
    [
        ("name", "", "nonempty name"),
        ("name", "   ", "nonempty name"),
        ("name", "temperature\x00invalid", "NUL"),
        ("unit", "celsius\x00invalid", "NUL"),
        ("value", float("nan"), "finite"),
        ("value", float("inf"), "finite"),
        ("value", float("-inf"), "finite"),
    ],
)
async def test_invalid_metric_fields_are_rejected_before_publish(
    api_client, field, value, detail
) -> None:
    client, producer = api_client
    event, _ = build_valid_payload()
    setattr(event.metrics[0], field, value)

    response = await client.post(
        "/v1/telemetry",
        content=event.SerializeToString(),
        headers={"Content-Type": PROTOBUF_MEDIA_TYPE},
    )

    assert response.status_code == 422
    assert detail in response.json()["detail"]
    assert producer.messages == []


async def test_duplicate_metric_names_are_rejected_before_publish(api_client) -> None:
    client, producer = api_client
    event, _ = build_valid_payload()
    event.metrics.add(name="temperature_c", value=85.0, unit="celsius")

    response = await client.post(
        "/v1/telemetry",
        content=event.SerializeToString(),
        headers={"Content-Type": PROTOBUF_MEDIA_TYPE},
    )

    assert response.status_code == 422
    assert "unique" in response.json()["detail"]
    assert producer.messages == []


async def test_valid_epoch_and_maximum_sequence_keep_original_protobuf_bytes(api_client) -> None:
    client, producer = api_client
    event, _ = build_valid_payload()
    event.observed_at.seconds = 0
    event.observed_at.nanos = 0
    event.sequence_no = 2**63 - 1
    event.metrics.add(name="humidity_pct", value=0.0, unit="percent")
    payload = event.SerializeToString()

    response = await client.post(
        "/v1/telemetry",
        content=payload,
        headers={"Content-Type": PROTOBUF_MEDIA_TYPE},
    )

    assert response.status_code == 202
    assert response.headers["content-type"] == PROTOBUF_MEDIA_TYPE
    assert producer.messages == [(TELEMETRY_TOPIC, payload, b"sensor-204")]
