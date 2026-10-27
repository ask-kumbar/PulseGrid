import base64
import json
from contextlib import AbstractAsyncContextManager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from generated.telemetry_pb2 import TelemetryEvent
from services.telemetry_processor.alert_rules import load_alert_rules
from services.telemetry_processor.main import store_event
from services.telemetry_processor import main as processor


class FakeTransaction(AbstractAsyncContextManager):
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        return None


class FakeDatabase:
    def __init__(self, returned_alert_id: str | None = "alert-1") -> None:
        self.returned_alert_id = returned_alert_id
        self.history_calls: list[tuple] = []
        self.alert_calls: list[tuple] = []

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def execute(self, query: str, *args) -> None:
        self.history_calls.append((query, *args))

    async def fetchval(self, query: str, *args) -> str | None:
        self.alert_calls.append((query, *args))
        return self.returned_alert_id


def build_event(temperature: float, humidity: float = 45.0) -> TelemetryEvent:
    event = TelemetryEvent(
        event_id="35fc27e5-16a3-4588-bc5a-59e39a885e6e",
        tenant_id="acme",
        device_id="sensor-204",
        sequence_no=9,
    )
    event.observed_at.FromDatetime(datetime(2026, 10, 4, 12, tzinfo=timezone.utc))
    event.metrics.add(name="temperature_c", value=temperature, unit="celsius")
    event.metrics.add(name="humidity_pct", value=humidity, unit="percent")
    return event


@pytest.fixture(scope="module")
def alert_rules():
    return load_alert_rules("config/alert_rules.yaml")


async def test_store_event_persists_history_and_a_critical_alert(alert_rules) -> None:
    database = FakeDatabase()

    created_alert_ids = await store_event(database, build_event(85.0), alert_rules)

    assert len(database.history_calls) == 2
    assert created_alert_ids == ["alert-1"]
    alert_query, *alert_values = database.alert_calls[0]
    assert "ON CONFLICT (event_id, metric) DO NOTHING" in alert_query
    assert alert_values[4:10] == [
        "temperature_c",
        "celsius",
        85.0,
        80.0,
        "high",
        "critical",
    ]


async def test_store_event_does_not_create_an_alert_for_normal_metrics(alert_rules) -> None:
    database = FakeDatabase()

    created_alert_ids = await store_event(database, build_event(27.4), alert_rules)

    assert len(database.history_calls) == 2
    assert created_alert_ids == []
    assert database.alert_calls == []


async def test_store_event_treats_an_existing_alert_as_already_saved(alert_rules) -> None:
    database = FakeDatabase(returned_alert_id=None)

    created_alert_ids = await store_event(database, build_event(85.0), alert_rules)

    assert created_alert_ids == []
    assert len(database.alert_calls) == 1


class FakeConsumer:
    def __init__(self, record) -> None:
        self.record = record
        self.start = AsyncMock()
        self.stop = AsyncMock()
        self.commit = AsyncMock()

    def __aiter__(self):
        async def records():
            yield self.record

        return records()


@pytest.fixture
def processor_runtime(monkeypatch):
    event = build_event(27.4)
    consumer = FakeConsumer(
        SimpleNamespace(
            topic=processor.TELEMETRY_TOPIC,
            value=event.SerializeToString(),
            key=b"sensor-204",
            partition=1,
            offset=5,
            timestamp=1_791_216_000_000,
        )
    )
    consumer_factory = Mock(return_value=consumer)
    dead_letter_producer = SimpleNamespace(
        start=AsyncMock(),
        stop=AsyncMock(),
        send_and_wait=AsyncMock(),
    )
    redis_client = SimpleNamespace(ping=AsyncMock(), set=AsyncMock(), aclose=AsyncMock())
    database = SimpleNamespace(close=AsyncMock())
    store = AsyncMock(return_value=[])
    operations = Mock()
    operations.attach_mock(store, "store_event")
    operations.attach_mock(redis_client.set, "redis_set")
    operations.attach_mock(dead_letter_producer.send_and_wait, "dead_letter")
    operations.attach_mock(consumer.commit, "commit")

    monkeypatch.setattr(processor, "AIOKafkaConsumer", consumer_factory)
    monkeypatch.setattr(processor, "AIOKafkaProducer", Mock(return_value=dead_letter_producer))
    monkeypatch.setattr(processor.redis, "from_url", Mock(return_value=redis_client))
    monkeypatch.setattr(processor.asyncpg, "connect", AsyncMock(return_value=database))
    monkeypatch.setattr(processor, "store_event", store)
    monkeypatch.setenv("ALERT_RULES_PATH", "config/alert_rules.yaml")

    return SimpleNamespace(
        consumer=consumer,
        consumer_factory=consumer_factory,
        dead_letter_producer=dead_letter_producer,
        redis_client=redis_client,
        database=database,
        store=store,
        operations=operations,
    )


async def test_processor_reads_backlog_and_commits_only_after_both_stores(
    processor_runtime,
) -> None:
    runtime = processor_runtime

    await processor.main()

    options = runtime.consumer_factory.call_args.kwargs
    assert options["auto_offset_reset"] == "earliest"
    assert options["enable_auto_commit"] is False
    assert [call[0] for call in runtime.operations.mock_calls] == [
        "store_event", "redis_set", "commit"
    ]
    runtime.consumer.stop.assert_awaited_once()
    runtime.dead_letter_producer.start.assert_awaited_once()
    runtime.dead_letter_producer.stop.assert_awaited_once()
    runtime.dead_letter_producer.send_and_wait.assert_not_awaited()
    runtime.database.close.assert_awaited_once()
    runtime.redis_client.aclose.assert_awaited_once()


@pytest.mark.parametrize("failing_store", ["database", "redis"])
async def test_processor_leaves_offset_uncommitted_if_a_store_fails(
    processor_runtime, failing_store
) -> None:
    runtime = processor_runtime
    if failing_store == "database":
        runtime.store.side_effect = RuntimeError("storage unavailable")
    else:
        runtime.redis_client.set.side_effect = RuntimeError("storage unavailable")

    with pytest.raises(RuntimeError, match="storage unavailable"):
        await processor.main()

    runtime.consumer.commit.assert_not_awaited()
    if failing_store == "database":
        runtime.redis_client.set.assert_not_awaited()
    runtime.consumer.stop.assert_awaited_once()
    runtime.dead_letter_producer.stop.assert_awaited_once()
    runtime.database.close.assert_awaited_once()
    runtime.redis_client.aclose.assert_awaited_once()


async def test_processor_dead_letters_semantically_invalid_event_before_committing(
    processor_runtime,
) -> None:
    runtime = processor_runtime
    invalid_event = build_event(27.4)
    invalid_event.event_id = "not-a-uuid"
    raw_payload = invalid_event.SerializeToString()
    runtime.consumer.record.value = raw_payload

    await processor.main()

    runtime.store.assert_not_awaited()
    runtime.redis_client.set.assert_not_awaited()
    runtime.dead_letter_producer.send_and_wait.assert_awaited_once()
    runtime.consumer.commit.assert_awaited_once()
    assert [call[0] for call in runtime.operations.mock_calls] == [
        "dead_letter",
        "commit",
    ]

    topic, envelope_bytes = runtime.dead_letter_producer.send_and_wait.await_args.args
    envelope = json.loads(envelope_bytes)
    assert topic == processor.DEAD_LETTER_TOPIC
    assert envelope == {
        "raw_payload_base64": base64.b64encode(raw_payload).decode("ascii"),
        "reason": "semantic_validation_error: event_id must be a valid UUID",
        "schema_version": 1,
        "source": {
            "key_base64": base64.b64encode(b"sensor-204").decode("ascii"),
            "offset": 5,
            "partition": 1,
            "timestamp_ms": 1_791_216_000_000,
            "topic": processor.TELEMETRY_TOPIC,
        },
    }
    assert runtime.dead_letter_producer.send_and_wait.await_args.kwargs == {
        "key": b"sensor-204"
    }


@pytest.mark.parametrize(
    ("mutate", "reason_fragment"),
    [
        (lambda event: setattr(event.observed_at, "seconds", 253_402_300_800), "timestamp"),
        (lambda event: event.ClearField("metrics"), "At least one metric"),
        (lambda event: setattr(event.metrics[0], "value", float("inf")), "finite"),
    ],
)
async def test_processor_dead_letters_other_semantic_failures(
    processor_runtime, mutate, reason_fragment
) -> None:
    runtime = processor_runtime
    invalid_event = build_event(27.4)
    mutate(invalid_event)
    runtime.consumer.record.value = invalid_event.SerializeToString()

    await processor.main()

    runtime.store.assert_not_awaited()
    runtime.redis_client.set.assert_not_awaited()
    runtime.consumer.commit.assert_awaited_once()
    _, envelope_bytes = runtime.dead_letter_producer.send_and_wait.await_args.args
    envelope = json.loads(envelope_bytes)
    assert envelope["reason"].startswith("semantic_validation_error:")
    assert reason_fragment in envelope["reason"]


async def test_processor_dead_letters_undecodable_protobuf_before_committing(
    processor_runtime,
) -> None:
    runtime = processor_runtime
    raw_payload = b"\xff"
    runtime.consumer.record.value = raw_payload

    await processor.main()

    runtime.store.assert_not_awaited()
    runtime.redis_client.set.assert_not_awaited()
    runtime.dead_letter_producer.send_and_wait.assert_awaited_once()
    runtime.consumer.commit.assert_awaited_once()
    _, envelope_bytes = runtime.dead_letter_producer.send_and_wait.await_args.args
    envelope = json.loads(envelope_bytes)
    assert envelope["raw_payload_base64"] == base64.b64encode(raw_payload).decode("ascii")
    assert envelope["reason"].startswith("protobuf_decode_error:")


async def test_processor_does_not_commit_if_dead_letter_publish_fails(processor_runtime) -> None:
    runtime = processor_runtime
    invalid_event = build_event(27.4)
    invalid_event.event_id = "not-a-uuid"
    runtime.consumer.record.value = invalid_event.SerializeToString()
    runtime.dead_letter_producer.send_and_wait.side_effect = RuntimeError("DLQ unavailable")

    with pytest.raises(RuntimeError, match="DLQ unavailable"):
        await processor.main()

    runtime.store.assert_not_awaited()
    runtime.redis_client.set.assert_not_awaited()
    runtime.consumer.commit.assert_not_awaited()
    runtime.dead_letter_producer.stop.assert_awaited_once()
