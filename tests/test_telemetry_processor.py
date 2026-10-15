from contextlib import AbstractAsyncContextManager
from datetime import datetime, timezone

import pytest

from generated.telemetry_pb2 import TelemetryEvent
from services.telemetry_processor.alert_rules import load_alert_rules
from services.telemetry_processor.main import store_event


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
