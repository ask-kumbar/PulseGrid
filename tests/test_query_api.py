import json
from datetime import datetime, timezone

import httpx
import pytest

from services.query_api.main import app


class FakeRedis:
    def __init__(self, values: dict[str, str] | None = None) -> None:
        self.values = values or {}

    async def get(self, key: str) -> str | None:
        return self.values.get(key)


class FakeDatabasePool:
    def __init__(self, rows: list[dict] | None = None) -> None:
        self.rows = rows or []
        self.calls: list[tuple] = []

    async def fetch(self, query: str, *args):
        self.calls.append((query, *args))
        return self.rows


@pytest.fixture
async def query_client() -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_latest_state_returns_redis_json(query_client) -> None:
    state = {
        "event_id": "event-1",
        "device_id": "sensor-204",
        "tenant_id": "acme",
        "sequence_no": 8,
        "metrics": [{"name": "temperature_c", "value": 27.4, "unit": "celsius"}],
    }
    app.state.redis_client = FakeRedis(
        {"telemetry:latest:acme:sensor-204": json.dumps(state)}
    )

    response = await query_client.get("/v1/tenants/acme/devices/sensor-204/latest")

    assert response.status_code == 200
    assert response.json() == state


async def test_unknown_device_returns_404(query_client) -> None:
    app.state.redis_client = FakeRedis()

    response = await query_client.get("/v1/tenants/acme/devices/unknown/latest")

    assert response.status_code == 404


async def test_history_returns_rows_and_passes_filters(query_client) -> None:
    timestamp = datetime(2026, 9, 25, 14, 30, tzinfo=timezone.utc)
    pool = FakeDatabasePool(
        [
            {
                "event_id": "event-1",
                "device_id": "sensor-204",
                "tenant_id": "acme",
                "observed_at": timestamp,
                "sequence_no": 8,
                "metric": "temperature_c",
                "value": 27.4,
                "unit": "celsius",
                "ingested_at": timestamp,
            }
        ]
    )
    app.state.database_pool = pool

    response = await query_client.get(
        "/v1/tenants/acme/devices/sensor-204/history",
        params={"hours": 48, "metric": "temperature_c", "limit": 10},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 1
    assert body["readings"][0]["unit"] == "celsius"
    assert pool.calls[0][1:] == ("acme", "sensor-204", 48, "temperature_c", 10)


async def test_history_rejects_invalid_hours_before_database_call(query_client) -> None:
    pool = FakeDatabasePool()
    app.state.database_pool = pool

    response = await query_client.get(
        "/v1/tenants/acme/devices/sensor-204/history",
        params={"hours": 0},
    )

    assert response.status_code == 422
    assert pool.calls == []
