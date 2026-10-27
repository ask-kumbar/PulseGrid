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


async def test_devices_returns_discovered_devices_for_requested_tenant(query_client) -> None:
    timestamp = datetime(2026, 10, 21, 10, 30, tzinfo=timezone.utc)
    pool = FakeDatabasePool(
        [
            {"device_id": "sensor-205", "last_observed_at": timestamp},
            {"device_id": "sensor-204", "last_observed_at": timestamp},
        ]
    )
    app.state.database_pool = pool

    response = await query_client.get(
        "/v1/tenants/acme/devices", params={"limit": 10}
    )

    assert response.status_code == 200
    assert response.json() == {
        "tenant_id": "acme",
        "count": 2,
        "devices": [
            {"device_id": "sensor-205", "last_observed_at": timestamp.isoformat()},
            {"device_id": "sensor-204", "last_observed_at": timestamp.isoformat()},
        ],
    }
    query, *params = pool.calls[0]
    assert params == ["acme", 10]
    # Tenant filtering must happen before aggregation, not after combining tenants.
    assert "WHERE tenant_id = $1" in query
    assert query.index("WHERE tenant_id = $1") < query.index("GROUP BY device_id")
    assert "ORDER BY last_observed_at DESC, device_id ASC" in query
    assert "LIMIT $2" in query


async def test_devices_is_scoped_to_each_requested_tenant(query_client) -> None:
    pool = FakeDatabasePool()
    app.state.database_pool = pool

    for tenant_id in ("acme", "other-tenant"):
        response = await query_client.get(f"/v1/tenants/{tenant_id}/devices")

        assert response.status_code == 200
        assert response.json() == {"tenant_id": tenant_id, "count": 0, "devices": []}
        assert pool.calls[-1][1:] == (tenant_id, 100)


@pytest.mark.parametrize("limit", [0, 1001])
async def test_devices_rejects_unbounded_limits_before_database_call(
    query_client, limit
) -> None:
    pool = FakeDatabasePool()
    app.state.database_pool = pool

    response = await query_client.get(
        "/v1/tenants/acme/devices", params={"limit": limit}
    )

    assert response.status_code == 422
    assert pool.calls == []


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


async def test_alerts_returns_rows_and_passes_filters(query_client) -> None:
    timestamp = datetime(2026, 10, 5, 10, 30, tzinfo=timezone.utc)
    pool = FakeDatabasePool(
        [
            {
                "alert_id": "alert-1",
                "event_id": "event-1",
                "tenant_id": "acme",
                "device_id": "sensor-204",
                "metric": "temperature_c",
                "unit": "celsius",
                "value": 85.0,
                "threshold": 80.0,
                "condition": "high",
                "severity": "critical",
                "message": "temperature_c is high",
                "observed_at": timestamp,
                "status": "active",
                "created_at": timestamp,
            }
        ]
    )
    app.state.database_pool = pool

    response = await query_client.get(
        "/v1/tenants/acme/alerts",
        params={"severity": "critical", "device_id": "sensor-204", "limit": 10},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "active"
    assert body["count"] == 1
    assert body["alerts"][0]["alert_id"] == "alert-1"
    assert body["alerts"][0]["observed_at"] == timestamp.isoformat()
    assert pool.calls[0][1:] == ("acme", "active", "critical", "sensor-204", 10)


async def test_alerts_reject_invalid_status_before_database_call(query_client) -> None:
    pool = FakeDatabasePool()
    app.state.database_pool = pool

    response = await query_client.get(
        "/v1/tenants/acme/alerts",
        params={"status": "open"},
    )

    assert response.status_code == 422
    assert pool.calls == []
