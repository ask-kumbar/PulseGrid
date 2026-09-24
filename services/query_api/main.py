"""Serve query-friendly JSON from PulseGrid's read stores."""

import json
import os
from contextlib import asynccontextmanager
from typing import Annotated

import asyncpg
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request, status
from redis import asyncio as redis

load_dotenv()

DATABASE_URL = "postgresql://pulsegrid:change-me-for-local-only@localhost:5432/pulsegrid"


def latest_state_key(tenant_id: str, device_id: str) -> str:
    """Return the same Redis key format used by the telemetry processor."""
    return f"telemetry:latest:{tenant_id}:{device_id}"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create shared Redis and TimescaleDB pools for the API process."""
    redis_client = redis.from_url(
        os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        decode_responses=True,
    )
    database_pool: asyncpg.Pool | None = None
    try:
        await redis_client.ping()
        database_pool = await asyncpg.create_pool(
            os.getenv("DATABASE_URL", DATABASE_URL),
            min_size=1,
            max_size=10,
        )
        app.state.redis_client = redis_client
        app.state.database_pool = database_pool
        yield
    finally:
        if database_pool is not None:
            await database_pool.close()
        await redis_client.aclose()


app = FastAPI(title="PulseGrid Query API", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    """Report whether this service started successfully."""
    return {"status": "ok"}


@app.get("/v1/tenants/{tenant_id}/devices/{device_id}/latest")
async def get_latest_state(tenant_id: str, device_id: str, request: Request) -> dict:
    """Return the newest known telemetry state for one device."""
    key = latest_state_key(tenant_id, device_id)
    value = await request.app.state.redis_client.get(key)
    if value is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No telemetry state exists for this device",
        )
    return json.loads(value)


@app.get("/v1/tenants/{tenant_id}/devices/{device_id}/history")
async def get_device_history(
    tenant_id: str,
    device_id: str,
    request: Request,
    hours: Annotated[int, Query(ge=1, le=24 * 30)] = 24,
    metric: str | None = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> dict[str, object]:
    """Return recent historical metrics for one device."""
    rows = await request.app.state.database_pool.fetch(
        """
        SELECT
          event_id::text,
          device_id,
          tenant_id,
          observed_at,
          sequence_no,
          metric,
          value,
          unit,
          ingested_at
        FROM telemetry_events
        WHERE tenant_id = $1
          AND device_id = $2
          AND observed_at >= now() - ($3::int * interval '1 hour')
          AND ($4::text IS NULL OR metric = $4)
        ORDER BY observed_at DESC
        LIMIT $5
        """,
        tenant_id,
        device_id,
        hours,
        metric,
        limit,
    )

    readings = [
        {
            "event_id": row["event_id"],
            "device_id": row["device_id"],
            "tenant_id": row["tenant_id"],
            "observed_at": row["observed_at"].isoformat(),
            "sequence_no": row["sequence_no"],
            "metric": row["metric"],
            "value": row["value"],
            "unit": row["unit"],
            "ingested_at": row["ingested_at"].isoformat(),
        }
        for row in rows
    ]

    return {
        "tenant_id": tenant_id,
        "device_id": device_id,
        "hours": hours,
        "metric": metric,
        "count": len(readings),
        "readings": readings,
    }
