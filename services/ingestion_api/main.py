import json
import os
from contextlib import asynccontextmanager
from datetime import datetime
from uuid import UUID

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from dotenv import load_dotenv

TOPIC = "telemetry.raw"
load_dotenv()


class TelemetryEvent(BaseModel):
    event_id: UUID = Field(alias="eventId")
    device_id: str = Field(alias="deviceId", min_length=1, max_length=128)
    tenant_id: str = Field(alias="tenantId", min_length=1, max_length=128)
    timestamp: datetime
    sequence_no: int = Field(alias="sequenceNo", ge=0)
    metric: str = Field(min_length=1, max_length=64)
    value: float


@asynccontextmanager
async def lifespan(app: FastAPI):
    producer = AIOKafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        acks="all",
    )
    await producer.start()
    app.state.producer = producer
    yield
    await producer.stop()


app = FastAPI(title="PulseGrid Ingestion API", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/v1/telemetry", status_code=202)
async def ingest(event: TelemetryEvent) -> dict[str, str]:
    try:
        payload = json.dumps(event.model_dump(mode="json", by_alias=True)).encode()
        await app.state.producer.send_and_wait(TOPIC, payload, key=event.device_id.encode())
    except Exception as error:
        raise HTTPException(status_code=503, detail="Kafka is unavailable") from error
    return {"status": "accepted", "eventId": str(event.event_id)}
