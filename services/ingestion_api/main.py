import os
from contextlib import asynccontextmanager

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, Response, status
from google.protobuf.message import DecodeError

from generated.telemetry_pb2 import IngestTelemetryResponse, TelemetryEvent

PROTOBUF_MEDIA_TYPE = "application/x-protobuf"
TELEMETRY_TOPIC = "telemetry.raw"

load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create one Kafka producer for the API process, then close it on shutdown."""
    producer = AIOKafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        acks="all",
    )
    await producer.start()
    app.state.kafka_producer = producer
    try:
        yield
    finally:
        await producer.stop()


app = FastAPI(title="PulseGrid Ingestion API", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/v1/telemetry", status_code=status.HTTP_202_ACCEPTED)
async def ingest(request: Request) -> Response:
    """Validate one Protobuf event and publish its unchanged bytes to Kafka."""
    content_type = request.headers.get("content-type", "")
    if not content_type.startswith(PROTOBUF_MEDIA_TYPE):
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail=f"Content-Type must be {PROTOBUF_MEDIA_TYPE}",
        )

    payload = await request.body()
    event = TelemetryEvent()
    try:
        event.ParseFromString(payload)
    except DecodeError as error:
        raise HTTPException(status_code=400, detail="Invalid Protobuf payload") from error

    if not event.event_id or not event.device_id or not event.tenant_id:
        raise HTTPException(status_code=422, detail="event_id, device_id, and tenant_id are required")
    if event.sequence_no == 0 or not event.metrics:
        raise HTTPException(status_code=422, detail="sequence_no and at least one metric are required")

    try:
        await request.app.state.kafka_producer.send_and_wait(
            TELEMETRY_TOPIC,
            payload,
            key=event.device_id.encode("utf-8"),
        )
    except KafkaError as error:
        raise HTTPException(status_code=503, detail="Kafka is unavailable") from error

    response = IngestTelemetryResponse(event_id=event.event_id, accepted=True)
    return Response(
        content=response.SerializeToString(),
        media_type=PROTOBUF_MEDIA_TYPE,
        status_code=status.HTTP_202_ACCEPTED,
    )
