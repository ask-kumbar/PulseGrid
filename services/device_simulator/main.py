import asyncio
import os
from uuid import uuid4

import httpx
from dotenv import load_dotenv
from google.protobuf.timestamp_pb2 import Timestamp

from generated.telemetry_pb2 import IngestTelemetryResponse, TelemetryEvent

load_dotenv()

API_URL = os.getenv("INGESTION_API_URL", "http://127.0.0.1:8000/v1/telemetry")
PROTOBUF_MEDIA_TYPE = "application/x-protobuf"


def build_event() -> TelemetryEvent:
    observed_at = Timestamp()
    observed_at.GetCurrentTime()

    event = TelemetryEvent(
        event_id=str(uuid4()),
        device_id="sensor-204",
        tenant_id="acme",
        observed_at=observed_at,
        sequence_no=1,
    )
    temperature = event.metrics.add()
    temperature.name = "temperature_c"
    temperature.value = 27.4
    temperature.unit = "celsius"
    return event


async def main() -> None:
    event = build_event()
    payload = event.SerializeToString()

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                API_URL,
                content=payload,
                headers={
                    "Content-Type": PROTOBUF_MEDIA_TYPE,
                    "Accept": PROTOBUF_MEDIA_TYPE,
                },
            )
        except httpx.ConnectError as error:
            raise SystemExit(f"Cannot reach the ingestion API at {API_URL}") from error

    response.raise_for_status()
    acknowledgment = IngestTelemetryResponse()
    acknowledgment.ParseFromString(response.content)

    print(f"HTTP status: {response.status_code}")
    print(f"Event accepted: {acknowledgment.accepted}")
    print(f"Event ID: {acknowledgment.event_id}")


if __name__ == "__main__":
    asyncio.run(main())
