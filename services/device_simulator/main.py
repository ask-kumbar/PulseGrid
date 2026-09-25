"""Generate one-shot or continuous PulseGrid telemetry traffic."""

import argparse
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


def build_event(device_id: str, sequence_no: int, device_index: int) -> TelemetryEvent:
    """Build one changing telemetry event for a simulated device."""
    observed_at = Timestamp()
    observed_at.GetCurrentTime()

    event = TelemetryEvent(
        event_id=str(uuid4()),
        device_id=device_id,
        tenant_id="acme",
        observed_at=observed_at,
        sequence_no=sequence_no,
    )

    temperature = event.metrics.add()
    temperature.name = "temperature_c"
    temperature.value = 24.0 + device_index + ((sequence_no - 1) % 20) * 0.2
    temperature.unit = "celsius"

    humidity = event.metrics.add()
    humidity.name = "humidity_pct"
    humidity.value = 40.0 + ((sequence_no - 1) % 15) * 0.5
    humidity.unit = "percent"

    return event


async def send_event(client: httpx.AsyncClient, event: TelemetryEvent) -> None:
    """Send one Protobuf event and print its acknowledgment."""
    response = await client.post(
        API_URL,
        content=event.SerializeToString(),
        headers={
            "Content-Type": PROTOBUF_MEDIA_TYPE,
            "Accept": PROTOBUF_MEDIA_TYPE,
        },
    )
    response.raise_for_status()

    acknowledgment = IngestTelemetryResponse()
    acknowledgment.ParseFromString(response.content)
    print(
        f"accepted={acknowledgment.accepted} "
        f"device={event.device_id} "
        f"sequence={event.sequence_no} "
        f"event_id={acknowledgment.event_id}"
    )


async def simulate_device(
    client: httpx.AsyncClient,
    device_id: str,
    device_index: int,
    count: int,
    interval: float,
    start_sequence: int,
) -> None:
    """Send an ordered stream for one device; count=0 runs continuously."""
    sent = 0
    sequence_no = start_sequence

    while count == 0 or sent < count:
        event = build_event(device_id, sequence_no, device_index)
        await send_event(client, event)
        sent += 1
        sequence_no += 1

        if count == 0 or sent < count:
            await asyncio.sleep(interval)


def parse_args() -> argparse.Namespace:
    """Read and validate simulator command-line options."""
    parser = argparse.ArgumentParser(description="Generate PulseGrid telemetry events")
    parser.add_argument("--devices", type=int, default=1, help="number of simulated devices")
    parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="events per device; use 0 to run until Ctrl+C",
    )
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between events")
    parser.add_argument("--start-sequence", type=int, default=1)
    args = parser.parse_args()

    if args.devices < 1:
        parser.error("--devices must be at least 1")
    if args.count < 0:
        parser.error("--count cannot be negative")
    if args.interval < 0:
        parser.error("--interval cannot be negative")
    if args.start_sequence < 1:
        parser.error("--start-sequence must be at least 1")
    return args


async def main() -> None:
    args = parse_args()

    try:
        async with httpx.AsyncClient() as client:
            await asyncio.gather(
                *(
                    simulate_device(
                        client=client,
                        device_id=f"sensor-{204 + device_index}",
                        device_index=device_index,
                        count=args.count,
                        interval=args.interval,
                        start_sequence=args.start_sequence,
                    )
                    for device_index in range(args.devices)
                )
            )
    except httpx.ConnectError as error:
        raise SystemExit(f"Cannot reach the ingestion API at {API_URL}") from error


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Simulator stopped.")
