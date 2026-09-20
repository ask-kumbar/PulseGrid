"""Create, serialize, and deserialize one PulseGrid telemetry event."""

from uuid import uuid4

from google.protobuf.timestamp_pb2 import Timestamp

from generated.telemetry_pb2 import TelemetryEvent


def main() -> None:
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

    payload = event.SerializeToString()
    print(f"Serialized event size: {len(payload)} bytes")

    restored_event = TelemetryEvent()
    restored_event.ParseFromString(payload)

    print("Restored event:")
    print(restored_event)


if __name__ == "__main__":
    main()
