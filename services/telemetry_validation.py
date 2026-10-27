"""Semantic validation shared by telemetry ingestion and Kafka processing.

Kafka can receive records from sources other than the HTTP ingestion API.  Keep
the rules that protect Redis and PostgreSQL independent of HTTP so every
consumer can apply the same checks.
"""

from math import isfinite
from uuid import UUID

from generated.telemetry_pb2 import TelemetryEvent


# PostgreSQL bigint is signed, while the Protobuf field is uint64.
MAX_SEQUENCE_NO = 2**63 - 1


class TelemetryValidationError(ValueError):
    """Raised when a decoded telemetry event is unsafe to persist."""


def validate_telemetry_event(event: TelemetryEvent) -> None:
    """Validate fields required by PulseGrid's Redis and TimescaleDB stores."""
    if not event.event_id or not event.device_id.strip() or not event.tenant_id.strip():
        raise TelemetryValidationError("event_id, device_id, and tenant_id are required")
    try:
        UUID(event.event_id)
    except ValueError as error:
        raise TelemetryValidationError("event_id must be a valid UUID") from error

    if not event.HasField("observed_at"):
        raise TelemetryValidationError("observed_at is required")
    try:
        # The Protobuf conversion validates the valid seconds and nanoseconds range.
        event.observed_at.ToDatetime()
    except (ValueError, OverflowError) as error:
        raise TelemetryValidationError("observed_at must be a valid timestamp") from error

    if not 1 <= event.sequence_no <= MAX_SEQUENCE_NO:
        raise TelemetryValidationError(
            f"sequence_no must be between 1 and {MAX_SEQUENCE_NO}"
        )
    if not event.metrics:
        raise TelemetryValidationError("At least one metric is required")

    metric_names: set[str] = set()
    text_values = [event.device_id, event.tenant_id]
    for metric in event.metrics:
        if not metric.name.strip():
            raise TelemetryValidationError("Each metric must have a nonempty name")
        if metric.name in metric_names:
            raise TelemetryValidationError("Metric names must be unique within an event")
        metric_names.add(metric.name)
        if not isfinite(metric.value):
            raise TelemetryValidationError("Metric values must be finite numbers")
        text_values.extend((metric.name, metric.unit))

    # PostgreSQL text columns cannot store a NUL character.
    if any("\x00" in value for value in text_values):
        raise TelemetryValidationError("Text fields must not contain NUL characters")
