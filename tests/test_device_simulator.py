from services.device_simulator.main import build_event


def test_build_event_contains_ordered_metrics() -> None:
    event = build_event(device_id="sensor-205", sequence_no=7, device_index=1)

    assert event.device_id == "sensor-205"
    assert event.tenant_id == "acme"
    assert event.sequence_no == 7
    assert event.event_id
    assert event.observed_at.seconds > 0
    assert [(metric.name, metric.unit) for metric in event.metrics] == [
        ("temperature_c", "celsius"),
        ("humidity_pct", "percent"),
    ]
