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


def test_build_event_can_generate_a_critical_temperature() -> None:
    event = build_event(
        device_id="sensor-204",
        sequence_no=1,
        device_index=0,
        scenario="temperature-critical",
    )

    assert event.metrics[0].name == "temperature_c"
    assert event.metrics[0].value == 85.0
    assert event.metrics[1].value == 40.0


def test_build_event_can_generate_a_low_humidity_alert() -> None:
    event = build_event(
        device_id="sensor-204",
        sequence_no=1,
        device_index=0,
        scenario="humidity-low",
    )

    assert event.metrics[0].value == 24.0
    assert event.metrics[1].name == "humidity_pct"
    assert event.metrics[1].value == 15.0
