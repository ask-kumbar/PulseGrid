from pathlib import Path

import pytest
from pydantic import ValidationError

from services.telemetry_processor.alert_rules import evaluate_metric, load_alert_rules


RULES_PATH = Path("config/alert_rules.yaml")


@pytest.fixture(scope="module")
def rules_file():
    return load_alert_rules(RULES_PATH)


def test_loads_versioned_alert_configuration(rules_file) -> None:
    assert rules_file.version == 1
    assert rules_file.rules["temperature_c"].critical_high == 80.0
    assert rules_file.rules["humidity_pct"].warning_low == 20.0


@pytest.mark.parametrize(
    ("value", "expected_severity", "expected_threshold"),
    [
        (60.0, "warning", 60.0),
        (79.9, "warning", 60.0),
        (80.0, "critical", 80.0),
        (95.0, "critical", 80.0),
    ],
)
def test_temperature_uses_highest_matching_severity(
    rules_file,
    value: float,
    expected_severity: str,
    expected_threshold: float,
) -> None:
    decision = evaluate_metric("temperature_c", value, "celsius", rules_file)

    assert decision is not None
    assert decision.condition == "high"
    assert decision.severity == expected_severity
    assert decision.threshold == expected_threshold


def test_humidity_can_alert_below_the_normal_range(rules_file) -> None:
    decision = evaluate_metric("humidity_pct", 18.0, "percent", rules_file)

    assert decision is not None
    assert decision.condition == "low"
    assert decision.severity == "warning"
    assert decision.threshold == 20.0


@pytest.mark.parametrize(
    ("metric", "value", "unit"),
    [
        ("temperature_c", 27.4, "celsius"),
        ("humidity_pct", 45.0, "percent"),
        ("pressure_kpa", 101.3, "kilopascal"),
    ],
)
def test_normal_or_unconfigured_metrics_do_not_alert(
    rules_file,
    metric: str,
    value: float,
    unit: str,
) -> None:
    assert evaluate_metric(metric, value, unit, rules_file) is None


def test_rejects_metric_unit_mismatch(rules_file) -> None:
    with pytest.raises(ValueError, match="unit mismatch"):
        evaluate_metric("temperature_c", 85.0, "fahrenheit", rules_file)


def test_rejects_invalid_threshold_order(tmp_path: Path) -> None:
    invalid_rules = tmp_path / "invalid-rules.yaml"
    invalid_rules.write_text(
        """
version: 1
rules:
  temperature_c:
    unit: celsius
    warning_high: 80
    critical_high: 60
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValidationError, match="critical_high must be greater"):
        load_alert_rules(invalid_rules)
