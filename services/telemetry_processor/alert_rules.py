"""Load and evaluate configurable telemetry alert rules."""

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


class MetricRule(BaseModel):
    """Validated thresholds for one telemetry metric."""

    model_config = ConfigDict(extra="forbid")

    unit: str = Field(min_length=1)
    warning_low: float | None = None
    warning_high: float | None = None
    critical_high: float | None = None

    @model_validator(mode="after")
    def validate_thresholds(self) -> "MetricRule":
        thresholds = (self.warning_low, self.warning_high, self.critical_high)
        if all(threshold is None for threshold in thresholds):
            raise ValueError("at least one threshold is required")

        if (
            self.warning_high is not None
            and self.critical_high is not None
            and self.critical_high <= self.warning_high
        ):
            raise ValueError("critical_high must be greater than warning_high")

        if (
            self.warning_low is not None
            and self.warning_high is not None
            and self.warning_low >= self.warning_high
        ):
            raise ValueError("warning_low must be less than warning_high")

        return self


class AlertRulesFile(BaseModel):
    """Versioned shape of config/alert_rules.yaml."""

    model_config = ConfigDict(extra="forbid")

    version: Literal[1]
    rules: dict[str, MetricRule] = Field(min_length=1)


@dataclass(frozen=True)
class AlertDecision:
    """An alert produced when a metric crosses a configured threshold."""

    metric: str
    value: float
    unit: str
    threshold: float
    condition: Literal["high", "low"]
    severity: Literal["warning", "critical"]
    message: str


def load_alert_rules(path: str | Path) -> AlertRulesFile:
    """Read YAML rules once and validate their structure and threshold ordering."""
    rules_path = Path(path)
    with rules_path.open(encoding="utf-8") as rules_file:
        raw_rules = yaml.safe_load(rules_file)

    return AlertRulesFile.model_validate(raw_rules)


def evaluate_metric(
    metric: str,
    value: float,
    unit: str,
    rules_file: AlertRulesFile,
) -> AlertDecision | None:
    """Return the highest-severity matching alert, or None for a normal value."""
    rule = rules_file.rules.get(metric)
    if rule is None:
        return None

    if unit != rule.unit:
        raise ValueError(
            f"unit mismatch for {metric}: expected {rule.unit!r}, received {unit!r}"
        )

    if rule.critical_high is not None and value >= rule.critical_high:
        return _decision(metric, value, unit, rule.critical_high, "high", "critical")

    if rule.warning_high is not None and value >= rule.warning_high:
        return _decision(metric, value, unit, rule.warning_high, "high", "warning")

    if rule.warning_low is not None and value <= rule.warning_low:
        return _decision(metric, value, unit, rule.warning_low, "low", "warning")

    return None


def _decision(
    metric: str,
    value: float,
    unit: str,
    threshold: float,
    condition: Literal["high", "low"],
    severity: Literal["warning", "critical"],
) -> AlertDecision:
    return AlertDecision(
        metric=metric,
        value=value,
        unit=unit,
        threshold=threshold,
        condition=condition,
        severity=severity,
        message=(
            f"{metric} is {condition}: {value:g} {unit} "
            f"crossed the {severity} threshold of {threshold:g} {unit}"
        ),
    )
