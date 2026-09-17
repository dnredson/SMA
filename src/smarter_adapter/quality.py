from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Tuple

from .models import Measurement, ParsedEvent


@dataclass(frozen=True)
class QualityIssue:
    measurement: str
    reason: str
    source_field: str = ""


@dataclass(frozen=True)
class QualityResult:
    event: ParsedEvent
    status: str
    issues: Tuple[QualityIssue, ...] = ()


class MeasurementQualityPolicy:
    """Generic post-parse quality gate.

    Parsers may mark a measurement as invalid through metadata instead of
    coupling themselves to persistence behavior. This gate removes invalid
    measurements before SenML serialization and emits compact diagnostic
    measurements so invalid source packets remain observable.
    """

    INVALID = "invalid"

    @staticmethod
    def _is_non_finite(measurement: Measurement) -> bool:
        value = measurement.value
        return isinstance(value, float) and not math.isfinite(value)

    @staticmethod
    def _timestamp(measurements: Iterable[Measurement]):
        for measurement in measurements:
            if measurement.timestamp is not None:
                return measurement.timestamp
        return None

    def apply(self, event: ParsedEvent) -> QualityResult:
        valid = []
        issues = []

        for measurement in event.measurements:
            quality = str(measurement.metadata.get("quality") or "").lower()
            reason = str(measurement.metadata.get("quality_reason") or "")
            source_field = str(measurement.metadata.get("source_field") or "")

            if quality == self.INVALID:
                issues.append(
                    QualityIssue(
                        measurement=measurement.name,
                        reason=reason or "parser_marked_invalid",
                        source_field=source_field,
                    )
                )
                continue

            if self._is_non_finite(measurement):
                issues.append(
                    QualityIssue(
                        measurement=measurement.name,
                        reason="non_finite_numeric_value",
                        source_field=source_field,
                    )
                )
                continue

            valid.append(measurement)

        if not issues:
            return QualityResult(event=event, status="valid")

        status = "degraded" if valid else "invalid"
        timestamp = self._timestamp(event.measurements)

        invalid_fields = []
        for issue in issues:
            field = issue.source_field or issue.measurement
            if field not in invalid_fields:
                invalid_fields.append(field)

        diagnostics = [
            Measurement(
                name="sensor.data_quality",
                value=status,
                timestamp=timestamp,
                metadata={"generated_by": "quality-gate"},
            ),
            Measurement(
                name="sensor.invalid_fields",
                value=",".join(invalid_fields),
                timestamp=timestamp,
                metadata={"generated_by": "quality-gate"},
            ),
        ]

        metadata = dict(event.metadata)
        metadata["data_quality"] = status
        metadata["quality_issues"] = [
            {
                "measurement": issue.measurement,
                "source_field": issue.source_field,
                "reason": issue.reason,
            }
            for issue in issues
        ]

        cleaned = ParsedEvent(
            external_device_id=event.external_device_id,
            measurements=tuple(valid + diagnostics),
            metadata=metadata,
        )
        return QualityResult(event=cleaned, status=status, issues=tuple(issues))


__all__ = ["MeasurementQualityPolicy", "QualityIssue", "QualityResult"]
