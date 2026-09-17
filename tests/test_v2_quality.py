from __future__ import annotations

import math
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from smarter_adapter.models import Measurement, ParsedEvent
from smarter_adapter.quality import MeasurementQualityPolicy


class DataQualityTests(unittest.TestCase):
    def test_valid_event_is_unchanged(self):
        event = ParsedEvent(
            external_device_id="sensor-1",
            measurements=(Measurement("soil.temperature", 22.0, unit="Cel", timestamp=10.0),),
            metadata={"sensor": "test"},
        )
        result = MeasurementQualityPolicy().apply(event)
        self.assertEqual(result.status, "valid")
        self.assertEqual(result.issues, ())
        self.assertIs(result.event, event)

    def test_parser_marked_invalid_measurement_is_removed_and_diagnosed(self):
        event = ParsedEvent(
            external_device_id="sensor-1",
            measurements=(
                Measurement(
                    "soil.raw.moisture_m1",
                    -1.0,
                    unit="mV",
                    timestamp=10.0,
                    metadata={
                        "quality": "invalid",
                        "quality_reason": "source_sentinel",
                        "source_field": "moisture",
                    },
                ),
                Measurement("soil.temperature", 21.0, unit="Cel", timestamp=10.0),
            ),
        )
        result = MeasurementQualityPolicy().apply(event)
        values = {item.name: item.value for item in result.event.measurements}

        self.assertEqual(result.status, "degraded")
        self.assertNotIn("soil.raw.moisture_m1", values)
        self.assertEqual(values["soil.temperature"], 21.0)
        self.assertEqual(values["sensor.data_quality"], "degraded")
        self.assertEqual(values["sensor.invalid_fields"], "moisture")
        self.assertEqual(result.event.metadata["data_quality"], "degraded")

    def test_all_invalid_measurements_emit_diagnostics_instead_of_empty_event(self):
        invalid = {
            "quality": "invalid",
            "quality_reason": "source_sentinel",
            "source_field": "moisture",
        }
        event = ParsedEvent(
            external_device_id="sensor-1",
            measurements=(Measurement("soil.raw.moisture_m1", -1.0, metadata=invalid),),
        )
        result = MeasurementQualityPolicy().apply(event)
        values = {item.name: item.value for item in result.event.measurements}

        self.assertEqual(result.status, "invalid")
        self.assertEqual(set(values), {"sensor.data_quality", "sensor.invalid_fields"})
        self.assertEqual(values["sensor.data_quality"], "invalid")

    def test_non_finite_numeric_measurement_is_rejected(self):
        event = ParsedEvent(
            external_device_id="sensor-1",
            measurements=(Measurement("sensor.value", math.nan),),
        )
        result = MeasurementQualityPolicy().apply(event)
        self.assertEqual(result.status, "invalid")
        self.assertEqual(result.issues[0].reason, "non_finite_numeric_value")


if __name__ == "__main__":
    unittest.main()
