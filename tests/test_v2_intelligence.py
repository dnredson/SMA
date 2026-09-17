from __future__ import annotations

import unittest

from smarter_adapter.intelligence import (
    LLMContextBuilder,
    MqttJsonPublisher,
    ThresholdPolicy,
    alerts_for_result,
)
from smarter_adapter.magistrala.control_plane import DeviceRef
from smarter_adapter.magistrala.publisher import PublishResult
from smarter_adapter.models import Measurement, ParsedEvent
from smarter_adapter.quality import QualityIssue
from smarter_adapter.runtime import ProcessResult


def _result(
    event: ParsedEvent,
    *,
    quality_status: str = "valid",
    quality_issues=(),
    profile_key: str = "smarter-adapter-teros12",
):
    return ProcessResult(
        parser="test",
        parsed_event=event,
        device=DeviceRef(
            id="atom-device-1",
            workspace_id="ws-1",
            external_id=event.external_device_id,
            name=event.external_device_id,
            profile_id="profile-teros12",
            profile_version_id="version-1",
        ),
        senml=(),
        publish=PublishResult(status=202, body={"status": "accepted"}),
        device_cache_hit=True,
        device_cache_source="persistent",
        quality_status=quality_status,
        quality_issues=tuple(quality_issues),
        profile_key=profile_key,
    )


class IntelligenceTests(unittest.TestCase):
    def test_legacy_thresholds_are_preserved_for_normalized_names(self):
        policy = ThresholdPolicy.from_json("")
        event = ParsedEvent(
            external_device_id="greenstick-sector3",
            measurements=(
                Measurement("soil.raw.moisture_m1", 2600.0, "mV", 100.0),
                Measurement("soil.temperature", 24.0, "Cel", 100.0),
            ),
            metadata={"sensor": "greenstick", "node_id": "3303", "bt": 100.0},
        )
        alerts = alerts_for_result(_result(event, profile_key="smarter-adapter-greenstick"), policy)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["type"], "threshold_violation")
        self.assertEqual(alerts[0]["name"], "soil.raw.moisture_m1")
        self.assertEqual(alerts[0]["threshold"], "soil.raw.moisture_*")

    def test_quality_sentinel_becomes_quality_alert_not_fake_zero_threshold(self):
        policy = ThresholdPolicy.from_json("")
        event = ParsedEvent(
            external_device_id="teros12-sector1.3",
            measurements=(
                Measurement("sensor.data_quality", "invalid", timestamp=200.0),
                Measurement(
                    "sensor.invalid_fields",
                    "moisture,temperature,electrical_conductivity",
                    timestamp=200.0,
                ),
            ),
            metadata={
                "sensor": "teros12",
                "node_id": "2313",
                "location": "Test_3",
                "sub_location": "mz_1",
                "depth": "15cm",
                "bt": 200.0,
            },
        )
        result = _result(
            event,
            quality_status="invalid",
            quality_issues=(
                QualityIssue("soil.raw.moisture_m1", "source_sentinel", "moisture"),
                QualityIssue("soil.temperature", "source_sentinel", "temperature"),
                QualityIssue(
                    "soil.electrical_conductivity",
                    "source_sentinel",
                    "electrical_conductivity",
                ),
            ),
        )
        alerts = alerts_for_result(result, policy)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["type"], "data_quality")
        self.assertEqual(alerts[0]["quality"], "invalid")
        self.assertEqual(
            alerts[0]["invalid_fields"],
            ["moisture", "temperature", "electrical_conductivity"],
        )

    def test_llm_context_contains_profile_deployment_quality_and_guardrail(self):
        event = ParsedEvent(
            external_device_id="teros12-sector1.3",
            measurements=(Measurement("sensor.data_quality", "invalid", timestamp=200.0),),
            metadata={
                "sensor": "teros12",
                "node_id": "2313",
                "location": "Test_3",
                "sub_location": "mz_1",
                "depth": "15cm",
                "f_port": 31,
                "bt": 200.0,
            },
        )
        result = _result(
            event,
            quality_status="invalid",
            quality_issues=(QualityIssue("soil.raw.moisture_m1", "source_sentinel", "moisture"),),
        )
        alerts = alerts_for_result(result, ThresholdPolicy.from_json(""))
        context = LLMContextBuilder().build(result, alerts=alerts)

        self.assertEqual(context["schema"], "smarter-adapter.llm-context/1")
        self.assertEqual(context["device"]["sensor_family"], "teros12")
        self.assertEqual(context["device"]["profile_key"], "smarter-adapter-teros12")
        self.assertEqual(context["deployment"]["location"], "Test_3")
        self.assertEqual(context["quality"]["status"], "invalid")
        self.assertIn("unavailable", context["quality"]["interpretation"])
        self.assertIn("not zero", context["text"])
        self.assertEqual(len(context["alerts"]), 1)

    def test_custom_threshold_json_replaces_defaults(self):
        policy = ThresholdPolicy.from_json('{"soil.temperature":{"min":10,"max":20}}')
        event = ParsedEvent(
            external_device_id="sensor-1",
            measurements=(Measurement("soil.temperature", 25.0, "Cel", 10.0),),
            metadata={"sensor": "teros12", "bt": 10.0},
        )
        alerts = alerts_for_result(_result(event), policy)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["min"], 10.0)
        self.assertEqual(alerts[0]["max"], 20.0)

    def test_disabled_mqtt_side_channel_is_non_failing(self):
        publisher = MqttJsonPublisher(address="", topic_base="adapter/alerts")
        self.assertFalse(publisher.enabled)
        ok, detail = publisher.publish({"type": "test"})
        self.assertFalse(ok)
        self.assertEqual(detail, "disabled")


if __name__ == "__main__":
    unittest.main()
