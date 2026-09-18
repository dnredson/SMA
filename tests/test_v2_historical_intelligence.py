from __future__ import annotations

import unittest
from types import SimpleNamespace

from smarter_adapter.historical_intelligence import (
    AsyncIntelligenceSideChannel,
    HistoricalLLMContextBuilder,
    TimescaleHistoryProvider,
)
from smarter_adapter.magistrala.control_plane import DeviceRef
from smarter_adapter.magistrala.publisher import PublishResult
from smarter_adapter.magistrala.reader import ReaderError
from smarter_adapter.models import Measurement, ParsedEvent
from smarter_adapter.runtime import ProcessResult


def _result():
    event = ParsedEvent(
        external_device_id="teros12-sector1.3",
        measurements=(Measurement("soil.moisture", 25.0, "%", 300.0),),
        metadata={
            "sensor": "teros12",
            "node_id": "2313",
            "location": "Test_3",
            "sub_location": "mz_1",
            "depth": "15cm",
            "bt": 300.0,
        },
    )
    return ProcessResult(
        parser="test",
        parsed_event=event,
        device=DeviceRef(
            id="atom-1",
            workspace_id="ws-1",
            external_id=event.external_device_id,
            name=event.external_device_id,
            profile_id="profile-teros",
            profile_version_id="version-teros",
        ),
        senml=(),
        publish=PublishResult(status=202, body={"status": "accepted"}),
        device_cache_hit=True,
        device_cache_source="persistent",
        quality_status="valid",
        profile_key="smarter-adapter-teros12",
    )


class _Reader:
    def __init__(self, messages, *, total=None, error=None):
        self.messages = tuple(messages)
        self.total = len(self.messages) if total is None else total
        self.error = error
        self.calls = []

    def list_device_messages(self, workspace_id, channel_id, device_id, **kwargs):
        self.calls.append((workspace_id, channel_id, device_id, kwargs))
        if self.error is not None:
            raise self.error
        return SimpleNamespace(messages=self.messages, total=self.total)


class _Publisher:
    def __init__(self, enabled=True):
        self.enabled = enabled
        self.items = []

    def publish(self, payload, *, suffix="", timeout=3.0):
        self.items.append((dict(payload), suffix))
        return True, suffix or "topic"


class HistoricalIntelligenceTests(unittest.TestCase):
    def test_timescale_history_summarizes_semantic_trend_and_quality(self):
        reader = _Reader(
            [
                {"name": "soil.moisture", "value": 24.0, "unit": "%", "time": 200.0},
                {"name": "soil.moisture", "value": 20.0, "unit": "%", "time": 100.0},
                {"name": "soil.temperature", "value": 22.0, "unit": "Cel", "time": 100.0},
                {"name": "sensor.data_quality", "string_value": "valid", "time": 200.0},
                {"name": "sensor.invalid_fields", "string_value": "", "time": 200.0},
            ],
            total=50,
        )
        provider = TimescaleHistoryProvider(reader, per_series_limit=12)
        history = provider.build(_result(), workspace_id="ws-1", channel_id="ch-1")

        self.assertEqual(history["status"], "available")
        self.assertEqual(history["rows_total"], 50)
        self.assertEqual(history["quality_counts"]["valid"], 1)
        moisture = next(item for item in history["series"] if item["name"] == "soil.moisture")
        self.assertEqual(moisture["samples"], 2)
        self.assertEqual(moisture["direction"], "increasing")
        self.assertEqual(moisture["delta"], 4.0)
        temperature = next(item for item in history["series"] if item["name"] == "soil.temperature")
        self.assertEqual(temperature["direction"], "insufficient")

    def test_timescale_nanoseconds_and_senml_device_prefix_are_normalized(self):
        external_id = "teros12-sector1.3"
        reader = _Reader(
            [
                {
                    "name": external_id + ":soil.moisture",
                    "value": 20.0,
                    "unit": "%",
                    "time": 1_789_675_729_000_000_000,
                },
                {
                    "name": external_id + ":soil.moisture",
                    "value": 24.0,
                    "unit": "%",
                    "time": 1_789_679_329_000_000_000,
                },
                {
                    "name": external_id + ":sensor.data_quality",
                    "string_value": "valid",
                    "time": 1_789_679_329_000_000_000,
                },
            ]
        )
        provider = TimescaleHistoryProvider(reader)
        history = provider.build(_result(), workspace_id="ws-1", channel_id="ch-1")

        self.assertEqual(history["coverage"]["first_at"], 1_789_675_729.0)
        self.assertEqual(history["coverage"]["last_at"], 1_789_679_329.0)
        self.assertEqual(history["quality_counts"]["valid"], 1)
        self.assertEqual(len(history["series"]), 1)
        moisture = history["series"][0]
        self.assertEqual(moisture["name"], "soil.moisture")
        self.assertEqual(moisture["duration_seconds"], 3600.0)
        self.assertEqual(moisture["slope_per_hour"], 4.0)
        self.assertEqual(moisture["direction"], "increasing")

    def test_history_reader_failure_becomes_unavailable_context_not_exception(self):
        reader = _Reader([], error=ReaderError("reader down"))
        provider = TimescaleHistoryProvider(reader)
        history = provider.build(_result(), workspace_id="ws-1", channel_id="ch-1")
        self.assertEqual(history["status"], "unavailable")
        self.assertEqual(history["series"], [])
        self.assertIn("ReaderError", history["error"])

    def test_historical_context_keeps_current_observation_authoritative(self):
        history = {
            "status": "available",
            "source": "magistrala-timescale",
            "series": [
                {
                    "name": "soil.moisture",
                    "unit": "%",
                    "samples": 3,
                    "first_value": 20.0,
                    "last_value": 24.0,
                    "direction": "increasing",
                }
            ],
        }
        context = HistoricalLLMContextBuilder().build(_result(), history=history)
        self.assertEqual(context["observation"]["measurements"][0]["value"], 25.0)
        self.assertEqual(context["history"]["series"][0]["direction"], "increasing")
        self.assertIn("persisted historical trends", context["text"])
        self.assertIn("current data-quality flags remain authoritative", context["text"])

    def test_async_worker_drains_alert_and_enriched_context(self):
        alert_pub = _Publisher()
        context_pub = _Publisher()
        reader = _Reader(
            [
                {"name": "soil.moisture", "value": 20.0, "unit": "%", "time": 100.0},
                {"name": "soil.moisture", "value": 24.0, "unit": "%", "time": 200.0},
            ]
        )
        worker = AsyncIntelligenceSideChannel(
            context_builder=HistoricalLLMContextBuilder(),
            history_provider=TimescaleHistoryProvider(reader),
            alert_publisher=alert_pub,
            context_publisher=context_pub,
            scope_provider=lambda: ("ws-1", "ch-1"),
            max_queue=4,
        )
        worker.start()
        accepted, state = worker.submit(
            _result(),
            ({"type": "test-alert", "external_id": "teros12-sector1.3"},),
        )
        self.assertTrue(accepted)
        self.assertEqual(state, "queued")
        worker.close(timeout=2.0)

        self.assertEqual(len(alert_pub.items), 1)
        self.assertEqual(len(context_pub.items), 1)
        context, suffix = context_pub.items[0]
        self.assertEqual(suffix, "teros12-sector1.3")
        self.assertEqual(context["history"]["status"], "available")
        self.assertEqual(worker.stats.processed, 1)
        self.assertEqual(worker.stats.dropped, 0)


if __name__ == "__main__":
    unittest.main()
