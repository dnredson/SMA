from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from smarter_adapter.inputs import MQTTInputConfig
from smarter_adapter.models import Measurement, ParsedEvent, RawEvent
from smarter_adapter.observer import MQTTObserver
from smarter_adapter.pipeline import ParsePipeline
from smarter_adapter.plugins import ParserRegistry


class _Parser:
    name = "observer-parser"

    def supports(self, event):
        return event.payload.startswith(b"ok")

    def parse(self, event):
        return ParsedEvent(
            external_device_id="dev-1",
            measurements=(Measurement("temperature", 21.5, "Cel"),),
            metadata={"source": event.source},
        )


class _Input:
    def __init__(self, config, callback):
        self.config = config
        self.callback = callback
        self.connected = False
        self.last_error = None

    def start(self):
        self.connected = True

    def stop(self):
        self.connected = False


class MQTTObserverTests(unittest.TestCase):
    def test_observer_parses_without_any_control_plane_dependency(self):
        seen = []
        rejected = []
        observer = MQTTObserver(
            pipeline=ParsePipeline(ParserRegistry([_Parser()])),
            config=MQTTInputConfig(host="broker", topic="application/#"),
            on_observation=seen.append,
            on_rejected=lambda raw, exc: rejected.append((raw, exc)),
            max_parsed=2,
            input_factory=_Input,
        )

        observer.start()
        observer.input.callback(RawEvent(source="mqtt:test", topic="a", payload=b"ok-1"))
        observer.input.callback(RawEvent(source="mqtt:test", topic="b", payload=b"bad"))
        observer.input.callback(RawEvent(source="mqtt:test", topic="c", payload=b"ok-2"))

        self.assertTrue(observer.done.is_set())
        self.assertEqual(observer.stats.received, 3)
        self.assertEqual(observer.stats.parsed, 2)
        self.assertEqual(observer.stats.rejected, 1)
        self.assertEqual(len(seen), 2)
        self.assertEqual(seen[0].event.external_device_id, "dev-1")
        self.assertEqual(len(rejected), 1)
        observer.stop()

    def test_max_parsed_must_be_positive(self):
        with self.assertRaises(ValueError):
            MQTTObserver(
                pipeline=ParsePipeline(ParserRegistry([_Parser()])),
                config=MQTTInputConfig(host="broker"),
                max_parsed=0,
                input_factory=_Input,
            )


if __name__ == "__main__":
    unittest.main()
