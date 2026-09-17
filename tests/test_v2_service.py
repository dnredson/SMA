from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from smarter_adapter.inputs import MQTTInputConfig
from smarter_adapter.models import RawEvent
from smarter_adapter.service import SmarterAdapterService


class _Runtime:
    def __init__(self, fail_on=b""):
        self.bootstrap_calls = 0
        self.events = []
        self.fail_on = fail_on

    def bootstrap(self):
        self.bootstrap_calls += 1

    def process(self, raw):
        self.events.append(raw)
        if self.fail_on and raw.payload == self.fail_on:
            raise RuntimeError("synthetic failure")
        return SimpleNamespace(parsed_event=SimpleNamespace(external_device_id="dev"))


class _Input:
    def __init__(self, config, callback):
        self.config = config
        self.callback = callback
        self.started = False
        self.stopped = False

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True

    def emit(self, payload=b"ok"):
        self.callback(
            RawEvent(
                source=self.config.source,
                topic=self.config.topic,
                payload=payload,
            )
        )


class ServiceTests(unittest.TestCase):
    def test_multiple_inputs_share_one_runtime(self):
        runtime = _Runtime()
        results = []
        created = []

        def factory(config, callback):
            item = _Input(config, callback)
            created.append(item)
            return item

        service = SmarterAdapterService(
            runtime,
            [
                MQTTInputConfig(host="broker-a", topic="a/#", source="mqtt:a"),
                MQTTInputConfig(host="broker-b", topic="b/#", source="mqtt:b"),
            ],
            on_result=results.append,
            input_factory=factory,
        )

        service.start()
        created[0].emit(b"one")
        created[1].emit(b"two")
        service.stop()

        self.assertEqual(runtime.bootstrap_calls, 1)
        self.assertEqual([event.source for event in runtime.events], ["mqtt:a", "mqtt:b"])
        self.assertTrue(all(item.started and item.stopped for item in created))
        self.assertEqual(len(results), 2)
        self.assertEqual(service.stats.received, 2)
        self.assertEqual(service.stats.processed, 2)
        self.assertEqual(service.stats.failed, 0)

    def test_processing_failure_is_counted_without_stopping_other_inputs(self):
        runtime = _Runtime(fail_on=b"bad")
        errors = []
        created = []

        def factory(config, callback):
            item = _Input(config, callback)
            created.append(item)
            return item

        service = SmarterAdapterService(
            runtime,
            [MQTTInputConfig(host="broker", topic="#", source="mqtt:test")],
            on_error=errors.append,
            input_factory=factory,
        )

        service.start()
        created[0].emit(b"bad")
        created[0].emit(b"good")
        service.stop()

        self.assertEqual(len(errors), 1)
        self.assertEqual(service.stats.received, 2)
        self.assertEqual(service.stats.failed, 1)
        self.assertEqual(service.stats.processed, 1)


if __name__ == "__main__":
    unittest.main()
