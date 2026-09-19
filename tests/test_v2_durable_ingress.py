from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from smarter_adapter.magistrala.atom import AtomError
from smarter_adapter.models import RawEvent
from smarter_adapter.reliability import RetryPolicy
from smarter_adapter.service import SmarterAdapterService
from smarter_adapter.storage.sqlite import SQLiteStateStore
from smarter_adapter.inputs import MQTTInputConfig


class FakeRuntime:
    def __init__(self, *, error=None):
        self.error = error
        self.processed = []
        self.bootstrapped = 0

    def bootstrap(self):
        self.bootstrapped += 1

    def process(self, raw):
        self.processed.append(raw)
        if self.error is not None:
            raise self.error
        return raw


class FakeInput:
    def __init__(self, config, callback):
        self.config = config
        self.callback = callback
        self.connected = False
        self.last_error = None

    def start(self):
        self.connected = True

    def stop(self):
        self.connected = False


class DurableIngressTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "state.sqlite3"
        self.config = MQTTInputConfig(host="127.0.0.1", topic="test/#")

    def tearDown(self):
        self.tmp.cleanup()

    def _raw(self):
        return RawEvent(
            source="mqtt:test",
            topic="test/device/up",
            payload=b'{"hello":"world"}',
            received_at=1000.0,
            metadata={"qos": 0, "retain": False},
        )

    def test_callback_persists_before_runtime_processing(self):
        store = SQLiteStateStore(self.path)
        try:
            runtime = FakeRuntime()
            service = SmarterAdapterService(
                runtime,
                [self.config],
                input_factory=FakeInput,
                reliability_store=store,
            )
            service._handle_event(self._raw())

            self.assertEqual(runtime.processed, [])
            self.assertEqual(store.count_ingress(), 1)
            item = store.pending_ingress(limit=1)[0]
            self.assertEqual(item.raw.topic, "test/device/up")
            self.assertEqual(item.raw.payload, b'{"hello":"world"}')
            self.assertEqual(item.raw.received_at, 1000.0)
            self.assertEqual(service.stats.received, 1)
            self.assertEqual(service.stats.ingressed, 1)
        finally:
            store.close()

    def test_successful_ingress_processing_acks_row(self):
        store = SQLiteStateStore(self.path)
        try:
            runtime = FakeRuntime()
            service = SmarterAdapterService(
                runtime,
                [self.config],
                input_factory=FakeInput,
                reliability_store=store,
            )
            store.enqueue_ingress(self._raw())
            item = store.pending_ingress(limit=1)[0]
            service._process_ingress_item(item)

            self.assertEqual(store.count_ingress(), 0)
            self.assertEqual(len(runtime.processed), 1)
            self.assertEqual(service.stats.processed, 1)
        finally:
            store.close()

    def test_transient_ingress_failure_moves_atomically_to_retry(self):
        store = SQLiteStateStore(self.path)
        try:
            runtime = FakeRuntime(error=AtomError("temporary", status=503))
            service = SmarterAdapterService(
                runtime,
                [self.config],
                input_factory=FakeInput,
                reliability_store=store,
                retry_policy=RetryPolicy(
                    max_attempts=3,
                    base_delay_seconds=0.01,
                    max_delay_seconds=0.1,
                    poll_interval_seconds=0.01,
                    batch_size=10,
                ),
            )
            store.enqueue_ingress(self._raw())
            item = store.pending_ingress(limit=1)[0]
            service._process_ingress_item(item)

            self.assertEqual(store.count_ingress(), 0)
            self.assertEqual(store.count_retries(), 1)
            self.assertEqual(store.count_dlq(), 0)
            retry = store.due_retries(now=time.time() + 10, limit=1)[0]
            self.assertEqual(retry.raw.payload, self._raw().payload)
            self.assertEqual(retry.attempts, 1)
        finally:
            store.close()

    def test_pending_ingress_survives_restart_and_is_recovered(self):
        first = SQLiteStateStore(self.path)
        first.enqueue_ingress(self._raw())
        first.close()

        second = SQLiteStateStore(self.path)
        runtime = FakeRuntime()
        service = SmarterAdapterService(
            runtime,
            [self.config],
            input_factory=FakeInput,
            reliability_store=second,
            ingress_poll_interval_seconds=0.01,
        )
        try:
            self.assertEqual(second.count_ingress(), 1)
            service.start()
            deadline = time.time() + 1.0
            while second.count_ingress() and time.time() < deadline:
                time.sleep(0.01)
            self.assertEqual(second.count_ingress(), 0)
            self.assertEqual(len(runtime.processed), 1)
        finally:
            service.stop()
            second.close()


if __name__ == "__main__":
    unittest.main()
