from __future__ import annotations

import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from smarter_adapter.magistrala.publisher import PublishError
from smarter_adapter.models import RawEvent
from smarter_adapter.plugins import ParserNotFound
from smarter_adapter.reliability import RetryPolicy, is_retryable_failure
from smarter_adapter.service import SmarterAdapterService
from smarter_adapter.storage import SQLiteStateStore


class _Input:
    def __init__(self, config, callback):
        self.callback = callback

    def start(self):
        pass

    def stop(self):
        pass


class _Runtime:
    def __init__(self, failures):
        self.failures = list(failures)
        self.calls = 0
        self.bootstraps = 0

    def bootstrap(self):
        self.bootstraps += 1

    def process(self, raw):
        self.calls += 1
        if self.failures:
            exc = self.failures.pop(0)
            if exc is not None:
                raise exc
        return object()


class _Config:
    pass


class ReliabilityTests(unittest.TestCase):
    def test_failure_classification(self):
        self.assertTrue(is_retryable_failure(PublishError("down")))
        self.assertTrue(is_retryable_failure(PublishError("busy", status=503)))
        self.assertTrue(is_retryable_failure(PublishError("rate", status=429)))
        self.assertFalse(is_retryable_failure(PublishError("forbidden", status=403)))
        self.assertFalse(is_retryable_failure(ParserNotFound("no parser")))

    def test_sqlite_retry_survives_and_can_move_to_dlq(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.sqlite3"
            raw = RawEvent(
                source="mqtt:test",
                topic="application/test",
                payload=b'{"x":1}',
                received_at=100.0,
                metadata={"qos": 1},
            )
            with SQLiteStateStore(path) as store:
                item_id = store.enqueue_retry(
                    raw,
                    PublishError("offline"),
                    attempts=1,
                    next_attempt_at=0.0,
                )
                self.assertGreater(item_id, 0)
                self.assertEqual(store.count_retries(), 1)

            with SQLiteStateStore(path) as store:
                items = store.due_retries(now=1.0, limit=10)
                self.assertEqual(len(items), 1)
                self.assertEqual(items[0].raw.payload, b'{"x":1}')
                self.assertEqual(items[0].raw.metadata["qos"], 1)
                dlq_id = store.move_retry_to_dlq(items[0].id, PublishError("still offline"))
                self.assertGreater(dlq_id, 0)
                self.assertEqual(store.count_retries(), 0)
                self.assertEqual(store.count_dlq(), 1)

    def test_service_recovers_transient_failure_from_persistent_queue(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteStateStore(Path(tmp) / "state.sqlite3")
            runtime = _Runtime([PublishError("offline", status=503), None])
            service = SmarterAdapterService(
                runtime,
                [_Config()],
                input_factory=_Input,
                reliability_store=store,
                retry_policy=RetryPolicy(
                    max_attempts=3,
                    base_delay_seconds=0.01,
                    max_delay_seconds=0.02,
                    poll_interval_seconds=0.01,
                ),
            )
            service.start()
            try:
                service._handle_event(RawEvent(source="mqtt:test", payload=b"x"))
                deadline = time.time() + 1.0
                while service.stats.recovered < 1 and time.time() < deadline:
                    time.sleep(0.01)
                stats = service.stats
                self.assertEqual(stats.received, 1)
                self.assertEqual(stats.queued, 1)
                self.assertEqual(stats.recovered, 1)
                self.assertEqual(stats.processed, 1)
                self.assertGreaterEqual(stats.retried, 1)
                self.assertEqual(store.count_retries(), 0)
                self.assertEqual(store.count_dlq(), 0)
            finally:
                service.stop()
                store.close()

    def test_permanent_failure_goes_directly_to_dlq(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteStateStore(Path(tmp) / "state.sqlite3")
            runtime = _Runtime([ParserNotFound("unknown payload")])
            service = SmarterAdapterService(
                runtime,
                [_Config()],
                input_factory=_Input,
                reliability_store=store,
                retry_policy=RetryPolicy(
                    base_delay_seconds=0.01,
                    max_delay_seconds=0.02,
                    poll_interval_seconds=0.01,
                ),
            )
            service._handle_event(RawEvent(source="mqtt:test", payload=b"bad"))
            self.assertEqual(store.count_retries(), 0)
            self.assertEqual(store.count_dlq(), 1)
            self.assertEqual(service.stats.dead_lettered, 1)
            store.close()


if __name__ == "__main__":
    unittest.main()
