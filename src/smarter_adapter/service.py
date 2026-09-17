from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Callable, Optional, Sequence

from .inputs import MQTTInput, MQTTInputConfig
from .reliability import (
    DeliveryQueueStore,
    RetryPolicy,
    is_retryable_failure,
    retry_deadline,
)
from .runtime import ProcessResult, SmarterAdapterRuntime


@dataclass(frozen=True)
class ServiceStats:
    received: int
    processed: int
    failed: int
    queued: int = 0
    retried: int = 0
    recovered: int = 0
    dead_lettered: int = 0


class SmarterAdapterService:
    """Run one Smarter Adapter runtime behind one or more MQTT inputs.

    Failed transient deliveries are persisted and retried by a background
    worker. Permanent failures and exhausted retries are moved to the DLQ.
    """

    def __init__(
        self,
        runtime: SmarterAdapterRuntime,
        inputs: Sequence[MQTTInputConfig],
        *,
        on_result: Optional[Callable[[ProcessResult], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
        input_factory=MQTTInput,
        reliability_store: Optional[DeliveryQueueStore] = None,
        retry_policy: RetryPolicy = RetryPolicy(),
    ) -> None:
        if not inputs:
            raise ValueError("at least one MQTT input is required")
        self.runtime = runtime
        self.input_configs = tuple(inputs)
        self.on_result = on_result
        self.on_error = on_error
        self.reliability_store = reliability_store
        self.retry_policy = retry_policy
        self._lock = threading.RLock()
        self._received = 0
        self._processed = 0
        self._failed = 0
        self._queued = 0
        self._retried = 0
        self._recovered = 0
        self._dead_lettered = 0
        self._inputs = [input_factory(config, self._handle_event) for config in self.input_configs]
        self._retry_stop = threading.Event()
        self._retry_thread: Optional[threading.Thread] = None

    def _report_error(self, exc: Exception) -> None:
        if self.on_error is not None:
            self.on_error(exc)

    def _route_failure(self, raw, exc: Exception) -> None:
        with self._lock:
            self._failed += 1

        store = self.reliability_store
        if store is None:
            self._report_error(exc)
            return

        if is_retryable_failure(exc):
            attempt = 1
            store.enqueue_retry(
                raw,
                exc,
                attempts=attempt,
                next_attempt_at=retry_deadline(self.retry_policy, attempt),
            )
            with self._lock:
                self._queued += 1
        else:
            store.add_dlq(raw, exc, attempts=1)
            with self._lock:
                self._dead_lettered += 1
        self._report_error(exc)

    def _handle_event(self, raw) -> None:
        with self._lock:
            self._received += 1
        try:
            result = self.runtime.process(raw)
        except Exception as exc:
            self._route_failure(raw, exc)
            return

        with self._lock:
            self._processed += 1
        if self.on_result is not None:
            self.on_result(result)

    def _retry_item(self, item) -> None:
        assert self.reliability_store is not None
        with self._lock:
            self._retried += 1
        try:
            result = self.runtime.process(item.raw)
        except Exception as exc:
            new_attempts = int(item.attempts) + 1
            if (not is_retryable_failure(exc)) or new_attempts >= self.retry_policy.max_attempts:
                self.reliability_store.move_retry_to_dlq(item.id, exc)
                with self._lock:
                    self._dead_lettered += 1
                self._report_error(exc)
                return

            self.reliability_store.reschedule_retry(
                item.id,
                exc,
                attempts=new_attempts,
                next_attempt_at=retry_deadline(self.retry_policy, new_attempts),
            )
            self._report_error(exc)
            return

        self.reliability_store.delete_retry(item.id)
        with self._lock:
            self._processed += 1
            self._recovered += 1
        if self.on_result is not None:
            self.on_result(result)

    def _retry_loop(self) -> None:
        assert self.reliability_store is not None
        while not self._retry_stop.wait(self.retry_policy.poll_interval_seconds):
            try:
                items = self.reliability_store.due_retries(limit=self.retry_policy.batch_size)
                for item in items:
                    if self._retry_stop.is_set():
                        return
                    self._retry_item(item)
            except Exception as exc:
                self._report_error(exc)
                time.sleep(min(self.retry_policy.poll_interval_seconds, 1.0))

    @property
    def stats(self) -> ServiceStats:
        with self._lock:
            return ServiceStats(
                received=self._received,
                processed=self._processed,
                failed=self._failed,
                queued=self._queued,
                retried=self._retried,
                recovered=self._recovered,
                dead_lettered=self._dead_lettered,
            )

    @property
    def inputs(self):
        return tuple(self._inputs)

    def start(self) -> None:
        self.runtime.bootstrap()
        self._retry_stop.clear()
        if self.reliability_store is not None:
            self._retry_thread = threading.Thread(
                target=self._retry_loop,
                name="smarter-adapter-retry",
                daemon=True,
            )
            self._retry_thread.start()

        started = []
        try:
            for item in self._inputs:
                item.start()
                started.append(item)
        except Exception:
            self._retry_stop.set()
            if self._retry_thread is not None:
                self._retry_thread.join(timeout=2.0)
            for item in reversed(started):
                try:
                    item.stop()
                except Exception:
                    pass
            raise

    def stop(self) -> None:
        for item in reversed(self._inputs):
            try:
                item.stop()
            except Exception as exc:
                self._report_error(exc)
        self._retry_stop.set()
        if self._retry_thread is not None:
            self._retry_thread.join(timeout=max(2.0, self.retry_policy.poll_interval_seconds * 2.0))
            self._retry_thread = None


__all__ = ["ServiceStats", "SmarterAdapterService"]
