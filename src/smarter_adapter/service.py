from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Callable, Optional, Sequence

from .inputs import MQTTInput, MQTTInputConfig
from .reliability import (
    DeliveryQueueStore,
    IngressItem,
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
    ingressed: int = 0


class SmarterAdapterService:
    """Run one Smarter Adapter runtime behind one or more MQTT inputs.

    When the configured state store implements the durable ingress contract,
    the MQTT callback performs only one synchronous durability operation: it
    writes the complete :class:`RawEvent` to SQLite. A separate worker then
    parses, reconciles and publishes the event. Successful events are removed
    from ingress; failures are atomically promoted to retry or DLQ.

    This closes the old crash window where a QoS-0 MQTT packet could disappear
    after Paho delivered it but before the adapter had persisted any state.
    Custom/legacy stores that do not implement ingress keep the previous direct
    processing behavior for backward compatibility.
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
        ingress_poll_interval_seconds: float = 0.1,
        ingress_batch_size: int = 50,
    ) -> None:
        if not inputs:
            raise ValueError("at least one MQTT input is required")
        if float(ingress_poll_interval_seconds) <= 0:
            raise ValueError("ingress_poll_interval_seconds must be > 0")
        if int(ingress_batch_size) < 1:
            raise ValueError("ingress_batch_size must be >= 1")
        self.runtime = runtime
        self.input_configs = tuple(inputs)
        self.on_result = on_result
        self.on_error = on_error
        self.reliability_store = reliability_store
        self.retry_policy = retry_policy
        self.ingress_poll_interval_seconds = float(ingress_poll_interval_seconds)
        self.ingress_batch_size = int(ingress_batch_size)
        self._lock = threading.RLock()
        self._received = 0
        self._processed = 0
        self._failed = 0
        self._queued = 0
        self._retried = 0
        self._recovered = 0
        self._dead_lettered = 0
        self._ingressed = 0
        self._inputs = [input_factory(config, self._handle_event) for config in self.input_configs]
        self._retry_stop = threading.Event()
        self._retry_thread: Optional[threading.Thread] = None
        self._ingress_stop = threading.Event()
        self._ingress_wakeup = threading.Event()
        self._ingress_thread: Optional[threading.Thread] = None

    @property
    def durable_ingress_enabled(self) -> bool:
        store = self.reliability_store
        if store is None:
            return False
        required = (
            "enqueue_ingress",
            "pending_ingress",
            "delete_ingress",
            "move_ingress_to_retry",
            "move_ingress_to_dlq",
            "count_ingress",
        )
        return all(callable(getattr(store, name, None)) for name in required)

    def _report_error(self, exc: Exception) -> None:
        if self.on_error is not None:
            self.on_error(exc)

    def _route_failure(self, raw, exc: Exception) -> None:
        """Legacy/direct failure routing when no durable ingress is available."""
        with self._lock:
            self._failed += 1

        store = self.reliability_store
        if store is None:
            self._report_error(exc)
            return

        if is_retryable_failure(exc) and self.retry_policy.max_attempts > 1:
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

    def _handle_direct_event(self, raw) -> None:
        try:
            result = self.runtime.process(raw)
        except Exception as exc:
            self._route_failure(raw, exc)
            return

        with self._lock:
            self._processed += 1
        if self.on_result is not None:
            self.on_result(result)

    def _handle_event(self, raw) -> None:
        """MQTT callback: persist first, process outside the network thread."""
        with self._lock:
            self._received += 1

        if not self.durable_ingress_enabled:
            self._handle_direct_event(raw)
            return

        assert self.reliability_store is not None
        try:
            self.reliability_store.enqueue_ingress(raw)
        except Exception as exc:
            # We deliberately do not fall back to unpersisted processing here:
            # doing so would silently re-open the crash window this queue exists
            # to close. Surface the storage failure instead.
            with self._lock:
                self._failed += 1
            self._report_error(exc)
            return

        with self._lock:
            self._ingressed += 1
        self._ingress_wakeup.set()

    def _route_ingress_failure(self, item: IngressItem, exc: Exception) -> None:
        assert self.reliability_store is not None
        with self._lock:
            self._failed += 1

        attempt = 1
        if is_retryable_failure(exc) and self.retry_policy.max_attempts > 1:
            self.reliability_store.move_ingress_to_retry(
                item.id,
                exc,
                attempts=attempt,
                next_attempt_at=retry_deadline(self.retry_policy, attempt),
            )
            with self._lock:
                self._queued += 1
        else:
            self.reliability_store.move_ingress_to_dlq(
                item.id,
                exc,
                attempts=attempt,
            )
            with self._lock:
                self._dead_lettered += 1
        self._report_error(exc)

    def _process_ingress_item(self, item: IngressItem) -> None:
        assert self.reliability_store is not None
        try:
            result = self.runtime.process(item.raw)
        except Exception as exc:
            self._route_ingress_failure(item, exc)
            return

        self.reliability_store.delete_ingress(item.id)
        with self._lock:
            self._processed += 1
        if self.on_result is not None:
            self.on_result(result)

    def _ingress_loop(self) -> None:
        assert self.reliability_store is not None
        while not self._ingress_stop.is_set():
            try:
                items = self.reliability_store.pending_ingress(
                    limit=self.ingress_batch_size
                )
                if not items:
                    self._ingress_wakeup.wait(self.ingress_poll_interval_seconds)
                    self._ingress_wakeup.clear()
                    continue
                for item in items:
                    if self._ingress_stop.is_set():
                        return
                    self._process_ingress_item(item)
            except Exception as exc:
                self._report_error(exc)
                self._ingress_stop.wait(min(self.ingress_poll_interval_seconds, 1.0))

    def _retry_item(self, item) -> None:
        assert self.reliability_store is not None
        with self._lock:
            self._retried += 1
        try:
            result = self.runtime.process(item.raw)
        except Exception as exc:
            new_attempts = int(item.attempts) + 1
            if (not is_retryable_failure(exc)) or new_attempts >= self.retry_policy.max_attempts:
                self.reliability_store.reschedule_retry(
                    item.id,
                    exc,
                    attempts=new_attempts,
                    next_attempt_at=time.time(),
                )
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
                ingressed=self._ingressed,
            )

    @property
    def inputs(self):
        return tuple(self._inputs)

    def start(self) -> None:
        self.runtime.bootstrap()
        self._retry_stop.clear()
        self._ingress_stop.clear()
        self._ingress_wakeup.clear()
        if self.reliability_store is not None:
            self._retry_thread = threading.Thread(
                target=self._retry_loop,
                name="smarter-adapter-retry",
                daemon=True,
            )
            self._retry_thread.start()

            if self.durable_ingress_enabled:
                self._ingress_thread = threading.Thread(
                    target=self._ingress_loop,
                    name="smarter-adapter-ingress",
                    daemon=True,
                )
                self._ingress_thread.start()
                # Process leftovers from an interrupted previous run immediately.
                self._ingress_wakeup.set()

        started = []
        try:
            for item in self._inputs:
                item.start()
                started.append(item)
        except Exception:
            self._retry_stop.set()
            self._ingress_stop.set()
            self._ingress_wakeup.set()
            if self._retry_thread is not None:
                self._retry_thread.join(timeout=2.0)
            if self._ingress_thread is not None:
                self._ingress_thread.join(timeout=2.0)
            for item in reversed(started):
                try:
                    item.stop()
                except Exception:
                    pass
            raise

    def stop(self) -> None:
        # Stop network delivery first. Anything already accepted by the callback
        # is on durable ingress and can safely remain there for the next start.
        for item in reversed(self._inputs):
            try:
                item.stop()
            except Exception as exc:
                self._report_error(exc)

        self._ingress_stop.set()
        self._ingress_wakeup.set()
        if self._ingress_thread is not None:
            self._ingress_thread.join(
                timeout=max(2.0, self.ingress_poll_interval_seconds * 2.0)
            )
            self._ingress_thread = None

        self._retry_stop.set()
        if self._retry_thread is not None:
            self._retry_thread.join(timeout=max(2.0, self.retry_policy.poll_interval_seconds * 2.0))
            self._retry_thread = None


__all__ = ["ServiceStats", "SmarterAdapterService"]
