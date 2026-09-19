from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Optional

from .device_lifecycle import DecommissionedEventSuppressed
from .reliability import IngressItem, is_retryable_failure, retry_deadline
from .service import SmarterAdapterService


@dataclass(frozen=True)
class LifecycleServiceStats:
    received: int
    processed: int
    failed: int
    queued: int = 0
    retried: int = 0
    recovered: int = 0
    dead_lettered: int = 0
    suppressed: int = 0
    ingressed: int = 0


class LifecycleSmarterAdapterService(SmarterAdapterService):
    """Service wrapper that treats decommission suppression as an admin decision.

    Suppressed events are neither failures nor dead letters. With durable
    ingress enabled they are simply acknowledged by deleting the persisted raw
    ingress row; retry items that become suppressed are likewise removed.
    """

    def __init__(
        self,
        *args,
        on_suppressed: Optional[Callable[[DecommissionedEventSuppressed], None]] = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.on_suppressed = on_suppressed
        self._suppressed = 0

    def _report_suppressed(self, exc: DecommissionedEventSuppressed) -> None:
        if self.on_suppressed is not None:
            self.on_suppressed(exc)

    def _handle_event(self, raw) -> None:
        # Let the base callback persist first when durable ingress is available.
        # The lifecycle-specific suppression decision then runs in the ingress
        # worker rather than inside Paho's network thread.
        if self.durable_ingress_enabled:
            super()._handle_event(raw)
            return

        with self._lock:
            self._received += 1
        try:
            result = self.runtime.process(raw)
        except DecommissionedEventSuppressed as exc:
            with self._lock:
                self._suppressed += 1
            self._report_suppressed(exc)
            return
        except Exception as exc:
            self._route_failure(raw, exc)
            return

        with self._lock:
            self._processed += 1
        if self.on_result is not None:
            self.on_result(result)

    def _process_ingress_item(self, item: IngressItem) -> None:
        assert self.reliability_store is not None
        try:
            result = self.runtime.process(item.raw)
        except DecommissionedEventSuppressed as exc:
            self.reliability_store.delete_ingress(item.id)
            with self._lock:
                self._suppressed += 1
            self._report_suppressed(exc)
            return
        except Exception as exc:
            self._route_ingress_failure(item, exc)
            return

        self.reliability_store.delete_ingress(item.id)
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
        except DecommissionedEventSuppressed as exc:
            self.reliability_store.delete_retry(item.id)
            with self._lock:
                self._suppressed += 1
            self._report_suppressed(exc)
            return
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

    @property
    def stats(self) -> LifecycleServiceStats:
        with self._lock:
            return LifecycleServiceStats(
                received=self._received,
                processed=self._processed,
                failed=self._failed,
                queued=self._queued,
                retried=self._retried,
                recovered=self._recovered,
                dead_lettered=self._dead_lettered,
                suppressed=self._suppressed,
                ingressed=self._ingressed,
            )


__all__ = ["LifecycleServiceStats", "LifecycleSmarterAdapterService"]
