from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Optional, Protocol, Sequence, runtime_checkable

from .magistrala.atom import AtomError
from .magistrala.publisher import PublishError
from .magistrala.rules import RulesError
from .models import RawEvent
from .plugins import ParserNotFound


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 5
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    poll_interval_seconds: float = 0.5
    batch_size: int = 50

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay_seconds <= 0:
            raise ValueError("base_delay_seconds must be > 0")
        if self.max_delay_seconds < self.base_delay_seconds:
            raise ValueError("max_delay_seconds must be >= base_delay_seconds")
        if self.poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be > 0")
        if self.batch_size < 1:
            raise ValueError("batch_size must be >= 1")

    def delay_for_attempt(self, attempt: int) -> float:
        # attempt=1 is the first failed delivery. The first retry waits base_delay.
        exponent = max(int(attempt) - 1, 0)
        delay = self.base_delay_seconds * math.pow(2.0, exponent)
        return min(delay, self.max_delay_seconds)


@dataclass(frozen=True)
class RetryItem:
    id: int
    raw: RawEvent
    attempts: int
    next_attempt_at: float
    first_failed_at: float
    last_error: str
    error_type: str


@dataclass(frozen=True)
class DeadLetterItem:
    id: int
    raw: RawEvent
    attempts: int
    failed_at: float
    last_error: str
    error_type: str


@runtime_checkable
class DeliveryQueueStore(Protocol):
    def enqueue_retry(
        self,
        raw: RawEvent,
        error: Exception,
        *,
        attempts: int,
        next_attempt_at: float,
    ) -> int: ...

    def due_retries(
        self,
        *,
        now: Optional[float] = None,
        limit: int = 50,
    ) -> Sequence[RetryItem]: ...

    def reschedule_retry(
        self,
        item_id: int,
        error: Exception,
        *,
        attempts: int,
        next_attempt_at: float,
    ) -> None: ...

    def delete_retry(self, item_id: int) -> None: ...

    def move_retry_to_dlq(self, item_id: int, error: Exception) -> int: ...

    def add_dlq(
        self,
        raw: RawEvent,
        error: Exception,
        *,
        attempts: int = 1,
    ) -> int: ...

    def count_retries(self) -> int: ...

    def count_dlq(self) -> int: ...


def _status(exc: Exception) -> Optional[int]:
    value = getattr(exc, "status", None)
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def is_retryable_failure(exc: Exception) -> bool:
    """Return whether a processing failure is likely transient.

    Transport failures have no HTTP status and are retried. HTTP 408/425/429
    and 5xx are retried. Parser/validation failures and other 4xx are treated as
    permanent. Atom/Rules/FluxMQ clients already perform their own one-shot auth
    refresh, so a surviving 401 is considered non-transient here.
    """

    if isinstance(exc, ParserNotFound):
        return False
    if isinstance(exc, (ValueError, TypeError, UnicodeError)):
        return False

    if isinstance(exc, (PublishError, AtomError, RulesError)):
        status = _status(exc)
        if status is None:
            return True
        if status in (408, 425, 429):
            return True
        return 500 <= status <= 599

    # Unknown exceptions are kept out of an infinite retry loop by default.
    return False


def retry_deadline(policy: RetryPolicy, attempt: int, *, now: Optional[float] = None) -> float:
    current = time.time() if now is None else float(now)
    return current + policy.delay_for_attempt(attempt)


__all__ = [
    "DeadLetterItem",
    "DeliveryQueueStore",
    "RetryItem",
    "RetryPolicy",
    "is_retryable_failure",
    "retry_deadline",
]
