from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Callable, Optional, Sequence

from .inputs import MQTTInput, MQTTInputConfig
from .runtime import ProcessResult, SmarterAdapterRuntime


@dataclass(frozen=True)
class ServiceStats:
    received: int
    processed: int
    failed: int


class SmarterAdapterService:
    """Run one Smarter Adapter runtime behind one or more MQTT inputs."""

    def __init__(
        self,
        runtime: SmarterAdapterRuntime,
        inputs: Sequence[MQTTInputConfig],
        *,
        on_result: Optional[Callable[[ProcessResult], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
        input_factory=MQTTInput,
    ) -> None:
        if not inputs:
            raise ValueError("at least one MQTT input is required")
        self.runtime = runtime
        self.input_configs = tuple(inputs)
        self.on_result = on_result
        self.on_error = on_error
        self._lock = threading.RLock()
        self._received = 0
        self._processed = 0
        self._failed = 0
        self._inputs = [
            input_factory(config, self._handle_event) for config in self.input_configs
        ]

    def _handle_event(self, raw) -> None:
        with self._lock:
            self._received += 1
        try:
            result = self.runtime.process(raw)
        except Exception as exc:
            with self._lock:
                self._failed += 1
            if self.on_error is not None:
                self.on_error(exc)
            return

        with self._lock:
            self._processed += 1
        if self.on_result is not None:
            self.on_result(result)

    @property
    def stats(self) -> ServiceStats:
        with self._lock:
            return ServiceStats(
                received=self._received,
                processed=self._processed,
                failed=self._failed,
            )

    @property
    def inputs(self):
        return tuple(self._inputs)

    def start(self) -> None:
        self.runtime.bootstrap()
        started = []
        try:
            for item in self._inputs:
                item.start()
                started.append(item)
        except Exception:
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
                if self.on_error is not None:
                    self.on_error(exc)


__all__ = ["ServiceStats", "SmarterAdapterService"]
