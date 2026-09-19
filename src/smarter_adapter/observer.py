from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Callable, Optional

from .inputs import MQTTInput, MQTTInputConfig
from .models import ParsedEvent, RawEvent
from .pipeline import ParsePipeline


@dataclass(frozen=True)
class Observation:
    raw: RawEvent
    parser: str
    event: ParsedEvent


@dataclass(frozen=True)
class ObserverStats:
    received: int
    parsed: int
    rejected: int


class MQTTObserver:
    """Read-only MQTT observer used to validate live sources safely.

    The observer deliberately has no Atom, publisher, Rules or state-store
    dependency. It only converts MQTT messages to RawEvent and runs the normal
    parser pipeline so a new source can be inspected before write-enabled
    Smarter Adapter operation is enabled.
    """

    def __init__(
        self,
        *,
        pipeline: ParsePipeline,
        config: MQTTInputConfig,
        on_observation: Optional[Callable[[Observation], None]] = None,
        on_rejected: Optional[Callable[[RawEvent, Exception], None]] = None,
        max_parsed: int = 5,
        input_factory=MQTTInput,
    ) -> None:
        if max_parsed < 1:
            raise ValueError("max_parsed must be >= 1")
        self.pipeline = pipeline
        self.config = config
        self.on_observation = on_observation
        self.on_rejected = on_rejected
        self.max_parsed = int(max_parsed)
        self._lock = threading.RLock()
        self._received = 0
        self._parsed = 0
        self._rejected = 0
        self._done = threading.Event()
        self._input = input_factory(config, self._handle_event)

    @property
    def input(self):
        return self._input

    @property
    def done(self) -> threading.Event:
        return self._done

    @property
    def stats(self) -> ObserverStats:
        with self._lock:
            return ObserverStats(
                received=self._received,
                parsed=self._parsed,
                rejected=self._rejected,
            )

    def _handle_event(self, raw: RawEvent) -> None:
        with self._lock:
            self._received += 1
        try:
            outcome = self.pipeline.process(raw)
        except Exception as exc:
            with self._lock:
                self._rejected += 1
            if self.on_rejected is not None:
                self.on_rejected(raw, exc)
            return

        observation = Observation(raw=raw, parser=outcome.parser, event=outcome.event)
        with self._lock:
            self._parsed += 1
            parsed = self._parsed
        if self.on_observation is not None:
            self.on_observation(observation)
        if parsed >= self.max_parsed:
            self._done.set()

    def start(self) -> None:
        self._done.clear()
        self._input.start()

    def stop(self) -> None:
        self._input.stop()


__all__ = ["MQTTObserver", "Observation", "ObserverStats"]
