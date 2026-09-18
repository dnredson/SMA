from __future__ import annotations

import math
import queue
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence

from .intelligence import LLMContextBuilder


@dataclass(frozen=True)
class TrendSeries:
    name: str
    unit: Optional[str]
    samples: int
    first_at: float
    last_at: float
    first_value: float
    last_value: float
    minimum: float
    maximum: float
    mean: float
    delta: float
    slope_per_hour: float
    direction: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "unit": self.unit,
            "samples": self.samples,
            "first_at": self.first_at,
            "last_at": self.last_at,
            "duration_seconds": max(0.0, self.last_at - self.first_at),
            "first_value": self.first_value,
            "last_value": self.last_value,
            "min": self.minimum,
            "max": self.maximum,
            "mean": self.mean,
            "delta": self.delta,
            "slope_per_hour": self.slope_per_hour,
            "direction": self.direction,
        }


class TimescaleHistoryProvider:
    """Read recent persisted measurements and summarize numeric trends.

    The current observation is deliberately not required to be visible in
    Timescale yet: the main LLM envelope already carries it. This provider is
    only historical context, so Rules/Timescale writer latency cannot change
    the meaning of the current observation.
    """

    def __init__(
        self,
        reader,
        *,
        limit: int = 120,
        per_series_limit: int = 12,
        max_series: int = 16,
    ) -> None:
        self.reader = reader
        self.limit = min(max(int(limit), 1), 1000)
        self.per_series_limit = max(int(per_series_limit), 2)
        self.max_series = max(int(max_series), 1)

    @staticmethod
    def _epoch_seconds(value: float) -> float:
        """Normalize common Unix epoch units to seconds.

        Magistrala's Timescale reader currently returns message ``time`` in
        nanoseconds, while parsed SMA events use Unix seconds. Accept seconds,
        milliseconds, microseconds and nanoseconds so trend duration/slope use
        one unit even if the reader representation changes.
        """
        magnitude = abs(value)
        if magnitude >= 1.0e17:  # nanoseconds, e.g. 1.789e18
            return value / 1.0e9
        if magnitude >= 1.0e14:  # microseconds
            return value / 1.0e6
        if magnitude >= 1.0e11:  # milliseconds
            return value / 1.0e3
        return value

    @classmethod
    def _time(cls, item: Mapping[str, Any]) -> Optional[float]:
        for key in ("time", "timestamp", "bt"):
            raw = item.get(key)
            if isinstance(raw, bool) or raw is None:
                continue
            try:
                value = float(raw)
            except (TypeError, ValueError):
                continue
            if math.isfinite(value):
                return cls._epoch_seconds(value)
        return None

    @staticmethod
    def _measurement_name(raw_name: object, external_id: str = "") -> str:
        """Remove the SenML base-name/device prefix from persisted names."""
        name = str(raw_name or "").strip()
        prefix = str(external_id or "").strip()
        if prefix and name.startswith(prefix + ":"):
            return name[len(prefix) + 1 :]
        return name

    @staticmethod
    def _numeric(item: Mapping[str, Any]) -> Optional[float]:
        raw = item.get("value")
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            return None
        value = float(raw)
        return value if math.isfinite(value) else None

    @staticmethod
    def _quality_value(item: Mapping[str, Any]) -> str:
        for key in ("string_value", "value"):
            value = item.get(key)
            if isinstance(value, str) and value:
                return value.strip().lower()
        return ""

    @staticmethod
    def _trend(name: str, unit: Optional[str], samples: Sequence[tuple[float, float]]) -> TrendSeries:
        ordered = sorted(samples, key=lambda pair: pair[0])
        first_at, first_value = ordered[0]
        last_at, last_value = ordered[-1]
        values = [value for _, value in ordered]
        mean = sum(values) / len(values)
        delta = last_value - first_value

        mean_t = sum(ts for ts, _ in ordered) / len(ordered)
        variance_t = sum((ts - mean_t) ** 2 for ts, _ in ordered)
        if variance_t <= 0.0:
            slope_per_second = 0.0
        else:
            slope_per_second = sum(
                (ts - mean_t) * (value - mean)
                for ts, value in ordered
            ) / variance_t
        slope_per_hour = slope_per_second * 3600.0

        # Generic trend classification, intentionally conservative. This is a
        # descriptive direction, not a domain alarm: threshold rules remain the
        # authority for alerts. One sample is never called a trend.
        if len(ordered) < 2 or last_at <= first_at:
            direction = "insufficient"
        else:
            scale = max(abs(first_value), abs(last_value), abs(mean), 1.0)
            tolerance = scale * 0.01
            if abs(delta) <= tolerance:
                direction = "stable"
            elif delta > 0:
                direction = "increasing"
            else:
                direction = "decreasing"

        return TrendSeries(
            name=name,
            unit=unit,
            samples=len(ordered),
            first_at=float(first_at),
            last_at=float(last_at),
            first_value=float(first_value),
            last_value=float(last_value),
            minimum=float(min(values)),
            maximum=float(max(values)),
            mean=float(mean),
            delta=float(delta),
            slope_per_hour=float(slope_per_hour),
            direction=direction,
        )

    def summarize_messages(
        self,
        messages: Iterable[Mapping[str, Any]],
        *,
        total: Optional[int] = None,
        external_id: str = "",
    ) -> dict[str, Any]:
        rows = [dict(item) for item in messages if isinstance(item, Mapping)]
        grouped: dict[str, list[tuple[float, float, Optional[str]]]] = {}
        quality_counts = {"valid": 0, "degraded": 0, "invalid": 0, "unknown": 0}

        for item in rows:
            name = self._measurement_name(item.get("name"), external_id)
            if not name:
                continue
            if name == "sensor.data_quality":
                quality = self._quality_value(item)
                if quality not in quality_counts:
                    quality = "unknown"
                quality_counts[quality] += 1
                continue
            if name.startswith("sensor."):
                continue
            ts = self._time(item)
            value = self._numeric(item)
            if ts is None or value is None:
                continue
            unit_raw = item.get("unit")
            unit = str(unit_raw) if unit_raw not in (None, "") else None
            grouped.setdefault(name, []).append((ts, value, unit))

        series: list[TrendSeries] = []
        for name, values in grouped.items():
            # Deduplicate identical persisted rows and retain the newest N for
            # one measurement before computing a trend.
            unique: dict[tuple[float, float], Optional[str]] = {}
            for ts, value, unit in values:
                unique[(float(ts), float(value))] = unit
            ordered = sorted(
                ((ts, value, unit) for (ts, value), unit in unique.items()),
                key=lambda item: item[0],
            )[-self.per_series_limit :]
            if not ordered:
                continue
            unit = next((item[2] for item in reversed(ordered) if item[2]), None)
            numeric = [(item[0], item[1]) for item in ordered]
            series.append(self._trend(name, unit, numeric))

        # Semantic/derived values are more useful to an LLM than raw ADC rows;
        # raw series remain available after them for diagnostics.
        series.sort(key=lambda item: (".raw." in item.name, item.name))
        series = series[: self.max_series]

        times = [ts for values in grouped.values() for ts, _, _ in values]
        return {
            "status": "available",
            "source": "magistrala-timescale",
            "rows_returned": len(rows),
            "rows_total": int(total if total is not None else len(rows)),
            "coverage": {
                "first_at": min(times) if times else None,
                "last_at": max(times) if times else None,
            },
            "quality_counts": quality_counts,
            "series": [item.as_dict() for item in series],
            "interpretation": (
                "Historical trends summarize persisted prior measurements. "
                "The current observation and its data-quality status remain authoritative."
            ),
        }

    def build(self, result, *, workspace_id: str, channel_id: str) -> dict[str, Any]:
        try:
            external_id = result.parsed_event.external_device_id
            page = self.reader.list_device_messages(
                workspace_id,
                channel_id,
                external_id,
                limit=self.limit,
                offset=0,
                order="time",
                direction="desc",
            )
            return self.summarize_messages(
                page.messages,
                total=page.total,
                external_id=external_id,
            )
        except Exception as exc:
            # History is optional intelligence context. Reader failures must not
            # propagate into the primary telemetry delivery path.
            return {
                "status": "unavailable",
                "source": "magistrala-timescale",
                "rows_returned": 0,
                "rows_total": 0,
                "coverage": {"first_at": None, "last_at": None},
                "quality_counts": {
                    "valid": 0,
                    "degraded": 0,
                    "invalid": 0,
                    "unknown": 0,
                },
                "series": [],
                "error": f"{type(exc).__name__}: {str(exc)[:300]}",
                "interpretation": (
                    "Historical context is temporarily unavailable; reason only from the current observation."
                ),
            }


class HistoricalLLMContextBuilder:
    """Add Timescale history to the provider-neutral LLM context envelope."""

    def __init__(self, base: Optional[LLMContextBuilder] = None) -> None:
        self.base = base or LLMContextBuilder()

    @staticmethod
    def _format_value(value: Any) -> str:
        if isinstance(value, float):
            return f"{value:.6g}"
        return str(value)

    def build(
        self,
        result,
        *,
        alerts: Iterable[Mapping[str, Any]] = (),
        history: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        context = self.base.build(result, alerts=alerts)
        history_obj = dict(history or {"status": "disabled", "series": []})
        context["history"] = history_obj

        if history_obj.get("status") == "available":
            series = list(history_obj.get("series") or [])
            if series:
                summaries = []
                for item in series[:4]:
                    unit = f" {item.get('unit')}" if item.get("unit") else ""
                    summaries.append(
                        f"{item.get('name')} {item.get('direction')} "
                        f"({self._format_value(item.get('first_value'))}{unit} -> "
                        f"{self._format_value(item.get('last_value'))}{unit}, "
                        f"n={item.get('samples')})"
                    )
                context["text"] = context["text"].rstrip(".") + (
                    "; persisted historical trends: " + ", ".join(summaries)
                    + "; current data-quality flags remain authoritative."
                )
            else:
                context["text"] = context["text"].rstrip(".") + (
                    "; no usable persisted numeric history is available yet."
                )
        elif history_obj.get("status") == "unavailable":
            context["text"] = context["text"].rstrip(".") + (
                "; historical context is temporarily unavailable; reason from the current observation only."
            )
        return context


@dataclass
class IntelligenceWorkerStats:
    queued: int = 0
    processed: int = 0
    dropped: int = 0
    failures: int = 0


class AsyncIntelligenceSideChannel:
    """Bounded asynchronous alert/history/context side channel.

    `submit` never waits for Timescale or MQTT. If the bounded queue fills, the
    intelligence event is dropped and reported, while primary telemetry remains
    successful. This is deliberate backpressure isolation until a durable
    intelligence queue is introduced.
    """

    def __init__(
        self,
        *,
        context_builder: HistoricalLLMContextBuilder,
        history_provider: TimescaleHistoryProvider,
        alert_publisher,
        context_publisher,
        scope_provider: Callable[[], tuple[str, str]],
        max_queue: int = 128,
        on_warning: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.context_builder = context_builder
        self.history_provider = history_provider
        self.alert_publisher = alert_publisher
        self.context_publisher = context_publisher
        self.scope_provider = scope_provider
        self.on_warning = on_warning
        self.stats = IntelligenceWorkerStats()
        self._queue: queue.Queue = queue.Queue(maxsize=max(int(max_queue), 1))
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()

    @property
    def enabled(self) -> bool:
        return bool(self.alert_publisher.enabled or self.context_publisher.enabled)

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()

    @property
    def max_queue(self) -> int:
        return self._queue.maxsize

    def _warn(self, message: str) -> None:
        if self.on_warning is not None:
            self.on_warning(message)

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._thread = threading.Thread(
                target=self._run,
                name="smarter-adapter-intelligence",
                daemon=True,
            )
            self._thread.start()

    def submit(self, result, alerts: Iterable[Mapping[str, Any]]) -> tuple[bool, str]:
        if not self.enabled:
            return False, "disabled"
        item = (result, tuple(dict(alert) for alert in alerts))
        try:
            self._queue.put_nowait(item)
            self.stats.queued += 1
            return True, "queued"
        except queue.Full:
            self.stats.dropped += 1
            self._warn(
                f"intelligence queue full; dropped context for {result.parsed_event.external_device_id}"
            )
            return False, "queue-full"

    def _process(self, result, alerts: Sequence[Mapping[str, Any]]) -> None:
        external_id = result.parsed_event.external_device_id

        # Alerts do not depend on the Reader and should leave the queue first.
        if self.alert_publisher.enabled:
            for alert in alerts:
                ok, detail = self.alert_publisher.publish(alert)
                if not ok:
                    self._warn(f"alert-mqtt external={external_id} {detail}")

        if self.context_publisher.enabled:
            workspace_id, channel_id = self.scope_provider()
            history = self.history_provider.build(
                result,
                workspace_id=workspace_id,
                channel_id=channel_id,
            )
            context = self.context_builder.build(
                result,
                alerts=alerts,
                history=history,
            )
            ok, detail = self.context_publisher.publish(context, suffix=external_id)
            if not ok:
                self._warn(f"llm-context-mqtt external={external_id} {detail}")

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            try:
                if item is None:
                    return
                result, alerts = item
                try:
                    self._process(result, alerts)
                    self.stats.processed += 1
                except Exception as exc:
                    self.stats.failures += 1
                    self._warn(
                        f"intelligence external={result.parsed_event.external_device_id} "
                        f"{type(exc).__name__}: {exc}"
                    )
            finally:
                self._queue.task_done()

    def close(self, *, timeout: float = 5.0) -> None:
        with self._lock:
            thread = self._thread
            if thread is None:
                return
            # FIFO sentinel drains items already accepted before shutdown.
            try:
                self._queue.put(None, timeout=max(float(timeout), 0.1))
            except queue.Full:
                self._warn("intelligence queue did not drain before shutdown timeout")
                return
        thread.join(timeout=max(float(timeout), 0.1))
        with self._lock:
            if self._thread is thread and not thread.is_alive():
                self._thread = None


__all__ = [
    "AsyncIntelligenceSideChannel",
    "HistoricalLLMContextBuilder",
    "IntelligenceWorkerStats",
    "TimescaleHistoryProvider",
    "TrendSeries",
]
