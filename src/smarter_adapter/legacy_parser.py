from __future__ import annotations

from typing import Any, Dict, Optional

from parsers import detect_and_parse

from .models import Measurement, ParsedEvent, RawEvent


class LegacySensorParser:
    """Compatibility adapter for the sensor parsers already validated in v1.

    This is deliberately a bridge, not the final parser architecture. It lets
    v2 preserve current WXT520/ATMOS41/TEROS12/Greenstick behavior while the
    rest of the pipeline is replaced safely.
    """

    name = "legacy-sensor-parsers"

    def supports(self, event: RawEvent) -> bool:
        # The legacy router itself decides whether a topic/payload is supported.
        return bool(event.topic)

    @staticmethod
    def _measurement(entry: Dict[str, Any]) -> Optional[Measurement]:
        name = str(entry.get("n") or "").strip()
        if not name:
            return None

        value: Any
        if "v" in entry:
            value = entry["v"]
        elif "vs" in entry:
            value = entry["vs"]
        elif "vb" in entry:
            value = entry["vb"]
        else:
            return None

        reserved = {"n", "u", "v", "vs", "vb", "t", "bn", "bt"}
        metadata = {key: value for key, value in entry.items() if key not in reserved}
        return Measurement(
            name=name,
            value=value,
            unit=entry.get("u"),
            timestamp=entry.get("t"),
            metadata=metadata,
        )

    def parse(self, event: RawEvent) -> Optional[ParsedEvent]:
        result = detect_and_parse(event.topic, event.payload)
        if not result.accept:
            return None

        measurements = tuple(
            measurement
            for entry in (result.entries or [])
            if (measurement := self._measurement(entry)) is not None
        )
        metadata = dict(result.meta or {})
        metadata.setdefault("source", event.source)
        metadata.setdefault("topic", event.topic)

        return ParsedEvent(
            external_device_id=result.external_id,
            measurements=measurements,
            metadata=metadata,
        )


__all__ = ["LegacySensorParser"]
