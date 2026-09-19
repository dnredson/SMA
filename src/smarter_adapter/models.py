from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple


@dataclass(frozen=True)
class RawEvent:
    """Transport-neutral event emitted by an input plugin."""

    source: str
    payload: bytes
    topic: str = ""
    received_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.source.strip():
            raise ValueError("RawEvent.source must not be empty")
        if not isinstance(self.payload, bytes):
            raise TypeError("RawEvent.payload must be bytes")


@dataclass(frozen=True)
class Measurement:
    """One normalized sensor measurement before SenML serialization."""

    name: str
    value: Any
    unit: Optional[str] = None
    timestamp: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("Measurement.name must not be empty")


@dataclass(frozen=True)
class ParsedEvent:
    """Canonical result produced by parser plugins."""

    external_device_id: str
    measurements: Tuple[Measurement, ...]
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.external_device_id.strip():
            raise ValueError("ParsedEvent.external_device_id must not be empty")


__all__ = ["Measurement", "ParsedEvent", "RawEvent"]
