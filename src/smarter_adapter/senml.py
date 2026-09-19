from __future__ import annotations

import base64
import time
from typing import Any, Dict, List, Optional

from .models import Measurement, ParsedEvent


def _value_field(value: Any) -> Dict[str, Any]:
    """Map a Python value to exactly one SenML value field."""
    if isinstance(value, bool):
        return {"vb": value}
    if isinstance(value, (int, float)):
        return {"v": value}
    if isinstance(value, bytes):
        return {"vd": base64.b64encode(value).decode("ascii")}
    if isinstance(value, str):
        return {"vs": value}
    if value is None:
        raise ValueError("SenML measurement value must not be None")
    return {"vs": str(value)}


def event_to_senml(
    event: ParsedEvent,
    *,
    default_timestamp: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """Convert one canonical ParsedEvent to a SenML JSON pack.

    The Smarter Adapter v2 boundary deliberately stores the device serial in
    ``bn`` *without* a separator. Magistrala's current SenML transformer keeps
    that base name verbatim as ``device_id`` in Timescale. Measurement names
    therefore carry the leading ``:`` separator, producing normalized names
    such as ``sensor-01:soil.moisture`` while preserving ``device_id`` as
    exactly ``sensor-01``.
    """

    measurements = list(event.measurements)
    timestamps = [float(item.timestamp) for item in measurements if item.timestamp is not None]
    if timestamps:
        base_time = timestamps[0]
    elif default_timestamp is not None:
        base_time = float(default_timestamp)
    else:
        base_time = time.time()

    if not measurements:
        return [{"bn": event.external_device_id, "bt": base_time}]

    pack: List[Dict[str, Any]] = []
    for measurement in measurements:
        name = measurement.name.lstrip(":")
        record: Dict[str, Any] = {"n": ":" + name}
        if measurement.unit:
            record["u"] = measurement.unit
        record.update(_value_field(measurement.value))

        if measurement.timestamp is not None:
            relative = float(measurement.timestamp) - base_time
            if abs(relative) > 1e-9:
                record["t"] = relative
        pack.append(record)

    pack[0]["bn"] = event.external_device_id
    pack[0]["bt"] = base_time
    return pack


__all__ = ["event_to_senml"]
