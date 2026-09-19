from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

from .models import ParsedEvent


@dataclass(frozen=True)
class DeviceProfileSpec:
    family: str
    key: str
    name: str
    description: str
    json_schema: Dict[str, Any]


def _family_schema(family: str) -> Dict[str, Any]:
    """Attribute schema for one physical sensor family.

    The schema intentionally validates identity/deployment metadata only. The
    telemetry payload itself is normalized to SenML and is not stored in Atom
    entity attributes.
    """
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": True,
        "properties": {
            "managed_by": {"type": "string"},
            "sensor": {"type": "string", "const": family},
            "node_id": {"type": "string"},
            "location": {"type": "string"},
            "sub_location": {"type": "string"},
            "depth": {"type": "string"},
            "application_id": {"type": "string"},
        },
    }


DEFAULT_DEVICE_PROFILES: Mapping[str, DeviceProfileSpec] = {
    "teros12": DeviceProfileSpec(
        family="teros12",
        key="smarter-adapter-teros12",
        name="Teros 12 Soil Sensor",
        description=(
            "Teros 12 soil sensor managed by Smarter Adapter 2.0. "
            "Telemetry is normalized to SenML; Atom attributes describe the deployment."
        ),
        json_schema=_family_schema("teros12"),
    ),
    "greenstick": DeviceProfileSpec(
        family="greenstick",
        key="smarter-adapter-greenstick",
        name="Greenstick Soil Sensor",
        description=(
            "Greenstick soil sensor managed by Smarter Adapter 2.0. "
            "Telemetry is normalized to SenML; Atom attributes describe the deployment."
        ),
        json_schema=_family_schema("greenstick"),
    ),
}


def normalize_sensor_family(value: object) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text


class DeviceProfileRegistry:
    """Resolve parsed events to typed Atom device profiles.

    Unknown sensor families deliberately return ``None`` so the runtime keeps
    the existing generic profile as a safe fallback.
    """

    def __init__(
        self,
        profiles: Mapping[str, DeviceProfileSpec] = DEFAULT_DEVICE_PROFILES,
    ) -> None:
        self._profiles = {
            normalize_sensor_family(key): value for key, value in profiles.items()
        }

    @property
    def families(self) -> tuple[str, ...]:
        return tuple(sorted(self._profiles))

    def get(self, family: object) -> Optional[DeviceProfileSpec]:
        return self._profiles.get(normalize_sensor_family(family))

    def resolve(self, event: ParsedEvent) -> Optional[DeviceProfileSpec]:
        return self.get(event.metadata.get("sensor"))


__all__ = [
    "DEFAULT_DEVICE_PROFILES",
    "DeviceProfileRegistry",
    "DeviceProfileSpec",
    "normalize_sensor_family",
]
