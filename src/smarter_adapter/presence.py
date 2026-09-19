from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional


@dataclass(frozen=True)
class PresenceThresholds:
    """One cadence-aware presence profile.

    ``expected_interval_seconds`` is informational and documents the nominal
    reporting cadence. Classification is controlled by ``stale_after_seconds``
    and ``offline_after_seconds`` so a deployment can tolerate missed packets
    without changing the expected sensor cadence itself.
    """

    expected_interval_seconds: float
    stale_after_seconds: float
    offline_after_seconds: float

    def __post_init__(self) -> None:
        expected = float(self.expected_interval_seconds)
        stale = float(self.stale_after_seconds)
        offline = float(self.offline_after_seconds)
        if expected <= 0:
            raise ValueError("expected_interval_seconds must be > 0")
        if stale <= 0:
            raise ValueError("stale_after_seconds must be > 0")
        if stale < expected:
            raise ValueError("stale_after_seconds must be >= expected_interval_seconds")
        if offline <= stale:
            raise ValueError("offline_after_seconds must be greater than stale_after_seconds")

    def as_dict(self) -> dict[str, float]:
        return {
            "expected_interval_seconds": float(self.expected_interval_seconds),
            "stale_after_seconds": float(self.stale_after_seconds),
            "offline_after_seconds": float(self.offline_after_seconds),
        }


def _normalize_family(value: object) -> str:
    return str(value or "").strip().lower()


def parse_presence_profiles(raw: str = "") -> dict[str, PresenceThresholds]:
    """Parse optional per-family presence thresholds from JSON.

    Example::

        {
          "teros12": {
            "expected_interval_seconds": 600,
            "stale_after_seconds": 900,
            "offline_after_seconds": 1800
          }
        }

    Unknown keys are ignored deliberately so the configuration can grow without
    coupling this small policy object to deployment-specific metadata.
    """

    text = str(raw or "").strip()
    if not text:
        return {}
    value = json.loads(text)
    if not isinstance(value, Mapping):
        raise ValueError("device presence profiles JSON must be an object")

    result: dict[str, PresenceThresholds] = {}
    for family, item in value.items():
        key = _normalize_family(family)
        if not key or not isinstance(item, Mapping):
            continue
        try:
            expected = float(item["expected_interval_seconds"])
            stale = float(item["stale_after_seconds"])
            offline = float(item["offline_after_seconds"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(
                f"presence profile {family!r} must define numeric "
                "expected_interval_seconds, stale_after_seconds and "
                "offline_after_seconds"
            ) from exc
        result[key] = PresenceThresholds(
            expected_interval_seconds=expected,
            stale_after_seconds=stale,
            offline_after_seconds=offline,
        )
    return result


@dataclass(frozen=True)
class DevicePresencePolicy:
    """Classify managed devices from their most recent observed event.

    Presence is derived at read time instead of being persisted: a device can
    become stale/offline while Smarter Adapter receives no new events.

    ``family_thresholds`` allows deployments to describe the real reporting
    cadence of each sensor family. A device without a matching family profile
    keeps the legacy global thresholds, preserving compatibility for unknown or
    plugin-provided sensor types.
    """

    stale_after_seconds: float = 300.0
    offline_after_seconds: float = 1800.0
    expected_interval_seconds: Optional[float] = None
    family_thresholds: Mapping[str, PresenceThresholds] = field(default_factory=dict)

    def __post_init__(self) -> None:
        stale = float(self.stale_after_seconds)
        offline = float(self.offline_after_seconds)
        if stale <= 0:
            raise ValueError("stale_after_seconds must be > 0")
        if offline <= stale:
            raise ValueError("offline_after_seconds must be greater than stale_after_seconds")
        if self.expected_interval_seconds is not None:
            expected = float(self.expected_interval_seconds)
            if expected <= 0:
                raise ValueError("expected_interval_seconds must be > 0 when configured")
            if stale < expected:
                raise ValueError("stale_after_seconds must be >= expected_interval_seconds")

        normalized: dict[str, PresenceThresholds] = {}
        for family, thresholds in dict(self.family_thresholds or {}).items():
            key = _normalize_family(family)
            if not key:
                continue
            if not isinstance(thresholds, PresenceThresholds):
                if not isinstance(thresholds, Mapping):
                    raise TypeError("family_thresholds values must be PresenceThresholds or mappings")
                thresholds = PresenceThresholds(
                    expected_interval_seconds=float(thresholds["expected_interval_seconds"]),
                    stale_after_seconds=float(thresholds["stale_after_seconds"]),
                    offline_after_seconds=float(thresholds["offline_after_seconds"]),
                )
            normalized[key] = thresholds
        object.__setattr__(self, "family_thresholds", normalized)

    def _default_thresholds(self) -> dict[str, Optional[float]]:
        return {
            "expected_interval_seconds": (
                float(self.expected_interval_seconds)
                if self.expected_interval_seconds is not None
                else None
            ),
            "stale_after_seconds": float(self.stale_after_seconds),
            "offline_after_seconds": float(self.offline_after_seconds),
        }

    @staticmethod
    def _device_family(device: Mapping[str, Any]) -> str:
        family = device.get("observed_sensor")
        if not family:
            metadata = device.get("observation_metadata")
            if isinstance(metadata, Mapping):
                family = metadata.get("sensor")
        return _normalize_family(family)

    def thresholds_for_family(self, family: object = "") -> dict[str, Optional[float]]:
        key = _normalize_family(family)
        profile = self.family_thresholds.get(key)
        if profile is None:
            return self._default_thresholds()
        return profile.as_dict()

    def age_seconds(self, last_seen: float, *, now: Optional[float] = None) -> float:
        current = time.time() if now is None else float(now)
        # Clock skew or fixtures with a future timestamp must never produce a
        # negative operational age.
        return max(0.0, current - float(last_seen))

    def classify(
        self,
        last_seen: float,
        *,
        now: Optional[float] = None,
        family: object = "",
    ) -> str:
        age = self.age_seconds(last_seen, now=now)
        thresholds = self.thresholds_for_family(family)
        stale = float(thresholds["stale_after_seconds"] or self.stale_after_seconds)
        offline = float(thresholds["offline_after_seconds"] or self.offline_after_seconds)
        if age < stale:
            return "online"
        if age < offline:
            return "stale"
        return "offline"

    def decorate(self, device: dict, *, now: Optional[float] = None) -> dict:
        result = dict(device)
        last_seen = float(result.get("last_seen") or 0.0)
        family = self._device_family(result)
        age = self.age_seconds(last_seen, now=now)
        thresholds = self.thresholds_for_family(family)
        result["operational_status"] = self.classify(
            last_seen,
            now=now,
            family=family,
        )
        result["last_seen_age_seconds"] = round(age, 3)
        result["presence_profile"] = family or "default"
        result["presence_expected_interval_seconds"] = thresholds[
            "expected_interval_seconds"
        ]
        result["presence_stale_after_seconds"] = thresholds["stale_after_seconds"]
        result["presence_offline_after_seconds"] = thresholds["offline_after_seconds"]
        return result


__all__ = [
    "DevicePresencePolicy",
    "PresenceThresholds",
    "parse_presence_profiles",
]
