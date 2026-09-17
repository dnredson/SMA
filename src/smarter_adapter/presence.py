from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class DevicePresencePolicy:
    """Classify managed devices from their most recent observed event.

    Presence is intentionally derived at read time instead of being persisted:
    a device can become stale/offline even while Smarter Adapter receives no
    new events, and storing the state would therefore require an unnecessary
    background writer just to advance clocks.
    """

    stale_after_seconds: float = 300.0
    offline_after_seconds: float = 1800.0

    def __post_init__(self) -> None:
        stale = float(self.stale_after_seconds)
        offline = float(self.offline_after_seconds)
        if stale <= 0:
            raise ValueError("stale_after_seconds must be > 0")
        if offline <= stale:
            raise ValueError("offline_after_seconds must be greater than stale_after_seconds")

    def age_seconds(self, last_seen: float, *, now: Optional[float] = None) -> float:
        current = time.time() if now is None else float(now)
        # Clock skew or fixtures with a future timestamp must never produce a
        # negative operational age.
        return max(0.0, current - float(last_seen))

    def classify(self, last_seen: float, *, now: Optional[float] = None) -> str:
        age = self.age_seconds(last_seen, now=now)
        if age < float(self.stale_after_seconds):
            return "online"
        if age < float(self.offline_after_seconds):
            return "stale"
        return "offline"

    def decorate(self, device: dict, *, now: Optional[float] = None) -> dict:
        result = dict(device)
        last_seen = float(result.get("last_seen") or 0.0)
        age = self.age_seconds(last_seen, now=now)
        result["operational_status"] = self.classify(last_seen, now=now)
        result["last_seen_age_seconds"] = round(age, 3)
        return result


__all__ = ["DevicePresencePolicy"]
