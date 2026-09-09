from __future__ import annotations
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class DeviceEntry:
    device_id: str
    external_id: str
    name: str
    tenant_id: str
    channel_ids: List[str]
    active: bool
    created_at: str
    updated_at: str
    last_seen: Optional[str]


# Compatibility alias for code that imported the old type name. Its fields are
# now Atom device fields and deliberately contain no client secret.
ClientEntry = DeviceEntry


class EntitiesStore:
    def __init__(
        self, path: Path, key_manager
    ) -> None:  # key_manager reservado p/ desencriptar no futuro
        self.path = path
        self.data: Dict[str, Any] = {
            "schema_version": 2,
            "last_updated": "1970-01-01T00:00:00Z",
            "devices": [],
        }
        if path.exists():
            self.data = json.loads(path.read_text("utf-8"))
        if not isinstance(self.data.get("devices"), list):
            self.data["devices"] = []

    def _write_atomic(self) -> None:
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(
            json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        tmp.replace(self.path)

    def get_by_external_id(self, external_id: str) -> Optional[Dict[str, Any]]:
        for device in self.data.get("devices", []):
            if device.get("external_id") == external_id:
                return device
        return None

    def list_devices(self) -> List[Dict[str, Any]]:
        return [dict(device) for device in self.data.get("devices", [])]

    def upsert_device(self, entry: Dict[str, Any]) -> None:
        existing = self.get_by_external_id(entry["external_id"])
        if existing:
            existing.update(entry)
        else:
            self.data["devices"].append(entry)
        self._write_atomic()

    def remove_device(self, external_id: str) -> None:
        self.data["devices"] = [
            d for d in self.data.get("devices", []) if d.get("external_id") != external_id
        ]
        self._write_atomic()

    # Kept as a migration-friendly alias for callers outside the adapter.
    def upsert_client(self, entry: Dict[str, Any]) -> None:
        self.upsert_device(entry)

    def touch_last_seen(self, external_id: str) -> None:
        import datetime as dt

        device = self.get_by_external_id(external_id)
        if device:
            device["last_seen"] = (
                dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
            )
            self._write_atomic()
