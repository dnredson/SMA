from __future__ import annotations
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class ClientEntry:
    device_raw_topic: str
    external_id: str
    client_id: str
    client_secret: Dict[str, Any]  # cifrado
    domain_id: str
    channel_ids: List[str]
    active: bool
    created_at: str
    updated_at: str
    last_seen: Optional[str]


class EntitiesStore:
    def __init__(
        self, path: Path, key_manager
    ) -> None:  # key_manager reservado p/ desencriptar no futuro
        self.path = path
        self.data: Dict[str, Any] = {
            "schema_version": 1,
            "last_updated": "1970-01-01T00:00:00Z",
            "clients": [],
        }
        if path.exists():
            self.data = json.loads(path.read_text("utf-8"))

    def _write_atomic(self) -> None:
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(
            json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        tmp.replace(self.path)

    def get_by_external_id(self, external_id: str) -> Optional[Dict[str, Any]]:
        for c in self.data.get("clients", []):
            if c.get("external_id") == external_id:
                return c
        return None

    def upsert_client(self, entry: Dict[str, Any]) -> None:
        existing = self.get_by_external_id(entry["external_id"])
        if existing:
            existing.update(entry)
        else:
            self.data["clients"].append(entry)
        self._write_atomic()

    def touch_last_seen(self, external_id: str) -> None:
        import datetime as dt

        c = self.get_by_external_id(external_id)
        if c:
            c["last_seen"] = (
                dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
            )
            self._write_atomic()
