from __future__ import annotations

import json
import sqlite3
import threading
import time
from typing import Any, Callable, Mapping, Optional

from .irrigap_config import IrrigapCatalogManager, irrigap_node_to_dict
from .storage.management import SQLiteManagementStore


BindingResolver = Callable[[str], Optional[Mapping[str, Any]]]


class BindingSQLiteManagementStore(SQLiteManagementStore):
    """Management store with durable node-id to managed-device observations."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        with self._lock, self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS managed_device_observations (
                    workspace_id TEXT NOT NULL,
                    channel_id TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    sensor TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    observed_at REAL NOT NULL,
                    PRIMARY KEY (workspace_id, channel_id, external_id),
                    FOREIGN KEY (workspace_id, channel_id, external_id)
                        REFERENCES managed_devices(workspace_id, channel_id, external_id)
                        ON DELETE CASCADE
                )
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_managed_device_observations_node
                ON managed_device_observations(workspace_id, channel_id, node_id, observed_at)
                """
            )

    @staticmethod
    def _observation_public(row: Optional[sqlite3.Row]) -> dict[str, Any]:
        if row is None:
            return {
                "node_id": None,
                "observed_sensor": None,
                "binding_observed_at": None,
                "observation_metadata": {},
            }
        try:
            metadata = json.loads(str(row["metadata_json"] or "{}"))
        except json.JSONDecodeError:
            metadata = {}
        if not isinstance(metadata, dict):
            metadata = {}
        return {
            "node_id": str(row["node_id"]),
            "observed_sensor": str(row["sensor"] or ""),
            "binding_observed_at": float(row["observed_at"]),
            "observation_metadata": metadata,
        }

    def _observation_for_device(
        self,
        workspace_id: str,
        channel_id: str,
        external_id: str,
    ) -> Optional[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                """
                SELECT node_id, sensor, metadata_json, observed_at
                FROM managed_device_observations
                WHERE workspace_id = ? AND channel_id = ? AND external_id = ?
                """,
                (str(workspace_id), str(channel_id), str(external_id)),
            ).fetchone()

    def _decorate_observation(self, item: Optional[dict]) -> Optional[dict]:
        if item is None:
            return None
        result = dict(item)
        result.update(
            self._observation_public(
                self._observation_for_device(
                    str(result.get("workspace_id") or ""),
                    str(result.get("channel_id") or ""),
                    str(result.get("external_id") or ""),
                )
            )
        )
        return result

    def set_device_observation(
        self,
        workspace_id: str,
        channel_id: str,
        external_id: str,
        *,
        node_id: str,
        sensor: str = "",
        metadata: Optional[Mapping[str, object]] = None,
        observed_at: Optional[float] = None,
    ) -> None:
        key = str(node_id or "").strip().upper()
        if not key:
            raise ValueError("node_id must not be empty")
        seen = float(time.time() if observed_at is None else observed_at)
        body = dict(metadata or {})
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO managed_device_observations (
                    workspace_id, channel_id, external_id, node_id,
                    sensor, metadata_json, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(workspace_id, channel_id, external_id) DO UPDATE SET
                    node_id = excluded.node_id,
                    sensor = excluded.sensor,
                    metadata_json = excluded.metadata_json,
                    observed_at = excluded.observed_at
                """,
                (
                    str(workspace_id),
                    str(channel_id),
                    str(external_id),
                    key,
                    str(sensor or ""),
                    json.dumps(body, ensure_ascii=False, separators=(",", ":")),
                    seen,
                ),
            )

    def list_devices(self, **kwargs):
        items = super().list_devices(**kwargs)
        return [self._decorate_observation(item) for item in items]

    def find_device(self, workspace_id: str, channel_id: str, external_id: str):
        return self._decorate_observation(
            super().find_device(workspace_id, channel_id, external_id)
        )

    def find_latest_device_by_node(
        self,
        workspace_id: str,
        channel_id: str,
        node_id: str,
    ):
        key = str(node_id or "").strip().upper()
        if not key:
            return None
        with self._lock:
            row = self._conn.execute(
                """
                SELECT o.external_id
                FROM managed_device_observations o
                JOIN managed_devices d ON
                     d.workspace_id = o.workspace_id
                 AND d.channel_id = o.channel_id
                 AND d.external_id = o.external_id
                WHERE o.workspace_id = ? AND o.channel_id = ? AND o.node_id = ?
                ORDER BY d.last_seen DESC, o.observed_at DESC, o.external_id ASC
                LIMIT 1
                """,
                (str(workspace_id), str(channel_id), key),
            ).fetchone()
        if row is None:
            return None
        return self.find_device(workspace_id, channel_id, str(row["external_id"]))


class BoundIrrigapCatalogManager(IrrigapCatalogManager):
    """Live Irrigap catalog whose list view can include observed-device state."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._binding_lock = threading.RLock()
        self._binding_resolver: Optional[BindingResolver] = None

    def set_binding_resolver(self, resolver: Optional[BindingResolver]) -> None:
        with self._binding_lock:
            self._binding_resolver = resolver

    def _binding(self, node_id: str) -> Optional[Mapping[str, Any]]:
        with self._binding_lock:
            resolver = self._binding_resolver
        if resolver is None:
            return None
        return resolver(node_id)

    def public_payload(self) -> dict[str, Any]:
        nodes = self.list_nodes()
        items = []
        managed = 0
        for node in nodes:
            item = irrigap_node_to_dict(node)
            binding = self._binding(node.id)
            if binding:
                item.update(dict(binding))
                item["managed"] = True
                managed += 1
            else:
                item.update(
                    {
                        "managed": False,
                        "external_id": None,
                        "atom_device_id": None,
                        "operational_status": None,
                        "last_seen": None,
                        "last_seen_age_seconds": None,
                        "data_quality": "unknown",
                        "invalid_fields": [],
                        "binding_observed_at": None,
                    }
                )
            items.append(item)
        return {
            "source": self.source,
            "writable": self.writable,
            "total": len(nodes),
            "managed": managed,
            "unbound": len(nodes) - managed,
            "items": items,
        }


__all__ = [
    "BindingSQLiteManagementStore",
    "BoundIrrigapCatalogManager",
]
