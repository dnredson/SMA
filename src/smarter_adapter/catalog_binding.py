from __future__ import annotations

import json
import sqlite3
import threading
import time
from typing import Any, Callable, Mapping, Optional

from .irrigap_config import IrrigapCatalogManager, IrrigapNode, irrigap_node_to_dict
from .storage.management import SQLiteManagementStore


BindingResolver = Callable[[str], Optional[Mapping[str, Any]]]
ObservationResolver = Callable[[str], Optional[Mapping[str, Any]]]


class BindingSQLiteManagementStore(SQLiteManagementStore):
    """Management store with durable physical-node observations and bindings.

    ``catalog_node_observations`` is intentionally independent from
    ``managed_devices``. It records that the adapter successfully parsed a
    physical node even when later control-plane reconciliation or publication
    fails. ``managed_device_observations`` remains the stronger binding between
    an observed node and a device that the adapter has successfully managed.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        with self._lock, self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS catalog_node_observations (
                    workspace_id TEXT NOT NULL,
                    channel_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    sensor TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    first_observed_at REAL NOT NULL,
                    last_observed_at REAL NOT NULL,
                    PRIMARY KEY (workspace_id, channel_id, node_id, external_id)
                )
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_catalog_node_observations_latest
                ON catalog_node_observations(
                    workspace_id, channel_id, node_id, last_observed_at
                )
                """
            )
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
    def _metadata(value: object) -> dict[str, Any]:
        try:
            result = json.loads(str(value or "{}"))
        except json.JSONDecodeError:
            result = {}
        return result if isinstance(result, dict) else {}

    @classmethod
    def _observation_public(cls, row: Optional[sqlite3.Row]) -> dict[str, Any]:
        if row is None:
            return {
                "node_id": None,
                "observed_sensor": None,
                "binding_observed_at": None,
                "observation_metadata": {},
            }
        return {
            "node_id": str(row["node_id"]),
            "observed_sensor": str(row["sensor"] or ""),
            "binding_observed_at": float(row["observed_at"]),
            "observation_metadata": cls._metadata(row["metadata_json"]),
        }

    @classmethod
    def _catalog_observation_public(cls, row: Optional[sqlite3.Row]) -> Optional[dict[str, Any]]:
        if row is None:
            return None
        return {
            "node_id": str(row["node_id"]),
            "observed_external_id": str(row["external_id"]),
            "observed_sensor": str(row["sensor"] or ""),
            "first_observed_at": float(row["first_observed_at"]),
            "last_observed_at": float(row["last_observed_at"]),
            "observation_metadata": cls._metadata(row["metadata_json"]),
        }

    def observe_catalog_node(
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
        """Record a parsed physical-node observation before remote management.

        This table has no foreign key to ``managed_devices`` on purpose: an
        observation must survive even when Atom/device reconciliation fails.
        """
        key = str(node_id or "").strip().upper()
        external = str(external_id or "").strip()
        if not key:
            raise ValueError("node_id must not be empty")
        if not external:
            raise ValueError("external_id must not be empty")
        seen = float(time.time() if observed_at is None else observed_at)
        body = dict(metadata or {})
        encoded = json.dumps(body, ensure_ascii=False, separators=(",", ":"))
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO catalog_node_observations (
                    workspace_id, channel_id, node_id, external_id,
                    sensor, metadata_json, first_observed_at, last_observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(workspace_id, channel_id, node_id, external_id) DO UPDATE SET
                    sensor = CASE
                        WHEN excluded.last_observed_at >= catalog_node_observations.last_observed_at
                        THEN excluded.sensor ELSE catalog_node_observations.sensor END,
                    metadata_json = CASE
                        WHEN excluded.last_observed_at >= catalog_node_observations.last_observed_at
                        THEN excluded.metadata_json ELSE catalog_node_observations.metadata_json END,
                    last_observed_at = MAX(
                        catalog_node_observations.last_observed_at,
                        excluded.last_observed_at
                    )
                """,
                (
                    str(workspace_id),
                    str(channel_id),
                    key,
                    external,
                    str(sensor or ""),
                    encoded,
                    seen,
                    seen,
                ),
            )

    def find_latest_catalog_observation_by_node(
        self,
        workspace_id: str,
        channel_id: str,
        node_id: str,
    ) -> Optional[dict[str, Any]]:
        key = str(node_id or "").strip().upper()
        if not key:
            return None
        with self._lock:
            row = self._conn.execute(
                """
                SELECT node_id, external_id, sensor, metadata_json,
                       first_observed_at, last_observed_at
                FROM catalog_node_observations
                WHERE workspace_id = ? AND channel_id = ? AND node_id = ?
                ORDER BY last_observed_at DESC, external_id ASC
                LIMIT 1
                """,
                (str(workspace_id), str(channel_id), key),
            ).fetchone()
        return self._catalog_observation_public(row)

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

        self.observe_catalog_node(
            workspace_id,
            channel_id,
            external_id,
            node_id=key,
            sensor=sensor,
            metadata=body,
            observed_at=seen,
        )

        encoded = json.dumps(body, ensure_ascii=False, separators=(",", ":"))
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO managed_device_observations (
                    workspace_id, channel_id, external_id, node_id,
                    sensor, metadata_json, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(workspace_id, channel_id, external_id) DO UPDATE SET
                    node_id = CASE
                        WHEN excluded.observed_at >= managed_device_observations.observed_at
                        THEN excluded.node_id ELSE managed_device_observations.node_id END,
                    sensor = CASE
                        WHEN excluded.observed_at >= managed_device_observations.observed_at
                        THEN excluded.sensor ELSE managed_device_observations.sensor END,
                    metadata_json = CASE
                        WHEN excluded.observed_at >= managed_device_observations.observed_at
                        THEN excluded.metadata_json ELSE managed_device_observations.metadata_json END,
                    observed_at = MAX(
                        managed_device_observations.observed_at,
                        excluded.observed_at
                    )
                """,
                (
                    str(workspace_id),
                    str(channel_id),
                    str(external_id),
                    key,
                    str(sensor or ""),
                    encoded,
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
    """Live catalog enriched with physical observation and managed state."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._binding_lock = threading.RLock()
        self._binding_resolver: Optional[BindingResolver] = None
        self._observation_resolver: Optional[ObservationResolver] = None

    def set_binding_resolver(self, resolver: Optional[BindingResolver]) -> None:
        with self._binding_lock:
            self._binding_resolver = resolver

    def set_observation_resolver(self, resolver: Optional[ObservationResolver]) -> None:
        with self._binding_lock:
            self._observation_resolver = resolver

    def _binding(self, node_id: str) -> Optional[Mapping[str, Any]]:
        with self._binding_lock:
            resolver = self._binding_resolver
        return None if resolver is None else resolver(node_id)

    def _observation(self, node_id: str) -> Optional[Mapping[str, Any]]:
        with self._binding_lock:
            resolver = self._observation_resolver
        return None if resolver is None else resolver(node_id)

    def _public_item(self, node: IrrigapNode) -> dict[str, Any]:
        item = irrigap_node_to_dict(node)
        observation = self._observation(node.id)
        binding = self._binding(node.id)

        if observation:
            item.update(
                {
                    "observed": True,
                    "observed_external_id": observation.get("observed_external_id"),
                    "observed_sensor": observation.get("observed_sensor"),
                    "first_observed_at": observation.get("first_observed_at"),
                    "last_observed_at": observation.get("last_observed_at"),
                }
            )
            if observation.get("observation_metadata"):
                item["observation_metadata"] = dict(
                    observation.get("observation_metadata") or {}
                )
        else:
            item.update(
                {
                    "observed": False,
                    "observed_external_id": None,
                    "observed_sensor": None,
                    "first_observed_at": None,
                    "last_observed_at": None,
                }
            )

        if binding:
            item.update(dict(binding))
            item["managed"] = True
            item["lifecycle_state"] = "managed"
            item["observed"] = True
            if item.get("observed_external_id") is None:
                item["observed_external_id"] = binding.get("external_id")
            if item.get("last_observed_at") is None:
                item["last_observed_at"] = binding.get("binding_observed_at")
            if item.get("first_observed_at") is None:
                item["first_observed_at"] = binding.get("binding_observed_at")
        elif observation:
            item.update(
                {
                    "managed": False,
                    "lifecycle_state": "observed",
                    "external_id": observation.get("observed_external_id"),
                    "atom_device_id": None,
                    "operational_status": None,
                    "last_seen": None,
                    "last_seen_age_seconds": None,
                    "data_quality": "unknown",
                    "invalid_fields": [],
                    "binding_observed_at": None,
                }
            )
        else:
            item.update(
                {
                    "managed": False,
                    "lifecycle_state": "planned",
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
        return item

    def public_item(self, node_id: str) -> Optional[dict[str, Any]]:
        node = self.get_node(node_id)
        return None if node is None else self._public_item(node)

    def public_payload(self) -> dict[str, Any]:
        nodes = self.list_nodes()
        items = [self._public_item(node) for node in nodes]
        managed = sum(item["lifecycle_state"] == "managed" for item in items)
        observed = sum(item["lifecycle_state"] == "observed" for item in items)
        planned = sum(item["lifecycle_state"] == "planned" for item in items)
        return {
            "source": self.source,
            "writable": self.writable,
            "total": len(nodes),
            "managed": managed,
            "observed": observed,
            "planned": planned,
            "unbound": len(nodes) - managed,
            "items": items,
        }


__all__ = [
    "BindingSQLiteManagementStore",
    "BoundIrrigapCatalogManager",
]
