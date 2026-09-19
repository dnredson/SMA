from __future__ import annotations

import sqlite3
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional

from .catalog_binding import BindingSQLiteManagementStore, BoundIrrigapCatalogManager
from .runtime import SmarterAdapterRuntime


LifecycleResolver = Callable[[str], Optional[Mapping[str, Any]]]


class DecommissionedEventSuppressed(RuntimeError):
    """Administrative suppression, not a delivery failure."""

    def __init__(self, *, node_id: str, external_id: str) -> None:
        self.node_id = str(node_id)
        self.external_id = str(external_id)
        super().__init__(
            f"device node {self.node_id!r} ({self.external_id!r}) is decommissioned"
        )


class LifecycleRemoteError(RuntimeError):
    """Remote Atom lifecycle action failed after local safety state was applied."""

    def __init__(self, message: str, *, node_id: str, local_state: str) -> None:
        self.node_id = str(node_id)
        self.local_state = str(local_state)
        super().__init__(message)


class LifecycleBindingSQLiteManagementStore(BindingSQLiteManagementStore):
    """Binding store with durable administrative lifecycle overrides."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        with self._lock, self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS catalog_node_lifecycle (
                    workspace_id TEXT NOT NULL,
                    channel_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    decommissioned_at REAL,
                    reactivated_at REAL,
                    reason TEXT NOT NULL DEFAULT '',
                    atom_publish_policy_revoked INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (workspace_id, channel_id, node_id)
                )
                """
            )

    @staticmethod
    def _lifecycle_public(row: Optional[sqlite3.Row]) -> Optional[dict[str, Any]]:
        if row is None:
            return None
        return {
            "administrative_state": str(row["state"] or "active"),
            "decommissioned_at": (
                float(row["decommissioned_at"])
                if row["decommissioned_at"] is not None
                else None
            ),
            "reactivated_at": (
                float(row["reactivated_at"])
                if row["reactivated_at"] is not None
                else None
            ),
            "decommission_reason": str(row["reason"] or ""),
            "atom_publish_policy_revoked": bool(row["atom_publish_policy_revoked"]),
            "lifecycle_error": str(row["last_error"] or ""),
            "lifecycle_updated_at": float(row["updated_at"]),
        }

    def get_node_lifecycle(
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
                SELECT state, decommissioned_at, reactivated_at, reason,
                       atom_publish_policy_revoked, last_error, updated_at
                FROM catalog_node_lifecycle
                WHERE workspace_id = ? AND channel_id = ? AND node_id = ?
                """,
                (str(workspace_id), str(channel_id), key),
            ).fetchone()
        return self._lifecycle_public(row)

    def is_node_decommissioned(
        self,
        workspace_id: str,
        channel_id: str,
        node_id: str,
    ) -> bool:
        state = self.get_node_lifecycle(workspace_id, channel_id, node_id)
        return bool(state and state["administrative_state"] == "decommissioned")

    def decommission_node(
        self,
        workspace_id: str,
        channel_id: str,
        node_id: str,
        *,
        reason: str = "",
        at: Optional[float] = None,
    ) -> dict[str, Any]:
        key = str(node_id or "").strip().upper()
        if not key:
            raise ValueError("node_id must not be empty")
        now = float(time.time() if at is None else at)
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO catalog_node_lifecycle (
                    workspace_id, channel_id, node_id, state,
                    decommissioned_at, reactivated_at, reason,
                    atom_publish_policy_revoked, last_error, updated_at
                ) VALUES (?, ?, ?, 'decommissioned', ?, NULL, ?, 0, '', ?)
                ON CONFLICT(workspace_id, channel_id, node_id) DO UPDATE SET
                    state = 'decommissioned',
                    decommissioned_at = CASE
                        WHEN catalog_node_lifecycle.state = 'decommissioned'
                        THEN catalog_node_lifecycle.decommissioned_at
                        ELSE excluded.decommissioned_at
                    END,
                    reason = CASE
                        WHEN excluded.reason <> '' THEN excluded.reason
                        ELSE catalog_node_lifecycle.reason
                    END,
                    atom_publish_policy_revoked = 0,
                    last_error = '',
                    updated_at = excluded.updated_at
                """,
                (
                    str(workspace_id),
                    str(channel_id),
                    key,
                    now,
                    str(reason or "").strip(),
                    now,
                ),
            )
        result = self.get_node_lifecycle(workspace_id, channel_id, key)
        assert result is not None
        return result

    def record_decommission_policy_result(
        self,
        workspace_id: str,
        channel_id: str,
        node_id: str,
        *,
        revoked: bool,
        error: str = "",
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE catalog_node_lifecycle
                SET atom_publish_policy_revoked = ?, last_error = ?, updated_at = ?
                WHERE workspace_id = ? AND channel_id = ? AND node_id = ?
                """,
                (
                    1 if revoked else 0,
                    str(error or ""),
                    time.time(),
                    str(workspace_id),
                    str(channel_id),
                    str(node_id or "").strip().upper(),
                ),
            )

    def reactivate_node(
        self,
        workspace_id: str,
        channel_id: str,
        node_id: str,
        *,
        at: Optional[float] = None,
    ) -> dict[str, Any]:
        key = str(node_id or "").strip().upper()
        if not key:
            raise ValueError("node_id must not be empty")
        now = float(time.time() if at is None else at)
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO catalog_node_lifecycle (
                    workspace_id, channel_id, node_id, state,
                    decommissioned_at, reactivated_at, reason,
                    atom_publish_policy_revoked, last_error, updated_at
                ) VALUES (?, ?, ?, 'active', NULL, ?, '', 0, '', ?)
                ON CONFLICT(workspace_id, channel_id, node_id) DO UPDATE SET
                    state = 'active',
                    reactivated_at = excluded.reactivated_at,
                    atom_publish_policy_revoked = 0,
                    last_error = '',
                    updated_at = excluded.updated_at
                """,
                (str(workspace_id), str(channel_id), key, now, now),
            )
        result = self.get_node_lifecycle(workspace_id, channel_id, key)
        assert result is not None
        return result


class LifecycleIrrigapCatalogManager(BoundIrrigapCatalogManager):
    """Catalog view that overlays administrative decommission state."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._lifecycle_lock = threading.RLock()
        self._lifecycle_resolver: Optional[LifecycleResolver] = None

    def set_lifecycle_resolver(self, resolver: Optional[LifecycleResolver]) -> None:
        with self._lifecycle_lock:
            self._lifecycle_resolver = resolver

    def _lifecycle(self, node_id: str) -> Optional[Mapping[str, Any]]:
        with self._lifecycle_lock:
            resolver = self._lifecycle_resolver
        return None if resolver is None else resolver(node_id)

    def _public_item(self, node):
        item = super()._public_item(node)
        lifecycle = self._lifecycle(node.id)
        if lifecycle:
            item.update(dict(lifecycle))
        else:
            item.update(
                {
                    "administrative_state": "active",
                    "decommissioned_at": None,
                    "reactivated_at": None,
                    "decommission_reason": "",
                    "atom_publish_policy_revoked": False,
                    "lifecycle_error": "",
                    "lifecycle_updated_at": None,
                }
            )

        if item["administrative_state"] == "decommissioned":
            previous = str(item.get("lifecycle_state") or "planned")
            item["historical_lifecycle_state"] = previous
            item["was_managed"] = bool(item.get("managed", False))
            item["managed"] = False
            item["lifecycle_state"] = "decommissioned"
            item["decommissioned"] = True
        else:
            item["historical_lifecycle_state"] = None
            item["was_managed"] = False
            item["decommissioned"] = False
        return item

    def public_payload(self) -> dict[str, Any]:
        nodes = self.list_nodes()
        items = [self._public_item(node) for node in nodes]
        counts = {
            state: sum(item["lifecycle_state"] == state for item in items)
            for state in ("planned", "observed", "managed", "decommissioned")
        }
        return {
            "source": self.source,
            "writable": self.writable,
            "total": len(nodes),
            **counts,
            "active": len(nodes) - counts["decommissioned"],
            "unbound": len(nodes) - counts["managed"],
            "items": items,
        }


class LifecycleSmarterAdapterRuntime(SmarterAdapterRuntime):
    """Runtime that suppresses administratively decommissioned nodes."""

    @property
    def lifecycle_lock(self):
        return self._control_lock

    def evict_device(self, external_id: str) -> bool:
        with self._control_lock:
            return self._devices.pop(str(external_id), None) is not None

    def _assert_node_active(self, base, parsed) -> None:
        store = self.state_store
        node_id = str(parsed.metadata.get("node_id") or "").strip()
        checker = getattr(store, "is_node_decommissioned", None) if store is not None else None
        if node_id and callable(checker) and checker(
            base.workspace.id,
            base.channel.id,
            node_id,
        ):
            raise DecommissionedEventSuppressed(
                node_id=node_id,
                external_id=parsed.external_device_id,
            )

    def _record_catalog_observation(self, base, parsed, raw) -> None:
        self._assert_node_active(base, parsed)
        super()._record_catalog_observation(base, parsed, raw)

    def _remote_device(self, base, device_type, parsed, raw):
        # Re-check here as well: a message may have passed the initial lifecycle
        # gate immediately before an administrator decommissioned the node. If
        # its publish then receives 403 and attempts reconciliation, this guard
        # prevents recreating the revoked publish policy.
        self._assert_node_active(base, parsed)
        return super()._remote_device(base, device_type, parsed, raw)


@dataclass
class DeviceLifecycleController:
    runtime: LifecycleSmarterAdapterRuntime
    store: LifecycleBindingSQLiteManagementStore
    catalog: LifecycleIrrigapCatalogManager
    atom: Any

    def _base(self):
        self.runtime.bootstrap()
        if self.runtime.base is None:
            raise RuntimeError("runtime is not bootstrapped")
        return self.runtime.base

    def _require_node(self, node_id: str) -> str:
        key = str(node_id or "").strip().upper()
        if not key or self.catalog.get_node(key) is None:
            raise KeyError(f"catalog node {key!r} not found")
        return key

    def decommission(self, node_id: str, *, reason: str = "") -> dict[str, Any]:
        key = self._require_node(node_id)
        base = self._base()
        with self.runtime.lifecycle_lock:
            binding = self.store.find_latest_device_by_node(
                base.workspace.id,
                base.channel.id,
                key,
            )
            self.store.decommission_node(
                base.workspace.id,
                base.channel.id,
                key,
                reason=reason,
            )
            if binding is not None:
                self.runtime.evict_device(str(binding["external_id"]))
            try:
                if binding is None:
                    revoked_count = 0
                else:
                    revoked_count = int(
                        self.atom.revoke_publish_policy(
                            base.workspace.id,
                            str(binding["atom_device_id"]),
                            base.channel.id,
                        )
                    )
                self.store.record_decommission_policy_result(
                    base.workspace.id,
                    base.channel.id,
                    key,
                    revoked=True,
                )
            except Exception as exc:
                self.store.record_decommission_policy_result(
                    base.workspace.id,
                    base.channel.id,
                    key,
                    revoked=False,
                    error=f"{type(exc).__name__}: {exc}",
                )
                raise LifecycleRemoteError(
                    "node is locally decommissioned, but Atom publish-policy revocation failed: "
                    + str(exc),
                    node_id=key,
                    local_state="decommissioned",
                ) from exc

        item = self.catalog.public_item(key)
        assert item is not None
        return {
            "status": "decommissioned",
            "node_id": key,
            "atom_policies_revoked": revoked_count,
            "item": item,
        }

    def reactivate(self, node_id: str) -> dict[str, Any]:
        key = self._require_node(node_id)
        base = self._base()
        current = self.store.get_node_lifecycle(
            base.workspace.id,
            base.channel.id,
            key,
        )
        if not current or current["administrative_state"] != "decommissioned":
            item = self.catalog.public_item(key)
            assert item is not None
            return {"status": "already_active", "node_id": key, "item": item}

        with self.runtime.lifecycle_lock:
            binding = self.store.find_latest_device_by_node(
                base.workspace.id,
                base.channel.id,
                key,
            )
            try:
                policy_created = False
                if binding is not None:
                    policy_created = bool(
                        self.atom.ensure_publish_policy(
                            base.workspace.id,
                            str(binding["atom_device_id"]),
                            base.channel.id,
                        )
                    )
            except Exception as exc:
                raise LifecycleRemoteError(
                    "node remains decommissioned because Atom publish-policy restore failed: "
                    + str(exc),
                    node_id=key,
                    local_state="decommissioned",
                ) from exc

            self.store.reactivate_node(
                base.workspace.id,
                base.channel.id,
                key,
            )
            if binding is not None:
                self.runtime.evict_device(str(binding["external_id"]))

        item = self.catalog.public_item(key)
        assert item is not None
        return {
            "status": "reactivated",
            "node_id": key,
            "atom_publish_policy_created": policy_created,
            "item": item,
        }


__all__ = [
    "DecommissionedEventSuppressed",
    "DeviceLifecycleController",
    "LifecycleBindingSQLiteManagementStore",
    "LifecycleIrrigapCatalogManager",
    "LifecycleRemoteError",
    "LifecycleSmarterAdapterRuntime",
]
