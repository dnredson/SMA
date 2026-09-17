from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path
from typing import Optional, Union

from ..magistrala.control_plane import DeviceRef


class SQLiteStateStore:
    """Small durable store for Smarter Adapter runtime state.

    The store intentionally persists only control-plane identity/state. Raw
    telemetry remains in the data plane and will later be handled by the retry
    queue/DLQ rather than this table.
    """

    def __init__(self, path: Union[str, Path]) -> None:
        self.path = Path(path).expanduser()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._conn:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS managed_devices (
                    workspace_id TEXT NOT NULL,
                    channel_id TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    atom_device_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    profile_id TEXT NOT NULL,
                    profile_version_id TEXT NOT NULL,
                    first_seen REAL NOT NULL,
                    last_seen REAL NOT NULL,
                    last_sync REAL NOT NULL,
                    PRIMARY KEY (workspace_id, channel_id, external_id)
                )
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_managed_devices_atom_id
                ON managed_devices(atom_device_id)
                """
            )

    def get_device(
        self,
        workspace_id: str,
        channel_id: str,
        external_id: str,
    ) -> Optional[DeviceRef]:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT atom_device_id, workspace_id, external_id, name,
                       profile_id, profile_version_id
                FROM managed_devices
                WHERE workspace_id = ? AND channel_id = ? AND external_id = ?
                """,
                (workspace_id, channel_id, external_id),
            ).fetchone()
        if row is None:
            return None
        return DeviceRef(
            id=str(row["atom_device_id"]),
            workspace_id=str(row["workspace_id"]),
            external_id=str(row["external_id"]),
            name=str(row["name"]),
            profile_id=str(row["profile_id"]),
            profile_version_id=str(row["profile_version_id"]),
            created=False,
            publish_policy_created=False,
        )

    def upsert_device(
        self,
        device: DeviceRef,
        *,
        channel_id: str,
        seen_at: Optional[float] = None,
    ) -> None:
        now = float(time.time() if seen_at is None else seen_at)
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO managed_devices (
                    workspace_id, channel_id, external_id, atom_device_id,
                    name, profile_id, profile_version_id,
                    first_seen, last_seen, last_sync
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(workspace_id, channel_id, external_id) DO UPDATE SET
                    atom_device_id = excluded.atom_device_id,
                    name = excluded.name,
                    profile_id = excluded.profile_id,
                    profile_version_id = excluded.profile_version_id,
                    last_seen = excluded.last_seen,
                    last_sync = excluded.last_sync
                """,
                (
                    device.workspace_id,
                    channel_id,
                    device.external_id,
                    device.id,
                    device.name,
                    device.profile_id,
                    device.profile_version_id,
                    now,
                    now,
                    now,
                ),
            )

    def touch_device(
        self,
        workspace_id: str,
        channel_id: str,
        external_id: str,
        *,
        seen_at: Optional[float] = None,
    ) -> None:
        now = float(time.time() if seen_at is None else seen_at)
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE managed_devices
                SET last_seen = ?
                WHERE workspace_id = ? AND channel_id = ? AND external_id = ?
                """,
                (now, workspace_id, channel_id, external_id),
            )

    def count_devices(self, workspace_id: str = "", channel_id: str = "") -> int:
        clauses = []
        values = []
        if workspace_id:
            clauses.append("workspace_id = ?")
            values.append(workspace_id)
        if channel_id:
            clauses.append("channel_id = ?")
            values.append(channel_id)
        query = "SELECT COUNT(*) AS n FROM managed_devices"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        with self._lock:
            row = self._conn.execute(query, values).fetchone()
        return int(row["n"] if row is not None else 0)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> "SQLiteStateStore":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


__all__ = ["SQLiteStateStore"]
