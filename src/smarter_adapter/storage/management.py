from __future__ import annotations

import time
from typing import Optional

from ..reliability import RetryItem
from .sqlite import SQLiteStateStore


class SQLiteManagementStore(SQLiteStateStore):
    """SQLite state store with read/admin operations for the management API."""

    def list_devices(
        self,
        *,
        workspace_id: str = "",
        channel_id: str = "",
        limit: int = 100,
    ):
        clauses = []
        values = []
        if workspace_id:
            clauses.append("workspace_id = ?")
            values.append(workspace_id)
        if channel_id:
            clauses.append("channel_id = ?")
            values.append(channel_id)
        query = (
            "SELECT workspace_id, channel_id, external_id, atom_device_id, name, "
            "profile_id, profile_version_id, first_seen, last_seen, last_sync "
            "FROM managed_devices"
        )
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY last_seen DESC, external_id ASC LIMIT ?"
        values.append(int(limit))
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [dict(row) for row in rows]

    def find_device(self, workspace_id: str, channel_id: str, external_id: str):
        """Return one managed-device record for management/read operations."""
        with self._lock:
            row = self._conn.execute(
                """
                SELECT workspace_id, channel_id, external_id, atom_device_id, name,
                       profile_id, profile_version_id, first_seen, last_seen, last_sync
                FROM managed_devices
                WHERE workspace_id = ? AND channel_id = ? AND external_id = ?
                """,
                (str(workspace_id), str(channel_id), str(external_id)),
            ).fetchone()
        return dict(row) if row is not None else None

    def list_retries(self, *, limit: int = 100):
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM retry_queue ORDER BY next_attempt_at ASC, id ASC LIMIT ?",
                (int(limit),),
            ).fetchall()
        return [
            RetryItem(
                id=int(row["id"]),
                raw=self._raw_from_row(row),
                attempts=int(row["attempts"]),
                next_attempt_at=float(row["next_attempt_at"]),
                first_failed_at=float(row["first_failed_at"]),
                last_error=str(row["last_error"]),
                error_type=str(row["error_type"]),
            )
            for row in rows
        ]

    def requeue_dlq(self, item_id: int, *, next_attempt_at: Optional[float] = None) -> int:
        """Move one dead letter back to the retry queue for manual replay."""
        due = time.time() if next_attempt_at is None else float(next_attempt_at)
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT * FROM dead_letters WHERE id = ?",
                (int(item_id),),
            ).fetchone()
            if row is None:
                return 0
            cursor = self._conn.execute(
                """
                INSERT INTO retry_queue (
                    source, topic, payload, received_at, metadata_json,
                    attempts, next_attempt_at, first_failed_at,
                    last_error, error_type, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["source"],
                    row["topic"],
                    row["payload"],
                    row["received_at"],
                    row["metadata_json"],
                    0,
                    due,
                    row["failed_at"],
                    "manual replay from DLQ",
                    "ManualReplay",
                    time.time(),
                ),
            )
            retry_id = int(cursor.lastrowid)
            self._conn.execute("DELETE FROM dead_letters WHERE id = ?", (int(item_id),))
            return retry_id


__all__ = ["SQLiteManagementStore"]
