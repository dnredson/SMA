from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Optional, Union

from ..magistrala.control_plane import DeviceRef
from ..models import RawEvent
from ..reliability import DeadLetterItem, RetryItem


class SQLiteStateStore:
    """Durable Smarter Adapter state, retry queue and dead-letter queue."""

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
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS retry_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    payload BLOB NOT NULL,
                    received_at REAL NOT NULL,
                    metadata_json TEXT NOT NULL,
                    attempts INTEGER NOT NULL,
                    next_attempt_at REAL NOT NULL,
                    first_failed_at REAL NOT NULL,
                    last_error TEXT NOT NULL,
                    error_type TEXT NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_retry_queue_due
                ON retry_queue(next_attempt_at, id)
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dead_letters (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    payload BLOB NOT NULL,
                    received_at REAL NOT NULL,
                    metadata_json TEXT NOT NULL,
                    attempts INTEGER NOT NULL,
                    failed_at REAL NOT NULL,
                    last_error TEXT NOT NULL,
                    error_type TEXT NOT NULL
                )
                """
            )

    @staticmethod
    def _metadata_json(raw: RawEvent) -> str:
        return json.dumps(raw.metadata or {}, ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def _raw_from_row(row: sqlite3.Row) -> RawEvent:
        try:
            metadata = json.loads(str(row["metadata_json"] or "{}"))
        except json.JSONDecodeError:
            metadata = {}
        return RawEvent(
            source=str(row["source"]),
            topic=str(row["topic"]),
            payload=bytes(row["payload"]),
            received_at=float(row["received_at"]),
            metadata=metadata if isinstance(metadata, dict) else {},
        )

    def get_device(self, workspace_id: str, channel_id: str, external_id: str) -> Optional[DeviceRef]:
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

    def upsert_device(self, device: DeviceRef, *, channel_id: str, seen_at: Optional[float] = None) -> None:
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
                    device.workspace_id, channel_id, device.external_id, device.id,
                    device.name, device.profile_id, device.profile_version_id,
                    now, now, now,
                ),
            )

    def touch_device(self, workspace_id: str, channel_id: str, external_id: str, *, seen_at: Optional[float] = None) -> None:
        now = float(time.time() if seen_at is None else seen_at)
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE managed_devices SET last_seen = ?
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

    def enqueue_retry(self, raw: RawEvent, error: Exception, *, attempts: int, next_attempt_at: float) -> int:
        now = time.time()
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                INSERT INTO retry_queue (
                    source, topic, payload, received_at, metadata_json,
                    attempts, next_attempt_at, first_failed_at,
                    last_error, error_type, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    raw.source, raw.topic, sqlite3.Binary(raw.payload), raw.received_at,
                    self._metadata_json(raw), int(attempts), float(next_attempt_at), now,
                    str(error), type(error).__name__, now,
                ),
            )
            return int(cursor.lastrowid)

    def due_retries(self, *, now: Optional[float] = None, limit: int = 50):
        current = time.time() if now is None else float(now)
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM retry_queue
                WHERE next_attempt_at <= ?
                ORDER BY next_attempt_at ASC, id ASC
                LIMIT ?
                """,
                (current, int(limit)),
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

    def reschedule_retry(self, item_id: int, error: Exception, *, attempts: int, next_attempt_at: float) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE retry_queue
                SET attempts = ?, next_attempt_at = ?, last_error = ?,
                    error_type = ?, updated_at = ?
                WHERE id = ?
                """,
                (int(attempts), float(next_attempt_at), str(error), type(error).__name__, time.time(), int(item_id)),
            )

    def delete_retry(self, item_id: int) -> None:
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM retry_queue WHERE id = ?", (int(item_id),))

    def move_retry_to_dlq(self, item_id: int, error: Exception) -> int:
        with self._lock, self._conn:
            row = self._conn.execute("SELECT * FROM retry_queue WHERE id = ?", (int(item_id),)).fetchone()
            if row is None:
                return 0
            cursor = self._conn.execute(
                """
                INSERT INTO dead_letters (
                    source, topic, payload, received_at, metadata_json,
                    attempts, failed_at, last_error, error_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["source"], row["topic"], row["payload"], row["received_at"],
                    row["metadata_json"], row["attempts"], time.time(), str(error), type(error).__name__,
                ),
            )
            self._conn.execute("DELETE FROM retry_queue WHERE id = ?", (int(item_id),))
            return int(cursor.lastrowid)

    def add_dlq(self, raw: RawEvent, error: Exception, *, attempts: int = 1) -> int:
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                INSERT INTO dead_letters (
                    source, topic, payload, received_at, metadata_json,
                    attempts, failed_at, last_error, error_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    raw.source, raw.topic, sqlite3.Binary(raw.payload), raw.received_at,
                    self._metadata_json(raw), int(attempts), time.time(), str(error), type(error).__name__,
                ),
            )
            return int(cursor.lastrowid)

    def count_retries(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) AS n FROM retry_queue").fetchone()
        return int(row["n"] if row is not None else 0)

    def count_dlq(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) AS n FROM dead_letters").fetchone()
        return int(row["n"] if row is not None else 0)

    def list_dlq(self, limit: int = 100):
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM dead_letters ORDER BY id DESC LIMIT ?", (int(limit),)
            ).fetchall()
        return [
            DeadLetterItem(
                id=int(row["id"]), raw=self._raw_from_row(row), attempts=int(row["attempts"]),
                failed_at=float(row["failed_at"]), last_error=str(row["last_error"]),
                error_type=str(row["error_type"]),
            )
            for row in rows
        ]

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> "SQLiteStateStore":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


__all__ = ["SQLiteStateStore"]
