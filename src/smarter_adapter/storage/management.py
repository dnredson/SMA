from __future__ import annotations

import json
import time
from typing import Optional

from ..reliability import RetryItem
from .sqlite import SQLiteStateStore


class SQLiteManagementStore(SQLiteStateStore):
    """SQLite state store with read/admin operations for the management API."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        with self._lock, self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS managed_device_quality (
                    workspace_id TEXT NOT NULL,
                    channel_id TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    quality_status TEXT NOT NULL,
                    invalid_fields_json TEXT NOT NULL,
                    evaluated_at REAL NOT NULL,
                    source_received_at REAL NOT NULL,
                    PRIMARY KEY (workspace_id, channel_id, external_id),
                    FOREIGN KEY (workspace_id, channel_id, external_id)
                        REFERENCES managed_devices(workspace_id, channel_id, external_id)
                        ON DELETE CASCADE
                )
                """
            )

    @staticmethod
    def _quality_fields(row) -> dict:
        status = row["quality_status"] if "quality_status" in row.keys() else None
        raw_fields = row["invalid_fields_json"] if "invalid_fields_json" in row.keys() else None
        try:
            invalid_fields = json.loads(str(raw_fields or "[]"))
        except json.JSONDecodeError:
            invalid_fields = []
        if not isinstance(invalid_fields, list):
            invalid_fields = []
        evaluated_at = row["quality_evaluated_at"] if "quality_evaluated_at" in row.keys() else None
        source_received_at = (
            row["quality_source_received_at"]
            if "quality_source_received_at" in row.keys()
            else None
        )
        return {
            "data_quality": str(status or "unknown"),
            "invalid_fields": [str(item) for item in invalid_fields],
            "quality_evaluated_at": float(evaluated_at) if evaluated_at is not None else None,
            "quality_source_received_at": (
                float(source_received_at) if source_received_at is not None else None
            ),
        }

    @classmethod
    def _device_public(cls, row) -> dict:
        result = {
            "workspace_id": row["workspace_id"],
            "channel_id": row["channel_id"],
            "external_id": row["external_id"],
            "atom_device_id": row["atom_device_id"],
            "name": row["name"],
            "profile_id": row["profile_id"],
            "profile_version_id": row["profile_version_id"],
            "first_seen": row["first_seen"],
            "last_seen": row["last_seen"],
            "last_sync": row["last_sync"],
        }
        result.update(cls._quality_fields(row))
        return result

    def set_device_quality(
        self,
        workspace_id: str,
        channel_id: str,
        external_id: str,
        *,
        quality_status: str,
        invalid_fields=(),
        evaluated_at: Optional[float] = None,
        source_received_at: Optional[float] = None,
    ) -> None:
        status = str(quality_status or "unknown").strip().lower()
        if status not in {"valid", "degraded", "invalid", "unknown"}:
            raise ValueError("quality_status must be valid, degraded, invalid or unknown")
        fields = []
        for item in invalid_fields:
            value = str(item or "").strip()
            if value and value not in fields:
                fields.append(value)
        now = time.time()
        evaluated = float(now if evaluated_at is None else evaluated_at)
        received = float(evaluated if source_received_at is None else source_received_at)
        encoded_fields = json.dumps(fields, ensure_ascii=False, separators=(",", ":"))
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO managed_device_quality (
                    workspace_id, channel_id, external_id, quality_status,
                    invalid_fields_json, evaluated_at, source_received_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(workspace_id, channel_id, external_id) DO UPDATE SET
                    quality_status = CASE
                        WHEN excluded.source_received_at >= managed_device_quality.source_received_at
                        THEN excluded.quality_status ELSE managed_device_quality.quality_status END,
                    invalid_fields_json = CASE
                        WHEN excluded.source_received_at >= managed_device_quality.source_received_at
                        THEN excluded.invalid_fields_json ELSE managed_device_quality.invalid_fields_json END,
                    evaluated_at = CASE
                        WHEN excluded.source_received_at >= managed_device_quality.source_received_at
                        THEN excluded.evaluated_at ELSE managed_device_quality.evaluated_at END,
                    source_received_at = MAX(
                        managed_device_quality.source_received_at,
                        excluded.source_received_at
                    )
                """,
                (
                    str(workspace_id),
                    str(channel_id),
                    str(external_id),
                    status,
                    encoded_fields,
                    evaluated,
                    received,
                ),
            )

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
            clauses.append("d.workspace_id = ?")
            values.append(workspace_id)
        if channel_id:
            clauses.append("d.channel_id = ?")
            values.append(channel_id)
        query = (
            "SELECT d.workspace_id, d.channel_id, d.external_id, d.atom_device_id, d.name, "
            "d.profile_id, d.profile_version_id, d.first_seen, d.last_seen, d.last_sync, "
            "q.quality_status, q.invalid_fields_json, "
            "q.evaluated_at AS quality_evaluated_at, "
            "q.source_received_at AS quality_source_received_at "
            "FROM managed_devices d "
            "LEFT JOIN managed_device_quality q ON "
            "q.workspace_id = d.workspace_id AND q.channel_id = d.channel_id "
            "AND q.external_id = d.external_id"
        )
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY d.last_seen DESC, d.external_id ASC LIMIT ?"
        values.append(int(limit))
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [self._device_public(row) for row in rows]

    def find_device(self, workspace_id: str, channel_id: str, external_id: str):
        """Return one managed-device record for management/read operations."""
        with self._lock:
            row = self._conn.execute(
                """
                SELECT d.workspace_id, d.channel_id, d.external_id, d.atom_device_id, d.name,
                       d.profile_id, d.profile_version_id, d.first_seen, d.last_seen, d.last_sync,
                       q.quality_status, q.invalid_fields_json,
                       q.evaluated_at AS quality_evaluated_at,
                       q.source_received_at AS quality_source_received_at
                FROM managed_devices d
                LEFT JOIN managed_device_quality q ON
                     q.workspace_id = d.workspace_id AND q.channel_id = d.channel_id
                     AND q.external_id = d.external_id
                WHERE d.workspace_id = ? AND d.channel_id = ? AND d.external_id = ?
                """,
                (str(workspace_id), str(channel_id), str(external_id)),
            ).fetchone()
        return self._device_public(row) if row is not None else None

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
