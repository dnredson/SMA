from __future__ import annotations

import math
import os
import time
from collections import defaultdict
from typing import Any, Mapping, Optional
from urllib.parse import parse_qs, unquote, urlparse

from .gateway_stats import GatewayStatsTopologySQLiteManagementStore


class RFHealthTopologySQLiteManagementStore(GatewayStatsTopologySQLiteManagementStore):
    """Persist LoRaWAN reception samples and summarize RF link trends.

    Gateway statistics describe the health of the gateway itself. RF samples
    describe the observed edge between one managed device and one gateway, so
    they are stored separately from gateway heartbeat/state.

    RSSI/SNR are intentionally kept as observations rather than converted into
    an absolute good/bad verdict. Absolute LoRa link margins depend on radio
    parameters and deployment conditions. The summary therefore reports raw
    statistics plus change/trend only.
    """

    schema = "smarter-adapter.rf-health/1"

    def __init__(
        self,
        *args,
        rf_retention_days: Optional[float] = None,
        rf_summary_window_hours: Optional[float] = None,
        rf_stable_slope_db_per_hour: Optional[float] = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.rf_retention_days = float(
            os.getenv("SMA_RF_RETENTION_DAYS", "90")
            if rf_retention_days is None
            else rf_retention_days
        )
        self.rf_summary_window_hours = float(
            os.getenv("SMA_RF_SUMMARY_WINDOW_HOURS", "24")
            if rf_summary_window_hours is None
            else rf_summary_window_hours
        )
        self.rf_stable_slope_db_per_hour = float(
            os.getenv("SMA_RF_STABLE_SLOPE_DB_PER_HOUR", "0.5")
            if rf_stable_slope_db_per_hour is None
            else rf_stable_slope_db_per_hour
        )
        if self.rf_retention_days <= 0:
            raise ValueError("rf_retention_days must be > 0")
        if self.rf_summary_window_hours <= 0:
            raise ValueError("rf_summary_window_hours must be > 0")
        if self.rf_stable_slope_db_per_hour < 0:
            raise ValueError("rf_stable_slope_db_per_hour must be >= 0")

        with self._lock, self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS rf_link_samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    workspace_id TEXT NOT NULL,
                    channel_id TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    gateway_id TEXT NOT NULL,
                    observed_at REAL NOT NULL,
                    rssi REAL,
                    snr REAL,
                    channel INTEGER,
                    rf_chain INTEGER,
                    crc_status TEXT NOT NULL DEFAULT '',
                    f_port INTEGER,
                    message_role TEXT NOT NULL DEFAULT '',
                    mqtt_topic TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL,
                    UNIQUE (
                        workspace_id, channel_id, external_id,
                        gateway_id, observed_at
                    )
                )
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_rf_link_samples_device_time
                ON rf_link_samples (
                    workspace_id, channel_id, external_id, observed_at DESC
                )
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_rf_link_samples_gateway_time
                ON rf_link_samples (
                    workspace_id, gateway_id, observed_at DESC
                )
                """
            )

    @staticmethod
    def _finite(value: object) -> Optional[float]:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number if math.isfinite(number) else None

    @staticmethod
    def _optional_int(value: object) -> Optional[int]:
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def record_device_gateway(
        self,
        workspace_id: str,
        channel_id: str,
        external_id: str,
        gateway: Mapping[str, Any],
        *,
        observed_at: Optional[float] = None,
    ) -> None:
        # Keep the existing latest-link/topology behavior first.
        super().record_device_gateway(
            workspace_id,
            channel_id,
            external_id,
            gateway,
            observed_at=observed_at,
        )

        gateway_id = str(gateway.get("gateway_id") or "").strip().lower()
        if not gateway_id:
            return
        seen = float(time.time() if observed_at is None else observed_at)
        rssi = self._finite(gateway.get("rssi"))
        snr = self._finite(gateway.get("snr"))
        channel = self._optional_int(gateway.get("channel"))
        rf_chain = self._optional_int(gateway.get("rf_chain"))
        f_port = self._optional_int(gateway.get("f_port"))
        role = str(gateway.get("message_role") or "").strip().lower()
        topic = str(gateway.get("mqtt_topic") or "")
        crc_status = str(gateway.get("crc_status") or "")

        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO rf_link_samples (
                    workspace_id, channel_id, external_id, gateway_id,
                    observed_at, rssi, snr, channel, rf_chain, crc_status,
                    f_port, message_role, mqtt_topic, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(
                    workspace_id, channel_id, external_id, gateway_id, observed_at
                ) DO UPDATE SET
                    rssi = excluded.rssi,
                    snr = excluded.snr,
                    channel = excluded.channel,
                    rf_chain = excluded.rf_chain,
                    crc_status = excluded.crc_status,
                    f_port = COALESCE(excluded.f_port, rf_link_samples.f_port),
                    message_role = CASE
                        WHEN excluded.message_role <> '' THEN excluded.message_role
                        ELSE rf_link_samples.message_role END,
                    mqtt_topic = CASE
                        WHEN excluded.mqtt_topic <> '' THEN excluded.mqtt_topic
                        ELSE rf_link_samples.mqtt_topic END
                """,
                (
                    str(workspace_id),
                    str(channel_id),
                    str(external_id),
                    gateway_id,
                    seen,
                    rssi,
                    snr,
                    channel,
                    rf_chain,
                    crc_status,
                    f_port,
                    role,
                    topic,
                    time.time(),
                ),
            )
            cutoff = seen - (self.rf_retention_days * 86400.0)
            self._conn.execute(
                "DELETE FROM rf_link_samples WHERE observed_at < ?",
                (cutoff,),
            )

    @staticmethod
    def _sample_public(row) -> dict[str, Any]:
        return {
            "id": int(row["id"]),
            "gateway_id": str(row["gateway_id"]),
            "observed_at": float(row["observed_at"]),
            "rssi": float(row["rssi"]) if row["rssi"] is not None else None,
            "snr": float(row["snr"]) if row["snr"] is not None else None,
            "channel": int(row["channel"]) if row["channel"] is not None else None,
            "rf_chain": int(row["rf_chain"]) if row["rf_chain"] is not None else None,
            "crc_status": str(row["crc_status"] or ""),
            "f_port": int(row["f_port"]) if row["f_port"] is not None else None,
            "message_role": str(row["message_role"] or ""),
            "mqtt_topic": str(row["mqtt_topic"] or ""),
        }

    def list_rf_samples(
        self,
        workspace_id: str,
        channel_id: str,
        external_id: str,
        *,
        since: Optional[float] = None,
        gateway_id: str = "",
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        clauses = [
            "workspace_id = ?",
            "channel_id = ?",
            "external_id = ?",
        ]
        values: list[Any] = [str(workspace_id), str(channel_id), str(external_id)]
        if since is not None:
            clauses.append("observed_at >= ?")
            values.append(float(since))
        gateway = str(gateway_id or "").strip().lower()
        if gateway:
            clauses.append("gateway_id = ?")
            values.append(gateway)
        values.append(max(1, int(limit)))
        query = (
            "SELECT * FROM rf_link_samples WHERE "
            + " AND ".join(clauses)
            + " ORDER BY observed_at DESC, id DESC LIMIT ?"
        )
        with self._lock:
            rows = self._conn.execute(query, values).fetchall()
        return [self._sample_public(row) for row in rows]

    def count_rf_samples(
        self,
        *,
        workspace_id: str = "",
        channel_id: str = "",
        external_id: str = "",
    ) -> int:
        clauses = []
        values: list[Any] = []
        for column, value in (
            ("workspace_id", workspace_id),
            ("channel_id", channel_id),
            ("external_id", external_id),
        ):
            if value:
                clauses.append(column + " = ?")
                values.append(str(value))
        query = "SELECT COUNT(*) AS n FROM rf_link_samples"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        with self._lock:
            row = self._conn.execute(query, values).fetchone()
        return int(row["n"] if row is not None else 0)

    def _series_summary(
        self,
        samples: list[dict[str, Any]],
        key: str,
    ) -> dict[str, Any]:
        points = sorted(
            (
                (float(item["observed_at"]), float(item[key]))
                for item in samples
                if item.get(key) is not None
            ),
            key=lambda item: item[0],
        )
        if not points:
            return {
                "samples": 0,
                "latest": None,
                "min": None,
                "max": None,
                "avg": None,
                "slope_db_per_hour": None,
                "trend": "insufficient",
            }

        values = [value for _, value in points]
        summary: dict[str, Any] = {
            "samples": len(points),
            "latest": values[-1],
            "min": min(values),
            "max": max(values),
            "avg": sum(values) / len(values),
            "slope_db_per_hour": None,
            "trend": "insufficient",
        }
        if len(points) < 2:
            return summary

        origin = points[0][0]
        xs = [(timestamp - origin) / 3600.0 for timestamp, _ in points]
        mean_x = sum(xs) / len(xs)
        mean_y = sum(values) / len(values)
        denominator = sum((x - mean_x) ** 2 for x in xs)
        if denominator <= 0:
            return summary
        slope = sum(
            (x - mean_x) * (value - mean_y)
            for x, value in zip(xs, values)
        ) / denominator
        summary["slope_db_per_hour"] = slope
        summary["estimated_change_db"] = slope * (xs[-1] - xs[0])

        if len(points) >= 3:
            threshold = self.rf_stable_slope_db_per_hour
            if slope < -threshold:
                summary["trend"] = "degrading"
            elif slope > threshold:
                summary["trend"] = "improving"
            else:
                summary["trend"] = "stable"
        return summary

    def _gateway_summary(
        self,
        gateway_id: str,
        samples: list[dict[str, Any]],
    ) -> dict[str, Any]:
        ordered = sorted(samples, key=lambda item: float(item["observed_at"]))
        latest = ordered[-1] if ordered else None
        channels = sorted(
            {int(item["channel"]) for item in ordered if item.get("channel") is not None}
        )
        roles = sorted(
            {str(item["message_role"]) for item in ordered if item.get("message_role")}
        )
        return {
            "gateway_id": gateway_id,
            "samples": len(ordered),
            "first_at": float(ordered[0]["observed_at"]) if ordered else None,
            "last_at": float(ordered[-1]["observed_at"]) if ordered else None,
            "latest": latest,
            "rssi": self._series_summary(ordered, "rssi"),
            "snr": self._series_summary(ordered, "snr"),
            "channels": channels,
            "message_roles": roles,
        }

    def rf_health_report(
        self,
        workspace_id: str,
        channel_id: str,
        external_id: str,
        *,
        window_hours: Optional[float] = None,
        gateway_id: str = "",
        limit: int = 500,
        now: Optional[float] = None,
    ) -> dict[str, Any]:
        current = time.time() if now is None else float(now)
        hours = float(
            self.rf_summary_window_hours if window_hours is None else window_hours
        )
        if hours <= 0:
            raise ValueError("window_hours must be > 0")
        hours = min(hours, self.rf_retention_days * 24.0)
        since = current - (hours * 3600.0)

        # Use a generous bounded set for the statistical summary while keeping
        # the response's raw sample list controlled by the requested limit.
        summary_samples = self.list_rf_samples(
            workspace_id,
            channel_id,
            external_id,
            since=since,
            gateway_id=gateway_id,
            limit=max(int(limit), 10000),
        )
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in summary_samples:
            grouped[str(item["gateway_id"])].append(item)
        gateways = [
            self._gateway_summary(key, grouped[key])
            for key in sorted(grouped)
        ]
        raw_samples = summary_samples[: max(1, int(limit))]
        all_times = [float(item["observed_at"]) for item in summary_samples]
        return {
            "schema": self.schema,
            "external_id": str(external_id),
            "generated_at": current,
            "window_hours": hours,
            "retention_days": self.rf_retention_days,
            "trend_stable_slope_db_per_hour": self.rf_stable_slope_db_per_hour,
            "summary": {
                "samples": len(summary_samples),
                "gateways": len(grouped),
                "first_at": min(all_times) if all_times else None,
                "last_at": max(all_times) if all_times else None,
            },
            "gateways": gateways,
            "samples": raw_samples,
        }

    def list_device_gateways(
        self,
        workspace_id: str,
        channel_id: str,
        external_id: str,
    ) -> list[dict[str, Any]]:
        links = super().list_device_gateways(workspace_id, channel_id, external_id)
        for link in links:
            report = self.rf_health_report(
                workspace_id,
                channel_id,
                external_id,
                window_hours=self.rf_summary_window_hours,
                gateway_id=str(link.get("gateway_id") or ""),
                limit=1,
            )
            link["rf_health"] = (
                report["gateways"][0]
                if report.get("gateways")
                else {
                    "gateway_id": link.get("gateway_id"),
                    "samples": 0,
                    "rssi": {"trend": "insufficient", "samples": 0},
                    "snr": {"trend": "insufficient", "samples": 0},
                }
            )
        return links


def install_rf_management_api() -> None:
    """Install the optional RF-history route on the lifecycle API handler."""

    from . import lifecycle_management

    handler = lifecycle_management._LifecycleHandler
    if getattr(handler, "_rf_health_route_installed", False):
        return

    original_do_get = handler.do_GET

    def do_get_with_rf(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        prefix = "/api/v2/devices/"
        suffix = "/rf"
        if path.startswith(prefix) and path.endswith(suffix):
            raw = path[len(prefix) : -len(suffix)].strip("/")
            if raw and "/" not in raw:
                if not self._authorized():
                    self._error(401, "unauthorized")
                    return
                base = self.runtime.base
                reporter = getattr(self.store, "rf_health_report", None)
                if base is None:
                    self._error(503, "runtime is not bootstrapped")
                    return
                if not callable(reporter):
                    self._error(503, "RF health history is not configured")
                    return
                external_id = unquote(raw)
                device = self.store.find_device(
                    base.workspace.id,
                    base.channel.id,
                    external_id,
                )
                if device is None:
                    self._error(404, "managed device not found")
                    return
                query = parse_qs(parsed.query)
                try:
                    default_hours = float(
                        getattr(self.store, "rf_summary_window_hours", 24.0)
                    )
                    hours = float((query.get("hours") or [default_hours])[0])
                    limit = int((query.get("limit") or [500])[0])
                except (TypeError, ValueError):
                    self._error(400, "hours must be numeric and limit must be an integer")
                    return
                if hours <= 0:
                    self._error(400, "hours must be > 0")
                    return
                if limit < 1 or limit > 2000:
                    self._error(400, "limit must be in 1..2000")
                    return
                gateway_id = str((query.get("gateway_id") or [""])[0]).strip().lower()
                try:
                    report = reporter(
                        base.workspace.id,
                        base.channel.id,
                        external_id,
                        window_hours=hours,
                        gateway_id=gateway_id,
                        limit=limit,
                    )
                except ValueError as exc:
                    self._error(400, str(exc))
                    return
                self._send(200, report)
                return
        original_do_get(self)

    handler.do_GET = do_get_with_rf
    handler._rf_health_route_installed = True


__all__ = [
    "RFHealthTopologySQLiteManagementStore",
    "install_rf_management_api",
]
