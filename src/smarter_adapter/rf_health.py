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
    parameters and deployment conditions. Aggregate slopes are retained as raw
    diagnostics, while the assessment explicitly refuses to treat mixed radio
    conditions as one comparable time-series.
    """

    schema = "smarter-adapter.rf-health/1"

    _RF_COLUMNS = {
        "frequency_hz": "INTEGER",
        "modulation": "TEXT NOT NULL DEFAULT ''",
        "spreading_factor": "INTEGER",
        "bandwidth_hz": "INTEGER",
        "code_rate": "TEXT NOT NULL DEFAULT ''",
        "bitrate_bps": "INTEGER",
    }

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
                    frequency_hz INTEGER,
                    modulation TEXT NOT NULL DEFAULT '',
                    spreading_factor INTEGER,
                    bandwidth_hz INTEGER,
                    code_rate TEXT NOT NULL DEFAULT '',
                    bitrate_bps INTEGER,
                    created_at REAL NOT NULL,
                    UNIQUE (
                        workspace_id, channel_id, external_id,
                        gateway_id, observed_at
                    )
                )
                """
            )
            # Existing field deployments already have rf_link_samples. Additive
            # migration keeps their observations while enabling richer samples.
            existing = {
                str(row["name"])
                for row in self._conn.execute("PRAGMA table_info(rf_link_samples)").fetchall()
            }
            for name, sql_type in self._RF_COLUMNS.items():
                if name not in existing:
                    self._conn.execute(
                        f"ALTER TABLE rf_link_samples ADD COLUMN {name} {sql_type}"
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
        frequency_hz = self._optional_int(gateway.get("frequency_hz"))
        modulation = str(gateway.get("modulation") or "").strip().lower()
        spreading_factor = self._optional_int(gateway.get("spreading_factor"))
        bandwidth_hz = self._optional_int(gateway.get("bandwidth_hz"))
        code_rate = str(gateway.get("code_rate") or "").strip()
        bitrate_bps = self._optional_int(gateway.get("bitrate_bps"))

        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO rf_link_samples (
                    workspace_id, channel_id, external_id, gateway_id,
                    observed_at, rssi, snr, channel, rf_chain, crc_status,
                    f_port, message_role, mqtt_topic,
                    frequency_hz, modulation, spreading_factor, bandwidth_hz,
                    code_rate, bitrate_bps, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        ELSE rf_link_samples.mqtt_topic END,
                    frequency_hz = COALESCE(excluded.frequency_hz, rf_link_samples.frequency_hz),
                    modulation = CASE
                        WHEN excluded.modulation <> '' THEN excluded.modulation
                        ELSE rf_link_samples.modulation END,
                    spreading_factor = COALESCE(excluded.spreading_factor, rf_link_samples.spreading_factor),
                    bandwidth_hz = COALESCE(excluded.bandwidth_hz, rf_link_samples.bandwidth_hz),
                    code_rate = CASE
                        WHEN excluded.code_rate <> '' THEN excluded.code_rate
                        ELSE rf_link_samples.code_rate END,
                    bitrate_bps = COALESCE(excluded.bitrate_bps, rf_link_samples.bitrate_bps)
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
                    frequency_hz,
                    modulation,
                    spreading_factor,
                    bandwidth_hz,
                    code_rate,
                    bitrate_bps,
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
            "frequency_hz": int(row["frequency_hz"]) if row["frequency_hz"] is not None else None,
            "modulation": str(row["modulation"] or ""),
            "spreading_factor": int(row["spreading_factor"]) if row["spreading_factor"] is not None else None,
            "bandwidth_hz": int(row["bandwidth_hz"]) if row["bandwidth_hz"] is not None else None,
            "code_rate": str(row["code_rate"] or ""),
            "bitrate_bps": int(row["bitrate_bps"]) if row["bitrate_bps"] is not None else None,
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

    @staticmethod
    def _radio_profile(item: Mapping[str, Any]) -> Optional[tuple[Any, ...]]:
        values = (
            item.get("frequency_hz"),
            str(item.get("modulation") or ""),
            item.get("spreading_factor"),
            item.get("bandwidth_hz"),
            str(item.get("code_rate") or ""),
            item.get("bitrate_bps"),
        )
        if all(value in (None, "") for value in values):
            return None
        return values

    @staticmethod
    def _radio_profile_public(profile: tuple[Any, ...]) -> dict[str, Any]:
        return {
            "frequency_hz": profile[0],
            "modulation": profile[1],
            "spreading_factor": profile[2],
            "bandwidth_hz": profile[3],
            "code_rate": profile[4],
            "bitrate_bps": profile[5],
        }

    def _condition_groups(self, samples: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        by_channel: dict[Optional[int], list[dict[str, Any]]] = defaultdict(list)
        by_profile: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
        for item in samples:
            channel = item.get("channel")
            by_channel[int(channel) if channel is not None else None].append(item)
            profile = self._radio_profile(item)
            if profile is not None:
                by_profile[profile].append(item)

        channel_summaries = [
            {
                "channel": channel,
                "samples": len(group),
                "rssi": self._series_summary(group, "rssi"),
                "snr": self._series_summary(group, "snr"),
            }
            for channel, group in sorted(
                by_channel.items(), key=lambda entry: (-1 if entry[0] is None else entry[0])
            )
        ]
        profile_summaries = [
            {
                "profile": self._radio_profile_public(profile),
                "samples": len(group),
                "rssi": self._series_summary(group, "rssi"),
                "snr": self._series_summary(group, "snr"),
            }
            for profile, group in sorted(by_profile.items(), key=lambda entry: repr(entry[0]))
        ]
        return channel_summaries, profile_summaries

    def _assessment(
        self,
        samples: list[dict[str, Any]],
        *,
        channels: list[int],
        rssi: Mapping[str, Any],
        snr: Mapping[str, Any],
    ) -> dict[str, Any]:
        profiles = [self._radio_profile(item) for item in samples]
        known_profiles = {profile for profile in profiles if profile is not None}
        missing_profiles = sum(1 for profile in profiles if profile is None)
        if len(samples) < 3:
            return {
                "status": "insufficient",
                "reason": "fewer_than_three_samples",
                "comparable": False,
            }
        if len(known_profiles) > 1:
            return {
                "status": "mixed_conditions",
                "reason": "multiple_radio_profiles",
                "comparable": False,
                "raw_aggregate_rssi_trend": rssi.get("trend"),
                "raw_aggregate_snr_trend": snr.get("trend"),
            }
        if known_profiles and missing_profiles:
            return {
                "status": "mixed_conditions",
                "reason": "partial_radio_profile_coverage",
                "comparable": False,
                "raw_aggregate_rssi_trend": rssi.get("trend"),
                "raw_aggregate_snr_trend": snr.get("trend"),
            }
        if not known_profiles and len(channels) > 1:
            return {
                "status": "mixed_conditions",
                "reason": "multiple_channels_without_radio_profile",
                "comparable": False,
                "raw_aggregate_rssi_trend": rssi.get("trend"),
                "raw_aggregate_snr_trend": snr.get("trend"),
            }
        return {
            "status": "comparable",
            "reason": "single_observed_radio_condition",
            "comparable": True,
            "rssi_trend": rssi.get("trend"),
            "snr_trend": snr.get("trend"),
        }

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
        rssi = self._series_summary(ordered, "rssi")
        snr = self._series_summary(ordered, "snr")
        by_channel, by_profile = self._condition_groups(ordered)
        return {
            "gateway_id": gateway_id,
            "samples": len(ordered),
            "first_at": float(ordered[0]["observed_at"]) if ordered else None,
            "last_at": float(ordered[-1]["observed_at"]) if ordered else None,
            "latest": latest,
            "rssi": rssi,
            "snr": snr,
            "channels": channels,
            "message_roles": roles,
            "assessment": self._assessment(
                ordered,
                channels=channels,
                rssi=rssi,
                snr=snr,
            ),
            "by_channel": by_channel,
            "by_radio_profile": by_profile,
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
                    "assessment": {
                        "status": "insufficient",
                        "reason": "no_samples",
                        "comparable": False,
                    },
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
