from __future__ import annotations

import logging
import math
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional

from .device_lifecycle import LifecycleBindingSQLiteManagementStore

logger = logging.getLogger("smarter_adapter.gateway")


GATEWAY_PROFILE_KEY = "smarter-adapter-lorawan-gateway"
GATEWAY_PROFILE_NAME = "LoRaWAN Gateway"
GATEWAY_PROFILE_DESCRIPTION = (
    "LoRaWAN gateway observed by Smarter Adapter. Presence is derived from "
    "periodic gateway traffic; the entity does not receive sensor publish permission."
)
GATEWAY_PROFILE_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "managed_by": {"type": "string"},
        "role": {"type": "string", "const": "lorawan-gateway"},
        "gateway_id": {"type": "string"},
        "topic_root": {"type": "string"},
    },
}

_GATEWAY_TOPIC_RE = re.compile(
    r"^(?:(?P<root>[^/]+)/)?gateway/(?P<gateway_id>[^/]+)/"
    r"(?P<section>event|state)/(?P<event>[^/]+)$"
)


@dataclass(frozen=True)
class GatewayPresencePolicy:
    """Derive gateway liveness from periodic non-retained observations."""

    expected_interval_seconds: float = 30.0
    stale_after_seconds: float = 90.0
    offline_after_seconds: float = 180.0

    def __post_init__(self) -> None:
        expected = float(self.expected_interval_seconds)
        stale = float(self.stale_after_seconds)
        offline = float(self.offline_after_seconds)
        if expected <= 0:
            raise ValueError("expected_interval_seconds must be > 0")
        if stale <= expected:
            raise ValueError("stale_after_seconds must be greater than expected interval")
        if offline <= stale:
            raise ValueError("offline_after_seconds must be greater than stale_after_seconds")

    @staticmethod
    def _age(value: object, now: float) -> Optional[float]:
        try:
            timestamp = float(value or 0.0)
        except (TypeError, ValueError):
            return None
        if timestamp <= 0.0:
            return None
        return max(0.0, now - timestamp)

    def classify(self, last_seen: object, *, now: Optional[float] = None) -> str:
        current = time.time() if now is None else float(now)
        age = self._age(last_seen, current)
        if age is None or age >= self.offline_after_seconds:
            return "offline"
        if age >= self.stale_after_seconds:
            return "stale"
        return "online"

    def decorate(self, gateway: Mapping[str, Any], *, now: Optional[float] = None) -> dict[str, Any]:
        current = time.time() if now is None else float(now)
        result = dict(gateway)
        result["operational_status"] = self.classify(result.get("last_seen"), now=current)
        for field in ("last_seen", "last_stats_at", "last_uplink_at", "last_conn_at"):
            age = self._age(result.get(field), current)
            result[field + "_age_seconds"] = None if age is None else round(age, 3)
        result["expected_interval_seconds"] = float(self.expected_interval_seconds)
        result["stale_after_seconds"] = float(self.stale_after_seconds)
        result["offline_after_seconds"] = float(self.offline_after_seconds)
        return result


class GatewayTopologySQLiteManagementStore(LifecycleBindingSQLiteManagementStore):
    """Production state store enriched with gateway topology and per-role quality."""

    _QUALITY_RANK = {"unknown": 0, "valid": 1, "degraded": 2, "invalid": 3}

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        with self._lock, self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS managed_gateways (
                    workspace_id TEXT NOT NULL,
                    gateway_id TEXT NOT NULL,
                    atom_entity_id TEXT NOT NULL DEFAULT '',
                    topic_root TEXT NOT NULL DEFAULT '',
                    first_seen REAL NOT NULL,
                    last_seen REAL NOT NULL DEFAULT 0,
                    last_stats_at REAL,
                    last_uplink_at REAL,
                    last_conn_at REAL,
                    stats_count INTEGER NOT NULL DEFAULT 0,
                    uplink_count INTEGER NOT NULL DEFAULT 0,
                    conn_count INTEGER NOT NULL DEFAULT 0,
                    last_topic TEXT NOT NULL DEFAULT '',
                    last_error TEXT NOT NULL DEFAULT '',
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (workspace_id, gateway_id)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS managed_device_gateways (
                    workspace_id TEXT NOT NULL,
                    channel_id TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    gateway_id TEXT NOT NULL,
                    first_seen REAL NOT NULL,
                    last_seen REAL NOT NULL,
                    last_rssi REAL,
                    last_snr REAL,
                    last_channel INTEGER,
                    last_rf_chain INTEGER,
                    last_crc_status TEXT NOT NULL DEFAULT '',
                    packet_count INTEGER NOT NULL DEFAULT 1,
                    PRIMARY KEY (workspace_id, channel_id, external_id, gateway_id)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS managed_device_role_quality (
                    workspace_id TEXT NOT NULL,
                    channel_id TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    message_role TEXT NOT NULL,
                    quality_status TEXT NOT NULL,
                    invalid_fields_json TEXT NOT NULL,
                    evaluated_at REAL NOT NULL,
                    source_received_at REAL NOT NULL,
                    PRIMARY KEY (workspace_id, channel_id, external_id, message_role)
                )
                """
            )

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
        role: str = "",
    ) -> None:
        super().set_device_quality(
            workspace_id,
            channel_id,
            external_id,
            quality_status=quality_status,
            invalid_fields=invalid_fields,
            evaluated_at=evaluated_at,
            source_received_at=source_received_at,
        )
        message_role = str(role or "").strip().lower()
        if not message_role or message_role == "unknown":
            return
        import json

        fields: list[str] = []
        for item in invalid_fields:
            value = str(item or "").strip()
            if value and value not in fields:
                fields.append(value)
        evaluated = float(time.time() if evaluated_at is None else evaluated_at)
        received = float(evaluated if source_received_at is None else source_received_at)
        encoded = json.dumps(fields, ensure_ascii=False, separators=(",", ":"))
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO managed_device_role_quality (
                    workspace_id, channel_id, external_id, message_role,
                    quality_status, invalid_fields_json, evaluated_at, source_received_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(workspace_id, channel_id, external_id, message_role) DO UPDATE SET
                    quality_status = CASE
                        WHEN excluded.source_received_at >= managed_device_role_quality.source_received_at
                        THEN excluded.quality_status ELSE managed_device_role_quality.quality_status END,
                    invalid_fields_json = CASE
                        WHEN excluded.source_received_at >= managed_device_role_quality.source_received_at
                        THEN excluded.invalid_fields_json ELSE managed_device_role_quality.invalid_fields_json END,
                    evaluated_at = CASE
                        WHEN excluded.source_received_at >= managed_device_role_quality.source_received_at
                        THEN excluded.evaluated_at ELSE managed_device_role_quality.evaluated_at END,
                    source_received_at = MAX(
                        managed_device_role_quality.source_received_at,
                        excluded.source_received_at
                    )
                """,
                (
                    str(workspace_id),
                    str(channel_id),
                    str(external_id),
                    message_role,
                    str(quality_status or "unknown").lower(),
                    encoded,
                    evaluated,
                    received,
                ),
            )

    def _role_quality(self, workspace_id: str, channel_id: str, external_id: str) -> dict[str, Any]:
        import json

        with self._lock:
            rows = self._conn.execute(
                """
                SELECT message_role, quality_status, invalid_fields_json,
                       evaluated_at, source_received_at
                FROM managed_device_role_quality
                WHERE workspace_id = ? AND channel_id = ? AND external_id = ?
                ORDER BY message_role ASC
                """,
                (str(workspace_id), str(channel_id), str(external_id)),
            ).fetchall()
        result: dict[str, Any] = {}
        for row in rows:
            try:
                fields = json.loads(str(row["invalid_fields_json"] or "[]"))
            except json.JSONDecodeError:
                fields = []
            result[str(row["message_role"])] = {
                "data_quality": str(row["quality_status"] or "unknown"),
                "invalid_fields": [str(item) for item in fields] if isinstance(fields, list) else [],
                "quality_evaluated_at": float(row["evaluated_at"]),
                "quality_source_received_at": float(row["source_received_at"]),
            }
        return result

    def _decorate_role_quality(self, item: Optional[dict]) -> Optional[dict]:
        if item is None:
            return None
        result = dict(item)
        roles = self._role_quality(
            str(result.get("workspace_id") or ""),
            str(result.get("channel_id") or ""),
            str(result.get("external_id") or ""),
        )
        result["quality_by_role"] = roles
        if not roles:
            return result

        worst = max(
            roles.values(),
            key=lambda value: self._QUALITY_RANK.get(str(value.get("data_quality")), 0),
        )
        aggregate_status = str(worst.get("data_quality") or "unknown")
        invalid_fields: list[str] = []
        for role_value in roles.values():
            if role_value.get("data_quality") != aggregate_status:
                continue
            for field in role_value.get("invalid_fields") or []:
                if field not in invalid_fields:
                    invalid_fields.append(field)
        result["data_quality"] = aggregate_status
        result["invalid_fields"] = invalid_fields
        result["quality_evaluated_at"] = max(
            float(value.get("quality_evaluated_at") or 0.0) for value in roles.values()
        )
        result["quality_source_received_at"] = max(
            float(value.get("quality_source_received_at") or 0.0) for value in roles.values()
        )
        return result

    def list_devices(self, **kwargs):
        return [self._decorate_role_quality(item) for item in super().list_devices(**kwargs)]

    def find_device(self, workspace_id: str, channel_id: str, external_id: str):
        return self._decorate_role_quality(
            super().find_device(workspace_id, channel_id, external_id)
        )

    def record_gateway_event(
        self,
        workspace_id: str,
        gateway_id: str,
        *,
        event_kind: str,
        received_at: Optional[float] = None,
        topic: str = "",
        topic_root: str = "",
        retained: bool = False,
    ) -> None:
        gateway = str(gateway_id or "").strip().lower()
        if not gateway:
            raise ValueError("gateway_id must not be empty")
        kind = str(event_kind or "").strip().lower()
        if kind not in {"stats", "uplink", "conn"}:
            raise ValueError("event_kind must be stats, uplink or conn")
        now = float(time.time() if received_at is None else received_at)
        active = kind in {"stats", "uplink"} or (kind == "conn" and not retained)
        active_seen = now if active else 0.0
        stats_at = now if kind == "stats" else None
        uplink_at = now if kind == "uplink" else None
        conn_at = now if kind == "conn" else None
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO managed_gateways (
                    workspace_id, gateway_id, topic_root, first_seen, last_seen,
                    last_stats_at, last_uplink_at, last_conn_at,
                    stats_count, uplink_count, conn_count, last_topic, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(workspace_id, gateway_id) DO UPDATE SET
                    topic_root = CASE WHEN excluded.topic_root <> '' THEN excluded.topic_root
                                      ELSE managed_gateways.topic_root END,
                    last_seen = MAX(managed_gateways.last_seen, excluded.last_seen),
                    last_stats_at = CASE WHEN excluded.last_stats_at IS NOT NULL
                                         THEN MAX(COALESCE(managed_gateways.last_stats_at, 0), excluded.last_stats_at)
                                         ELSE managed_gateways.last_stats_at END,
                    last_uplink_at = CASE WHEN excluded.last_uplink_at IS NOT NULL
                                          THEN MAX(COALESCE(managed_gateways.last_uplink_at, 0), excluded.last_uplink_at)
                                          ELSE managed_gateways.last_uplink_at END,
                    last_conn_at = CASE WHEN excluded.last_conn_at IS NOT NULL
                                        THEN MAX(COALESCE(managed_gateways.last_conn_at, 0), excluded.last_conn_at)
                                        ELSE managed_gateways.last_conn_at END,
                    stats_count = managed_gateways.stats_count + excluded.stats_count,
                    uplink_count = managed_gateways.uplink_count + excluded.uplink_count,
                    conn_count = managed_gateways.conn_count + excluded.conn_count,
                    last_topic = CASE WHEN excluded.last_topic <> '' THEN excluded.last_topic
                                      ELSE managed_gateways.last_topic END,
                    updated_at = excluded.updated_at
                """,
                (
                    str(workspace_id),
                    gateway,
                    str(topic_root or ""),
                    now,
                    active_seen,
                    stats_at,
                    uplink_at,
                    conn_at,
                    1 if kind == "stats" else 0,
                    1 if kind == "uplink" else 0,
                    1 if kind == "conn" else 0,
                    str(topic or ""),
                    time.time(),
                ),
            )

    def bind_gateway_entity(self, workspace_id: str, gateway_id: str, atom_entity_id: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE managed_gateways
                SET atom_entity_id = ?, last_error = '', updated_at = ?
                WHERE workspace_id = ? AND gateway_id = ?
                """,
                (
                    str(atom_entity_id or ""),
                    time.time(),
                    str(workspace_id),
                    str(gateway_id or "").strip().lower(),
                ),
            )

    def record_gateway_error(self, workspace_id: str, gateway_id: str, error: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE managed_gateways SET last_error = ?, updated_at = ?
                WHERE workspace_id = ? AND gateway_id = ?
                """,
                (
                    str(error or "")[:500],
                    time.time(),
                    str(workspace_id),
                    str(gateway_id or "").strip().lower(),
                ),
            )

    @staticmethod
    def _gateway_public(row) -> dict[str, Any]:
        return {
            "workspace_id": str(row["workspace_id"]),
            "gateway_id": str(row["gateway_id"]),
            "atom_entity_id": str(row["atom_entity_id"] or ""),
            "topic_root": str(row["topic_root"] or ""),
            "first_seen": float(row["first_seen"]),
            "last_seen": float(row["last_seen"]),
            "last_stats_at": float(row["last_stats_at"]) if row["last_stats_at"] is not None else None,
            "last_uplink_at": float(row["last_uplink_at"]) if row["last_uplink_at"] is not None else None,
            "last_conn_at": float(row["last_conn_at"]) if row["last_conn_at"] is not None else None,
            "stats_count": int(row["stats_count"]),
            "uplink_count": int(row["uplink_count"]),
            "conn_count": int(row["conn_count"]),
            "last_topic": str(row["last_topic"] or ""),
            "last_error": str(row["last_error"] or ""),
        }

    def list_gateways(self, workspace_id: str, *, limit: int = 1000) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM managed_gateways
                WHERE workspace_id = ?
                ORDER BY last_seen DESC, gateway_id ASC
                LIMIT ?
                """,
                (str(workspace_id), int(limit)),
            ).fetchall()
        return [self._gateway_public(row) for row in rows]

    def find_gateway(self, workspace_id: str, gateway_id: str) -> Optional[dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM managed_gateways WHERE workspace_id = ? AND gateway_id = ?",
                (str(workspace_id), str(gateway_id or "").strip().lower()),
            ).fetchone()
        return None if row is None else self._gateway_public(row)

    def record_device_gateway(
        self,
        workspace_id: str,
        channel_id: str,
        external_id: str,
        gateway: Mapping[str, Any],
        *,
        observed_at: Optional[float] = None,
    ) -> None:
        gateway_id = str(gateway.get("gateway_id") or "").strip().lower()
        if not gateway_id:
            return
        seen = float(time.time() if observed_at is None else observed_at)
        self.record_gateway_event(
            workspace_id,
            gateway_id,
            event_kind="uplink",
            received_at=seen,
        )

        def finite(value: object) -> Optional[float]:
            try:
                result = float(value)
            except (TypeError, ValueError):
                return None
            return result if math.isfinite(result) else None

        rssi = finite(gateway.get("rssi"))
        snr = finite(gateway.get("snr"))
        channel = gateway.get("channel")
        rf_chain = gateway.get("rf_chain")
        try:
            channel_int = int(channel) if channel is not None else None
        except (TypeError, ValueError):
            channel_int = None
        try:
            rf_chain_int = int(rf_chain) if rf_chain is not None else None
        except (TypeError, ValueError):
            rf_chain_int = None
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO managed_device_gateways (
                    workspace_id, channel_id, external_id, gateway_id,
                    first_seen, last_seen, last_rssi, last_snr,
                    last_channel, last_rf_chain, last_crc_status, packet_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                ON CONFLICT(workspace_id, channel_id, external_id, gateway_id) DO UPDATE SET
                    last_seen = MAX(managed_device_gateways.last_seen, excluded.last_seen),
                    last_rssi = CASE WHEN excluded.last_seen >= managed_device_gateways.last_seen
                                     THEN excluded.last_rssi ELSE managed_device_gateways.last_rssi END,
                    last_snr = CASE WHEN excluded.last_seen >= managed_device_gateways.last_seen
                                    THEN excluded.last_snr ELSE managed_device_gateways.last_snr END,
                    last_channel = CASE WHEN excluded.last_seen >= managed_device_gateways.last_seen
                                        THEN excluded.last_channel ELSE managed_device_gateways.last_channel END,
                    last_rf_chain = CASE WHEN excluded.last_seen >= managed_device_gateways.last_seen
                                         THEN excluded.last_rf_chain ELSE managed_device_gateways.last_rf_chain END,
                    last_crc_status = CASE WHEN excluded.last_seen >= managed_device_gateways.last_seen
                                           THEN excluded.last_crc_status ELSE managed_device_gateways.last_crc_status END,
                    packet_count = managed_device_gateways.packet_count + 1
                """,
                (
                    str(workspace_id),
                    str(channel_id),
                    str(external_id),
                    gateway_id,
                    seen,
                    seen,
                    rssi,
                    snr,
                    channel_int,
                    rf_chain_int,
                    str(gateway.get("crc_status") or ""),
                ),
            )

    def list_device_gateways(
        self,
        workspace_id: str,
        channel_id: str,
        external_id: str,
    ) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT gateway_id, first_seen, last_seen, last_rssi, last_snr,
                       last_channel, last_rf_chain, last_crc_status, packet_count
                FROM managed_device_gateways
                WHERE workspace_id = ? AND channel_id = ? AND external_id = ?
                ORDER BY last_seen DESC, gateway_id ASC
                """,
                (str(workspace_id), str(channel_id), str(external_id)),
            ).fetchall()
        return [
            {
                "gateway_id": str(row["gateway_id"]),
                "first_seen": float(row["first_seen"]),
                "last_seen": float(row["last_seen"]),
                "rssi": float(row["last_rssi"]) if row["last_rssi"] is not None else None,
                "snr": float(row["last_snr"]) if row["last_snr"] is not None else None,
                "channel": int(row["last_channel"]) if row["last_channel"] is not None else None,
                "rf_chain": int(row["last_rf_chain"]) if row["last_rf_chain"] is not None else None,
                "crc_status": str(row["last_crc_status"] or ""),
                "packet_count": int(row["packet_count"]),
            }
            for row in rows
        ]


class GatewayRegistry:
    """Persist gateway heartbeat first, then best-effort reconcile its Atom entity."""

    def __init__(self, *, runtime, control, store: GatewayTopologySQLiteManagementStore) -> None:
        self.runtime = runtime
        self.control = control
        self.store = store
        self._profile = None
        self._entities: dict[str, str] = {}

    def _base(self):
        self.runtime.bootstrap()
        if self.runtime.base is None:
            raise RuntimeError("runtime is not bootstrapped")
        return self.runtime.base

    def _profile_ref(self, workspace_id: str):
        if self._profile is None:
            self._profile = self.control.ensure_device_type(
                workspace_id,
                key=GATEWAY_PROFILE_KEY,
                name=GATEWAY_PROFILE_NAME,
                description=GATEWAY_PROFILE_DESCRIPTION,
                json_schema=GATEWAY_PROFILE_SCHEMA,
            )
        return self._profile

    def _ensure_atom_entity(self, workspace_id: str, gateway_id: str, topic_root: str) -> str:
        cached = self._entities.get(gateway_id)
        if cached:
            return cached
        profile = self._profile_ref(workspace_id)
        external_id = "lorawan-gateway-" + gateway_id
        exact = [
            item
            for item in self.control.atom.list_devices(workspace_id, external_id=external_id, limit=10)
            if str(item.get("externalId") or "") == external_id
        ]
        if len(exact) > 1:
            raise RuntimeError(f"multiple Atom gateway entities share external_id {external_id!r}")
        if exact:
            entity = exact[0]
            profile_mismatch = (
                str(entity.get("profileId") or "") != profile.id
                or str(entity.get("profileVersionId") or "") != profile.version_id
            )
            if profile_mismatch:
                attributes = entity.get("attributes") or {}
                if not isinstance(attributes, dict) or attributes.get("managed_by") != "smarter-adapter":
                    raise RuntimeError(f"gateway entity {external_id!r} is not SMA-owned")
                entity = self.control._update_device_profile(str(entity.get("id") or ""), profile)
        else:
            entity = self.control.atom.create_device(
                workspace_id,
                external_id,
                profile_id=profile.id,
                profile_version_id=profile.version_id,
                name="LoRaWAN Gateway " + gateway_id,
                alias="lorawan-gateway-" + gateway_id,
                attributes={
                    "managed_by": "smarter-adapter",
                    "role": "lorawan-gateway",
                    "gateway_id": gateway_id,
                    "topic_root": topic_root,
                },
            )
        entity_id = str(entity.get("id") or "")
        if not entity_id:
            raise RuntimeError("Atom gateway entity response is missing id")
        self._entities[gateway_id] = entity_id
        return entity_id

    def observe(
        self,
        *,
        gateway_id: str,
        event_kind: str,
        topic: str,
        topic_root: str,
        received_at: float,
        retained: bool,
    ) -> None:
        base = self._base()
        gateway = str(gateway_id).strip().lower()
        self.store.record_gateway_event(
            base.workspace.id,
            gateway,
            event_kind=event_kind,
            received_at=received_at,
            topic=topic,
            topic_root=topic_root,
            retained=retained,
        )
        current = self.store.find_gateway(base.workspace.id, gateway)
        if current and current.get("atom_entity_id"):
            self._entities[gateway] = str(current["atom_entity_id"])
            return
        try:
            entity_id = self._ensure_atom_entity(base.workspace.id, gateway, topic_root)
            self.store.bind_gateway_entity(base.workspace.id, gateway, entity_id)
        except Exception as exc:
            self.store.record_gateway_error(
                base.workspace.id,
                gateway,
                f"{type(exc).__name__}: {exc}",
            )
            raise


@dataclass(frozen=True)
class GatewayMqttConfig:
    host: str
    port: int = 1883
    qos: int = 0
    username: str = ""
    password: str = ""
    client_id: str = "smarter-adapter-gateway-monitor"
    topics: tuple[str, ...] = (
        "+/gateway/+/event/stats",
        "gateway/+/state/conn",
        "+/gateway/+/state/conn",
    )


class GatewayMqttObserver:
    """Read-only MQTT side input for gateway presence/control-plane discovery."""

    def __init__(self, config: GatewayMqttConfig, registry: GatewayRegistry) -> None:
        self.config = config
        self.registry = registry
        self.connected = False
        self.last_error: Optional[str] = None
        self._client = None

    @staticmethod
    def _reason_value(reason_code: object) -> int:
        value = getattr(reason_code, "value", reason_code)
        try:
            return int(value)
        except (TypeError, ValueError):
            return 1

    @staticmethod
    def parse_topic(topic: str) -> Optional[tuple[str, str, str]]:
        match = _GATEWAY_TOPIC_RE.match(str(topic or ""))
        if not match:
            return None
        event = str(match.group("event") or "").lower()
        section = str(match.group("section") or "").lower()
        if section == "event" and event == "stats":
            kind = "stats"
        elif section == "state" and event == "conn":
            kind = "conn"
        else:
            return None
        return (
            str(match.group("gateway_id") or "").lower(),
            kind,
            str(match.group("root") or ""),
        )

    def start(self) -> None:
        try:
            import paho.mqtt.client as mqtt
        except ImportError as exc:
            raise RuntimeError("paho-mqtt is required for gateway monitoring") from exc

        kwargs: dict[str, Any] = {
            "client_id": self.config.client_id + "-" + str(os.getpid()),
            "protocol": mqtt.MQTTv311,
            "transport": "tcp",
        }
        callback_api = getattr(mqtt, "CallbackAPIVersion", None)
        if callback_api is not None:
            kwargs["callback_api_version"] = callback_api.VERSION2
        client = mqtt.Client(**kwargs)
        if self.config.username:
            client.username_pw_set(self.config.username, self.config.password or None)

        def on_connect(client, userdata, flags, reason_code, properties=None):
            rc = self._reason_value(reason_code)
            if rc != 0:
                self.connected = False
                self.last_error = f"MQTT connect rc={rc}"
                return
            subscriptions = [(topic, self.config.qos) for topic in self.config.topics]
            result, _mid = client.subscribe(subscriptions)
            if result != mqtt.MQTT_ERR_SUCCESS:
                self.connected = False
                self.last_error = f"MQTT subscribe rc={result}"
                return
            self.connected = True
            self.last_error = None

        def on_disconnect(client, userdata, *callback_args):
            self.connected = False

        def on_message(client, userdata, msg):
            parsed = self.parse_topic(msg.topic)
            if parsed is None:
                return
            gateway_id, kind, root = parsed
            try:
                self.registry.observe(
                    gateway_id=gateway_id,
                    event_kind=kind,
                    topic=msg.topic,
                    topic_root=root,
                    received_at=time.time(),
                    retained=bool(msg.retain),
                )
                self.last_error = None
            except Exception as exc:
                self.last_error = f"{type(exc).__name__}: {exc}"
                logger.warning("gateway observation failed gateway=%s: %s", gateway_id, exc)

        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_message = on_message
        client.connect(self.config.host, int(self.config.port), keepalive=60)
        client.loop_start()
        self._client = client

    def stop(self) -> None:
        client = self._client
        self._client = None
        if client is None:
            return
        try:
            client.disconnect()
        finally:
            client.loop_stop()
        self.connected = False


__all__ = [
    "GATEWAY_PROFILE_KEY",
    "GatewayMqttConfig",
    "GatewayMqttObserver",
    "GatewayPresencePolicy",
    "GatewayRegistry",
    "GatewayTopologySQLiteManagementStore",
]
