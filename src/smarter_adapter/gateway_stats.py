from __future__ import annotations

import json
import logging
import math
import os
import struct
import time
from typing import Any, Mapping, Optional

from .gateway_monitor import (
    GatewayMqttObserver as BaseGatewayMqttObserver,
    GatewayTopologySQLiteManagementStore as BaseGatewayTopologySQLiteManagementStore,
)


logger = logging.getLogger("smarter_adapter.gateway.stats")


class GatewayStatsDecodeError(ValueError):
    """Gateway statistics payload is not a supported ChirpStack representation."""


class _Wire:
    """Minimal protobuf wire reader for ChirpStack ``gw.GatewayStats``.

    SMA deliberately decodes only the stable fields it needs operationally.
    Keeping this wire reader local avoids adding generated ChirpStack bindings
    (and their transitive dependency surface) just to inspect one MQTT message.
    Unknown protobuf fields are skipped according to their wire type, so newer
    ChirpStack versions remain forward-compatible as long as the fields we use
    keep their published field numbers.
    """

    @staticmethod
    def varint(data: bytes, offset: int) -> tuple[int, int]:
        value = 0
        shift = 0
        pos = int(offset)
        while pos < len(data) and shift <= 70:
            byte = data[pos]
            pos += 1
            value |= (byte & 0x7F) << shift
            if not (byte & 0x80):
                return value, pos
            shift += 7
        raise GatewayStatsDecodeError("invalid/truncated protobuf varint")

    @classmethod
    def fields(cls, data: bytes):
        pos = 0
        size = len(data)
        while pos < size:
            key, pos = cls.varint(data, pos)
            field_number = key >> 3
            wire_type = key & 0x07
            if field_number <= 0:
                raise GatewayStatsDecodeError("invalid protobuf field number")

            if wire_type == 0:
                value, pos = cls.varint(data, pos)
            elif wire_type == 1:
                end = pos + 8
                if end > size:
                    raise GatewayStatsDecodeError("truncated protobuf fixed64")
                value = data[pos:end]
                pos = end
            elif wire_type == 2:
                length, pos = cls.varint(data, pos)
                end = pos + int(length)
                if end > size:
                    raise GatewayStatsDecodeError("truncated protobuf bytes field")
                value = data[pos:end]
                pos = end
            elif wire_type == 5:
                end = pos + 4
                if end > size:
                    raise GatewayStatsDecodeError("truncated protobuf fixed32")
                value = data[pos:end]
                pos = end
            else:
                raise GatewayStatsDecodeError(
                    f"unsupported protobuf wire type {wire_type}"
                )
            yield field_number, wire_type, value


def _decode_utf8(value: bytes) -> str:
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise GatewayStatsDecodeError("invalid UTF-8 protobuf string") from exc


def _timestamp(value: bytes) -> Optional[float]:
    seconds = 0
    nanos = 0
    found = False
    for field, wire, raw in _Wire.fields(value):
        if field == 1 and wire == 0:
            seconds = int(raw)
            found = True
        elif field == 2 and wire == 0:
            nanos = int(raw)
    if not found:
        return None
    return float(seconds) + (float(nanos) / 1_000_000_000.0)


def _location(value: bytes) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for field, wire, raw in _Wire.fields(value):
        if field in {1, 2, 3} and wire == 1:
            number = struct.unpack("<d", raw)[0]
            key = {1: "latitude", 2: "longitude", 3: "altitude"}[field]
            if math.isfinite(number):
                result[key] = number
        elif field == 4 and wire == 0:
            result["source"] = int(raw)
        elif field == 5 and wire == 5:
            number = struct.unpack("<f", raw)[0]
            if math.isfinite(number):
                result["accuracy"] = number
    return result


def _map_string_string(value: bytes) -> tuple[str, str]:
    key = ""
    result = ""
    for field, wire, raw in _Wire.fields(value):
        if field == 1 and wire == 2:
            key = _decode_utf8(raw)
        elif field == 2 and wire == 2:
            result = _decode_utf8(raw)
    return key, result


def _map_uint_uint(value: bytes) -> tuple[int, int]:
    key = 0
    result = 0
    for field, wire, raw in _Wire.fields(value):
        if field == 1 and wire == 0:
            key = int(raw)
        elif field == 2 and wire == 0:
            result = int(raw)
    return key, result


def _map_string_uint(value: bytes) -> tuple[str, int]:
    key = ""
    result = 0
    for field, wire, raw in _Wire.fields(value):
        if field == 1 and wire == 2:
            key = _decode_utf8(raw)
        elif field == 2 and wire == 0:
            result = int(raw)
    return key, result


def _finite_float(value: object) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _metadata_first(metadata: Mapping[str, Any], *keys: str) -> Optional[str]:
    lowered = {str(key).lower(): value for key, value in metadata.items()}
    for key in keys:
        value = lowered.get(key.lower())
        if value not in (None, ""):
            return str(value)
    return None


def _health(metadata: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    model = _metadata_first(metadata, "model", "gateway_model", "hardware_model")
    if model:
        result["model"] = model

    forwarder = _metadata_first(
        metadata,
        "mqtt_forwarder_version",
        "mqtt-forwarder-version",
        "gateway_bridge_version",
        "gateway-bridge-version",
        "forwarder_version",
    )
    if forwarder:
        result["forwarder_version"] = forwarder

    raw_temp = _metadata_first(
        metadata,
        "concentrator_temp",
        "concentrator_temperature",
        "concentrator_temperature_c",
        "temperature",
    )
    temperature = _finite_float(raw_temp)
    if temperature is not None:
        result["concentrator_temperature_c"] = temperature
    return result


class ChirpStackGatewayStatsDecoder:
    """Decode ChirpStack Gateway Bridge gateway stats (protobuf or JSON)."""

    name = "chirpstack-gateway-stats-v1"
    schema = "smarter-adapter.chirpstack-gateway-stats/1"

    @classmethod
    def _normalize_json(
        cls,
        raw: Mapping[str, Any],
        *,
        gateway_id_hint: str,
    ) -> dict[str, Any]:
        def first(*names: str, default=None):
            for name in names:
                if name in raw:
                    return raw[name]
            return default

        metadata_raw = first("metadata", default={})
        metadata = (
            {str(key): str(value) for key, value in metadata_raw.items()}
            if isinstance(metadata_raw, Mapping)
            else {}
        )
        gateway_id = str(
            first("gatewayId", "gateway_id", default=gateway_id_hint) or gateway_id_hint
        ).strip().lower()
        counters = {
            "rx_packets_received": int(first("rxPacketsReceived", "rx_packets_received", default=0) or 0),
            "rx_packets_received_ok": int(first("rxPacketsReceivedOk", "rx_packets_received_ok", default=0) or 0),
            "tx_packets_received": int(first("txPacketsReceived", "tx_packets_received", default=0) or 0),
            "tx_packets_emitted": int(first("txPacketsEmitted", "tx_packets_emitted", default=0) or 0),
        }
        result: dict[str, Any] = {
            "schema": cls.schema,
            "gateway_id": gateway_id,
            "config_version": str(first("configVersion", "config_version", default="") or ""),
            "counters": counters,
            "metadata": metadata,
            "health": _health(metadata),
        }
        timestamp = first("time")
        if isinstance(timestamp, str):
            # Preserve the source representation; parsing this is optional for
            # liveness because received_at remains authoritative in SMA.
            result["gateway_time"] = timestamp
        location = first("location")
        if isinstance(location, Mapping):
            result["location"] = dict(location)
        for source, target in (
            (("txPacketsPerFrequency", "tx_packets_per_frequency"), "tx_packets_per_frequency"),
            (("rxPacketsPerFrequency", "rx_packets_per_frequency"), "rx_packets_per_frequency"),
            (("txPacketsPerStatus", "tx_packets_per_status"), "tx_packets_per_status"),
        ):
            value = None
            for key in source:
                if key in raw:
                    value = raw[key]
                    break
            if isinstance(value, Mapping):
                result[target] = {str(key): int(item) for key, item in value.items()}
        return result

    @classmethod
    def _decode_protobuf(
        cls,
        payload: bytes,
        *,
        gateway_id_hint: str,
    ) -> dict[str, Any]:
        gateway_id = ""
        legacy_gateway_id = ""
        gateway_time: Optional[float] = None
        location: dict[str, Any] = {}
        config_version = ""
        metadata: dict[str, str] = {}
        counters = {
            "rx_packets_received": 0,
            "rx_packets_received_ok": 0,
            "tx_packets_received": 0,
            "tx_packets_emitted": 0,
        }
        tx_frequency: dict[str, int] = {}
        rx_frequency: dict[str, int] = {}
        tx_status: dict[str, int] = {}

        seen = False
        for field, wire, raw in _Wire.fields(payload):
            seen = True
            if field == 1 and wire == 2:
                legacy_gateway_id = bytes(raw).hex()
            elif field == 17 and wire == 2:
                gateway_id = _decode_utf8(raw).strip().lower()
            elif field == 2 and wire == 2:
                gateway_time = _timestamp(raw)
            elif field == 3 and wire == 2:
                location = _location(raw)
            elif field == 4 and wire == 2:
                config_version = _decode_utf8(raw)
            elif field == 5 and wire == 0:
                counters["rx_packets_received"] = int(raw)
            elif field == 6 and wire == 0:
                counters["rx_packets_received_ok"] = int(raw)
            elif field == 7 and wire == 0:
                counters["tx_packets_received"] = int(raw)
            elif field == 8 and wire == 0:
                counters["tx_packets_emitted"] = int(raw)
            elif field == 10 and wire == 2:
                key, value = _map_string_string(raw)
                if key:
                    metadata[key] = value
            elif field == 12 and wire == 2:
                key, value = _map_uint_uint(raw)
                tx_frequency[str(key)] = value
            elif field == 13 and wire == 2:
                key, value = _map_uint_uint(raw)
                rx_frequency[str(key)] = value
            elif field == 16 and wire == 2:
                key, value = _map_string_uint(raw)
                if key:
                    tx_status[key] = value

        if not seen:
            raise GatewayStatsDecodeError("empty GatewayStats protobuf payload")

        gateway_id = gateway_id or legacy_gateway_id or str(gateway_id_hint or "").lower()
        result: dict[str, Any] = {
            "schema": cls.schema,
            "gateway_id": gateway_id,
            "config_version": config_version,
            "counters": counters,
            "metadata": metadata,
            "health": _health(metadata),
        }
        if gateway_time is not None:
            result["gateway_time"] = gateway_time
        if location:
            result["location"] = location
        if tx_frequency:
            result["tx_packets_per_frequency"] = tx_frequency
        if rx_frequency:
            result["rx_packets_per_frequency"] = rx_frequency
        if tx_status:
            result["tx_packets_per_status"] = tx_status
        return result

    @classmethod
    def decode(
        cls,
        payload: bytes,
        *,
        gateway_id_hint: str = "",
    ) -> dict[str, Any]:
        raw = bytes(payload or b"")
        if not raw:
            raise GatewayStatsDecodeError("empty gateway stats payload")
        if raw.lstrip().startswith(b"{"):
            try:
                value = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise GatewayStatsDecodeError("invalid gateway stats JSON") from exc
            if not isinstance(value, Mapping):
                raise GatewayStatsDecodeError("gateway stats JSON must be an object")
            return cls._normalize_json(value, gateway_id_hint=gateway_id_hint)
        return cls._decode_protobuf(raw, gateway_id_hint=gateway_id_hint)


class GatewayStatsTopologySQLiteManagementStore(BaseGatewayTopologySQLiteManagementStore):
    """Gateway topology store with the latest successfully decoded stats snapshot."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        with self._lock, self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS managed_gateway_stats (
                    workspace_id TEXT NOT NULL,
                    gateway_id TEXT NOT NULL,
                    stats_json TEXT NOT NULL DEFAULT '{}',
                    decoder TEXT NOT NULL DEFAULT '',
                    observed_at REAL,
                    last_decode_error TEXT NOT NULL DEFAULT '',
                    last_decode_error_at REAL,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (workspace_id, gateway_id)
                )
                """
            )

    def record_gateway_stats(
        self,
        workspace_id: str,
        gateway_id: str,
        *,
        stats: Optional[Mapping[str, Any]] = None,
        decoder: str = "",
        observed_at: Optional[float] = None,
        decode_error: str = "",
    ) -> None:
        gateway = str(gateway_id or "").strip().lower()
        if not gateway:
            raise ValueError("gateway_id must not be empty")
        seen = float(time.time() if observed_at is None else observed_at)
        now = time.time()
        body = dict(stats or {})
        encoded = json.dumps(body, ensure_ascii=False, separators=(",", ":"))
        error = str(decode_error or "")[:500]
        with self._lock, self._conn:
            row = self._conn.execute(
                """
                SELECT stats_json, decoder, observed_at,
                       last_decode_error, last_decode_error_at
                FROM managed_gateway_stats
                WHERE workspace_id = ? AND gateway_id = ?
                """,
                (str(workspace_id), gateway),
            ).fetchone()
            if row is None:
                self._conn.execute(
                    """
                    INSERT INTO managed_gateway_stats (
                        workspace_id, gateway_id, stats_json, decoder, observed_at,
                        last_decode_error, last_decode_error_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(workspace_id),
                        gateway,
                        encoded if stats is not None else "{}",
                        str(decoder or "") if stats is not None else "",
                        seen if stats is not None else None,
                        error,
                        seen if error else None,
                        now,
                    ),
                )
                return

            if stats is not None:
                previous = row["observed_at"]
                if previous is None or seen >= float(previous):
                    self._conn.execute(
                        """
                        UPDATE managed_gateway_stats
                        SET stats_json = ?, decoder = ?, observed_at = ?,
                            last_decode_error = '', last_decode_error_at = NULL,
                            updated_at = ?
                        WHERE workspace_id = ? AND gateway_id = ?
                        """,
                        (
                            encoded,
                            str(decoder or ""),
                            seen,
                            now,
                            str(workspace_id),
                            gateway,
                        ),
                    )
            elif error:
                self._conn.execute(
                    """
                    UPDATE managed_gateway_stats
                    SET last_decode_error = ?, last_decode_error_at = ?, updated_at = ?
                    WHERE workspace_id = ? AND gateway_id = ?
                    """,
                    (error, seen, now, str(workspace_id), gateway),
                )

    def _stats_for_gateway(self, workspace_id: str, gateway_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT stats_json, decoder, observed_at,
                       last_decode_error, last_decode_error_at
                FROM managed_gateway_stats
                WHERE workspace_id = ? AND gateway_id = ?
                """,
                (str(workspace_id), str(gateway_id or "").strip().lower()),
            ).fetchone()
        if row is None:
            return {
                "stats": {},
                "stats_decoder": "",
                "stats_observed_at": None,
                "stats_decode_error": "",
                "stats_decode_error_at": None,
            }
        try:
            stats = json.loads(str(row["stats_json"] or "{}"))
        except json.JSONDecodeError:
            stats = {}
        if not isinstance(stats, dict):
            stats = {}
        return {
            "stats": stats,
            "stats_decoder": str(row["decoder"] or ""),
            "stats_observed_at": (
                float(row["observed_at"]) if row["observed_at"] is not None else None
            ),
            "stats_decode_error": str(row["last_decode_error"] or ""),
            "stats_decode_error_at": (
                float(row["last_decode_error_at"])
                if row["last_decode_error_at"] is not None
                else None
            ),
        }

    def _enrich_stats(self, item: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
        if item is None:
            return None
        result = dict(item)
        result.update(
            self._stats_for_gateway(
                str(result.get("workspace_id") or ""),
                str(result.get("gateway_id") or ""),
            )
        )
        return result

    def list_gateways(self, workspace_id: str, *, limit: int = 1000) -> list[dict[str, Any]]:
        return [
            self._enrich_stats(item) or item
            for item in super().list_gateways(workspace_id, limit=limit)
        ]

    def find_gateway(self, workspace_id: str, gateway_id: str) -> Optional[dict[str, Any]]:
        return self._enrich_stats(super().find_gateway(workspace_id, gateway_id))


class GatewayStatsMqttObserver(BaseGatewayMqttObserver):
    """Gateway observer that also decodes ``event/stats`` payloads."""

    def __init__(self, config, registry, *, decoder=ChirpStackGatewayStatsDecoder) -> None:
        super().__init__(config, registry)
        self.decoder = decoder

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
            received_at = time.time()
            try:
                # Presence/entity discovery never depends on protobuf decoding.
                self.registry.observe(
                    gateway_id=gateway_id,
                    event_kind=kind,
                    topic=msg.topic,
                    topic_root=root,
                    received_at=received_at,
                    retained=bool(msg.retain),
                )

                if kind == "stats":
                    store = self.registry.store
                    recorder = getattr(store, "record_gateway_stats", None)
                    if callable(recorder):
                        try:
                            stats = self.decoder.decode(
                                bytes(msg.payload or b""),
                                gateway_id_hint=gateway_id,
                            )
                            base = self.registry.runtime.base
                            if base is not None:
                                recorder(
                                    base.workspace.id,
                                    gateway_id,
                                    stats=stats,
                                    decoder=self.decoder.name,
                                    observed_at=received_at,
                                )
                            self.last_error = None
                        except Exception as exc:
                            base = self.registry.runtime.base
                            if base is not None:
                                recorder(
                                    base.workspace.id,
                                    gateway_id,
                                    stats=None,
                                    decoder=self.decoder.name,
                                    observed_at=received_at,
                                    decode_error=f"{type(exc).__name__}: {exc}",
                                )
                            self.last_error = f"gateway stats decode: {type(exc).__name__}: {exc}"
                            logger.warning(
                                "gateway stats decode failed gateway=%s: %s",
                                gateway_id,
                                exc,
                            )
                else:
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


__all__ = [
    "ChirpStackGatewayStatsDecoder",
    "GatewayStatsDecodeError",
    "GatewayStatsMqttObserver",
    "GatewayStatsTopologySQLiteManagementStore",
]
