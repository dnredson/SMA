from __future__ import annotations

import base64
import datetime as dt
import json
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Mapping, Optional, Tuple

from ..models import Measurement, ParsedEvent, RawEvent


@dataclass(frozen=True)
class IrrigapNode:
    """Deployment metadata for one Irrigap/Greenstick node."""

    id: str
    device: str
    location: str
    sub_location: str
    depths: Mapping[int, str] = field(default_factory=dict)


_KEY_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
NodeResolver = Callable[[str], Optional[IrrigapNode]]


DEFAULT_PORT_ROLES: Mapping[int, str] = {
    1: "battery",
}


def _iso_epoch(value: Any) -> Optional[float]:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.timestamp()
    except ValueError:
        return None


def _pairs(text: str) -> Dict[str, str]:
    parts = [part.strip() for part in text.split("|") if part.strip() != ""]
    result: Dict[str, str] = {}
    index = 0
    while index + 1 < len(parts):
        key = parts[index]
        value = parts[index + 1]
        if _KEY_RE.match(key):
            result[key.upper()] = value
            index += 2
        else:
            index += 1
    return result


def _first_numeric(mapping: Mapping[str, str], prefix: str) -> Optional[float]:
    candidates = sorted(key for key in mapping if key.startswith(prefix.upper()))
    for key in candidates:
        try:
            return float(mapping[key])
        except (TypeError, ValueError):
            continue
    return None


def _exact_numeric(mapping: Mapping[str, str], key: str) -> Optional[float]:
    raw = mapping.get(key.upper())
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _greenstick_vwc(node_id: str, raw: float) -> float:
    """Calibration provided by the Irrigap deployment collaborator.

    The node ID is interpreted as hexadecimal, matching the existing Node-RED
    flow. Values outside the physical 0..100 % range are reported as 0, also
    matching that flow. Source sentinels are filtered before this function is
    called, so an invalid ``-1`` cannot become a plausible ``0 %`` reading.
    """

    try:
        numeric_id = int(node_id, 16)
    except ValueError:
        numeric_id = 0

    if numeric_id < 0x3000:
        value = (0.0003879 * raw - 0.6956) * 100.0
    else:
        value = 100.0 * (-0.000000003 * raw**2 + 0.0003 * raw + 0.0799)

    if 0.0 < value < 100.0:
        return value
    return 0.0


def _invalid_source(source_field: str) -> Dict[str, Any]:
    return {
        "quality": "invalid",
        "quality_reason": "source_sentinel",
        "source_field": source_field,
    }


def _normalized_rx_info(envelope: Mapping[str, Any]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    raw_items = envelope.get("rxInfo")
    if not isinstance(raw_items, list):
        return result
    for raw in raw_items:
        if not isinstance(raw, Mapping):
            continue
        gateway_id = str(raw.get("gatewayId") or "").strip().lower()
        if not gateway_id:
            continue
        item: dict[str, Any] = {"gateway_id": gateway_id}
        for source_key, target_key in (
            ("rssi", "rssi"),
            ("snr", "snr"),
            ("channel", "channel"),
            ("rfChain", "rf_chain"),
            ("crcStatus", "crc_status"),
        ):
            value = raw.get(source_key)
            if value is not None:
                item[target_key] = value
        result.append(item)
    return result


def _mapping_value(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None


def _optional_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalized_tx_info(envelope: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize frame-level ChirpStack radio parameters when present.

    In the ChirpStack application uplink event, ``txInfo`` describes the radio
    transmission shared by all gateway receptions in ``rxInfo``. Keep it
    separate from sensor semantics so every LoRaWAN sensor can reuse RF health.
    """

    raw = _mapping_value(envelope, "txInfo", "tx_info")
    if not isinstance(raw, Mapping):
        return {}

    result: dict[str, Any] = {}
    frequency = _optional_int(_mapping_value(raw, "frequency", "frequencyHz", "frequency_hz"))
    if frequency is not None:
        result["frequency_hz"] = frequency

    modulation = _mapping_value(raw, "modulation")
    if not isinstance(modulation, Mapping):
        return result

    lora = _mapping_value(modulation, "lora", "LoRa")
    if isinstance(lora, Mapping):
        result["modulation"] = "lora"
        spreading_factor = _optional_int(
            _mapping_value(lora, "spreadingFactor", "spreading_factor")
        )
        bandwidth = _optional_int(_mapping_value(lora, "bandwidth", "bandwidthHz", "bandwidth_hz"))
        code_rate = str(_mapping_value(lora, "codeRate", "code_rate") or "").strip()
        if spreading_factor is not None:
            result["spreading_factor"] = spreading_factor
        if bandwidth is not None:
            result["bandwidth_hz"] = bandwidth
        if code_rate:
            result["code_rate"] = code_rate
        return result

    fsk = _mapping_value(modulation, "fsk", "FSK")
    if isinstance(fsk, Mapping):
        result["modulation"] = "fsk"
        bitrate = _optional_int(_mapping_value(fsk, "datarate", "dataRate", "bitrate"))
        if bitrate is not None:
            result["bitrate_bps"] = bitrate
    return result


class IrrigapChirpStackParser:
    """Parse the ChirpStack MQTT envelope used by the Irrigap deployment.

    The same physical device can send different logical payloads on different
    LoRaWAN fPorts. Payload keys remain authoritative for interpretation while
    the fPort is preserved as transport metadata and as a configurable role
    hint. This prevents a battery frame (for example ``VB``/``BT`` on fPort 1)
    from being misread positionally as soil moisture/temperature.
    """

    name = "chirpstack-irrigap-v2"

    def __init__(
        self,
        nodes: Tuple[IrrigapNode, ...] = (),
        *,
        node_resolver: Optional[NodeResolver] = None,
        port_roles: Mapping[int, str] = DEFAULT_PORT_ROLES,
    ) -> None:
        self._nodes = {node.id.upper(): node for node in nodes}
        self._node_resolver = node_resolver
        self._port_roles = {
            int(port): str(role).strip().lower()
            for port, role in port_roles.items()
            if str(role).strip()
        }

    def _resolve_node(self, node_id: str) -> Optional[IrrigapNode]:
        if self._node_resolver is not None:
            return self._node_resolver(node_id)
        return self._nodes.get(node_id.upper())

    def supports(self, event: RawEvent) -> bool:
        if not event.payload.lstrip().startswith(b"{"):
            return False
        try:
            payload = json.loads(event.payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return False
        return (
            isinstance(payload, dict)
            and isinstance(payload.get("data"), str)
            and isinstance(payload.get("deviceInfo"), dict)
            and "fPort" in payload
        )

    @staticmethod
    def _message_role(values: Mapping[str, str]) -> str:
        if any(key in values for key in ("VB", "BT")):
            return "battery"
        if any(
            key.startswith(prefix)
            for key in values
            for prefix in ("M", "T", "C")
        ):
            return "soil"
        return "unknown"

    def _port_role(self, f_port: int, node: Optional[IrrigapNode]) -> str:
        explicit = self._port_roles.get(int(f_port))
        if explicit:
            return explicit
        if node is not None and int(f_port) in node.depths:
            return "soil"
        return "unknown"

    def parse(self, event: RawEvent) -> Optional[ParsedEvent]:
        try:
            envelope = json.loads(event.payload.decode("utf-8"))
            raw_text = base64.b64decode(str(envelope["data"]), validate=True).decode(
                "utf-8", errors="strict"
            )
        except (KeyError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
            return None

        values = _pairs(raw_text)
        node_id = str(values.get("I") or "").strip().upper()
        if not node_id:
            pieces = raw_text.split("|")
            if len(pieces) > 3:
                node_id = pieces[3].strip().upper()
        if not node_id:
            return None

        device_info = envelope.get("deviceInfo") or {}
        device_name = str(device_info.get("deviceName") or node_id).strip()
        if not device_name:
            return None

        try:
            f_port = int(envelope.get("fPort"))
        except (TypeError, ValueError):
            return None

        node = self._resolve_node(node_id)
        timestamp = _iso_epoch(envelope.get("time")) or event.received_at
        message_role = self._message_role(values)
        port_role = self._port_role(f_port, node)

        measurements: list[Measurement] = []

        if message_role == "battery":
            voltage = _exact_numeric(values, "VB")
            level = _exact_numeric(values, "BT")
            if voltage is not None:
                measurements.append(
                    Measurement(
                        name="battery.voltage",
                        value=voltage,
                        unit="V",
                        timestamp=timestamp,
                        metadata=_invalid_source("battery_voltage") if voltage < 0 else {},
                    )
                )
            if level is not None:
                measurements.append(
                    Measurement(
                        name="battery.level",
                        value=level,
                        unit="%",
                        timestamp=timestamp,
                        metadata=_invalid_source("battery_level") if level < 0 else {},
                    )
                )
        elif message_role == "soil":
            moisture_raw = _first_numeric(values, "M")
            temperature = _first_numeric(values, "T")
            ec_raw = _first_numeric(values, "C")

            moisture_invalid = moisture_raw is not None and moisture_raw < 0.0
            ec_invalid = ec_raw is not None and ec_raw < 0.0
            packet_sentinel = bool(
                moisture_invalid
                and ec_invalid
                and temperature is not None
                and temperature == -1.0
            )

            if moisture_raw is not None:
                if not moisture_invalid:
                    # Greenstick calibration is only valid for Greenstick. For
                    # Teros12 we preserve the raw reading until a family-specific
                    # calibration is explicitly configured/validated.
                    if node is not None and str(node.device).strip().lower() == "greenstick":
                        measurements.append(
                            Measurement(
                                name="soil.moisture",
                                value=_greenstick_vwc(node_id, moisture_raw),
                                unit="%",
                                timestamp=timestamp,
                            )
                        )
                measurements.append(
                    Measurement(
                        name="soil.raw.moisture_m1",
                        value=moisture_raw,
                        unit="mV",
                        timestamp=timestamp,
                        metadata=_invalid_source("moisture") if moisture_invalid else {},
                    )
                )
            if temperature is not None:
                measurements.append(
                    Measurement(
                        name="soil.temperature",
                        value=temperature,
                        unit="Cel",
                        timestamp=timestamp,
                        metadata=_invalid_source("temperature") if packet_sentinel else {},
                    )
                )
            if ec_raw is not None:
                ec_metadata = _invalid_source("electrical_conductivity") if ec_invalid else {}
                measurements.append(
                    Measurement(
                        name="soil.electrical_conductivity",
                        value=ec_raw,
                        timestamp=timestamp,
                        metadata=ec_metadata,
                    )
                )
                measurements.append(
                    Measurement(
                        name="soil.raw.ec_c1",
                        value=ec_raw,
                        unit="mV",
                        timestamp=timestamp,
                        metadata=ec_metadata,
                    )
                )

        if not measurements:
            return None

        metadata: Dict[str, Any] = {
            "source": event.source,
            "topic": event.topic,
            "transport": {
                "type": "chirpstack",
                "mqtt_topic": event.topic,
                "f_port": f_port,
                "application_id": str(device_info.get("applicationId") or ""),
                "port_role": port_role,
            },
            "sensor": node.device if node else "irrigap",
            "device_id": device_name,
            "node_id": node_id,
            "f_port": f_port,
            "message_role": message_role,
            "port_role": port_role,
            "bt": timestamp,
            "status": "on-line",
            "raw_ultralight": raw_text,
            "gateway_rx": _normalized_rx_info(envelope),
            "rf_tx": _normalized_tx_info(envelope),
        }
        if port_role != "unknown" and message_role != "unknown" and port_role != message_role:
            metadata["port_role_mismatch"] = True
        topic_parts = [part for part in event.topic.split("/") if part]
        if len(topic_parts) >= 2 and topic_parts[0] == "application":
            metadata["application_id"] = topic_parts[1]
        elif device_info.get("applicationId"):
            metadata["application_id"] = str(device_info.get("applicationId"))
        if node is not None:
            if node.location:
                metadata["location"] = node.location
            if node.sub_location:
                metadata["sub_location"] = node.sub_location
            depth = node.depths.get(f_port)
            if depth:
                metadata["depth"] = depth

        return ParsedEvent(
            external_device_id=device_name,
            measurements=tuple(measurements),
            metadata=metadata,
        )


__all__ = [
    "DEFAULT_PORT_ROLES",
    "IrrigapChirpStackParser",
    "IrrigapNode",
    "NodeResolver",
]
