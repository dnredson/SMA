from __future__ import annotations

import base64
import datetime as dt
import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional, Tuple

from ..models import Measurement, ParsedEvent, RawEvent


@dataclass(frozen=True)
class IrrigapNode:
    """Site metadata for one deployed Irrigap/Greenstick node."""

    id: str
    device: str
    location: str
    sub_location: str
    depths: Mapping[int, str] = field(default_factory=dict)


DEFAULT_IRRIGAP_NODES: Tuple[IrrigapNode, ...] = (
    IrrigapNode("3303", "greenstick", "Sector_3", "mz_1", {31: "15cm", 32: "35cm", 33: "55cm"}),
    IrrigapNode("3304", "greenstick", "Sector_4", "mz_1", {31: "15cm", 32: "35cm", 33: "55cm"}),
    IrrigapNode("3306", "greenstick", "Sector_6", "mz_1", {31: "15cm", 32: "35cm", 33: "55cm"}),
    IrrigapNode("3307", "greenstick", "Sector_7", "mz_1", {31: "15cm", 32: "35cm", 33: "55cm"}),
    IrrigapNode("2303", "teros12", "Sector_3", "mz_1", {31: "15cm"}),
    IrrigapNode("2304", "teros12", "Sector_4", "mz_1", {31: "15cm"}),
    IrrigapNode("2305", "teros12", "Sector_5", "mz_1", {31: "15cm"}),
    IrrigapNode("2306", "teros12", "Sector_6", "mz_1", {31: "15cm"}),
    IrrigapNode("2307", "teros12", "Sector_7", "mz_1", {31: "15cm"}),
    IrrigapNode("2311", "teros12", "Test_1", "mz_1", {31: "15cm"}),
    IrrigapNode("2313", "teros12", "Test_3", "mz_1", {31: "15cm"}),
)

_KEY_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


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


def _greenstick_vwc(node_id: str, raw: float) -> float:
    """Calibration provided by the Irrigap deployment collaborator.

    The node ID is interpreted as hexadecimal, matching the existing Node-RED
    flow. Values outside the physical 0..100 % range are reported as 0, also
    matching that flow.
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


class IrrigapChirpStackParser:
    """Parse the ChirpStack MQTT envelope used by the Irrigap deployment.

    Expected envelope fields are the ones supplied by the deployment flow:
    `data` (base64 sensor payload), `time`, `deviceInfo.deviceName` and
    `fPort`. The decoded payload is key/value ultralight text, for example::

        S|2509170900|I|3303|M1|1261|T1|22.1|C1|640

    Site metadata is injected as a node catalog instead of being coupled to
    the transport. The collaborator-provided catalog is the default for now
    and can later move entirely to configuration without changing the parser
    contract.
    """

    name = "chirpstack-irrigap-v2"

    def __init__(self, nodes: Tuple[IrrigapNode, ...] = DEFAULT_IRRIGAP_NODES) -> None:
        self._nodes = {node.id.upper(): node for node in nodes}

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
            # Compatibility fallback for the collaborator's positional flow.
            pieces = raw_text.split("|")
            if len(pieces) > 3:
                node_id = pieces[3].strip().upper()
        if not node_id:
            return None

        moisture_raw = _first_numeric(values, "M")
        temperature = _first_numeric(values, "T")
        ec_raw = _first_numeric(values, "C")

        # Positional fallback mirrors the supplied Node-RED implementation.
        pieces = raw_text.split("|")
        if moisture_raw is None and len(pieces) > 5:
            try:
                moisture_raw = float(pieces[5])
            except ValueError:
                pass
        if temperature is None and len(pieces) > 7:
            try:
                temperature = float(pieces[7])
            except ValueError:
                pass
        if ec_raw is None and len(pieces) > 9:
            try:
                ec_raw = float(pieces[9])
            except ValueError:
                pass

        device_info = envelope.get("deviceInfo") or {}
        device_name = str(device_info.get("deviceName") or node_id).strip()
        if not device_name:
            return None

        try:
            f_port = int(envelope.get("fPort"))
        except (TypeError, ValueError):
            return None

        node = self._nodes.get(node_id)
        timestamp = _iso_epoch(envelope.get("time")) or event.received_at

        measurements = []
        if moisture_raw is not None:
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
                )
            )
        if temperature is not None:
            measurements.append(
                Measurement(
                    name="soil.temperature",
                    value=temperature,
                    unit="Cel",
                    timestamp=timestamp,
                )
            )
        if ec_raw is not None:
            measurements.append(
                Measurement(
                    name="soil.electrical_conductivity",
                    value=ec_raw,
                    timestamp=timestamp,
                )
            )
            measurements.append(
                Measurement(
                    name="soil.raw.ec_c1",
                    value=ec_raw,
                    unit="mV",
                    timestamp=timestamp,
                )
            )

        if not measurements:
            return None

        metadata: Dict[str, Any] = {
            "source": event.source,
            "topic": event.topic,
            "sensor": node.device if node else "irrigap",
            "device_id": device_name,
            "node_id": node_id,
            "f_port": f_port,
            "bt": timestamp,
            "status": "on-line",
            "raw_ultralight": raw_text,
        }
        topic_parts = [part for part in event.topic.split("/") if part]
        if len(topic_parts) >= 2 and topic_parts[0] == "application":
            metadata["application_id"] = topic_parts[1]
        if node is not None:
            metadata.update(
                {
                    "location": node.location,
                    "sub_location": node.sub_location,
                }
            )
            depth = node.depths.get(f_port)
            if depth:
                metadata["depth"] = depth

        return ParsedEvent(
            external_device_id=device_name,
            measurements=tuple(measurements),
            metadata=metadata,
        )


__all__ = [
    "DEFAULT_IRRIGAP_NODES",
    "IrrigapChirpStackParser",
    "IrrigapNode",
]
