from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime
from typing import Any, Mapping, Optional
from zoneinfo import ZoneInfo

from ..models import Measurement, ParsedEvent, RawEvent


_TOPIC_RE = re.compile(r"^pitaya/(?P<location>[^/]+)_DATA$", re.IGNORECASE)
_SENSOR_SLOT_RE = re.compile(r"^s(?P<slot>[1-9]|[1-4]\d|50)$", re.IGNORECASE)
_RELAY_SLOT_RE = re.compile(r"^rele(?P<slot>[1-7])$", re.IGNORECASE)
_FIELD_RE = re.compile(
    r'"(?P<key>[A-Za-z0-9_]+)"\s*:\s*(?P<value>-?\d+(?:\.\d+)?)'
)

INVALID_SENTINEL = 99.0


def _number(value: str) -> int | float:
    return float(value) if "." in value else int(value)


def _fragment(value: Any) -> dict[str, int | float]:
    """Decode the numeric key/value fragment emitted by the Pitaya/SACI gateway."""

    if isinstance(value, Mapping):
        result: dict[str, int | float] = {}
        for raw_key, raw_value in value.items():
            key = str(raw_key).strip().upper()
            if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
                raise ValueError(f"Pitaya field {key!r} is not numeric")
            result[key] = raw_value
        return result

    text = str(value or "").strip()
    matches = list(_FIELD_RE.finditer(text))
    if not matches:
        raise ValueError("Pitaya block does not contain numeric key/value fields")

    result = {}
    for match in matches:
        key = match.group("key").upper()
        if key in result:
            raise ValueError(f"duplicate Pitaya field {key!r}")
        result[key] = _number(match.group("value"))
    return result


def _safe_location(value: str) -> str:
    text = re.sub(r"[^A-Za-z0-9_-]+", "_", str(value or "").strip()).strip("_")
    if not text:
        raise ValueError("Pitaya topic does not contain a valid location")
    return text.upper()


def _measurement(
    name: str,
    value: Any,
    *,
    timestamp: float,
    unit: Optional[str] = None,
    source_field: str = "",
    **metadata: Any,
) -> Measurement:
    item_metadata = dict(metadata)
    if source_field:
        item_metadata["source_field"] = source_field
    return Measurement(
        name=name,
        value=value,
        unit=unit,
        timestamp=timestamp,
        metadata=item_metadata,
    )


class PitayaSaciParser:
    """Parse Pitaya/SACI aggregate MQTT snapshots.

    Topic convention: pitaya/<LOCATION>_DATA.

    V1 keeps one logical Atom device per Pitaya installation/location and emits
    only usable telemetry values. Source value 99 is a firmware sentinel meaning
    unavailable/not in use and is therefore ignored rather than persisted as a
    measurement.

    Source slot and reported board ID are kept in measurement names/metadata so
    the original bus identity remains traceable without creating one Atom entity
    for every source slot during this validation phase.
    """

    name = "pitaya-saci-v1"

    def __init__(self, *, source_timezone: str = "America/Sao_Paulo") -> None:
        self.source_timezone = str(source_timezone or "America/Sao_Paulo").strip()
        self._tz = ZoneInfo(self.source_timezone)

    def supports(self, event: RawEvent) -> bool:
        return _TOPIC_RE.fullmatch(str(event.topic or "").strip()) is not None

    def _timestamp(self, payload: Mapping[str, Any], event: RawEvent) -> tuple[float, str]:
        raw_date = str(payload.get("date") or "").strip()
        raw_hour = str(payload.get("hour") or "").strip()
        if raw_date and raw_hour:
            try:
                parsed = datetime.strptime(
                    f"{raw_date} {raw_hour}", "%d/%m/%Y %H:%M:%S"
                ).replace(tzinfo=self._tz)
                return parsed.timestamp(), "payload"
            except ValueError:
                pass
        return float(event.received_at), "received_at"

    def parse(self, event: RawEvent) -> Optional[ParsedEvent]:
        topic_match = _TOPIC_RE.fullmatch(str(event.topic or "").strip())
        if topic_match is None:
            return None

        try:
            payload = json.loads(event.payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"invalid Pitaya JSON payload: {exc}") from exc
        if not isinstance(payload, dict):
            raise ValueError("Pitaya payload root must be a JSON object")

        location = _safe_location(topic_match.group("location"))
        timestamp, timestamp_source = self._timestamp(payload, event)
        measurements: list[Measurement] = []

        malformed_blocks: list[str] = []
        slot_mismatches: list[str] = []
        board_ids: list[int] = []

        sensor_items = []
        relay_items = []
        for raw_key, raw_value in payload.items():
            key = str(raw_key)
            sensor_match = _SENSOR_SLOT_RE.fullmatch(key)
            if sensor_match is not None:
                sensor_items.append((int(sensor_match.group("slot")), key, raw_value))
                continue
            relay_match = _RELAY_SLOT_RE.fullmatch(key)
            if relay_match is not None:
                relay_items.append((int(relay_match.group("slot")), key, raw_value))

        for slot_index, slot_key, raw_block in sorted(sensor_items):
            try:
                block = _fragment(raw_block)
                board_id = int(block["ID"])
            except (KeyError, TypeError, ValueError):
                malformed_blocks.append(slot_key)
                continue

            board_ids.append(board_id)
            if board_id != slot_index:
                slot_mismatches.append(f"{slot_key}->ID{board_id:02d}")

            prefix = f"sensorboard.s{slot_index:02d}.id{board_id:02d}"
            board_has_valid_sensor = False

            for sensor_index in range(1, 7):
                field = f"S{sensor_index}"
                if field not in block:
                    continue
                value = float(block[field])
                if value == INVALID_SENTINEL:
                    continue
                board_has_valid_sensor = True
                measurements.append(
                    _measurement(
                        f"{prefix}.s{sensor_index}.raw",
                        block[field],
                        timestamp=timestamp,
                        source_field=f"{slot_key}.{field}",
                        board_id=board_id,
                        slot=slot_key,
                    )
                )

            if "BAT" in block:
                battery = float(block["BAT"])
                if battery != INVALID_SENTINEL and 0.0 <= battery <= 100.0:
                    measurements.append(
                        _measurement(
                            f"{prefix}.battery.level",
                            block["BAT"],
                            unit="%",
                            timestamp=timestamp,
                            source_field=f"{slot_key}.BAT",
                            board_id=board_id,
                            slot=slot_key,
                        )
                    )

            # T values only make sense when the board has at least one usable S
            # input. This prevents firmware defaults such as T1=T2=T3=0 on an
            # entirely unavailable board from becoming scientific telemetry.
            if board_has_valid_sensor:
                for tension_index in range(1, 4):
                    field = f"T{tension_index}"
                    if field not in block:
                        continue
                    value = float(block[field])
                    if value == INVALID_SENTINEL:
                        continue
                    measurements.append(
                        _measurement(
                            f"{prefix}.soil.tension.t{tension_index}",
                            block[field],
                            unit="kPa",
                            timestamp=timestamp,
                            source_field=f"{slot_key}.{field}",
                            board_id=board_id,
                            slot=slot_key,
                        )
                    )

        for relay_index, relay_key, raw_block in sorted(relay_items):
            try:
                block = _fragment(raw_block)
                board_id = int(block["ID"])
            except (KeyError, TypeError, ValueError):
                malformed_blocks.append(relay_key)
                continue

            prefix = f"relayboard.rele{relay_index}.id{board_id:02d}"
            for relay_number in range(1, 9):
                field = f"R{relay_number}"
                if field not in block:
                    continue
                value = float(block[field])
                if value == INVALID_SENTINEL:
                    continue
                raw_state = int(value)
                if raw_state not in (0, 1):
                    continue
                measurements.append(
                    _measurement(
                        f"{prefix}.r{relay_number}.state",
                        bool(raw_state),
                        timestamp=timestamp,
                        source_field=f"{relay_key}.{field}",
                        board_id=board_id,
                        slot=relay_key,
                    )
                )

        duplicate_ids = sorted(
            board_id for board_id, count in Counter(board_ids).items() if count > 1
        )

        # Structural anomalies remain available as parser metadata for debugging,
        # but are not persisted as synthetic telemetry rows. The data plane only
        # receives actual usable measurements from the source.
        metadata = {
            "sensor": "pitaya",
            "location": location,
            "source": event.source,
            "topic": event.topic,
            "message_role": "telemetry",
            "transport": {"mqtt_topic": event.topic},
            "source_date": str(payload.get("date") or ""),
            "source_hour": str(payload.get("hour") or ""),
            "source_timezone": self.source_timezone,
            "timestamp_source": timestamp_source,
            "pitaya": {
                "duplicate_board_ids": duplicate_ids,
                "slot_mismatches": slot_mismatches,
                "malformed_blocks": malformed_blocks,
            },
        }

        return ParsedEvent(
            external_device_id=f"PITAYA_{location}",
            measurements=tuple(measurements),
            metadata=metadata,
        )


__all__ = ["PitayaSaciParser", "INVALID_SENTINEL"]
