from __future__ import annotations
from typing import Any, Dict, List, Tuple, Optional
import re
import json
import datetime as dt

# Convenções de nomes:
#  - Umidade de solo (raw mV):  soil.raw.moisture_m<idx>   u="mV"
#  - Temperatura de solo (°C):  soil.raw.temperature_t<idx> u="Cel"
#  - Condutividade elétrica (raw mV): soil.raw.ec_c<idx>    u="mV"
#  - Bateria: battery.voltage (V), battery.level (1)

_M_RE = re.compile(r"^M(\d+)$", re.IGNORECASE)
_T_RE = re.compile(r"^T(\d+)$", re.IGNORECASE)
_C_RE = re.compile(r"^C(\d+)$", re.IGNORECASE)


def _yyMMddHHmm_to_epoch(ts10: str) -> Optional[int]:
    # ts em "yymmddhhmm" → epoch (UTC)
    if not (isinstance(ts10, str) and len(ts10) == 10 and ts10.isdigit()):
        return None
    yy = int(ts10[0:2])
    year = 2000 + yy  # assume século 2000
    try:
        dt_utc = dt.datetime(
            year,
            int(ts10[2:4]),
            int(ts10[4:6]),
            int(ts10[6:8]),
            int(ts10[8:10]),
            tzinfo=dt.timezone.utc,
        )
        return int(dt_utc.timestamp())
    except Exception:
        return None


def _push(
    entries: List[Dict[str, Any]], name: str, unit: str, val: float | int
) -> None:
    entries.append({"n": name, "u": unit, "v": float(val)})


def parse(ul: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    UL "S|yymmddhhmm|I|hhhh|M1|1261|M2|2|...|VB|3.9|BT|70"
    Retorna (entries, meta) — meta inclui {"sensor":"greenstick","bt":<epoch>}
    """
    s = ul.strip()
    # Alguns firmwares mandam linhas começando com "|" (sem 'S'); tratar igual.
    parts = [p for p in s.split("|") if p != ""]
    entries: List[Dict[str, Any]] = []
    meta: Dict[str, Any] = {"sensor": "greenstick"}

    ts_epoch: Optional[int] = None

    i = 0
    while i < len(parts):
        k = parts[i].strip()
        v = parts[i + 1].strip() if (i + 1) < len(parts) else ""
        # Timestamp
        if k.upper() == "S":
            maybe = _yyMMddHHmm_to_epoch(v)
            if maybe:
                ts_epoch = maybe
            i += 2
            continue
        # Device ID (hex) — mantemos apenas em meta
        if k.upper() == "I":
            meta["greenstick_hex_id"] = v
            i += 2
            continue
        # VB / BT
        if k.upper() == "VB":
            try:
                _push(entries, "battery.voltage", "V", float(v))
            except Exception:
                pass
            i += 2
            continue
        if k.upper() == "BT":
            try:
                # nível 0–100 -> dimensionless (1)
                _push(entries, "battery.level", "1", float(v))
            except Exception:
                pass
            i += 2
            continue
        # M#, T#, C#
        m = _M_RE.match(k)
        if m:
            idx = m.group(1)
            try:
                _push(entries, f"soil.raw.moisture_m{idx}", "mV", float(v))
            except Exception:
                pass
            i += 2
            continue
        t = _T_RE.match(k)
        if t:
            idx = t.group(1)
            try:
                _push(entries, f"soil.raw.temperature_t{idx}", "Cel", float(v))
            except Exception:
                pass
            i += 2
            continue
        c = _C_RE.match(k)
        if c:
            idx = c.group(1)
            try:
                _push(entries, f"soil.raw.ec_c{idx}", "mV", float(v))
            except Exception:
                pass
            i += 2
            continue

        # Se chave não reconhecida, avança 1 para evitar loop infinito
        i += 1

    if ts_epoch is not None:
        meta["bt"] = ts_epoch

    return entries, meta


def parse_ttn_json(
    js: Dict[str, Any],
) -> Tuple[str, List[Dict[str, Any]], Dict[str, Any]]:
    """
    TTN envelope com 'decoded_payload.ultralight'
    Retorna (external_id, entries, meta).
    external_id é derivado de end_device_ids.device_id → NORMALIZADO PARA TOPIC (UPPER + _).
    """
    # device_id → "GREENSTICK_8_NSAAB"
    dev = (js.get("end_device_ids") or {}).get("device_id") or "GREENSTICK_UNKNOWN"
    external_id = re.sub(r"[^A-Za-z0-9_-]", "_", dev).upper()

    up = js.get("uplink_message") or {}
    dp = up.get("decoded_payload") or {}
    ul = dp.get("ultralight") or ""
    if not isinstance(ul, str):
        return external_id, [], {"sensor": "greenstick"}
    entries, meta = parse(ul)
    # tenta bt do TTN (seconds)
    # Last resort: o TTN pode ter "received_at"
    if "bt" not in meta:
        ra = up.get("received_at") or js.get("received_at")
        if isinstance(ra, str):
            try:
                # 2025-09-10T12:48:12Z / ...Z
                t = dt.datetime.fromisoformat(ra.replace("Z", "+00:00"))
                meta["bt"] = int(t.timestamp())
            except Exception:
                pass
    return external_id, entries, meta
