from __future__ import annotations
import re
import time
from typing import Any, Dict, List, Tuple

_UL_KV = re.compile(r"\|")  # split por '|'


def _parse_ts_yymmddhhmm(syy: str) -> int:
    # syy = '2509091140' -> 2025-09-09 11:40:00 UTC (assumindo século 2000+)
    if len(syy) != 10 or not syy.isdigit():
        return int(time.time())
    yy = int(syy[0:2])
    mm = int(syy[2:4])
    dd = int(syy[4:6])
    hh = int(syy[6:8])
    mi = int(syy[8:10])
    year = 2000 + yy
    import datetime as dt

    try:
        return int(
            dt.datetime(year, mm, dd, hh, mi, tzinfo=dt.timezone.utc).timestamp()
        )
    except Exception:
        return int(time.time())


def parse_ultralight_line(ul: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Recebe algo como:
      'S|2509091140|I|3306|M1|1261|M2|2|M3|154'
      'S|2509091140|I|3306|T1|27.4|T2|25.9|T3|25.4'
      'S|2509091140|I|3306|C1|919|C2|2500|C3|2500'
      'S|2509091140|I|3306|VB|3.9|BT|100'
    Retorna (entries SenML sem bn/bt, meta com bt/sensor).
    """
    s = ul.strip()
    if s.startswith("S|"):
        s = s[2:]
    parts = _UL_KV.split(s)
    # transforma em pares k,v
    kv = []
    i = 0
    while i < len(parts):
        k = parts[i].strip()
        v = parts[i + 1].strip() if i + 1 < len(parts) else ""
        kv.append((k, v))
        i += 2

    entries: List[Dict[str, Any]] = []
    meta: Dict[str, Any] = {"sensor": "greenstick"}

    for k, v in kv:
        if k == "S":  # timestamp yymmddhhmm
            meta["bt"] = _parse_ts_yymmddhhmm(v)
        elif k == "I":  # id hex do device (opcional)
            meta["device_hex_id"] = v
        elif k == "VB":
            try:
                entries.append({"n": "battery.voltage", "u": "V", "v": float(v)})
            except Exception:
                pass
        elif k == "BT":
            # manter "raw" (percentual reportado pelo device)
            try:
                entries.append({"n": "battery.level", "u": "1", "v": float(v)})
            except Exception:
                pass
        else:
            # M#, T#, C# → soil.raw.*
            m = re.match(r"^([MTC])(\d+)$", k)
            if not m:
                continue
            kind = m.group(1)
            idx = m.group(2)
            name = None
            unit = None
            try:
                if kind == "M":  # Moisture RAW mV
                    name = f"soil.raw.moisture.{idx}"
                    unit = "mV"
                    val = float(v)
                elif kind == "T":  # Temperature Celsius
                    name = f"soil.raw.temp.{idx}"
                    unit = "Cel"
                    val = float(v)
                elif kind == "C":  # EC RAW mV
                    name = f"soil.raw.ec.{idx}"
                    unit = "mV"
                    val = float(v)
                if name:
                    entries.append({"n": name, "u": unit, "v": val})
            except Exception:
                continue

    return entries, meta
