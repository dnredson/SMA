from __future__ import annotations
import re
from typing import Any, Dict, List, Tuple

_NUM = re.compile(r"[-+]?\d+(?:\.\d+)?")


def _extract_numbers(after_first_plus: str) -> List[float]:
    # Splita por '+' e depois quebra tokens que trazem múltiplos números com '-' colados (ex.: "0-0.46-0.41")
    nums: List[float] = []
    for token in after_first_plus.split("+"):
        for m in _NUM.finditer(token):
            nums.append(float(m.group(0)))
    return nums


def parse(payload: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Parser para WXT520 no formato SDI-12 compactado ("0+<dir>+<spd>+<temp>+<rh%>+<press>+<rain>+<vbatt>+Spare")."""
    s = payload.strip()
    if "+" not in s:
        return [], {"sensor": "wxt520", "format": "sdi"}
    # Remove o endereço e pega o resto
    try:
        after = s.split("+", 1)[1]
    except Exception:
        after = ""
    values = _extract_numbers(after)
    entries: List[Dict[str, Any]] = []
    if len(values) >= 7:
        direction, speed, temp, rh_percent, press_val, rain_mm, vbatt = values[:7]
        entries.append({"n": "wind.direction", "u": "deg", "v": int(direction) % 360})
        entries.append({"n": "wind.speed", "u": "m/s", "v": float(speed)})
        entries.append({"n": "air.temperature", "u": "Cel", "v": float(temp)})
        entries.append({"n": "rel.humidity", "u": "1", "v": float(rh_percent) / 100.0})
        # Pressão: se 50..120 → kPa; se 200..1200 → hPa → kPa
        p = float(press_val)
        if 50.0 <= p <= 120.0:
            entries.append({"n": "pressure", "u": "kPa", "v": p})
        else:
            entries.append({"n": "pressure", "u": "kPa", "v": p * 0.1})  # assume hPa
        entries.append({"n": "precip.accum", "u": "mm", "v": float(rain_mm)})
        entries.append({"n": "battery.voltage", "u": "V", "v": float(vbatt)})
    return entries, {"sensor": "wxt520", "format": "sdi"}
