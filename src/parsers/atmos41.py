from __future__ import annotations
import re
from typing import Any, Dict, List, Tuple

_NUM = re.compile(r"[-+]?\d+(?:\.\d+)?")


def _extract_numbers(after_first_plus: str) -> List[float]:
    nums: List[float] = []
    for token in after_first_plus.split("+"):
        for m in _NUM.finditer(token):
            nums.append(float(m.group(0)))
    return nums


# Ordem (aR0 contínuo):
# 0 solar(W/m2), 1 precip(mm), 2 strikes, 3 strikeDist(km), 4 wind(m/s), 5 dir(deg),
# 6 gust(m/s), 7 Tair(C), 8 vaporPress(kPa), 9 pressure(kPa), 10 RH(fração 0-1),
# 11 T_hum(C), 12 xOri(deg), 13 yOri(deg), 14 null, 15 windNorth(m/s), 16 windEast(m/s)


def parse(payload: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    s = payload.strip()
    if "+" not in s:
        return [], {"sensor": "atmos41"}
    try:
        after = s.split("+", 1)[1]
    except Exception:
        after = ""
    v = _extract_numbers(after)
    e: List[Dict[str, Any]] = []
    if len(v) >= 11:
        e.append({"n": "solar.irradiance", "u": "W/m2", "v": v[0]})
        e.append({"n": "precip.accum", "u": "mm", "v": v[1]})
        e.append({"n": "lightning.strikes", "u": "1", "v": v[2]})
        e.append({"n": "lightning.avg_distance", "u": "km", "v": v[3]})
        e.append({"n": "wind.speed", "u": "m/s", "v": v[4]})
        e.append({"n": "wind.direction", "u": "deg", "v": v[5] % 360})
        e.append({"n": "wind.gust.max", "u": "m/s", "v": v[6]})
        e.append({"n": "air.temperature", "u": "Cel", "v": v[7]})
        e.append({"n": "vapor.pressure", "u": "kPa", "v": v[8]})
        e.append({"n": "pressure", "u": "kPa", "v": v[9]})
        e.append({"n": "rel.humidity", "u": "1", "v": v[10]})
        if len(v) > 11:
            e.append({"n": "sensor.temp.humidity", "u": "Cel", "v": v[11]})
        if len(v) > 12:
            e.append({"n": "device.x_orientation", "u": "deg", "v": v[12]})
        if len(v) > 13:
            e.append({"n": "device.y_orientation", "u": "deg", "v": v[13]})
        if len(v) > 15:
            e.append({"n": "wind.north", "u": "m/s", "v": v[15]})
        if len(v) > 16:
            e.append({"n": "wind.east", "u": "m/s", "v": v[16]})
    return e, {"sensor": "atmos41"}
