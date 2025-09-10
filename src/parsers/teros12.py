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


def parse(payload: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    s = payload.strip()
    if "+" not in s:
        return [], {"sensor": "teros12"}
    try:
        after = s.split("+", 1)[1]
    except Exception:
        after = ""
    vals = _extract_numbers(after)
    e: List[Dict[str, Any]] = []
    if len(vals) >= 3:
        counts_vwc, temp_c, ec_ds_m = vals[:3]
        e.append({"n": "soil.vwc.counts", "u": "1", "v": counts_vwc})
        e.append({"n": "soil.temperature", "u": "Cel", "v": temp_c})
        e.append({"n": "soil.ec.bulk", "u": "dS/m", "v": ec_ds_m})
    return e, {"sensor": "teros12"}
