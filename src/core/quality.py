from __future__ import annotations
import json
import fnmatch
from typing import Any, Dict, List, Optional, Tuple

# Defaults (pode sobrescrever via config)
DEFAULT_THRESHOLDS: Dict[str, Dict[str, float]] = {
    # WXT/Atmos
    "rel.humidity": {"min": 0.0, "max": 1.0},  # 0–100% como fração
    "wind.speed": {"min": 0.0},  # sem max aqui; opcional no config
    # Greenstick RAW
    "soil.raw.moisture_*": {"min": 0.0, "max": 2500.0},  # mV
    "soil.raw.ec_*": {"min": 0.0, "max": 2500.0},  # mV
    "soil.raw.temp_*": {"min": -25.0, "max": 100.0},  # °C
    # Bateria
    "battery.level": {"min": 0.0, "max": 100.0},
    "battery.voltage": {"min": 0.0},  # define seu max no config se quiser
}


def _load_cfg_thresholds(cfg: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
    raw = cfg.get("quality_thresholds_json")
    if not raw:
        return {}
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
        out: Dict[str, Dict[str, float]] = {}
        for k, v in (data or {}).items():
            if isinstance(v, dict):
                mn = v.get("min", None)
                mx = v.get("max", None)
                d: Dict[str, float] = {}
                if mn is not None:
                    d["min"] = float(mn)
                if mx is not None:
                    d["max"] = float(mx)
                if d:
                    out[str(k)] = d
        return out
    except Exception:
        return {}


def _bounds_for(
    name: str, rules: Dict[str, Dict[str, float]]
) -> Optional[Tuple[Optional[float], Optional[float]]]:
    # prioridade: regras do config (com curinga), depois defaults (com curinga)
    for src in (rules, DEFAULT_THRESHOLDS):
        best_key = None
        for pat in src.keys():
            if fnmatch.fnmatch(name, pat):
                # primeira correspondência serve (poderia refinar p/ escolher a mais específica)
                best_key = pat
                break
        if best_key:
            r = src[best_key]
            return r.get("min"), r.get("max")
    return None


def check_thresholds(
    entries: List[Dict[str, Any]], cfg: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Devolve uma lista de violações: [{name, value, unit, min, max}]"""
    if not entries:
        return []
    enable = bool(cfg.get("quality_enable", True))
    if not enable:
        return []

    rules = _load_cfg_thresholds(cfg)
    violations: List[Dict[str, Any]] = []

    for it in entries:
        name = it.get("n")
        if not name:
            continue
        v = it.get("v")
        if not isinstance(v, (int, float)):
            continue
        b = _bounds_for(name, rules)
        if not b:
            continue
        mn, mx = b
        bad = False
        if (mn is not None) and (v < mn):
            bad = True
        if (mx is not None) and (v > mx):
            bad = True
        if bad:
            violations.append(
                {
                    "name": name,
                    "value": float(v),
                    "unit": it.get("u") or "",
                    "min": mn,
                    "max": mx,
                }
            )
    return violations
