from __future__ import annotations
import re
from typing import Any, Dict, List, Tuple

_NUM = re.compile(r"[-+]?\d+(?:\.\d+)?")

# Converte string numérica possivelmente com sufixo de unidade (ex.: "081D", "1.9M")


def _num(s: str) -> float:
    m = _NUM.search(s)
    if not m:
        raise ValueError(f"valor numérico inválido: {s!r}")
    return float(m.group(0))


def parse(payload: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Parser para frames ASCII da Vaisala WXT520 no formato CSV (0R0, 0R3).

    Retorna (entries, meta). Entries são itens SenML SEM bn/bt.
    """
    payload = payload.strip()
    parts = payload.split(",")
    if not parts:
        return [], {"sensor": "wxt520", "format": "csv"}

    rec = parts[0].strip()
    kv = {}
    for p in parts[1:]:
        if "=" in p:
            k, v = p.split("=", 1)
            kv[k.strip()] = v.strip()

    entries: List[Dict[str, Any]] = []

    # 0R0: composto (vento, temp, UR, pressão, chuva acumulada, bateria…)
    if rec == "0R0":
        if "Dm" in kv:
            entries.append(
                {"n": "wind.direction", "u": "deg", "v": int(_num(kv["Dm"])) % 360}
            )
        if "Sm" in kv:
            entries.append({"n": "wind.speed", "u": "m/s", "v": _num(kv["Sm"])})
        if "Ta" in kv:
            entries.append({"n": "air.temperature", "u": "Cel", "v": _num(kv["Ta"])})
        if "Ua" in kv:
            # Ua chega em %, convertemos para fração (SenML unidade "1")
            entries.append({"n": "rel.humidity", "u": "1", "v": _num(kv["Ua"]) / 100.0})
        if "Pa" in kv:
            # Pa chega como hPa (sufixo H). Padronizamos para kPa.
            hpa = _num(kv["Pa"])  # hPa
            entries.append({"n": "pressure", "u": "kPa", "v": hpa * 0.1})
        if "Rc" in kv:
            entries.append({"n": "precip.accum", "u": "mm", "v": _num(kv["Rc"])})
        if "Rd" in kv:
            entries.append({"n": "precip.duration", "u": "s", "v": _num(kv["Rd"])})
        if "Ri" in kv:
            entries.append({"n": "precip.intensity", "u": "mm/h", "v": _num(kv["Ri"])})
        if "Vs" in kv:
            entries.append({"n": "battery.voltage", "u": "V", "v": _num(kv["Vs"])})

    # 0R3: precipitação (acumulada, duração, intensidade)
    elif rec == "0R3":
        # Alguns firmwares usam chaves explícitas RainAcc/RainDur/RainInt
        if "RainAcc" in kv:
            entries.append({"n": "precip.accum", "u": "mm", "v": _num(kv["RainAcc"])})
        if "RainDur" in kv:
            entries.append({"n": "precip.duration", "u": "s", "v": _num(kv["RainDur"])})
        if "RainInt" in kv:
            entries.append(
                {"n": "precip.intensity", "u": "mm/h", "v": _num(kv["RainInt"])}
            )
        # E alguns colocam Rc/Rd/Ri aqui também
        if not entries:
            if "Rc" in kv:
                entries.append({"n": "precip.accum", "u": "mm", "v": _num(kv["Rc"])})
            if "Rd" in kv:
                entries.append({"n": "precip.duration", "u": "s", "v": _num(kv["Rd"])})
            if "Ri" in kv:
                entries.append(
                    {"n": "precip.intensity", "u": "mm/h", "v": _num(kv["Ri"])}
                )

    return entries, {"sensor": "wxt520", "format": "csv", "record": rec}
