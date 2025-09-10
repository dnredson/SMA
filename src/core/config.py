# src/core/config.py
from __future__ import annotations
from pathlib import Path
from typing import Dict, Any

try:
    import tomllib  # Py>=3.11
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore


def load_config(path: Path) -> Dict[str, Any]:
    with path.open("rb") as f:
        return tomllib.load(f)


def _toml_quote_str(s: str) -> str:
    return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _toml_format_value(v: Any) -> str:
    if isinstance(v, str):
        return _toml_quote_str(v)
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, list):
        inner = ", ".join(_toml_format_value(x) for x in v)
        return f"[{inner}]"
    if isinstance(v, dict):
        # inline table: { key = value, key2 = value2 }
        # chaves simples do nosso uso (a-z, _, etc.) podem ser não-aspas; por segurança, não as cito.
        pairs = []
        for k, val in v.items():
            pairs.append(f"{k} = {_toml_format_value(val)}")
        return "{ " + ", ".join(pairs) + " }"
    # fallback: string
    return _toml_quote_str(str(v))


def write_config_atomic(path: Path, data: Dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    # grava chaves em ordem estável
    lines = []
    for k in sorted(data.keys()):
        v = data[k]
        lines.append(f"{k} = {_toml_format_value(v)}\n")
    tmp.write_text("".join(lines), encoding="utf-8")
    tmp.replace(path)
