# src/core/config.py
from __future__ import annotations
import json
import os
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
    """
    Serializa o dicionário de configuração em TOML simples,
    preservando strings multilinha com aspas triplas LITERAIS (''').
    """

    def _quote_str(s: str) -> str:
        # Multilinha → usa literal string ''' ... '''
        if ("\n" in s) or ("\r" in s):
            return "'''\n" + s + "\n'''"
        # Uma linha → escapa para string básica
        esc = s.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{esc}"'

    lines = []
    for k in sorted(data.keys()):
        v = data[k]
        if isinstance(v, str):
            lines.append(f"{k} = {_quote_str(v)}\n")
        elif isinstance(v, bool):
            lines.append(f"{k} = {str(v).lower()}\n")
        elif isinstance(v, (int, float)):
            lines.append(f"{k} = {v}\n")
        elif isinstance(v, list):
            # listas como JSON inline
            lines.append(f"{k} = {json.dumps(v, ensure_ascii=False)}\n")
        elif isinstance(v, dict):
            # dicts como JSON inline
            lines.append(f"{k} = {json.dumps(v, ensure_ascii=False)}\n")
        else:
            # fallback: string
            lines.append(f"{k} = {_quote_str(str(v))}\n")

    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text("".join(lines), encoding="utf-8")
    os.replace(tmp, path)
