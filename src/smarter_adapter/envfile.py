from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping, MutableMapping, Optional


class EnvFileError(ValueError):
    """A local SMA environment file could not be parsed."""


def _strip_inline_comment(value: str) -> str:
    """Remove a shell-style inline comment outside quoted strings."""
    quote: Optional[str] = None
    escaped = False
    for index, char in enumerate(value):
        if escaped:
            escaped = False
            continue
        if char == "\\" and quote == '"':
            escaped = True
            continue
        if quote is not None:
            if char == quote:
                quote = None
            continue
        if char in {"'", '"'}:
            quote = char
            continue
        if char == "#" and (index == 0 or value[index - 1].isspace()):
            return value[:index].rstrip()
    return value.strip()


def _decode_value(raw: str, *, path: Path, line_number: int) -> str:
    value = _strip_inline_comment(raw.strip())
    if not value:
        return ""
    if value[0] not in {"'", '"'}:
        return value
    quote = value[0]
    if len(value) < 2 or value[-1] != quote:
        raise EnvFileError(f"{path}:{line_number}: unterminated quoted value")
    body = value[1:-1]
    if quote == "'":
        return body
    # Keep the format intentionally small and deterministic; support the common
    # escapes useful in credentials without performing shell expansion.
    return (
        body.replace("\\n", "\n")
        .replace("\\r", "\r")
        .replace("\\t", "\t")
        .replace('\\"', '"')
        .replace("\\\\", "\\")
    )


def parse_env_file(path: str | Path) -> dict[str, str]:
    env_path = Path(path).expanduser()
    try:
        text = env_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise EnvFileError(f"cannot read environment file {env_path}: {exc}") from exc

    values: dict[str, str] = {}
    for line_number, original in enumerate(text.splitlines(), start=1):
        line = original.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            raise EnvFileError(f"{env_path}:{line_number}: expected KEY=VALUE")
        key, raw = line.split("=", 1)
        key = key.strip()
        if not key or not (key[0].isalpha() or key[0] == "_"):
            raise EnvFileError(f"{env_path}:{line_number}: invalid environment key {key!r}")
        if not all(char.isalnum() or char == "_" for char in key):
            raise EnvFileError(f"{env_path}:{line_number}: invalid environment key {key!r}")
        values[key] = _decode_value(raw, path=env_path, line_number=line_number)
    return values


def load_env_file(
    path: str | Path,
    *,
    environ: Optional[MutableMapping[str, str]] = None,
    override: bool = False,
    required: bool = True,
) -> dict[str, str]:
    """Load KEY=VALUE pairs into an environment mapping.

    Existing process environment values win by default. This makes precedence
    explicit: shell/systemd environment > local file > application defaults.
    """
    env_path = Path(path).expanduser()
    target = os.environ if environ is None else environ
    if not env_path.exists():
        if required:
            raise EnvFileError(f"environment file does not exist: {env_path}")
        return {}
    values = parse_env_file(env_path)
    loaded: dict[str, str] = {}
    for key, value in values.items():
        if override or key not in target:
            target[key] = value
            loaded[key] = value
    return loaded


def load_default_env(
    root: str | Path,
    *,
    environ: Optional[MutableMapping[str, str]] = None,
) -> tuple[Path, dict[str, str]]:
    """Load SMA's persistent local config.

    `SMA_CONFIG_FILE` selects an explicit file and is therefore required when
    set. Without it, `<repo>/.env` is loaded opportunistically. The repository
    ignores `.env`, so lab credentials remain local.
    """
    target = os.environ if environ is None else environ
    root_path = Path(root).expanduser().resolve()
    explicit = str(target.get("SMA_CONFIG_FILE") or "").strip()
    if explicit:
        path = Path(explicit).expanduser()
        if not path.is_absolute():
            path = root_path / path
        path = path.resolve()
        return path, load_env_file(path, environ=target, required=True)
    path = root_path / ".env"
    return path, load_env_file(path, environ=target, required=False)


__all__ = ["EnvFileError", "load_default_env", "load_env_file", "parse_env_file"]
