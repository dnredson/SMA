#!/usr/bin/env python3
from __future__ import annotations

import runpy
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from smarter_adapter.envfile import EnvFileError, load_default_env


def main() -> int:
    try:
        config_path, loaded = load_default_env(ROOT)
    except EnvFileError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if config_path.exists():
        print(
            f"Config:    {config_path} "
            f"loaded={len(loaded)} precedence=shell>file>defaults",
            flush=True,
        )
    elif str(config_path).strip():
        print(
            f"Config:    {config_path} not-found; using shell/defaults",
            flush=True,
        )

    # Keep run_v2_mqtt.py as the canonical application entrypoint; this small
    # launcher only supplies reboot-persistent local configuration first.
    try:
        runpy.run_path(str(ROOT / "scripts" / "run_v2_mqtt.py"), run_name="__main__")
    except SystemExit as exc:
        code = exc.code
        if code is None:
            return 0
        if isinstance(code, int):
            return code
        print(str(code), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
