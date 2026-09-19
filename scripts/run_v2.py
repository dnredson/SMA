#!/usr/bin/env python3
from __future__ import annotations

import os
import runpy
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from smarter_adapter.envfile import EnvFileError, load_default_env


_IRRIGAP_PRESENCE_DEFAULT = (
    '{"teros12":{"expected_interval_seconds":600,'
    '"stale_after_seconds":900,"offline_after_seconds":1800}}'
)


def _install_runtime_extensions() -> None:
    """Install optional production integrations before the canonical runner imports.

    ``run_v2_mqtt.py`` intentionally remains the canonical application wiring.
    This launcher adds richer implementations without making field-specific
    behavior a dependency of the transport-neutral core modules.
    """

    import smarter_adapter.gateway_monitor as gateway_monitor
    import smarter_adapter.presence as presence_module
    from smarter_adapter.gateway_stats import (
        GatewayStatsMqttObserver,
        GatewayStatsTopologySQLiteManagementStore,
    )
    from smarter_adapter.presence import (
        DevicePresencePolicy as BaseDevicePresencePolicy,
        parse_presence_profiles,
    )

    # The canonical runner imports these names from gateway_monitor after this
    # function returns, so it transparently gets stats decoding/persistence.
    gateway_monitor.GatewayMqttObserver = GatewayStatsMqttObserver
    gateway_monitor.GatewayTopologySQLiteManagementStore = (
        GatewayStatsTopologySQLiteManagementStore
    )

    class ConfiguredDevicePresencePolicy(BaseDevicePresencePolicy):
        def __init__(self, *args, family_thresholds=None, **kwargs):
            if family_thresholds is None:
                raw = str(os.getenv("SMA_DEVICE_PRESENCE_PROFILES_JSON", "")).strip()
                environment = str(os.getenv("SMA_ENVIRONMENT", "test")).strip().lower()
                if not raw and environment in {
                    "irrigap",
                    "field",
                    "production",
                    "prod",
                }:
                    raw = _IRRIGAP_PRESENCE_DEFAULT
                family_thresholds = parse_presence_profiles(raw)
            super().__init__(
                *args,
                family_thresholds=family_thresholds,
                **kwargs,
            )

    presence_module.DevicePresencePolicy = ConfiguredDevicePresencePolicy


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

    try:
        _install_runtime_extensions()
    except (ValueError, TypeError) as exc:
        print(f"ERROR: invalid runtime extension configuration: {exc}", file=sys.stderr)
        return 2

    environment = str(os.getenv("SMA_ENVIRONMENT", "test")).strip().lower()
    raw_profiles = str(os.getenv("SMA_DEVICE_PRESENCE_PROFILES_JSON", "")).strip()
    if not raw_profiles and environment in {"irrigap", "field", "production", "prod"}:
        print(
            "Presence profiles: teros12 expected=600s stale>900s offline>1800s; "
            "other families use fallback thresholds",
            flush=True,
        )
    elif raw_profiles:
        print("Presence profiles: configured by SMA_DEVICE_PRESENCE_PROFILES_JSON", flush=True)

    # Keep run_v2_mqtt.py as the canonical application entrypoint; this small
    # launcher supplies reboot-persistent local configuration and runtime
    # extensions first.
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
