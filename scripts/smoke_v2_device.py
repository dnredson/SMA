#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from smarter_adapter.magistrala import AtomClient, AtomConfig, ControlPlane


def env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default)).strip()


def main() -> int:
    base_url = env("ATOM_URL", "http://127.0.0.1")
    token = (
        env("ATOM_SERVICE_TOKEN")
        or env("ATOM_ADMIN_TOKEN")
        or env("ATOM_TOKEN")
    )
    username = env("ATOM_USERNAME", "admin")
    password = env("ATOM_PASSWORD")

    if not token and not password:
        print(
            "ERROR: set ATOM_SERVICE_TOKEN/ATOM_ADMIN_TOKEN or ATOM_PASSWORD",
            file=sys.stderr,
        )
        return 2

    client = AtomClient(
        AtomConfig(
            base_url=base_url,
            graphql_url=env("ATOM_GRAPHQL_URL"),
            token=token,
            username=username,
            password=password,
        )
    )
    control = ControlPlane(client)

    kwargs = {
        "workspace_name": env("SMA_WORKSPACE_NAME", "Smarter Adapter Test"),
        "workspace_alias": env("SMA_WORKSPACE_ALIAS", "smarter-adapter-test"),
        "channel_name": env("SMA_CHANNEL_NAME", "Telemetry"),
        "channel_alias": env("SMA_CHANNEL_ALIAS", "telemetry"),
        "external_id": env("SMA_SMOKE_DEVICE_ID", "SMA_SMOKE_DEVICE_001"),
        "device_name": env("SMA_SMOKE_DEVICE_NAME", "SMA Smoke Device 001"),
        "device_alias": env("SMA_SMOKE_DEVICE_ALIAS", "sma-smoke-device-001"),
        "attributes": {
            "sensor": "smoke",
            "purpose": "smarter-adapter-v2-control-plane-test",
        },
    }

    print(f"Atom: {base_url}")
    print("Reconciling workspace/channel/device-type/device/publish-policy (pass 1)...")
    first = control.ensure_managed_device(**kwargs)

    print(
        "workspace   ",
        first.base.workspace.id,
        f"created={first.base.workspace.created}",
    )
    print(
        "channel     ",
        first.base.channel.id,
        f"created={first.base.channel.created}",
    )
    print(
        "device type ",
        first.device_type.id,
        f"version={first.device_type.version}",
        f"created={first.device_type.created}",
        f"version_created={first.device_type.version_created}",
    )
    print(
        "device      ",
        first.device.id,
        f"external_id={first.device.external_id}",
        f"created={first.device.created}",
        f"publish_policy_created={first.device.publish_policy_created}",
    )

    print("Reconciling the same desired state again (idempotency check)...")
    second = control.ensure_managed_device(**kwargs)

    if first.base.workspace.id != second.base.workspace.id:
        raise RuntimeError("workspace ID changed between reconcile passes")
    if first.base.channel.id != second.base.channel.id:
        raise RuntimeError("channel ID changed between reconcile passes")
    if first.device_type.id != second.device_type.id:
        raise RuntimeError("device type ID changed between reconcile passes")
    if first.device_type.version_id != second.device_type.version_id:
        raise RuntimeError("device type version changed between reconcile passes")
    if first.device.id != second.device.id:
        raise RuntimeError("device ID changed between reconcile passes")

    if (
        second.base.workspace.created
        or second.base.channel.created
        or second.device_type.created
        or second.device_type.version_created
        or second.device.created
        or second.device.publish_policy_created
    ):
        raise RuntimeError("second reconcile unexpectedly created remote state")

    print("PASS: device control plane converged and the second pass was idempotent")
    print(f"SMA_WORKSPACE_ID={second.base.workspace.id}")
    print(f"SMA_CHANNEL_ID={second.base.channel.id}")
    print(f"SMA_DEVICE_TYPE_ID={second.device_type.id}")
    print(f"SMA_DEVICE_TYPE_VERSION_ID={second.device_type.version_id}")
    print(f"SMA_DEVICE_ID={second.device.id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
