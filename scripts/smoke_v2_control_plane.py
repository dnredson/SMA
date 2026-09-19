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
    token = env("ATOM_SERVICE_TOKEN") or env("ATOM_ADMIN_TOKEN") or env("ATOM_TOKEN")
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
    }

    print(f"Atom: {base_url}")
    print("Reconciling workspace/channel (pass 1)...")
    first = control.ensure_base(**kwargs)
    print(
        "workspace",
        first.workspace.id,
        f"created={first.workspace.created}",
        f"alias={first.workspace.alias}",
    )
    print(
        "channel  ",
        first.channel.id,
        f"created={first.channel.created}",
        f"alias={first.channel.alias}",
    )

    print("Reconciling the same desired state again (idempotency check)...")
    second = control.ensure_base(**kwargs)

    if first.workspace.id != second.workspace.id:
        raise RuntimeError("workspace ID changed between reconcile passes")
    if first.channel.id != second.channel.id:
        raise RuntimeError("channel ID changed between reconcile passes")
    if second.workspace.created or second.channel.created:
        raise RuntimeError("second reconcile unexpectedly created a resource")

    print("PASS: base control plane converged and the second pass was idempotent")
    print(f"SMA_WORKSPACE_ID={second.workspace.id}")
    print(f"SMA_CHANNEL_ID={second.channel.id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
