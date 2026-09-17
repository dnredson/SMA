#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from urllib import error, parse, request

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from smarter_adapter.magistrala import AtomClient, AtomConfig, ControlPlane
from smarter_adapter.magistrala.publisher import FluxMQPublisher
from smarter_adapter.models import Measurement, ParsedEvent
from smarter_adapter.senml import event_to_senml


def env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default)).strip()


def fetch_messages(
    *,
    reader_url: str,
    workspace_id: str,
    channel_id: str,
    publisher_id: str,
    token: str,
    timeout: float = 5.0,
):
    query = parse.urlencode(
        {
            "limit": 100,
            "publisher": publisher_id,
            "order": "time",
            "dir": "desc",
        }
    )
    url = (
        reader_url.rstrip("/")
        + "/"
        + parse.quote(workspace_id, safe="")
        + "/channels/"
        + parse.quote(channel_id, safe="")
        + "/messages?"
        + query
    )
    req = request.Request(
        url,
        headers={
            "Accept": "application/json",
            "Authorization": "Bearer " + token,
        },
        method="GET",
    )
    try:
        with request.urlopen(req, timeout=timeout) as response:
            raw = response.read() or b"{}"
    except error.HTTPError as exc:
        detail = (exc.read() or b"").decode("utf-8", errors="replace")
        raise RuntimeError(f"reader HTTP {exc.code}: {detail[:500]}") from exc
    return json.loads(raw.decode("utf-8"))


def main() -> int:
    atom_url = env("ATOM_URL", "http://127.0.0.1")
    publish_url = env("MAGISTRALA_PUBLISH_URL", atom_url)
    reader_url = env("MAGISTRALA_READER_URL", "http://127.0.0.1:9011")
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
            base_url=atom_url,
            graphql_url=env("ATOM_GRAPHQL_URL"),
            token=token,
            username=username,
            password=password,
        )
    )
    control = ControlPlane(client)

    managed = control.ensure_managed_device(
        workspace_name=env("SMA_WORKSPACE_NAME", "Smarter Adapter Test"),
        workspace_alias=env("SMA_WORKSPACE_ALIAS", "smarter-adapter-test"),
        channel_name=env("SMA_CHANNEL_NAME", "Telemetry"),
        channel_alias=env("SMA_CHANNEL_ALIAS", "telemetry"),
        external_id=env("SMA_SMOKE_DEVICE_ID", "SMA_SMOKE_DEVICE_001"),
        device_name=env("SMA_SMOKE_DEVICE_NAME", "SMA Smoke Device 001"),
        device_alias=env("SMA_SMOKE_DEVICE_ALIAS", "sma-smoke-device-001"),
        attributes={
            "sensor": "smoke",
            "purpose": "smarter-adapter-v2-data-plane-test",
        },
    )

    now = time.time()
    marker = f"sma-v2-{int(now * 1000)}"
    event = ParsedEvent(
        external_device_id=managed.device.external_id,
        measurements=(
            Measurement("smoke.moisture", 42.125, "%", now),
            Measurement("smoke.temperature", 22.1, "Cel", now),
            Measurement("smoke.marker", marker, None, now),
        ),
        metadata={"purpose": "v2-data-plane-smoke"},
    )
    senml = event_to_senml(event)

    publisher = FluxMQPublisher(
        publish_url,
        client.token,
        invalidate_token=client.tokens.invalidate,
    )

    print(f"Atom:      {atom_url}")
    print(f"Publish:   {publish_url}")
    print(f"Reader:    {reader_url}")
    print(f"Workspace: {managed.base.workspace.id}")
    print(f"Channel:   {managed.base.channel.id}")
    print(f"Device:    {managed.device.id}")
    print(f"External:  {managed.device.external_id}")
    print(f"Marker:    {marker}")
    print("SenML:")
    print(json.dumps(senml, indent=2, ensure_ascii=False))

    result = publisher.publish(
        workspace_id=managed.base.workspace.id,
        channel_id=managed.base.channel.id,
        device_id=managed.device.id,
        senml=senml,
    )
    print(f"Publish response: HTTP {result.status} {result.raw_body}")

    expected_names = {
        managed.device.external_id + ":smoke.moisture": 42.125,
        managed.device.external_id + ":smoke.temperature": 22.1,
    }
    deadline = time.time() + float(env("SMA_SMOKE_READ_WAIT", "20"))
    last_payload = None

    while time.time() < deadline:
        payload = fetch_messages(
            reader_url=reader_url,
            workspace_id=managed.base.workspace.id,
            channel_id=managed.base.channel.id,
            publisher_id=managed.device.id,
            token=client.token(),
        )
        last_payload = payload
        messages = payload.get("messages") or []

        marker_row = next(
            (
                row
                for row in messages
                if row.get("name") == managed.device.external_id + ":smoke.marker"
                and row.get("string_value") == marker
            ),
            None,
        )
        numeric = {row.get("name"): row.get("value") for row in messages}

        if marker_row is not None and all(
            name in numeric and abs(float(numeric[name]) - expected) < 1e-9
            for name, expected in expected_names.items()
        ):
            if marker_row.get("publisher") != managed.device.id:
                raise RuntimeError(
                    "Timescale row publisher does not match Atom device UUID: "
                    f"{marker_row.get('publisher')!r}"
                )
            if marker_row.get("device_id") != managed.device.external_id:
                raise RuntimeError(
                    "Timescale row device_id does not preserve external device id: "
                    f"{marker_row.get('device_id')!r}"
                )

            print("PASS: SenML was accepted by FluxMQ and observed through the Timescale reader")
            print(f"TIMESCALE_MARKER={marker}")
            return 0

        time.sleep(0.5)

    print("ERROR: published marker did not appear in Timescale before timeout", file=sys.stderr)
    if last_payload is not None:
        print(json.dumps(last_payload, indent=2, ensure_ascii=False), file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
