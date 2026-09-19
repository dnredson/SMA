#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import threading
import time
from pathlib import Path
from urllib import error, parse, request

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from smarter_adapter.inputs import MQTTInput, MQTTInputConfig
from smarter_adapter.magistrala import AtomClient, AtomConfig, ControlPlane, RulesClient
from smarter_adapter.magistrala.publisher import FluxMQPublisher
from smarter_adapter.parsers import IrrigapChirpStackParser
from smarter_adapter.pipeline import ParsePipeline
from smarter_adapter.plugins import ParserRegistry
from smarter_adapter.runtime import RuntimeConfig, SmarterAdapterRuntime


def env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default)).strip()


def fetch_messages(reader_url, workspace_id, channel_id, publisher_id, token):
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
        with request.urlopen(req, timeout=5) as response:
            return json.loads((response.read() or b"{}").decode("utf-8"))
    except error.HTTPError as exc:
        detail = (exc.read() or b"").decode("utf-8", errors="replace")
        raise RuntimeError(f"reader HTTP {exc.code}: {detail[:500]}") from exc


def expected_rows(senml):
    base_name = ""
    result = []
    for record in senml:
        if record.get("bn") is not None:
            base_name = str(record.get("bn") or "")
        name = base_name + str(record.get("n") or "")
        if "v" in record:
            result.append((name, "value", record["v"]))
        elif "vs" in record:
            result.append((name, "string_value", record["vs"]))
        elif "vb" in record:
            result.append((name, "bool_value", record["vb"]))
        elif "vd" in record:
            result.append((name, "data_value", record["vd"]))
    return result


def row_matches(row, field, expected):
    actual = row.get(field)
    if field == "value":
        try:
            return abs(float(actual) - float(expected)) < 1e-9
        except (TypeError, ValueError):
            return False
    return actual == expected


def main() -> int:
    atom_url = env("ATOM_URL", "http://127.0.0.1")
    publish_url = env("MAGISTRALA_PUBLISH_URL", atom_url)
    rules_url = env("MAGISTRALA_RULES_URL", atom_url)
    reader_url = env("MAGISTRALA_READER_URL", "http://127.0.0.1:9011")
    token = env("ATOM_SERVICE_TOKEN") or env("ATOM_ADMIN_TOKEN") or env("ATOM_TOKEN")
    username = env("ATOM_USERNAME", "admin")
    password = env("ATOM_PASSWORD")

    if not token and not password:
        print("ERROR: set ATOM_SERVICE_TOKEN/ATOM_ADMIN_TOKEN or ATOM_PASSWORD", file=sys.stderr)
        return 2

    mqtt_host = env("SMA_MQTT_HOST", "127.0.0.1")
    mqtt_port = int(env("SMA_MQTT_PORT", "1884"))
    mqtt_topic = env(
        "SMA_MQTT_TOPIC",
        "application/bf9286b1-b02c-4e86-976f-f7d66b75aeb7/#",
    )
    mqtt_username = env("SMA_MQTT_USERNAME")
    mqtt_password = env("SMA_MQTT_PASSWORD")
    wait_seconds = float(env("SMA_MQTT_WAIT", "90"))
    read_wait = float(env("SMA_SMOKE_READ_WAIT", "30"))

    atom = AtomClient(
        AtomConfig(
            base_url=atom_url,
            graphql_url=env("ATOM_GRAPHQL_URL"),
            token=token,
            username=username,
            password=password,
        )
    )
    control = ControlPlane(atom)
    rules = RulesClient(
        rules_url,
        atom.token,
        invalidate_token=atom.tokens.invalidate,
    )
    publisher = FluxMQPublisher(
        publish_url,
        atom.token,
        invalidate_token=atom.tokens.invalidate,
    )
    runtime = SmarterAdapterRuntime(
        pipeline=ParsePipeline(ParserRegistry([IrrigapChirpStackParser()])),
        control=control,
        rules=rules,
        publisher=publisher,
        config=RuntimeConfig(
            workspace_name=env("SMA_WORKSPACE_NAME", "Smarter Adapter Test"),
            workspace_alias=env("SMA_WORKSPACE_ALIAS", "smarter-adapter-test"),
            channel_name=env("SMA_CHANNEL_NAME", "Telemetry"),
            channel_alias=env("SMA_CHANNEL_ALIAS", "telemetry"),
        ),
    )

    print("Bootstrapping Magistrala resources...")
    runtime.bootstrap()
    assert runtime.base is not None
    assert runtime.persistence_rule is not None
    print(f"Workspace: {runtime.base.workspace.id}")
    print(f"Channel:   {runtime.base.channel.id}")
    print(
        "Rule:      "
        f"{runtime.persistence_rule.id} status={runtime.persistence_rule.status} "
        f"created={runtime.persistence_rule.created} enabled={runtime.persistence_rule.enabled}"
    )

    done = threading.Event()
    holder = {}

    def on_event(raw):
        if done.is_set():
            return
        try:
            result = runtime.process(raw)
            holder["result"] = result
            print(f"Received MQTT topic: {raw.topic}")
            print(f"Parser:     {result.parser}")
            print(f"External:   {result.parsed_event.external_device_id}")
            print(f"Device:     {result.device.id}")
            print(f"Cache hit:  {result.device_cache_hit}")
            print(f"Publish:    HTTP {result.publish.status} {result.publish.raw_body}")
            print("SenML:")
            print(json.dumps(list(result.senml), indent=2, ensure_ascii=False))
        except Exception as exc:  # smoke tool: surface callback failures to caller
            holder["error"] = exc
        finally:
            done.set()

    mqtt_input = MQTTInput(
        MQTTInputConfig(
            host=mqtt_host,
            port=mqtt_port,
            topic=mqtt_topic,
            username=mqtt_username,
            password=mqtt_password,
            client_id=env("SMA_MQTT_CLIENT_ID", "smarter-adapter-v2-smoke"),
            source=f"mqtt:{mqtt_host}:{mqtt_port}",
        ),
        on_event,
    )

    print(f"MQTT:      {mqtt_host}:{mqtt_port}")
    print(f"Topic:     {mqtt_topic}")
    mqtt_input.start()
    try:
        connect_deadline = time.time() + min(wait_seconds, 15.0)
        while not mqtt_input.connected and time.time() < connect_deadline:
            if mqtt_input.last_error:
                raise RuntimeError(mqtt_input.last_error)
            time.sleep(0.1)
        if not mqtt_input.connected:
            raise RuntimeError("MQTT input did not connect before timeout")

        print("READY: publish one ChirpStack/Irrigap MQTT message now")
        if not done.wait(wait_seconds):
            raise RuntimeError("no matching MQTT message arrived before timeout")
    finally:
        mqtt_input.stop()

    if "error" in holder:
        raise holder["error"]

    result = holder["result"]
    expected = expected_rows(result.senml)
    deadline = time.time() + read_wait
    last_payload = None
    while time.time() < deadline:
        payload = fetch_messages(
            reader_url,
            runtime.base.workspace.id,
            runtime.base.channel.id,
            result.device.id,
            atom.token(),
        )
        last_payload = payload
        rows = payload.get("messages") or []
        all_found = True
        for name, field, value in expected:
            found = any(
                row.get("name") == name
                and row.get("publisher") == result.device.id
                and row_matches(row, field, value)
                for row in rows
            )
            if not found:
                all_found = False
                break
        if all_found and expected:
            print("PASS: MQTT -> parser -> device reconcile -> SenML -> FluxMQ -> Rules -> Timescale")
            print(f"TIMESCALE_DEVICE={result.parsed_event.external_device_id}")
            return 0
        time.sleep(0.5)

    print("ERROR: MQTT message was published but expected rows did not appear in Timescale", file=sys.stderr)
    if last_payload is not None:
        print(json.dumps(last_payload, indent=2, ensure_ascii=False), file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
