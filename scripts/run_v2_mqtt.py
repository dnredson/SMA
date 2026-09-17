#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import signal
import sys
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from smarter_adapter.inputs import MQTTInputConfig
from smarter_adapter.legacy_parser import LegacySensorParser
from smarter_adapter.magistrala import AtomClient, AtomConfig, ControlPlane, RulesClient
from smarter_adapter.magistrala.publisher import FluxMQPublisher
from smarter_adapter.parsers import IrrigapChirpStackParser
from smarter_adapter.pipeline import ParsePipeline
from smarter_adapter.plugins import ParserRegistry
from smarter_adapter.runtime import RuntimeConfig, SmarterAdapterRuntime
from smarter_adapter.service import SmarterAdapterService
from smarter_adapter.storage import SQLiteStateStore


def env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default)).strip()


def mqtt_inputs() -> tuple[MQTTInputConfig, ...]:
    raw = env("SMA_MQTT_INPUTS_JSON")
    if raw:
        try:
            values = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"SMA_MQTT_INPUTS_JSON is invalid JSON: {exc}") from exc
        if not isinstance(values, list) or not values:
            raise RuntimeError("SMA_MQTT_INPUTS_JSON must be a non-empty JSON array")

        result = []
        for index, item in enumerate(values, start=1):
            if not isinstance(item, dict):
                raise RuntimeError(f"MQTT input #{index} must be an object")
            name = str(item.get("name") or f"mqtt-{index}")
            host = str(item.get("host") or "").strip()
            if not host:
                raise RuntimeError(f"MQTT input {name!r} is missing host")
            result.append(
                MQTTInputConfig(
                    host=host,
                    port=int(item.get("port", 1883)),
                    topic=str(item.get("topic") or "#"),
                    qos=int(item.get("qos", 0)),
                    username=str(item.get("username") or ""),
                    password=str(item.get("password") or ""),
                    client_id=str(item.get("client_id") or f"smarter-adapter-v2-{index}"),
                    keepalive=int(item.get("keepalive", 60)),
                    source=str(item.get("source") or f"mqtt:{name}"),
                )
            )
        return tuple(result)

    host = env("SMA_MQTT_HOST", "127.0.0.1")
    port = int(env("SMA_MQTT_PORT", "1884"))
    topic = env("SMA_MQTT_TOPIC", "#")
    return (
        MQTTInputConfig(
            host=host,
            port=port,
            topic=topic,
            qos=int(env("SMA_MQTT_QOS", "0")),
            username=env("SMA_MQTT_USERNAME"),
            password=env("SMA_MQTT_PASSWORD"),
            client_id=env("SMA_MQTT_CLIENT_ID", "smarter-adapter-v2"),
            source=env("SMA_MQTT_SOURCE", f"mqtt:{host}:{port}"),
        ),
    )


def main() -> int:
    atom_url = env("ATOM_URL", "http://127.0.0.1")
    publish_url = env("MAGISTRALA_PUBLISH_URL", atom_url)
    rules_url = env("MAGISTRALA_RULES_URL", atom_url)
    token = env("ATOM_SERVICE_TOKEN") or env("ATOM_ADMIN_TOKEN") or env("ATOM_TOKEN")
    username = env("ATOM_USERNAME", "admin")
    password = env("ATOM_PASSWORD")
    if not token and not password:
        print(
            "ERROR: set ATOM_SERVICE_TOKEN/ATOM_ADMIN_TOKEN or ATOM_PASSWORD",
            file=sys.stderr,
        )
        return 2

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

    parsers = ParserRegistry(
        [
            IrrigapChirpStackParser(),
            LegacySensorParser(),
        ]
    )
    pipeline = ParsePipeline(parsers)

    state_path = Path(env("SMA_STATE_DB", str(ROOT / ".state" / "smarter_adapter.sqlite3")))
    state_store = SQLiteStateStore(state_path)
    runtime = SmarterAdapterRuntime(
        pipeline=pipeline,
        control=control,
        rules=rules,
        publisher=publisher,
        config=RuntimeConfig(
            workspace_name=env("SMA_WORKSPACE_NAME", "Smarter Adapter Test"),
            workspace_alias=env("SMA_WORKSPACE_ALIAS", "smarter-adapter-test"),
            channel_name=env("SMA_CHANNEL_NAME", "Telemetry"),
            channel_alias=env("SMA_CHANNEL_ALIAS", "telemetry"),
            persistence_rule_name=env(
                "SMA_PERSISTENCE_RULE_NAME",
                "smarter-adapter-save-senml",
            ),
        ),
        state_store=state_store,
    )

    def on_result(result):
        print(
            "OK "
            f"parser={result.parser} "
            f"external={result.parsed_event.external_device_id} "
            f"device={result.device.id} "
            f"cache={result.device_cache_source} "
            f"records={len(result.senml)} "
            f"http={result.publish.status}",
            flush=True,
        )

    def on_error(exc):
        print(f"ERROR {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)

    configs = mqtt_inputs()
    service = SmarterAdapterService(
        runtime,
        configs,
        on_result=on_result,
        on_error=on_error,
    )

    stop = threading.Event()

    def request_stop(signum, frame):
        stop.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    try:
        print(f"State DB:  {state_path}")
        print(f"Atom:      {atom_url}")
        print(f"Publish:   {publish_url}")
        print(f"Rules:     {rules_url}")
        for index, config in enumerate(configs, start=1):
            print(
                f"Input {index}: {config.host}:{config.port} "
                f"topic={config.topic} source={config.source}"
            )
        print("Bootstrapping and starting inputs...")
        service.start()
        assert runtime.base is not None
        assert runtime.persistence_rule is not None
        print(f"Workspace: {runtime.base.workspace.id}")
        print(f"Channel:   {runtime.base.channel.id}")
        print(f"Rule:      {runtime.persistence_rule.id}")
        print("RUNNING: Ctrl+C to stop", flush=True)
        stop.wait()
    finally:
        service.stop()
        stats = service.stats
        state_store.close()
        print(
            f"STOPPED received={stats.received} processed={stats.processed} failed={stats.failed}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
