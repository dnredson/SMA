#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from smarter_adapter.inputs import MQTTInputConfig
from smarter_adapter.irrigap_config import load_irrigap_catalog
from smarter_adapter.legacy_parser import LegacySensorParser
from smarter_adapter.observer import MQTTObserver
from smarter_adapter.parsers import IrrigapChirpStackParser
from smarter_adapter.pipeline import ParsePipeline
from smarter_adapter.plugins import ParserRegistry


def env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default)).strip()


def main() -> int:
    host = env("SMA_MQTT_HOST", "189.18.9.13")
    port = int(env("SMA_MQTT_PORT", "1883"))
    topic = env(
        "SMA_MQTT_TOPIC",
        "application/bf9286b1-b02c-4e86-976f-f7d66b75aeb7/#",
    )
    username = env("SMA_MQTT_USERNAME")
    password = env("SMA_MQTT_PASSWORD")
    max_parsed = int(env("SMA_OBSERVE_MAX", "5"))
    timeout = float(env("SMA_OBSERVE_TIMEOUT", "180"))

    catalog_inline = env("SMA_IRRIGAP_NODES_JSON")
    catalog_file = env("SMA_IRRIGAP_NODES_FILE")
    if not catalog_inline and not catalog_file:
        catalog_file = str(ROOT / "config" / "irrigap.nodes.json")
    catalog = load_irrigap_catalog(
        file_path=catalog_file,
        inline_json=catalog_inline,
    )
    pipeline = ParsePipeline(
        ParserRegistry([
            IrrigapChirpStackParser(catalog.nodes),
            LegacySensorParser(),
        ])
    )

    def on_observation(obs):
        event = obs.event
        print("\n=== PARSED ===")
        print(f"topic:       {obs.raw.topic}")
        print(f"parser:      {obs.parser}")
        print(f"external_id: {event.external_device_id}")
        print("metadata:")
        print(json.dumps(event.metadata, indent=2, ensure_ascii=False, default=str))
        print("measurements:")
        print(
            json.dumps(
                [
                    {
                        "name": m.name,
                        "value": m.value,
                        "unit": m.unit,
                        "timestamp": m.timestamp,
                        "metadata": m.metadata,
                    }
                    for m in event.measurements
                ],
                indent=2,
                ensure_ascii=False,
                default=str,
            )
        )

    def on_rejected(raw, exc):
        preview = raw.payload[:160]
        print(
            "\n=== REJECTED ===\n"
            f"topic:  {raw.topic}\n"
            f"error:  {type(exc).__name__}: {exc}\n"
            f"bytes:  {len(raw.payload)}\n"
            f"preview:{preview!r}",
            file=sys.stderr,
            flush=True,
        )

    config = MQTTInputConfig(
        host=host,
        port=port,
        topic=topic,
        qos=int(env("SMA_MQTT_QOS", "0")),
        username=username,
        password=password,
        client_id=env("SMA_MQTT_CLIENT_ID", "smarter-adapter-v2-observer"),
        source=env("SMA_MQTT_SOURCE", f"mqtt-observe:{host}:{port}"),
    )
    observer = MQTTObserver(
        pipeline=pipeline,
        config=config,
        on_observation=on_observation,
        on_rejected=on_rejected,
        max_parsed=max_parsed,
    )

    print("Smarter Adapter 2.0 MQTT observer (READ ONLY)")
    print(f"Broker:    {host}:{port}")
    print(f"Topic:     {topic}")
    print(f"Catalog:   {catalog.source} nodes={len(catalog.nodes)}")
    print(f"Target:    {max_parsed} parsed message(s)")
    print(f"Timeout:   {timeout:.0f}s")
    print("Writes:    DISABLED (no Atom / no FluxMQ / no Timescale writes)")

    observer.start()
    try:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if observer.done.wait(timeout=0.25):
                break
            if observer.input.last_error:
                raise RuntimeError(observer.input.last_error)
    finally:
        observer.stop()

    stats = observer.stats
    print(
        "\nDONE "
        f"received={stats.received} parsed={stats.parsed} rejected={stats.rejected}"
    )
    if stats.parsed == 0:
        print("ERROR: no parseable MQTT message observed", file=sys.stderr)
        return 1
    if stats.parsed < max_parsed:
        print("NOTE: observer timed out before reaching the requested parsed-message count")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
