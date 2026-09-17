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

from smarter_adapter.catalog_binding import (
    BindingSQLiteManagementStore,
    BoundIrrigapCatalogManager,
)
from smarter_adapter.inputs import MQTTInputConfig
from smarter_adapter.irrigap_config import load_irrigap_catalog
from smarter_adapter.legacy_parser import LegacySensorParser
from smarter_adapter.management import start_management_server
from smarter_adapter.magistrala import (
    AtomClient,
    AtomConfig,
    ControlPlane,
    RulesClient,
    TimescaleReaderClient,
)
from smarter_adapter.magistrala.publisher import FluxMQPublisher
from smarter_adapter.parsers import IrrigapChirpStackParser
from smarter_adapter.pipeline import ParsePipeline
from smarter_adapter.plugins import ParserRegistry
from smarter_adapter.presence import DevicePresencePolicy
from smarter_adapter.reliability import RetryPolicy
from smarter_adapter.runtime import RuntimeConfig, SmarterAdapterRuntime
from smarter_adapter.service import SmarterAdapterService


def env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default)).strip()


def environment_defaults(name: str) -> dict[str, str]:
    normalized = str(name or "test").strip().lower()
    if normalized in ("irrigap", "field", "production", "prod"):
        return {
            "environment": "irrigap",
            "workspace_name": "Irrigap",
            "workspace_alias": "irrigap",
            "channel_name": "Telemetry",
            "channel_alias": "telemetry",
            "state_db": str(ROOT / ".state" / "smarter_adapter-irrigap.sqlite3"),
            "irrigap_nodes_file": str(ROOT / "config" / "irrigap.nodes.json"),
        }
    if normalized in ("test", "dev", "development"):
        return {
            "environment": "test",
            "workspace_name": "Smarter Adapter Test",
            "workspace_alias": "smarter-adapter-test",
            "channel_name": "Telemetry",
            "channel_alias": "telemetry",
            "state_db": str(ROOT / ".state" / "smarter_adapter-test.sqlite3"),
            "irrigap_nodes_file": "",
        }
    raise RuntimeError(
        "SMA_ENVIRONMENT must be one of: test, dev, irrigap, field, production, prod"
    )


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
    deployment = environment_defaults(env("SMA_ENVIRONMENT", "test"))

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
    rules = RulesClient(rules_url, atom.token, invalidate_token=atom.tokens.invalidate)
    publisher = FluxMQPublisher(publish_url, atom.token, invalidate_token=atom.tokens.invalidate)
    reader = TimescaleReaderClient(
        reader_url,
        atom.token,
        invalidate_token=atom.tokens.invalidate,
    )

    catalog_inline = env("SMA_IRRIGAP_NODES_JSON")
    catalog_file = env("SMA_IRRIGAP_NODES_FILE")
    if not catalog_inline and not catalog_file:
        catalog_file = deployment["irrigap_nodes_file"]
    irrigap_catalog = load_irrigap_catalog(
        file_path=catalog_file,
        inline_json=catalog_inline,
    )
    catalog_manager = BoundIrrigapCatalogManager(
        irrigap_catalog,
        file_path=catalog_file if catalog_file and not catalog_inline else "",
    )
    parsers = ParserRegistry(
        [
            IrrigapChirpStackParser(node_resolver=catalog_manager.get_node),
            LegacySensorParser(),
        ]
    )
    pipeline = ParsePipeline(parsers)

    state_path = Path(env("SMA_STATE_DB", deployment["state_db"]))
    state_store = BindingSQLiteManagementStore(state_path)
    runtime_config = RuntimeConfig(
        workspace_name=env("SMA_WORKSPACE_NAME", deployment["workspace_name"]),
        workspace_alias=env("SMA_WORKSPACE_ALIAS", deployment["workspace_alias"]),
        channel_name=env("SMA_CHANNEL_NAME", deployment["channel_name"]),
        channel_alias=env("SMA_CHANNEL_ALIAS", deployment["channel_alias"]),
        persistence_rule_name=env("SMA_PERSISTENCE_RULE_NAME", "smarter-adapter-save-senml"),
    )
    runtime = SmarterAdapterRuntime(
        pipeline=pipeline,
        control=control,
        rules=rules,
        publisher=publisher,
        config=runtime_config,
        state_store=state_store,
    )

    def on_result(result):
        parsed = result.parsed_event
        node_id = str(parsed.metadata.get("node_id") or "").strip()
        if node_id and runtime.base is not None:
            observation_metadata = {
                key: parsed.metadata[key]
                for key in (
                    "sensor",
                    "node_id",
                    "location",
                    "sub_location",
                    "depth",
                    "application_id",
                    "f_port",
                )
                if key in parsed.metadata
            }
            try:
                state_store.set_device_observation(
                    runtime.base.workspace.id,
                    runtime.base.channel.id,
                    parsed.external_device_id,
                    node_id=node_id,
                    sensor=str(parsed.metadata.get("sensor") or ""),
                    metadata=observation_metadata,
                    observed_at=(
                        float(parsed.metadata["bt"])
                        if parsed.metadata.get("bt") is not None
                        else None
                    ),
                )
            except Exception as exc:
                print(
                    f"WARN catalog-binding {type(exc).__name__}: {exc}",
                    file=sys.stderr,
                    flush=True,
                )

        issue_fields = []
        for issue in result.quality_issues:
            field = issue.source_field or issue.measurement
            if field not in issue_fields:
                issue_fields.append(field)
        quality_detail = ""
        if issue_fields:
            quality_detail = " invalid=" + ",".join(issue_fields)
        print(
            "OK "
            f"parser={result.parser} external={parsed.external_device_id} "
            f"device={result.device.id} cache={result.device_cache_source} "
            f"quality={result.quality_status}{quality_detail} "
            f"records={len(result.senml)} http={result.publish.status}",
            flush=True,
        )

    def on_error(exc):
        print(f"ERROR {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)

    configs = mqtt_inputs()
    retry_policy = RetryPolicy(
        max_attempts=int(env("SMA_RETRY_MAX_ATTEMPTS", "5")),
        base_delay_seconds=float(env("SMA_RETRY_BASE_DELAY", "1")),
        max_delay_seconds=float(env("SMA_RETRY_MAX_DELAY", "60")),
        poll_interval_seconds=float(env("SMA_RETRY_POLL_INTERVAL", "0.5")),
        batch_size=int(env("SMA_RETRY_BATCH_SIZE", "50")),
    )
    presence_policy = DevicePresencePolicy(
        stale_after_seconds=float(env("SMA_DEVICE_STALE_AFTER", "300")),
        offline_after_seconds=float(env("SMA_DEVICE_OFFLINE_AFTER", "1800")),
    )

    def catalog_binding(node_id: str):
        base = runtime.base
        if base is None:
            return None
        item = state_store.find_latest_device_by_node(
            base.workspace.id,
            base.channel.id,
            node_id,
        )
        if item is None:
            return None
        decorated = presence_policy.decorate(item)
        return {
            "external_id": decorated["external_id"],
            "atom_device_id": decorated["atom_device_id"],
            "operational_status": decorated["operational_status"],
            "last_seen": decorated["last_seen"],
            "last_seen_age_seconds": decorated["last_seen_age_seconds"],
            "data_quality": decorated.get("data_quality", "unknown"),
            "invalid_fields": decorated.get("invalid_fields", []),
            "quality_evaluated_at": decorated.get("quality_evaluated_at"),
            "binding_observed_at": decorated.get("binding_observed_at"),
        }

    catalog_manager.set_binding_resolver(catalog_binding)

    service = SmarterAdapterService(
        runtime,
        configs,
        on_result=on_result,
        on_error=on_error,
        reliability_store=state_store,
        retry_policy=retry_policy,
    )

    api_host = env("SMA_API_HOST", "127.0.0.1")
    api_port = int(env("SMA_API_PORT", "8082"))
    api_token = env("SMA_API_TOKEN")
    management = None

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda signum, frame: stop.set())
    signal.signal(signal.SIGTERM, lambda signum, frame: stop.set())

    try:
        print(f"Environment:{deployment['environment']}")
        print(f"State DB:  {state_path}")
        print(f"Workspace: {runtime_config.workspace_name} ({runtime_config.workspace_alias})")
        print(f"Channel:   {runtime_config.channel_name} ({runtime_config.channel_alias})")
        print(
            "Irrigap:   "
            f"catalog={catalog_manager.source} nodes={len(catalog_manager.list_nodes())} "
            f"writable={str(catalog_manager.writable).lower()}"
        )
        print(f"Atom:      {atom_url}")
        print(f"Publish:   {publish_url}")
        print(f"Rules:     {rules_url}")
        print(f"Reader:    {reader_url}")
        print(f"API:       http://{api_host}:{api_port}")
        print(
            "Presence:  "
            f"stale>{presence_policy.stale_after_seconds}s "
            f"offline>{presence_policy.offline_after_seconds}s"
        )
        print(
            "Retry:     "
            f"max={retry_policy.max_attempts} base={retry_policy.base_delay_seconds}s "
            f"max_delay={retry_policy.max_delay_seconds}s"
        )
        print(f"Pending:   retry={state_store.count_retries()} dlq={state_store.count_dlq()}")
        for index, config in enumerate(configs, start=1):
            print(f"Input {index}: {config.host}:{config.port} topic={config.topic} source={config.source}")
        print("Bootstrapping and starting inputs...")
        service.start()
        management = start_management_server(
            host=api_host,
            port=api_port,
            service=service,
            runtime=runtime,
            store=state_store,
            reader=reader,
            presence_policy=presence_policy,
            catalog_manager=catalog_manager,
            api_token=api_token,
        )
        assert runtime.base is not None
        assert runtime.persistence_rule is not None
        print(f"Workspace ID: {runtime.base.workspace.id}")
        print(f"Channel ID:   {runtime.base.channel.id}")
        print(f"Rule:         {runtime.persistence_rule.id}")
        print("RUNNING: Ctrl+C to stop", flush=True)
        stop.wait()
    finally:
        if management is not None:
            management.shutdown()
            management.server_close()
        service.stop()
        stats = service.stats
        pending_retry = state_store.count_retries()
        pending_dlq = state_store.count_dlq()
        state_store.close()
        print(
            "STOPPED "
            f"received={stats.received} processed={stats.processed} failed={stats.failed} "
            f"queued={stats.queued} retried={stats.retried} recovered={stats.recovered} "
            f"dead_lettered={stats.dead_lettered} pending_retry={pending_retry} dlq={pending_dlq}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
