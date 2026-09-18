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

from smarter_adapter.device_lifecycle import (
    DeviceLifecycleController,
    LifecycleIrrigapCatalogManager,
    LifecycleSmarterAdapterRuntime,
)
from smarter_adapter.gateway_monitor import (
    GatewayMqttConfig,
    GatewayMqttObserver,
    GatewayPresencePolicy,
    GatewayRegistry,
    GatewayTopologySQLiteManagementStore,
)
from smarter_adapter.historical_intelligence import (
    AsyncIntelligenceSideChannel,
    HistoricalLLMContextBuilder,
    TimescaleHistoryProvider,
)
from smarter_adapter.inputs import MQTTInputConfig
from smarter_adapter.intelligence import (
    MqttJsonPublisher,
    ThresholdPolicy,
    alerts_for_result,
)
from smarter_adapter.irrigap_config import load_irrigap_catalog
from smarter_adapter.legacy_parser import LegacySensorParser
from smarter_adapter.lifecycle_management import start_lifecycle_management_server
from smarter_adapter.lifecycle_service import LifecycleSmarterAdapterService
from smarter_adapter.magistrala import AtomConfig, ControlPlane, RulesClient, TimescaleReaderClient
from smarter_adapter.magistrala.lifecycle import LifecycleAtomClient
from smarter_adapter.magistrala.publisher import FluxMQPublisher
from smarter_adapter.parsers import IrrigapChirpStackParser
from smarter_adapter.pipeline import ParsePipeline
from smarter_adapter.plugins import ParserRegistry
from smarter_adapter.presence import DevicePresencePolicy
from smarter_adapter.reliability import RetryPolicy
from smarter_adapter.runtime import RuntimeConfig


def env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default)).strip()


def env_bool(name: str, default: bool = False) -> bool:
    raw = env(name, "true" if default else "false").lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError(f"{name} must be true/false")


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

    atom = LifecycleAtomClient(
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
    history_reader = TimescaleReaderClient(
        reader_url,
        atom.token,
        invalidate_token=atom.tokens.invalidate,
        timeout=float(env("SMA_LLM_HISTORY_TIMEOUT", "2")),
    )

    catalog_inline = env("SMA_IRRIGAP_NODES_JSON")
    catalog_file = env("SMA_IRRIGAP_NODES_FILE")
    if not catalog_inline and not catalog_file:
        catalog_file = deployment["irrigap_nodes_file"]
    irrigap_catalog = load_irrigap_catalog(
        file_path=catalog_file,
        inline_json=catalog_inline,
    )
    catalog_manager = LifecycleIrrigapCatalogManager(
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
    state_store = GatewayTopologySQLiteManagementStore(state_path)
    runtime_config = RuntimeConfig(
        workspace_name=env("SMA_WORKSPACE_NAME", deployment["workspace_name"]),
        workspace_alias=env("SMA_WORKSPACE_ALIAS", deployment["workspace_alias"]),
        channel_name=env("SMA_CHANNEL_NAME", deployment["channel_name"]),
        channel_alias=env("SMA_CHANNEL_ALIAS", deployment["channel_alias"]),
        persistence_rule_name=env("SMA_PERSISTENCE_RULE_NAME", "smarter-adapter-save-senml"),
    )
    runtime = LifecycleSmarterAdapterRuntime(
        pipeline=pipeline,
        control=control,
        rules=rules,
        publisher=publisher,
        config=runtime_config,
        state_store=state_store,
    )

    try:
        threshold_policy = ThresholdPolicy.from_json(env("SMA_QUALITY_THRESHOLDS_JSON"))
    except (ValueError, json.JSONDecodeError) as exc:
        print(f"ERROR: invalid SMA_QUALITY_THRESHOLDS_JSON: {exc}", file=sys.stderr)
        return 2

    alerts_address = env("SMA_ALERTS_MQTT_ADDRESS", env("ALERTS_MQTT_ADDRESS"))
    alert_publisher = MqttJsonPublisher(
        address=alerts_address,
        topic_base=env("SMA_ALERTS_TOPIC_BASE", "adapter/alerts"),
        qos=int(env("SMA_ALERTS_QOS", "0")),
        username=env("SMA_ALERTS_MQTT_USERNAME"),
        password=env("SMA_ALERTS_MQTT_PASSWORD"),
        client_id=env("SMA_ALERTS_CLIENT_ID", "smarter-adapter-alerts-v2"),
    )
    context_address = env("SMA_LLM_CONTEXT_MQTT_ADDRESS")
    context_publisher = MqttJsonPublisher(
        address=context_address,
        topic_base=env("SMA_LLM_CONTEXT_TOPIC_BASE", "adapter/llm-context"),
        qos=int(env("SMA_LLM_CONTEXT_QOS", "0")),
        username=env("SMA_LLM_CONTEXT_MQTT_USERNAME"),
        password=env("SMA_LLM_CONTEXT_MQTT_PASSWORD"),
        client_id=env("SMA_LLM_CONTEXT_CLIENT_ID", "smarter-adapter-llm-context-v2"),
    )
    history_provider = TimescaleHistoryProvider(
        history_reader,
        limit=int(env("SMA_LLM_HISTORY_LIMIT", "120")),
        per_series_limit=int(env("SMA_LLM_HISTORY_SERIES_SAMPLES", "12")),
        max_series=int(env("SMA_LLM_HISTORY_MAX_SERIES", "16")),
    )
    historical_context_builder = HistoricalLLMContextBuilder()

    def intelligence_scope() -> tuple[str, str]:
        base = runtime.base
        if base is None:
            raise RuntimeError("runtime is not bootstrapped")
        return base.workspace.id, base.channel.id

    def intelligence_warning(message: str) -> None:
        print(f"WARN {message}", file=sys.stderr, flush=True)

    intelligence_worker = AsyncIntelligenceSideChannel(
        context_builder=historical_context_builder,
        history_provider=history_provider,
        alert_publisher=alert_publisher,
        context_publisher=context_publisher,
        scope_provider=intelligence_scope,
        max_queue=int(env("SMA_INTELLIGENCE_QUEUE_MAX", "128")),
        on_warning=intelligence_warning,
    )

    def on_result(result):
        parsed = result.parsed_event
        issue_fields = []
        for issue in result.quality_issues:
            field = issue.source_field or issue.measurement
            if field not in issue_fields:
                issue_fields.append(field)
        quality_detail = ""
        if issue_fields:
            quality_detail = " invalid=" + ",".join(issue_fields)

        adapter_alerts = alerts_for_result(result, threshold_policy)
        queued, intelligence_state = intelligence_worker.submit(result, adapter_alerts)
        if context_publisher.enabled:
            context_state = intelligence_state
        else:
            context_state = "builder-only"
        if intelligence_worker.enabled and not queued and intelligence_state != "disabled":
            intelligence_warning(
                f"side-channel external={parsed.external_device_id} state={intelligence_state}"
            )

        role = str(parsed.metadata.get("message_role") or "unknown")
        print(
            "OK "
            f"parser={result.parser} external={parsed.external_device_id} "
            f"device={result.device.id} cache={result.device_cache_source} "
            f"profile={result.profile_key} role={role} "
            f"quality={result.quality_status}{quality_detail} "
            f"alerts={len(adapter_alerts)} context={context_state} "
            f"records={len(result.senml)} http={result.publish.status}",
            flush=True,
        )

    def on_error(exc):
        print(f"ERROR {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)

    def on_suppressed(exc):
        print(
            f"SUPPRESSED lifecycle=decommissioned node={exc.node_id} "
            f"external={exc.external_id}",
            flush=True,
        )

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
    gateway_presence_policy = GatewayPresencePolicy(
        expected_interval_seconds=float(env("SMA_GATEWAY_EXPECTED_INTERVAL", "30")),
        stale_after_seconds=float(env("SMA_GATEWAY_STALE_AFTER", "90")),
        offline_after_seconds=float(env("SMA_GATEWAY_OFFLINE_AFTER", "180")),
    )
    gateway_enabled = env_bool(
        "SMA_GATEWAY_MONITOR_ENABLED",
        default=deployment["environment"] == "irrigap",
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
            "profile_id": decorated.get("profile_id"),
            "profile_version_id": decorated.get("profile_version_id"),
            "operational_status": decorated["operational_status"],
            "last_seen": decorated["last_seen"],
            "last_seen_age_seconds": decorated["last_seen_age_seconds"],
            "data_quality": decorated.get("data_quality", "unknown"),
            "quality_by_role": decorated.get("quality_by_role", {}),
            "invalid_fields": decorated.get("invalid_fields", []),
            "quality_evaluated_at": decorated.get("quality_evaluated_at"),
            "quality_source_received_at": decorated.get("quality_source_received_at"),
            "binding_observed_at": decorated.get("binding_observed_at"),
        }

    def catalog_observation(node_id: str):
        base = runtime.base
        if base is None:
            return None
        return state_store.find_latest_catalog_observation_by_node(
            base.workspace.id,
            base.channel.id,
            node_id,
        )

    def catalog_lifecycle(node_id: str):
        base = runtime.base
        if base is None:
            return None
        return state_store.get_node_lifecycle(
            base.workspace.id,
            base.channel.id,
            node_id,
        )

    catalog_manager.set_binding_resolver(catalog_binding)
    catalog_manager.set_observation_resolver(catalog_observation)
    catalog_manager.set_lifecycle_resolver(catalog_lifecycle)

    service = LifecycleSmarterAdapterService(
        runtime,
        configs,
        on_result=on_result,
        on_error=on_error,
        on_suppressed=on_suppressed,
        reliability_store=state_store,
        retry_policy=retry_policy,
    )
    lifecycle_controller = DeviceLifecycleController(
        runtime=runtime,
        store=state_store,
        catalog=catalog_manager,
        atom=atom,
    )

    gateway_registry = GatewayRegistry(runtime=runtime, control=control, store=state_store)
    gateway_observer = None
    if gateway_enabled:
        primary = configs[0]
        raw_topics = env("SMA_GATEWAY_MQTT_TOPICS")
        topics = tuple(
            item.strip() for item in raw_topics.split(",") if item.strip()
        ) if raw_topics else GatewayMqttConfig.topics
        gateway_observer = GatewayMqttObserver(
            GatewayMqttConfig(
                host=env("SMA_GATEWAY_MQTT_HOST", primary.host),
                port=int(env("SMA_GATEWAY_MQTT_PORT", str(primary.port))),
                qos=int(env("SMA_GATEWAY_MQTT_QOS", "0")),
                username=env("SMA_GATEWAY_MQTT_USERNAME", primary.username),
                password=env("SMA_GATEWAY_MQTT_PASSWORD", primary.password),
                client_id=env("SMA_GATEWAY_MQTT_CLIENT_ID", "smarter-adapter-gateway-monitor"),
                topics=topics,
            ),
            gateway_registry,
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
        print(
            "Profiles:  generic fallback; typed="
            + ",".join(runtime.profile_registry.families)
            + " migration=in-place; gateway=lorawan"
        )
        print(
            "Alerts:    "
            f"mqtt={'enabled' if alert_publisher.enabled else 'disabled'} "
            f"topic={alert_publisher.topic_base} rules={len(threshold_policy.rules)}"
        )
        print(
            "LLM ctx:   builder=enabled "
            f"mqtt={'enabled' if context_publisher.enabled else 'disabled'} "
            f"topic={context_publisher.topic_base}/<external_id>"
        )
        print(
            "LLM hist:  timescale=enabled "
            f"rows={history_provider.limit} series_samples={history_provider.per_series_limit} "
            f"max_series={history_provider.max_series} "
            f"async_queue={intelligence_worker._queue.maxsize}"
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
            "Gateways:  "
            f"monitor={'enabled' if gateway_enabled else 'disabled'} "
            f"expected={gateway_presence_policy.expected_interval_seconds}s "
            f"stale>{gateway_presence_policy.stale_after_seconds}s "
            f"offline>{gateway_presence_policy.offline_after_seconds}s"
        )
        if gateway_observer is not None:
            print(
                "GW input:  "
                f"{gateway_observer.config.host}:{gateway_observer.config.port} "
                f"topics={','.join(gateway_observer.config.topics)}"
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
        intelligence_worker.start()
        service.start()
        if gateway_observer is not None:
            gateway_observer.start()
        management = start_lifecycle_management_server(
            host=api_host,
            port=api_port,
            service=service,
            runtime=runtime,
            store=state_store,
            lifecycle_controller=lifecycle_controller,
            reader=reader,
            presence_policy=presence_policy,
            catalog_manager=catalog_manager,
            api_token=api_token,
            gateway_presence_policy=gateway_presence_policy,
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
        if gateway_observer is not None:
            gateway_observer.stop()
        service.stop()
        intelligence_worker.close()
        alert_publisher.close()
        context_publisher.close()
        stats = service.stats
        intelligence_stats = intelligence_worker.stats
        pending_retry = state_store.count_retries()
        pending_dlq = state_store.count_dlq()
        state_store.close()
        print(
            "STOPPED "
            f"received={stats.received} processed={stats.processed} failed={stats.failed} "
            f"queued={stats.queued} retried={stats.retried} recovered={stats.recovered} "
            f"dead_lettered={stats.dead_lettered} suppressed={stats.suppressed} "
            f"intelligence_queued={intelligence_stats.queued} "
            f"intelligence_processed={intelligence_stats.processed} "
            f"intelligence_dropped={intelligence_stats.dropped} "
            f"intelligence_failures={intelligence_stats.failures} "
            f"pending_retry={pending_retry} dlq={pending_dlq}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
