from __future__ import annotations

from typing import Any, Mapping


def _num(value: Any) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    try:
        return repr(float(value))
    except (TypeError, ValueError):
        return "0"


def _label(value: Any) -> str:
    text = str(value or "")
    return text.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def render_prometheus_metrics(status: Mapping[str, Any], *, ready: bool) -> str:
    """Render a dependency-free Prometheus/OpenMetrics-compatible text snapshot."""
    service = dict(status.get("service") or {})
    queues = dict(status.get("queues") or {})
    devices = dict(status.get("devices") or {})
    gateways = dict(status.get("gateways") or {})
    quality = dict(status.get("quality") or {})
    catalog = dict(status.get("catalog") or {})
    runtime = dict(status.get("runtime") or {})
    inputs = list(status.get("inputs") or [])

    lines = [
        "# HELP sma_up Smarter Adapter management process is responding.",
        "# TYPE sma_up gauge",
        "sma_up 1",
        "# HELP sma_ready Smarter Adapter runtime and inputs are ready.",
        "# TYPE sma_ready gauge",
        f"sma_ready {_num(ready)}",
    ]

    counters = (
        ("received", "sma_events_received_total", "Raw events received from inputs."),
        ("processed", "sma_events_processed_total", "Events processed and published successfully."),
        ("failed", "sma_events_failed_total", "Initial processing failures."),
        ("queued", "sma_events_queued_total", "Failures queued for retry."),
        ("retried", "sma_events_retried_total", "Retry attempts executed."),
        ("recovered", "sma_events_recovered_total", "Retry attempts that recovered successfully."),
        ("dead_lettered", "sma_events_dead_lettered_total", "Events moved to the dead-letter queue."),
        (
            "suppressed",
            "sma_events_suppressed_total",
            "Events intentionally suppressed by administrative lifecycle policy.",
        ),
    )
    for key, metric, help_text in counters:
        lines.extend(
            [
                f"# HELP {metric} {help_text}",
                f"# TYPE {metric} counter",
                f"{metric} {_num(service.get(key, 0))}",
            ]
        )

    lines.extend(
        [
            "# HELP sma_retry_queue_size Events currently waiting in the retry queue.",
            "# TYPE sma_retry_queue_size gauge",
            f"sma_retry_queue_size {_num(queues.get('retry', 0))}",
            "# HELP sma_dlq_size Events currently stored in the dead-letter queue.",
            "# TYPE sma_dlq_size gauge",
            f"sma_dlq_size {_num(queues.get('dlq', 0))}",
            "# HELP sma_devices Managed devices by operational presence state.",
            "# TYPE sma_devices gauge",
        ]
    )
    for state in ("online", "stale", "offline"):
        lines.append(f'sma_devices{{status="{state}"}} {_num(devices.get(state, 0))}')

    lines.extend(
        [
            "# HELP sma_gateways Observed LoRaWAN gateways by operational presence state.",
            "# TYPE sma_gateways gauge",
        ]
    )
    for state in ("online", "stale", "offline"):
        lines.append(f'sma_gateways{{status="{state}"}} {_num(gateways.get(state, 0))}')

    lines.extend(
        [
            "# HELP sma_gateway_expected_interval_seconds Expected gateway stats/heartbeat interval.",
            "# TYPE sma_gateway_expected_interval_seconds gauge",
            f"sma_gateway_expected_interval_seconds {_num(gateways.get('expected_interval_seconds', 0))}",
            "# HELP sma_gateway_stale_after_seconds Gateway stale threshold.",
            "# TYPE sma_gateway_stale_after_seconds gauge",
            f"sma_gateway_stale_after_seconds {_num(gateways.get('stale_after_seconds', 0))}",
            "# HELP sma_gateway_offline_after_seconds Gateway offline threshold.",
            "# TYPE sma_gateway_offline_after_seconds gauge",
            f"sma_gateway_offline_after_seconds {_num(gateways.get('offline_after_seconds', 0))}",
        ]
    )

    lines.extend(
        [
            "# HELP sma_device_quality Managed devices by latest known data-quality state.",
            "# TYPE sma_device_quality gauge",
        ]
    )
    for state in ("valid", "degraded", "invalid", "unknown"):
        lines.append(f'sma_device_quality{{status="{state}"}} {_num(quality.get(state, 0))}')

    lines.extend(
        [
            "# HELP sma_catalog_lifecycle Catalog nodes by lifecycle state.",
            "# TYPE sma_catalog_lifecycle gauge",
        ]
    )
    for state in ("planned", "observed", "managed", "decommissioned"):
        lines.append(
            f'sma_catalog_lifecycle{{state="{state}"}} {_num(catalog.get(state, 0))}'
        )

    lines.extend(
        [
            "# HELP sma_device_cache_size Devices held in the in-memory runtime cache.",
            "# TYPE sma_device_cache_size gauge",
            f"sma_device_cache_size {_num(runtime.get('device_cache_size', 0))}",
            "# HELP sma_reader_configured Whether the Timescale reader is configured.",
            "# TYPE sma_reader_configured gauge",
            f"sma_reader_configured {_num(bool(runtime.get('reader_configured', False)))}",
            "# HELP sma_input_connected MQTT input connection state.",
            "# TYPE sma_input_connected gauge",
        ]
    )
    for item in inputs:
        source = _label(item.get("source", ""))
        host = _label(item.get("host", ""))
        port = _label(item.get("port", ""))
        lines.append(
            f'sma_input_connected{{source="{source}",host="{host}",port="{port}"}} '
            f'{_num(bool(item.get("connected", False)))}'
        )

    return "\n".join(lines) + "\n"


__all__ = ["render_prometheus_metrics"]
