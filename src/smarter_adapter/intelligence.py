from __future__ import annotations

import fnmatch
import json
import math
import threading
import time
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional, Sequence
from urllib.parse import urlparse

try:
    import paho.mqtt.client as mqtt
except ImportError:  # pragma: no cover - optional at import time
    mqtt = None


LEGACY_QUALITY_THRESHOLDS: Mapping[str, Mapping[str, float]] = {
    "rel.humidity": {"min": 0.0, "max": 1.0},
    "battery.level": {"min": 0.0, "max": 100.0},
    "soil.raw.moisture_*": {"min": 0.0, "max": 2500.0},
    "soil.raw.ec_*": {"min": 0.0, "max": 2500.0},
    "soil.raw.temp_*": {"min": -25.0, "max": 100.0},
}


@dataclass(frozen=True)
class ThresholdRule:
    pattern: str
    minimum: float
    maximum: float

    def matches(self, name: str) -> bool:
        return fnmatch.fnmatchcase(name, self.pattern)


class ThresholdPolicy:
    """Legacy-compatible numeric range checks for normalized measurements."""

    def __init__(self, rules: Sequence[ThresholdRule]) -> None:
        self.rules = tuple(rules)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ThresholdPolicy":
        rules = []
        for pattern, raw in value.items():
            if not isinstance(raw, Mapping) or "min" not in raw or "max" not in raw:
                continue
            rules.append(
                ThresholdRule(
                    pattern=str(pattern),
                    minimum=float(raw["min"]),
                    maximum=float(raw["max"]),
                )
            )
        return cls(rules)

    @classmethod
    def from_json(
        cls,
        raw: str = "",
        *,
        default: Mapping[str, Any] = LEGACY_QUALITY_THRESHOLDS,
    ) -> "ThresholdPolicy":
        if not str(raw or "").strip():
            return cls.from_mapping(default)
        value = json.loads(raw)
        if not isinstance(value, Mapping):
            raise ValueError("quality thresholds JSON must be an object")
        return cls.from_mapping(value)

    def find(self, name: str) -> Optional[ThresholdRule]:
        # Preserve the old precedence: exact names before wildcard patterns.
        for rule in self.rules:
            if rule.pattern == name:
                return rule
        for rule in self.rules:
            if rule.pattern != name and rule.matches(name):
                return rule
        return None

    def violations(self, event) -> tuple[dict[str, Any], ...]:
        results = []
        for measurement in event.measurements:
            value = measurement.value
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                continue
            numeric = float(value)
            if not math.isfinite(numeric):
                continue
            rule = self.find(measurement.name)
            if rule is None or rule.minimum <= numeric <= rule.maximum:
                continue
            results.append(
                {
                    "type": "threshold_violation",
                    "severity": "warning",
                    "external_id": event.external_device_id,
                    "sensor": str(event.metadata.get("sensor") or "unknown"),
                    "node_id": str(event.metadata.get("node_id") or ""),
                    "name": measurement.name,
                    "value": numeric,
                    "unit": measurement.unit,
                    "min": rule.minimum,
                    "max": rule.maximum,
                    "threshold": rule.pattern,
                    "bt": float(
                        measurement.timestamp
                        if measurement.timestamp is not None
                        else event.metadata.get("bt") or time.time()
                    ),
                }
            )
        return tuple(results)


def alerts_for_result(result, policy: ThresholdPolicy) -> tuple[dict[str, Any], ...]:
    """Build quality and threshold alerts without coupling them to delivery.

    Source-sentinel failures are represented by the quality gate, so they are
    emitted as ``data_quality`` alerts instead of pretending the invalid raw
    value is a physical threshold violation.
    """
    event = result.parsed_event
    alerts = []
    if str(result.quality_status) in {"degraded", "invalid"}:
        fields = []
        for issue in result.quality_issues:
            value = str(issue.source_field or issue.measurement or "").strip()
            if value and value not in fields:
                fields.append(value)
        alerts.append(
            {
                "type": "data_quality",
                "severity": "warning" if result.quality_status == "degraded" else "critical",
                "external_id": event.external_device_id,
                "sensor": str(event.metadata.get("sensor") or "unknown"),
                "node_id": str(event.metadata.get("node_id") or ""),
                "quality": str(result.quality_status),
                "invalid_fields": fields,
                "bt": float(event.metadata.get("bt") or time.time()),
            }
        )
    alerts.extend(policy.violations(event))
    return tuple(alerts)


class LLMContextBuilder:
    """Prepare a model-neutral, deterministic context envelope for an LLM.

    This class does not call an LLM. The returned object can be sent to Ollama,
    an agent, a RAG pipeline, MQTT, or an HTTP service without coupling the
    adapter to any model provider.
    """

    schema = "smarter-adapter.llm-context/1"

    @staticmethod
    def _measurement_public(item) -> dict[str, Any]:
        value = item.value
        if isinstance(value, float) and not math.isfinite(value):
            value = str(value)
        return {
            "name": item.name,
            "value": value,
            "unit": item.unit,
            "timestamp": item.timestamp,
        }

    def build(
        self,
        result,
        *,
        alerts: Iterable[Mapping[str, Any]] = (),
    ) -> dict[str, Any]:
        event = result.parsed_event
        metadata = event.metadata
        alert_items = [dict(item) for item in alerts]
        invalid_fields = []
        for issue in result.quality_issues:
            value = str(issue.source_field or issue.measurement or "").strip()
            if value and value not in invalid_fields:
                invalid_fields.append(value)

        context = {
            "schema": self.schema,
            "kind": "sensor_observation",
            "device": {
                "external_id": event.external_device_id,
                "atom_device_id": result.device.id,
                "profile_id": result.device.profile_id,
                "profile_version_id": result.device.profile_version_id,
                "profile_key": str(getattr(result, "profile_key", "") or ""),
                "sensor_family": str(metadata.get("sensor") or "unknown"),
                "node_id": str(metadata.get("node_id") or ""),
            },
            "deployment": {
                "location": metadata.get("location"),
                "sub_location": metadata.get("sub_location"),
                "depth": metadata.get("depth"),
                "application_id": metadata.get("application_id"),
                "f_port": metadata.get("f_port"),
            },
            "observation": {
                "at": float(metadata.get("bt") or time.time()),
                "measurements": [
                    self._measurement_public(item) for item in event.measurements
                ],
            },
            "quality": {
                "status": str(result.quality_status),
                "invalid_fields": invalid_fields,
                "interpretation": (
                    "Invalid fields are unavailable sensor readings and must not be interpreted as physical zero."
                    if invalid_fields
                    else "Measurements passed the adapter data-quality gate."
                ),
            },
            "alerts": alert_items,
        }
        context["text"] = self._text(context)
        return context

    @staticmethod
    def _text(context: Mapping[str, Any]) -> str:
        device = context["device"]
        deployment = context["deployment"]
        observation = context["observation"]
        quality = context["quality"]
        measurements = []
        for item in observation["measurements"]:
            unit = f" {item['unit']}" if item.get("unit") else ""
            measurements.append(f"{item['name']}={item['value']}{unit}")
        where = "/".join(
            str(value)
            for value in (deployment.get("location"), deployment.get("sub_location"))
            if value
        )
        parts = [
            f"Device {device['external_id']} is a {device['sensor_family']} sensor",
            f"node {device['node_id']}" if device.get("node_id") else "",
            f"at {where}" if where else "",
            f"depth {deployment['depth']}" if deployment.get("depth") else "",
            "reported " + ", ".join(measurements) if measurements else "reported no usable measurements",
            f"data quality is {quality['status']}",
        ]
        if quality.get("invalid_fields"):
            parts.append(
                "invalid fields: " + ", ".join(quality["invalid_fields"])
                + "; treat them as unavailable, not zero"
            )
        if context.get("alerts"):
            parts.append(f"active adapter alerts: {len(context['alerts'])}")
        return "; ".join(part for part in parts if part) + "."


class MqttJsonPublisher:
    """Small lazy MQTT JSON side-channel used by alerts and LLM context.

    Failures are returned to the caller and never affect the primary Magistrala
    delivery path. This keeps an alert broker outage from duplicating sensor
    telemetry through the adapter retry queue.
    """

    def __init__(
        self,
        *,
        address: str,
        topic_base: str,
        qos: int = 0,
        username: str = "",
        password: str = "",
        client_id: str = "smarter-adapter-sidechannel",
    ) -> None:
        self.address = str(address or "").strip()
        self.topic_base = str(topic_base or "").strip().strip("/")
        self.qos = int(qos)
        self.username = str(username or "")
        self.password = str(password or "")
        self.client_id = str(client_id or "smarter-adapter-sidechannel")
        self._lock = threading.RLock()
        self._client = None
        self._loop_started = False

        if self.qos not in (0, 1, 2):
            raise ValueError("MQTT side-channel qos must be 0, 1, or 2")

    @property
    def enabled(self) -> bool:
        return bool(self.address and self.topic_base and mqtt is not None)

    def _endpoint(self) -> tuple[str, int, bool]:
        raw = self.address
        if "://" not in raw:
            raw = "tcp://" + raw
        parsed = urlparse(raw)
        scheme = parsed.scheme.lower()
        tls = scheme in {"ssl", "tls", "mqtts"}
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or (8883 if tls else 1883)
        return host, port, tls

    def _ensure_client(self):
        if not self.enabled:
            return None
        if self._client is not None:
            return self._client
        host, port, tls = self._endpoint()
        try:
            client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id=self.client_id,
                protocol=mqtt.MQTTv311,
            )
        except (AttributeError, TypeError):  # pragma: no cover - paho 1.x compatibility
            client = mqtt.Client(client_id=self.client_id, protocol=mqtt.MQTTv311)
        if self.username:
            client.username_pw_set(self.username, self.password or None)
        if tls:
            client.tls_set()
        client.connect(host, port, keepalive=30)
        client.loop_start()
        self._loop_started = True
        self._client = client
        return client

    def publish(
        self,
        payload: Mapping[str, Any],
        *,
        suffix: str = "",
        timeout: float = 3.0,
    ) -> tuple[bool, str]:
        if not self.enabled:
            return False, "disabled"
        topic = self.topic_base
        clean_suffix = str(suffix or "").strip().strip("/")
        if clean_suffix:
            topic += "/" + clean_suffix
        body = json.dumps(dict(payload), ensure_ascii=False, separators=(",", ":"))
        with self._lock:
            try:
                client = self._ensure_client()
                if client is None:
                    return False, "disabled"
                info = client.publish(topic, payload=body, qos=self.qos, retain=False)
                if self.qos > 0:
                    published = info.wait_for_publish(timeout=timeout)
                    if published is False:
                        return False, "publish timeout"
                return True, topic
            except Exception as exc:
                self.close()
                return False, f"{type(exc).__name__}: {exc}"

    def close(self) -> None:
        with self._lock:
            client = self._client
            self._client = None
            if client is None:
                self._loop_started = False
                return
            try:
                client.disconnect()
            except Exception:
                pass
            if self._loop_started:
                try:
                    client.loop_stop()
                except Exception:
                    pass
            self._loop_started = False


__all__ = [
    "LEGACY_QUALITY_THRESHOLDS",
    "LLMContextBuilder",
    "MqttJsonPublisher",
    "ThresholdPolicy",
    "ThresholdRule",
    "alerts_for_result",
]
