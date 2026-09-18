from __future__ import annotations

import math
import time
from typing import Any, Mapping, Optional

from .historical_intelligence import TimescaleHistoryProvider


class DeviceContextProvider:
    """Build provider-neutral device context without waiting for new telemetry.

    Durable adapter state (identity, presence and latest quality snapshots) comes
    from the SMA management store. Measurements and trends come from persisted
    Magistrala Timescale rows. The two sources are intentionally kept distinct:
    a delayed Timescale writer must never overwrite newer adapter quality state.
    """

    schema = "smarter-adapter.device-context/1"

    def __init__(
        self,
        *,
        reader,
        store,
        presence_policy,
        history_provider: TimescaleHistoryProvider,
        gateway_presence_policy=None,
    ) -> None:
        self.reader = reader
        self.store = store
        self.presence = presence_policy
        self.history_provider = history_provider
        self.gateway_presence = gateway_presence_policy

    @staticmethod
    def _public_value(item: Mapping[str, Any]) -> Any:
        raw = item.get("value")
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, (int, float)):
            value = float(raw)
            return value if math.isfinite(value) else str(value)
        string_value = item.get("string_value")
        if string_value is not None:
            return str(string_value)
        return raw

    def _latest_observation(
        self,
        messages,
        *,
        external_id: str,
    ) -> dict[str, Any]:
        rows: list[tuple[float, dict[str, Any]]] = []
        for raw in messages:
            if not isinstance(raw, Mapping):
                continue
            ts = self.history_provider._time(raw)
            if ts is None:
                continue
            name = self.history_provider._measurement_name(raw.get("name"), external_id)
            if not name:
                continue
            unit_raw = raw.get("unit")
            unit = str(unit_raw) if unit_raw not in (None, "") else None
            rows.append(
                (
                    float(ts),
                    {
                        "name": name,
                        "value": self._public_value(raw),
                        "unit": unit,
                        "timestamp": float(ts),
                    },
                )
            )

        if not rows:
            return {
                "status": "empty",
                "source": "magistrala-timescale",
                "persisted": True,
                "at": None,
                "measurements": [],
            }

        latest_at = max(ts for ts, _ in rows)
        # Timescale persists one row per SenML measurement. Rows belonging to
        # the same observation have the same timestamp after epoch normalization.
        latest = [item for ts, item in rows if abs(ts - latest_at) < 1.0e-6]
        latest.sort(key=lambda item: (".raw." in item["name"], item["name"]))
        return {
            "status": "available",
            "source": "magistrala-timescale",
            "persisted": True,
            "at": latest_at,
            "measurements": latest,
        }

    @staticmethod
    def _unavailable_history(exc: Exception) -> dict[str, Any]:
        return {
            "status": "unavailable",
            "source": "magistrala-timescale",
            "rows_returned": 0,
            "rows_total": 0,
            "coverage": {"first_at": None, "last_at": None},
            "quality_counts": {
                "valid": 0,
                "degraded": 0,
                "invalid": 0,
                "unknown": 0,
            },
            "series": [],
            "error": f"{type(exc).__name__}: {str(exc)[:300]}",
            "interpretation": (
                "Historical context is temporarily unavailable; reason from durable adapter state only."
            ),
        }

    def _gateway_links(
        self,
        workspace_id: str,
        channel_id: str,
        external_id: str,
    ) -> list[dict[str, Any]]:
        lister = getattr(self.store, "list_device_gateways", None)
        if not callable(lister):
            return []
        links = [
            dict(item)
            for item in lister(workspace_id, channel_id, external_id)
            if isinstance(item, Mapping)
        ]
        if self.gateway_presence is None:
            return links

        finder = getattr(self.store, "find_gateway", None)
        if not callable(finder):
            return links

        enriched: list[dict[str, Any]] = []
        for link in links:
            item = dict(link)
            gateway_id = str(item.get("gateway_id") or "").strip().lower()
            gateway = finder(workspace_id, gateway_id) if gateway_id else None
            if gateway is not None:
                decorated = self.gateway_presence.decorate(gateway)
                item["gateway_operational_status"] = decorated.get("operational_status")
                item["gateway_last_seen"] = decorated.get("last_seen")
                item["gateway_last_seen_age_seconds"] = decorated.get(
                    "last_seen_age_seconds"
                )
                item["gateway_last_stats_at"] = decorated.get("last_stats_at")
                item["gateway_last_stats_at_age_seconds"] = decorated.get(
                    "last_stats_at_age_seconds"
                )
            enriched.append(item)
        return enriched

    @staticmethod
    def _text(context: Mapping[str, Any]) -> str:
        device = context["device"]
        state = context["state"]
        deployment = context["deployment"]
        latest = context["latest_observation"]
        history = context["history"]
        network = context.get("network") or {}

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
            f"operational status is {state['operational_status']}",
            f"adapter data quality is {state['data_quality']}",
        ]

        quality_by_role = state.get("quality_by_role") or {}
        if quality_by_role:
            rendered_roles = ", ".join(
                f"{role}={value.get('data_quality', 'unknown')}"
                for role, value in sorted(quality_by_role.items())
            )
            parts.append("quality by message role: " + rendered_roles)

        if state.get("invalid_fields"):
            parts.append(
                "invalid fields: " + ", ".join(state["invalid_fields"])
                + "; treat them as unavailable, not zero"
            )

        gateways = list(network.get("observed_by_gateways") or [])
        if gateways:
            rendered_gateways = []
            for item in gateways[:3]:
                extras = []
                status = item.get("gateway_operational_status")
                if status:
                    extras.append(f"gateway {status}")
                if item.get("rssi") is not None:
                    extras.append(f"RSSI {item['rssi']}")
                if item.get("snr") is not None:
                    extras.append(f"SNR {item['snr']}")
                suffix = " (" + ", ".join(extras) + ")" if extras else ""
                rendered_gateways.append(str(item.get("gateway_id")) + suffix)
            parts.append("observed by LoRaWAN gateway(s): " + ", ".join(rendered_gateways))

        if latest.get("status") == "available":
            rendered = []
            for item in latest.get("measurements", [])[:8]:
                unit = f" {item['unit']}" if item.get("unit") else ""
                rendered.append(f"{item['name']}={item['value']}{unit}")
            if rendered:
                parts.append("latest persisted observation: " + ", ".join(rendered))
        elif latest.get("status") == "unavailable":
            parts.append("latest persisted observation is temporarily unavailable")
        else:
            parts.append("no persisted observation is available yet")

        series = list(history.get("series") or [])
        if history.get("status") == "available" and series:
            trends = []
            for item in series[:4]:
                trends.append(
                    f"{item.get('name')} {item.get('direction')} "
                    f"(n={item.get('samples')})"
                )
            parts.append("historical trends: " + ", ".join(trends))
        elif history.get("status") == "unavailable":
            parts.append("historical trends are temporarily unavailable")

        return "; ".join(part for part in parts if part) + "."

    def build(
        self,
        *,
        workspace_id: str,
        channel_id: str,
        external_id: str,
    ) -> dict[str, Any]:
        device = self.store.find_device(workspace_id, channel_id, external_id)
        if device is None:
            raise KeyError("managed device not found")

        decorated = self.presence.decorate(device)
        metadata = dict(decorated.get("observation_metadata") or {})
        node_id = str(decorated.get("node_id") or "")
        sensor = str(decorated.get("observed_sensor") or metadata.get("sensor") or "unknown")

        administrative_state = "active"
        lifecycle = None
        lifecycle_getter = getattr(self.store, "get_node_lifecycle", None)
        if node_id and callable(lifecycle_getter):
            lifecycle = lifecycle_getter(workspace_id, channel_id, node_id)
            if lifecycle:
                administrative_state = str(lifecycle.get("administrative_state") or "active")

        observed_gateways = self._gateway_links(workspace_id, channel_id, external_id)

        try:
            page = self.reader.list_device_messages(
                workspace_id,
                channel_id,
                external_id,
                limit=self.history_provider.limit,
                offset=0,
                order="time",
                direction="desc",
            )
            latest = self._latest_observation(page.messages, external_id=external_id)
            history = self.history_provider.summarize_messages(
                page.messages,
                total=page.total,
                external_id=external_id,
            )
        except Exception as exc:
            latest = {
                "status": "unavailable",
                "source": "magistrala-timescale",
                "persisted": True,
                "at": None,
                "measurements": [],
                "error": f"{type(exc).__name__}: {str(exc)[:300]}",
            }
            history = self._unavailable_history(exc)

        transport = metadata.get("transport")
        if not isinstance(transport, dict):
            transport = {}

        context: dict[str, Any] = {
            "schema": self.schema,
            "kind": "managed_device_context",
            "generated_at": time.time(),
            "device": {
                "external_id": external_id,
                "atom_device_id": decorated.get("atom_device_id"),
                "profile_id": decorated.get("profile_id"),
                "profile_version_id": decorated.get("profile_version_id"),
                "sensor_family": sensor,
                "node_id": node_id or None,
            },
            "deployment": {
                "location": metadata.get("location"),
                "sub_location": metadata.get("sub_location"),
                "depth": metadata.get("depth"),
                "application_id": metadata.get("application_id"),
                "f_port": metadata.get("f_port"),
                "message_role": metadata.get("message_role"),
                "port_role": metadata.get("port_role"),
                "mqtt_topic": metadata.get("topic") or transport.get("mqtt_topic"),
                "transport": transport,
            },
            "state": {
                "administrative_state": administrative_state,
                "operational_status": decorated.get("operational_status"),
                "last_seen": decorated.get("last_seen"),
                "last_seen_age_seconds": decorated.get("last_seen_age_seconds"),
                "data_quality": decorated.get("data_quality", "unknown"),
                "invalid_fields": list(decorated.get("invalid_fields") or []),
                "quality_evaluated_at": decorated.get("quality_evaluated_at"),
                "quality_source_received_at": decorated.get("quality_source_received_at"),
                "quality_by_role": dict(decorated.get("quality_by_role") or {}),
            },
            "network": {
                "observed_by_gateways": observed_gateways,
            },
            "latest_observation": latest,
            "history": history,
        }
        if lifecycle:
            context["state"]["decommissioned_at"] = lifecycle.get("decommissioned_at")
            context["state"]["reactivated_at"] = lifecycle.get("reactivated_at")
            context["state"]["decommission_reason"] = lifecycle.get("decommission_reason")
        context["text"] = self._text(context)
        return context


__all__ = ["DeviceContextProvider"]
