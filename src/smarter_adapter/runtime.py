from __future__ import annotations

import re
import threading
import time
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

from .device_profiles import DeviceProfileRegistry
from .magistrala.control_plane import (
    GENERIC_DEVICE_TYPE_KEY,
    BaseResources,
    ControlPlane,
    DeviceRef,
    DeviceTypeRef,
)
from .magistrala.publisher import FluxMQPublisher, PublishError, PublishResult
from .magistrala.rules import PersistenceRuleRef, RulesClient
from .models import ParsedEvent, RawEvent
from .pipeline import ParsePipeline
from .quality import QualityIssue
from .senml import event_to_senml
from .storage import DeviceStateStore


@dataclass(frozen=True)
class RuntimeConfig:
    workspace_name: str = "Smarter Adapter"
    workspace_alias: str = "smarter-adapter"
    channel_name: str = "Telemetry"
    channel_alias: str = "telemetry"
    persistence_rule_name: str = "smarter-adapter-save-senml"


@dataclass(frozen=True)
class ProcessResult:
    parser: str
    parsed_event: ParsedEvent
    device: DeviceRef
    senml: Tuple[dict, ...]
    publish: PublishResult
    device_cache_hit: bool
    device_cache_source: str = "remote"
    quality_status: str = "valid"
    quality_issues: Tuple[QualityIssue, ...] = ()
    profile_key: str = GENERIC_DEVICE_TYPE_KEY


def _safe_alias(value: str) -> str:
    text = re.sub(r"[^a-z0-9-]+", "-", value.strip().lower()).strip("-")
    return text[:120] or "device"


def _device_attributes(event: ParsedEvent) -> dict:
    allowed = (
        "sensor",
        "node_id",
        "location",
        "sub_location",
        "depth",
        "application_id",
    )
    return {key: event.metadata[key] for key in allowed if key in event.metadata}


def _observation_metadata(event: ParsedEvent) -> dict:
    # Observation state deliberately keeps transport provenance separate from
    # Atom entity attributes. fPort/topic/message role describe a particular
    # ingress frame and may change during the lifetime of one physical sensor.
    allowed = (
        "sensor",
        "node_id",
        "location",
        "sub_location",
        "depth",
        "application_id",
        "f_port",
        "bt",
        "source",
        "topic",
        "transport",
        "message_role",
        "port_role",
        "port_role_mismatch",
        "raw_ultralight",
        "gateway_rx",
    )
    return {key: event.metadata[key] for key in allowed if key in event.metadata}


def _quality_invalid_fields(issues: Tuple[QualityIssue, ...]) -> tuple[str, ...]:
    fields = []
    for issue in issues:
        value = str(issue.source_field or issue.measurement or "").strip()
        if value and value not in fields:
            fields.append(value)
    return tuple(fields)


class SmarterAdapterRuntime:
    """Join parsing, typed Atom reconciliation, SenML and publication."""

    def __init__(
        self,
        *,
        pipeline: ParsePipeline,
        control: ControlPlane,
        rules: RulesClient,
        publisher: FluxMQPublisher,
        config: RuntimeConfig = RuntimeConfig(),
        state_store: Optional[DeviceStateStore] = None,
        profile_registry: Optional[DeviceProfileRegistry] = None,
    ) -> None:
        self.pipeline = pipeline
        self.control = control
        self.rules = rules
        self.publisher = publisher
        self.config = config
        self.state_store = state_store
        self.profile_registry = profile_registry or DeviceProfileRegistry()
        self.base: Optional[BaseResources] = None
        # Backward-compatible generic fallback used for readiness and unknown families.
        self.device_type: Optional[DeviceTypeRef] = None
        self.persistence_rule: Optional[PersistenceRuleRef] = None
        self._device_types: Dict[str, DeviceTypeRef] = {}
        self._devices: Dict[str, DeviceRef] = {}
        self._control_lock = threading.RLock()

    @property
    def device_cache_size(self) -> int:
        with self._control_lock:
            return len(self._devices)

    @property
    def profile_cache_size(self) -> int:
        with self._control_lock:
            return len(self._device_types)

    @property
    def profile_keys(self) -> tuple[str, ...]:
        with self._control_lock:
            return tuple(sorted(self._device_types))

    def clear_device_cache(self) -> int:
        """Drop only the in-memory device fast path.

        Persistent mappings remain intact, so the next event resolves from the
        state store before falling back to Atom. This is useful for explicit
        administrative reconciliation without losing durable state.
        """
        with self._control_lock:
            count = len(self._devices)
            self._devices.clear()
            return count

    def bootstrap(self, *, force: bool = False) -> None:
        with self._control_lock:
            if (
                not force
                and self.base is not None
                and self.device_type is not None
                and self.persistence_rule is not None
            ):
                return
            base = self.control.ensure_base(
                workspace_name=self.config.workspace_name,
                workspace_alias=self.config.workspace_alias,
                channel_name=self.config.channel_name,
                channel_alias=self.config.channel_alias,
            )
            generic_type = self.control.ensure_device_type(base.workspace.id)
            persistence = self.rules.ensure_senml_persistence(
                base.workspace.id,
                base.channel.id,
                name=self.config.persistence_rule_name,
            )
            self.base = base
            self.device_type = generic_type
            self._device_types[GENERIC_DEVICE_TYPE_KEY] = generic_type
            self.persistence_rule = persistence

    def _ensure_bootstrapped(self) -> tuple[BaseResources, DeviceTypeRef]:
        self.bootstrap()
        assert self.base is not None
        assert self.device_type is not None
        return self.base, self.device_type

    def _device_type_for(
        self,
        base: BaseResources,
        parsed: ParsedEvent,
        fallback: DeviceTypeRef,
    ) -> tuple[DeviceTypeRef, str]:
        spec = self.profile_registry.resolve(parsed)
        if spec is None:
            return fallback, GENERIC_DEVICE_TYPE_KEY
        cached = self._device_types.get(spec.key)
        if cached is None:
            cached = self.control.ensure_device_type(
                base.workspace.id,
                key=spec.key,
                name=spec.name,
                description=spec.description,
                json_schema=spec.json_schema,
            )
            self._device_types[spec.key] = cached
        return cached, spec.key

    @staticmethod
    def _profile_matches(device: DeviceRef, device_type: DeviceTypeRef) -> bool:
        return (
            device.profile_id == device_type.id
            and device.profile_version_id == device_type.version_id
        )

    def _record_catalog_observation(
        self,
        base: BaseResources,
        parsed: ParsedEvent,
        raw: RawEvent,
    ) -> None:
        """Persist successful physical parsing before device reconciliation."""
        if self.state_store is None:
            return
        setter = getattr(self.state_store, "observe_catalog_node", None)
        if not callable(setter):
            return
        node_id = str(parsed.metadata.get("node_id") or "").strip()
        if not node_id:
            return
        setter(
            base.workspace.id,
            base.channel.id,
            parsed.external_device_id,
            node_id=node_id,
            sensor=str(parsed.metadata.get("sensor") or ""),
            metadata=_observation_metadata(parsed),
            observed_at=raw.received_at,
        )

    def _record_managed_observation(
        self,
        base: BaseResources,
        parsed: ParsedEvent,
        raw: RawEvent,
    ) -> None:
        """Promote an observed physical node to a durable managed binding."""
        if self.state_store is None:
            return
        setter = getattr(self.state_store, "set_device_observation", None)
        if not callable(setter):
            return
        node_id = str(parsed.metadata.get("node_id") or "").strip()
        if not node_id:
            return
        setter(
            base.workspace.id,
            base.channel.id,
            parsed.external_device_id,
            node_id=node_id,
            sensor=str(parsed.metadata.get("sensor") or ""),
            metadata=_observation_metadata(parsed),
            observed_at=raw.received_at,
        )

    def _record_gateway_observations(
        self,
        base: BaseResources,
        parsed: ParsedEvent,
        raw: RawEvent,
    ) -> None:
        if self.state_store is None:
            return
        setter = getattr(self.state_store, "record_device_gateway", None)
        if not callable(setter):
            return
        items = parsed.metadata.get("gateway_rx") or []
        if not isinstance(items, (list, tuple)):
            return

        transport = parsed.metadata.get("transport")
        if not isinstance(transport, dict):
            transport = {}
        f_port = parsed.metadata.get("f_port")
        if f_port is None:
            f_port = transport.get("f_port")
        message_role = str(parsed.metadata.get("message_role") or "").strip().lower()
        mqtt_topic = str(
            parsed.metadata.get("topic")
            or transport.get("mqtt_topic")
            or raw.topic
            or ""
        )

        for item in items:
            if not isinstance(item, dict):
                continue
            # rxInfo owns radio observations (RSSI/SNR/channel). Frame-level
            # provenance lives beside rxInfo in ParsedEvent metadata, so enrich
            # the link sample at the runtime boundary before persistence.
            enriched = dict(item)
            if f_port is not None:
                enriched.setdefault("f_port", f_port)
            if message_role and message_role != "unknown":
                enriched.setdefault("message_role", message_role)
            if mqtt_topic:
                enriched.setdefault("mqtt_topic", mqtt_topic)
            setter(
                base.workspace.id,
                base.channel.id,
                parsed.external_device_id,
                enriched,
                observed_at=raw.received_at,
            )

    def _remote_device(
        self,
        base: BaseResources,
        device_type: DeviceTypeRef,
        parsed: ParsedEvent,
        raw: RawEvent,
    ) -> DeviceRef:
        device = self.control.ensure_device(
            base.workspace.id,
            base.channel.id,
            parsed.external_device_id,
            device_type=device_type,
            name=parsed.external_device_id,
            alias=_safe_alias(parsed.external_device_id),
            attributes=_device_attributes(parsed),
            allow_profile_migration=True,
        )
        self._devices[parsed.external_device_id] = device
        if self.state_store is not None:
            self.state_store.upsert_device(
                device,
                channel_id=base.channel.id,
                seen_at=raw.received_at,
            )
        return device

    def _resolve_device(
        self,
        base: BaseResources,
        device_type: DeviceTypeRef,
        parsed: ParsedEvent,
        raw: RawEvent,
    ) -> tuple[DeviceRef, str]:
        device = self._devices.get(parsed.external_device_id)
        if device is not None:
            if self._profile_matches(device, device_type):
                return device, "memory"
            migrated = self._remote_device(base, device_type, parsed, raw)
            return migrated, "profile-migrated" if migrated.profile_migrated else "remote"

        if self.state_store is not None:
            device = self.state_store.get_device(
                base.workspace.id,
                base.channel.id,
                parsed.external_device_id,
            )
            if device is not None:
                if self._profile_matches(device, device_type):
                    self._devices[parsed.external_device_id] = device
                    return device, "persistent"
                migrated = self._remote_device(base, device_type, parsed, raw)
                return migrated, "profile-migrated" if migrated.profile_migrated else "remote"

        device = self._remote_device(base, device_type, parsed, raw)
        return device, "profile-migrated" if device.profile_migrated else "remote"

    def _record_quality(
        self,
        base: BaseResources,
        parsed: ParsedEvent,
        raw: RawEvent,
        *,
        status: str,
        issues: Tuple[QualityIssue, ...],
    ) -> None:
        if self.state_store is None:
            return
        setter = getattr(self.state_store, "set_device_quality", None)
        if not callable(setter):
            return
        kwargs = {
            "quality_status": status,
            "invalid_fields": _quality_invalid_fields(issues),
            "evaluated_at": time.time(),
            "source_received_at": raw.received_at,
        }
        role = str(parsed.metadata.get("message_role") or "").strip().lower()
        if role and role != "unknown":
            kwargs["role"] = role
        try:
            setter(
                base.workspace.id,
                base.channel.id,
                parsed.external_device_id,
                **kwargs,
            )
        except TypeError as exc:
            # Backward compatibility for custom state stores that implement the
            # pre-role quality setter. Production storage accepts ``role``.
            if "role" not in kwargs or "role" not in str(exc):
                raise
            kwargs.pop("role", None)
            setter(
                base.workspace.id,
                base.channel.id,
                parsed.external_device_id,
                **kwargs,
            )

    def process(self, raw: RawEvent) -> ProcessResult:
        outcome = self.pipeline.process(raw)
        parsed = outcome.event

        with self._control_lock:
            base, fallback_type = self._ensure_bootstrapped()
            self._record_catalog_observation(base, parsed, raw)
            device_type, profile_key = self._device_type_for(base, parsed, fallback_type)
            device, cache_source = self._resolve_device(base, device_type, parsed, raw)

        senml = tuple(event_to_senml(parsed))
        try:
            published = self.publisher.publish(
                workspace_id=base.workspace.id,
                channel_id=base.channel.id,
                device_id=device.id,
                senml=list(senml),
            )
        except PublishError as exc:
            # A persistent/local mapping may be stale, or the device->channel
            # policy may have been removed. Reconcile once before giving the
            # reliability layer a chance to queue the raw event.
            if exc.status not in (403, 404):
                raise
            with self._control_lock:
                self._devices.pop(parsed.external_device_id, None)
                device = self._remote_device(base, device_type, parsed, raw)
                cache_source = "reconciled"
            published = self.publisher.publish(
                workspace_id=base.workspace.id,
                channel_id=base.channel.id,
                device_id=device.id,
                senml=list(senml),
            )

        if self.state_store is not None:
            self.state_store.touch_device(
                base.workspace.id,
                base.channel.id,
                parsed.external_device_id,
                seen_at=raw.received_at,
            )
            self._record_managed_observation(base, parsed, raw)
            self._record_gateway_observations(base, parsed, raw)
            self._record_quality(
                base,
                parsed,
                raw,
                status=outcome.quality_status,
                issues=outcome.quality_issues,
            )

        return ProcessResult(
            parser=outcome.parser,
            parsed_event=parsed,
            device=device,
            senml=senml,
            publish=published,
            device_cache_hit=cache_source not in (
                "remote",
                "reconciled",
                "profile-migrated",
            ),
            device_cache_source=cache_source,
            quality_status=outcome.quality_status,
            quality_issues=outcome.quality_issues,
            profile_key=profile_key,
        )


__all__ = ["ProcessResult", "RuntimeConfig", "SmarterAdapterRuntime"]
