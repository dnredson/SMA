from __future__ import annotations

import re
import threading
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

from .magistrala.control_plane import BaseResources, ControlPlane, DeviceRef, DeviceTypeRef
from .magistrala.publisher import FluxMQPublisher, PublishError, PublishResult
from .magistrala.rules import PersistenceRuleRef, RulesClient
from .models import ParsedEvent, RawEvent
from .pipeline import ParsePipeline
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


class SmarterAdapterRuntime:
    """Join v2 parsing, Atom reconciliation, SenML and FluxMQ publication."""

    def __init__(
        self,
        *,
        pipeline: ParsePipeline,
        control: ControlPlane,
        rules: RulesClient,
        publisher: FluxMQPublisher,
        config: RuntimeConfig = RuntimeConfig(),
        state_store: Optional[DeviceStateStore] = None,
    ) -> None:
        self.pipeline = pipeline
        self.control = control
        self.rules = rules
        self.publisher = publisher
        self.config = config
        self.state_store = state_store
        self.base: Optional[BaseResources] = None
        self.device_type: Optional[DeviceTypeRef] = None
        self.persistence_rule: Optional[PersistenceRuleRef] = None
        self._devices: Dict[str, DeviceRef] = {}
        self._control_lock = threading.RLock()

    @property
    def device_cache_size(self) -> int:
        with self._control_lock:
            return len(self._devices)

    def bootstrap(self) -> None:
        with self._control_lock:
            if self.base is not None and self.device_type is not None and self.persistence_rule is not None:
                return
            base = self.control.ensure_base(
                workspace_name=self.config.workspace_name,
                workspace_alias=self.config.workspace_alias,
                channel_name=self.config.channel_name,
                channel_alias=self.config.channel_alias,
            )
            device_type = self.control.ensure_device_type(base.workspace.id)
            persistence = self.rules.ensure_senml_persistence(
                base.workspace.id,
                base.channel.id,
                name=self.config.persistence_rule_name,
            )
            self.base = base
            self.device_type = device_type
            self.persistence_rule = persistence

    def _ensure_bootstrapped(self) -> tuple[BaseResources, DeviceTypeRef]:
        self.bootstrap()
        assert self.base is not None
        assert self.device_type is not None
        return self.base, self.device_type

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
            return device, "memory"

        if self.state_store is not None:
            device = self.state_store.get_device(
                base.workspace.id,
                base.channel.id,
                parsed.external_device_id,
            )
            if device is not None:
                self._devices[parsed.external_device_id] = device
                return device, "persistent"

        return self._remote_device(base, device_type, parsed, raw), "remote"

    def process(self, raw: RawEvent) -> ProcessResult:
        outcome = self.pipeline.process(raw)
        parsed = outcome.event

        with self._control_lock:
            base, device_type = self._ensure_bootstrapped()
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

        return ProcessResult(
            parser=outcome.parser,
            parsed_event=parsed,
            device=device,
            senml=senml,
            publish=published,
            device_cache_hit=cache_source not in ("remote", "reconciled"),
            device_cache_source=cache_source,
        )


__all__ = ["ProcessResult", "RuntimeConfig", "SmarterAdapterRuntime"]
