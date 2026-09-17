from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

from .magistrala.control_plane import BaseResources, ControlPlane, DeviceRef, DeviceTypeRef
from .magistrala.publisher import FluxMQPublisher, PublishResult
from .magistrala.rules import PersistenceRuleRef, RulesClient
from .models import ParsedEvent, RawEvent
from .pipeline import ParsePipeline
from .senml import event_to_senml


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


def _safe_alias(value: str) -> str:
    text = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip("-._")
    return text[:120] or "device"


def _device_attributes(event: ParsedEvent) -> dict:
    # Device metadata is relatively stable. Do not copy raw payloads or values
    # into Atom attributes on every message; those belong in the data plane.
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
    """Join v2 parsing, Atom reconciliation, SenML and FluxMQ publication.

    Base resources and the persistence rule are reconciled once at bootstrap.
    Devices are reconciled lazily on their first observed message and then kept
    in an in-memory fast-path cache. A persistent state store will replace or
    back this cache in the reliability milestone without changing this API.
    """

    def __init__(
        self,
        *,
        pipeline: ParsePipeline,
        control: ControlPlane,
        rules: RulesClient,
        publisher: FluxMQPublisher,
        config: RuntimeConfig = RuntimeConfig(),
    ) -> None:
        self.pipeline = pipeline
        self.control = control
        self.rules = rules
        self.publisher = publisher
        self.config = config
        self.base: Optional[BaseResources] = None
        self.device_type: Optional[DeviceTypeRef] = None
        self.persistence_rule: Optional[PersistenceRuleRef] = None
        self._devices: Dict[str, DeviceRef] = {}

    @property
    def device_cache_size(self) -> int:
        return len(self._devices)

    def bootstrap(self) -> None:
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
        if self.base is None or self.device_type is None:
            self.bootstrap()
        assert self.base is not None
        assert self.device_type is not None
        return self.base, self.device_type

    def process(self, raw: RawEvent) -> ProcessResult:
        base, device_type = self._ensure_bootstrapped()
        outcome = self.pipeline.process(raw)
        parsed = outcome.event

        device = self._devices.get(parsed.external_device_id)
        cache_hit = device is not None
        if device is None:
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

        senml = tuple(event_to_senml(parsed))
        published = self.publisher.publish(
            workspace_id=base.workspace.id,
            channel_id=base.channel.id,
            device_id=device.id,
            senml=list(senml),
        )

        return ProcessResult(
            parser=outcome.parser,
            parsed_event=parsed,
            device=device,
            senml=senml,
            publish=published,
            device_cache_hit=cache_hit,
        )


__all__ = ["ProcessResult", "RuntimeConfig", "SmarterAdapterRuntime"]
