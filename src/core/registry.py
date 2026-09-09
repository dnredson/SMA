from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from .atom_client import AtomClient, AtomConfig, AtomError

logger = logging.getLogger("registry")


@dataclass
class EnsureResult:
    ok: bool
    device_id: Optional[str] = None
    tenant_id: Optional[str] = None
    channel_id: Optional[str] = None
    error: Optional[str] = None

class Registry:
    """Atom-backed device lifecycle registry."""

    def __init__(
        self,
        cfg: Dict[str, Any],
        key_manager: Any,
        store: Any,
        cfg_path: Optional[Path] = None,
    ) -> None:
        self.cfg = cfg
        self.store = store
        self.cfg_path = cfg_path
        self.atom = AtomClient(AtomConfig.from_mapping(cfg))

    def list_devices(self, tenant_id: Optional[str] = None) -> List[Dict[str, Any]]:
        return self.atom.list_devices(tenant_id=tenant_id)

    def get_device(self, device_id: str) -> Dict[str, Any]:
        return self.atom.get_device(device_id)

    def create_device(
        self,
        external_id: str,
        *,
        name: Optional[str] = None,
        tenant_id: Optional[str] = None,
        attributes: Optional[Dict[str, Any]] = None,
        ensure_publish: bool = True,
    ) -> Dict[str, Any]:
        device = self.atom.create_device(
            external_id,
            name=name,
            tenant_id=tenant_id,
            attributes=attributes,
        )
        if ensure_publish and self.atom.cfg.manage_policies:
            self.atom.ensure_publish_policy(device["id"])
        self._save_device(device)
        return device

    def update_device(self, device_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        current = self.atom.get_device(device_id)
        attributes = payload.get("attributes")
        if attributes is not None and payload.get("merge_attributes", True):
            merged = dict(current.get("attributes") or {})
            merged.update(attributes)
            attributes = merged
        device = self.atom.update_device(
            device_id,
            name=payload.get("name"),
            external_id=payload.get("external_id", payload.get("externalId")),
            status=payload.get("status"),
            attributes=attributes,
        )
        if self.atom.cfg.manage_policies:
            self.atom.ensure_publish_policy(device_id)
        self._save_device(device)
        return device

    def delete_device(self, device_id: str) -> None:
        device = self.atom.get_device(device_id)
        self.atom.delete_device(device_id)
        external_id = str(device.get("externalId") or device.get("external_id") or "")
        if external_id:
            self.store.remove_device(external_id)

    def ensure_client(
        self, external_id: str, meta: Optional[Dict[str, Any]] = None
    ) -> EnsureResult:
        """Find or create the Atom device associated with an input topic."""
        tenant_id = self.atom.cfg.tenant_id
        channel_id = self.atom.cfg.channel_id
        if not tenant_id or not channel_id:
            return EnsureResult(
                ok=False,
                error="atom_tenant_id/atom_channel_id (workspace/channel) are required",
            )

        try:
            device: Optional[Dict[str, Any]] = None
            local = self.store.get_by_external_id(external_id)
            if local and local.get("device_id"):
                try:
                    device = self.atom.get_device(str(local["device_id"]))
                except AtomError as exc:
                    logger.info("device local stale (%s): %s", external_id, exc)

            if device is None:
                matches = self.atom.list_devices(
                    tenant_id=tenant_id, external_id=external_id, limit=10
                )
                device = next(
                    (
                        item
                        for item in matches
                        if str(item.get("externalId") or item.get("external_id"))
                        == external_id
                    ),
                    None,
                )

            if device is None:
                attrs = {
                    "smartadapter": {
                        "sensor": (meta or {}).get("sensor", "unknown")
                    }
                }
                device = self.atom.create_device(
                    external_id,
                    name=external_id,
                    tenant_id=tenant_id,
                    attributes=attrs,
                )

            if self.atom.cfg.manage_policies:
                self.atom.ensure_publish_policy(str(device["id"]), channel_id)
            self._save_device(device)
            return EnsureResult(
                ok=True,
                device_id=str(device["id"]),
                tenant_id=tenant_id,
                channel_id=channel_id,
            )
        except Exception as exc:
            logger.exception("Atom device ensure failed for %s", external_id)
            return EnsureResult(ok=False, error=str(exc))

    def _save_device(self, device: Dict[str, Any]) -> None:
        external_id = str(device.get("externalId") or device.get("external_id") or "")
        device_id = str(device.get("id") or "")
        if not external_id or not device_id:
            return
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        self.store.upsert_device(
            {
                "device_id": device_id,
                "external_id": external_id,
                "name": device.get("name") or external_id,
                "tenant_id": device.get("tenantId") or device.get("tenant_id") or self.atom.cfg.tenant_id,
                "channel_ids": [self.atom.cfg.channel_id] if self.atom.cfg.channel_id else [],
                "active": (device.get("status") or "active") not in {"disabled", "deleted"},
                "created_at": device.get("createdAt") or device.get("created_at") or now,
                "updated_at": device.get("updatedAt") or device.get("updated_at") or now,
                "last_seen": None,
            }
        )


__all__ = ["EnsureResult", "Registry"]
