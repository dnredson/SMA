from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Protocol


class AtomControlPlaneAPI(Protocol):
    def list_workspaces(self, limit: int = 100): ...

    def create_workspace(
        self,
        name: str,
        *,
        alias: str = "",
        attributes: Optional[Dict[str, Any]] = None,
    ): ...

    def list_channels(self, tenant_id: str, *, kind: str = "channel", limit: int = 100): ...

    def create_channel(
        self,
        tenant_id: str,
        name: str,
        *,
        alias: str = "",
        attributes: Optional[Dict[str, Any]] = None,
    ): ...


@dataclass(frozen=True)
class WorkspaceRef:
    id: str
    name: str
    alias: str = ""
    created: bool = False


@dataclass(frozen=True)
class ChannelRef:
    id: str
    workspace_id: str
    name: str
    alias: str = ""
    created: bool = False


@dataclass(frozen=True)
class BaseResources:
    workspace: WorkspaceRef
    channel: ChannelRef


class ControlPlane:
    """Idempotent reconciliation of base Magistrala resources."""

    def __init__(self, atom: AtomControlPlaneAPI) -> None:
        self.atom = atom

    @staticmethod
    def _match(items, *, name: str, alias: str):
        if alias:
            for item in items:
                if str(item.get("alias") or "") == alias:
                    return item
        by_name = [item for item in items if str(item.get("name") or "") == name]
        if len(by_name) == 1:
            return by_name[0]
        if len(by_name) > 1:
            raise RuntimeError(f"multiple remote resources share name {name!r}")
        return None

    def ensure_workspace(
        self,
        name: str,
        *,
        alias: str = "",
        attributes: Optional[Dict[str, Any]] = None,
    ) -> WorkspaceRef:
        existing = self._match(self.atom.list_workspaces(), name=name, alias=alias)
        created = False
        if existing is None:
            existing = self.atom.create_workspace(
                name,
                alias=alias,
                attributes=attributes,
            )
            created = True
        workspace_id = str(existing.get("id") or "")
        if not workspace_id:
            raise RuntimeError("Atom workspace response is missing id")
        return WorkspaceRef(
            id=workspace_id,
            name=str(existing.get("name") or name),
            alias=str(existing.get("alias") or alias),
            created=created,
        )

    def ensure_channel(
        self,
        workspace_id: str,
        name: str,
        *,
        alias: str = "",
        attributes: Optional[Dict[str, Any]] = None,
    ) -> ChannelRef:
        existing = self._match(
            self.atom.list_channels(workspace_id, kind="channel"),
            name=name,
            alias=alias,
        )
        created = False
        if existing is None:
            existing = self.atom.create_channel(
                workspace_id,
                name,
                alias=alias,
                attributes=attributes,
            )
            created = True
        channel_id = str(existing.get("id") or "")
        if not channel_id:
            raise RuntimeError("Atom channel response is missing id")
        return ChannelRef(
            id=channel_id,
            workspace_id=workspace_id,
            name=str(existing.get("name") or name),
            alias=str(existing.get("alias") or alias),
            created=created,
        )

    def ensure_base(
        self,
        *,
        workspace_name: str,
        workspace_alias: str = "",
        channel_name: str,
        channel_alias: str = "",
    ) -> BaseResources:
        workspace = self.ensure_workspace(
            workspace_name,
            alias=workspace_alias,
            attributes={"managed_by": "smarter-adapter"},
        )
        channel = self.ensure_channel(
            workspace.id,
            channel_name,
            alias=channel_alias,
            attributes={"managed_by": "smarter-adapter"},
        )
        return BaseResources(workspace=workspace, channel=channel)


__all__ = [
    "AtomControlPlaneAPI",
    "BaseResources",
    "ChannelRef",
    "ControlPlane",
    "WorkspaceRef",
]
