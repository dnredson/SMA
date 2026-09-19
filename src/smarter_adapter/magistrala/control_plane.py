from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Protocol


GENERIC_DEVICE_TYPE_KEY = "smarter-adapter-sensor"
GENERIC_DEVICE_TYPE_NAME = "Smarter Adapter Sensor"
GENERIC_DEVICE_TYPE_DESCRIPTION = (
    "Generic sensor managed by Smarter Adapter 2.0. "
    "The profile constrains device attributes only; telemetry is normalized to SenML."
)
GENERIC_DEVICE_ATTRIBUTES_SCHEMA: Dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": True,
}


class AtomControlPlaneAPI(Protocol):
    def list_workspaces(self, limit: int = 100): ...

    def create_workspace(
        self,
        name: str,
        *,
        alias: str = "",
        attributes: Optional[Dict[str, Any]] = None,
    ): ...

    def list_channels(
        self,
        tenant_id: str,
        *,
        kind: str = "channel",
        limit: int = 100,
    ): ...

    def create_channel(
        self,
        tenant_id: str,
        name: str,
        *,
        alias: str = "",
        attributes: Optional[Dict[str, Any]] = None,
    ): ...

    def list_device_types(
        self,
        tenant_id: str,
        *,
        status: str = "",
        limit: int = 100,
    ): ...

    def create_device_type(
        self,
        tenant_id: str,
        key: str,
        name: str,
        *,
        description: str = "",
        status: str = "active",
    ): ...

    def list_device_type_versions(self, profile_id: str): ...

    def create_device_type_version(
        self,
        profile_id: str,
        *,
        version: int,
        json_schema: Dict[str, Any],
        ui_schema: Optional[Dict[str, Any]] = None,
        status: str = "active",
    ): ...

    def list_devices(
        self,
        tenant_id: str,
        *,
        external_id: str = "",
        limit: int = 100,
    ): ...

    def create_device(
        self,
        tenant_id: str,
        external_id: str,
        *,
        profile_id: str,
        profile_version_id: str = "",
        name: str = "",
        alias: str = "",
        attributes: Optional[Dict[str, Any]] = None,
    ): ...

    def update_device_profile(
        self,
        device_id: str,
        *,
        profile_id: str,
        profile_version_id: str,
    ): ...

    def ensure_publish_policy(
        self,
        tenant_id: str,
        device_id: str,
        channel_id: str,
    ) -> bool: ...


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


@dataclass(frozen=True)
class DeviceTypeRef:
    id: str
    workspace_id: str
    key: str
    name: str
    version_id: str
    version: int
    created: bool = False
    version_created: bool = False


@dataclass(frozen=True)
class DeviceRef:
    id: str
    workspace_id: str
    external_id: str
    name: str
    profile_id: str
    profile_version_id: str
    created: bool = False
    publish_policy_created: bool = False
    profile_migrated: bool = False


@dataclass(frozen=True)
class ManagedDeviceResources:
    base: BaseResources
    device_type: DeviceTypeRef
    device: DeviceRef


class ControlPlane:
    """Idempotent reconciliation of Magistrala/Atom resources."""

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

    def ensure_device_type(
        self,
        workspace_id: str,
        *,
        key: str = GENERIC_DEVICE_TYPE_KEY,
        name: str = GENERIC_DEVICE_TYPE_NAME,
        description: str = GENERIC_DEVICE_TYPE_DESCRIPTION,
        json_schema: Optional[Dict[str, Any]] = None,
    ) -> DeviceTypeRef:
        matches = [
            item
            for item in self.atom.list_device_types(workspace_id)
            if str(item.get("key") or "") == key
        ]
        if len(matches) > 1:
            raise RuntimeError(f"multiple Atom device types share key {key!r}")

        created = False
        if matches:
            profile = matches[0]
        else:
            profile = self.atom.create_device_type(
                workspace_id,
                key,
                name,
                description=description,
                status="active",
            )
            created = True

        profile_id = str(profile.get("id") or "")
        if not profile_id:
            raise RuntimeError("Atom device type response is missing id")

        status = str(profile.get("status") or "active")
        if status != "active":
            raise RuntimeError(
                f"managed device type {key!r} is not active (status={status!r})"
            )

        versions = list(self.atom.list_device_type_versions(profile_id))
        active_versions = [
            item for item in versions if str(item.get("status") or "") == "active"
        ]
        version_created = False
        if active_versions:
            version_obj = max(
                active_versions,
                key=lambda item: int(item.get("version") or 0),
            )
        else:
            next_version = (
                max((int(item.get("version") or 0) for item in versions), default=0) + 1
            )
            version_obj = self.atom.create_device_type_version(
                profile_id,
                version=next_version,
                json_schema=json_schema or dict(GENERIC_DEVICE_ATTRIBUTES_SCHEMA),
                ui_schema={},
                status="active",
            )
            version_created = True

        version_id = str(version_obj.get("id") or "")
        version_number = int(version_obj.get("version") or 0)
        if not version_id or version_number <= 0:
            raise RuntimeError("Atom device type version response is incomplete")

        return DeviceTypeRef(
            id=profile_id,
            workspace_id=workspace_id,
            key=str(profile.get("key") or key),
            name=str(profile.get("name") or name),
            version_id=version_id,
            version=version_number,
            created=created,
            version_created=version_created,
        )

    def _update_device_profile(
        self,
        device_id: str,
        device_type: DeviceTypeRef,
    ) -> Dict[str, Any]:
        """Rebind one SMA-owned Atom entity without changing its identity.

        ``LifecycleAtomClient`` currently inherits the base Atom client, whose
        public surface predates profile rebinding. Prefer a public helper when
        available and otherwise use Atom's documented ``updateEntity`` GraphQL
        mutation. The latter is a partial update: omitted attributes remain
        unchanged.
        """
        updater = getattr(self.atom, "update_device_profile", None)
        if callable(updater):
            result = updater(
                device_id,
                profile_id=device_type.id,
                profile_version_id=device_type.version_id,
            )
            return dict(result or {})

        graphql = getattr(self.atom, "_graphql", None)
        if not callable(graphql):
            raise RuntimeError("Atom client cannot update a device profile in place")
        fields = (
            "id kind profileId profileVersionId name alias externalId tenantId "
            "objectGroupIds status attributes createdAt updatedAt"
        )
        mutation = f"""
        mutation UpdateDeviceProfile($id: ID!, $input: UpdateEntityInput!) {{
          updateEntity(id: $id, input: $input) {{ {fields} }}
        }}
        """
        return dict(
            graphql(
                mutation,
                {
                    "id": device_id,
                    "input": {
                        "profileId": device_type.id,
                        "profileVersionId": device_type.version_id,
                    },
                },
            ).get("updateEntity")
            or {}
        )

    def ensure_device(
        self,
        workspace_id: str,
        channel_id: str,
        external_id: str,
        *,
        device_type: Optional[DeviceTypeRef] = None,
        name: str = "",
        alias: str = "",
        attributes: Optional[Dict[str, Any]] = None,
        allow_profile_migration: bool = False,
    ) -> DeviceRef:
        external_id = str(external_id or "").strip()
        if not external_id:
            raise ValueError("external_id must not be empty")

        if device_type is None:
            device_type = self.ensure_device_type(workspace_id)

        exact = [
            item
            for item in self.atom.list_devices(
                workspace_id,
                external_id=external_id,
                limit=10,
            )
            if str(item.get("externalId") or "") == external_id
        ]
        if len(exact) > 1:
            raise RuntimeError(
                f"multiple Atom devices share external_id {external_id!r}"
            )

        created = False
        profile_migrated = False
        if exact:
            device = exact[0]
            current_profile = str(device.get("profileId") or "")
            current_version = str(device.get("profileVersionId") or "")
            profile_mismatch = bool(
                (current_profile and current_profile != device_type.id)
                or (current_version and current_version != device_type.version_id)
            )
            if profile_mismatch:
                remote_attributes = device.get("attributes") or {}
                sma_owned = (
                    isinstance(remote_attributes, dict)
                    and str(remote_attributes.get("managed_by") or "") == "smarter-adapter"
                )
                if not allow_profile_migration or not sma_owned:
                    raise RuntimeError(
                        f"device {external_id!r} is bound to unexpected profile/version "
                        f"{current_profile!r}/{current_version!r}; expected "
                        f"{device_type.id!r}/{device_type.version_id!r}"
                    )
                device = self._update_device_profile(str(device.get("id") or ""), device_type)
                if not device:
                    raise RuntimeError(
                        f"Atom did not return device {external_id!r} after profile migration"
                    )
                profile_migrated = True
        else:
            merged_attributes: Dict[str, Any] = {
                "managed_by": "smarter-adapter",
            }
            merged_attributes.update(attributes or {})
            device = self.atom.create_device(
                workspace_id,
                external_id,
                profile_id=device_type.id,
                profile_version_id=device_type.version_id,
                name=name or external_id,
                alias=alias,
                attributes=merged_attributes,
            )
            created = True

        device_id = str(device.get("id") or "")
        if not device_id:
            raise RuntimeError("Atom device response is missing id")

        policy_created = self.atom.ensure_publish_policy(
            workspace_id,
            device_id,
            channel_id,
        )

        return DeviceRef(
            id=device_id,
            workspace_id=workspace_id,
            external_id=str(device.get("externalId") or external_id),
            name=str(device.get("name") or name or external_id),
            profile_id=str(device.get("profileId") or device_type.id),
            profile_version_id=str(
                device.get("profileVersionId") or device_type.version_id
            ),
            created=created,
            publish_policy_created=policy_created,
            profile_migrated=profile_migrated,
        )

    def ensure_managed_device(
        self,
        *,
        workspace_name: str,
        workspace_alias: str = "",
        channel_name: str,
        channel_alias: str = "",
        external_id: str,
        device_name: str = "",
        device_alias: str = "",
        attributes: Optional[Dict[str, Any]] = None,
    ) -> ManagedDeviceResources:
        base = self.ensure_base(
            workspace_name=workspace_name,
            workspace_alias=workspace_alias,
            channel_name=channel_name,
            channel_alias=channel_alias,
        )
        device_type = self.ensure_device_type(base.workspace.id)
        device = self.ensure_device(
            base.workspace.id,
            base.channel.id,
            external_id,
            device_type=device_type,
            name=device_name,
            alias=device_alias,
            attributes=attributes,
        )
        return ManagedDeviceResources(
            base=base,
            device_type=device_type,
            device=device,
        )


__all__ = [
    "AtomControlPlaneAPI",
    "BaseResources",
    "ChannelRef",
    "ControlPlane",
    "DeviceRef",
    "DeviceTypeRef",
    "GENERIC_DEVICE_ATTRIBUTES_SCHEMA",
    "GENERIC_DEVICE_TYPE_DESCRIPTION",
    "GENERIC_DEVICE_TYPE_KEY",
    "GENERIC_DEVICE_TYPE_NAME",
    "ManagedDeviceResources",
    "WorkspaceRef",
]
