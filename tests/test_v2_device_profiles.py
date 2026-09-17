from __future__ import annotations

import unittest

from smarter_adapter.device_profiles import DeviceProfileRegistry
from smarter_adapter.magistrala.control_plane import ControlPlane
from smarter_adapter.magistrala.publisher import PublishResult
from smarter_adapter.magistrala.rules import PersistenceRuleRef
from smarter_adapter.models import Measurement, ParsedEvent, RawEvent
from smarter_adapter.pipeline import ParseOutcome
from smarter_adapter.runtime import SmarterAdapterRuntime


class _ProfileAtom:
    def __init__(self):
        self.workspaces = []
        self.channels = []
        self.device_types = []
        self.versions = {}
        self.devices = []
        self.policies = set()
        self.profile_updates = []

    def list_workspaces(self, limit=100):
        return list(self.workspaces)

    def create_workspace(self, name, *, alias="", attributes=None):
        item = {"id": "ws-1", "name": name, "alias": alias, "attributes": attributes or {}}
        self.workspaces.append(item)
        return dict(item)

    def list_channels(self, tenant_id, *, kind="channel", limit=100):
        return [item for item in self.channels if item["tenantId"] == tenant_id]

    def create_channel(self, tenant_id, name, *, alias="", attributes=None):
        item = {
            "id": "ch-1",
            "tenantId": tenant_id,
            "kind": "channel",
            "name": name,
            "alias": alias,
            "attributes": attributes or {},
        }
        self.channels.append(item)
        return dict(item)

    def list_device_types(self, tenant_id, *, status="", limit=100):
        items = [item for item in self.device_types if item["tenantId"] == tenant_id]
        return [item for item in items if not status or item["status"] == status]

    def create_device_type(self, tenant_id, key, name, *, description="", status="active"):
        item = {
            "id": f"profile-{key}",
            "tenantId": tenant_id,
            "key": key,
            "name": name,
            "description": description,
            "status": status,
        }
        self.device_types.append(item)
        return dict(item)

    def list_device_type_versions(self, profile_id):
        return list(self.versions.get(profile_id, ()))

    def create_device_type_version(
        self,
        profile_id,
        *,
        version,
        json_schema,
        ui_schema=None,
        status="active",
    ):
        item = {
            "id": f"version-{profile_id}-{version}",
            "profileId": profile_id,
            "version": version,
            "jsonSchema": json_schema,
            "uiSchema": ui_schema or {},
            "status": status,
        }
        self.versions.setdefault(profile_id, []).append(item)
        return dict(item)

    def list_devices(self, tenant_id, *, external_id="", limit=100):
        items = [item for item in self.devices if item["tenantId"] == tenant_id]
        if external_id:
            items = [item for item in items if item["externalId"] == external_id]
        return [dict(item) for item in items]

    def create_device(
        self,
        tenant_id,
        external_id,
        *,
        profile_id,
        profile_version_id="",
        name="",
        alias="",
        attributes=None,
    ):
        item = {
            "id": f"device-{len(self.devices) + 1}",
            "tenantId": tenant_id,
            "kind": "device",
            "profileId": profile_id,
            "profileVersionId": profile_version_id,
            "externalId": external_id,
            "name": name or external_id,
            "alias": alias,
            "attributes": attributes or {},
        }
        self.devices.append(item)
        return dict(item)

    def update_device_profile(self, device_id, *, profile_id, profile_version_id):
        for item in self.devices:
            if item["id"] == device_id:
                item["profileId"] = profile_id
                item["profileVersionId"] = profile_version_id
                self.profile_updates.append((device_id, profile_id, profile_version_id))
                return dict(item)
        raise AssertionError("device not found")

    def ensure_publish_policy(self, tenant_id, device_id, channel_id):
        key = (tenant_id, device_id, channel_id)
        created = key not in self.policies
        self.policies.add(key)
        return created


class _Pipeline:
    def __init__(self, event):
        self.event = event

    def process(self, raw):
        return ParseOutcome(parser="typed-test", event=self.event)


class _Rules:
    def ensure_senml_persistence(self, workspace_id, channel_id, *, name):
        return PersistenceRuleRef(
            id="rule-1",
            workspace_id=workspace_id,
            channel_id=channel_id,
            name=name,
            status="enabled",
        )


class _Publisher:
    def publish(self, **kwargs):
        return PublishResult(status=202, body={"status": "accepted"})


class DeviceProfileTests(unittest.TestCase):
    def test_registry_maps_known_families_and_leaves_unknown_generic(self):
        registry = DeviceProfileRegistry()
        teros = registry.resolve(
            ParsedEvent("d1", (Measurement("x", 1),), {"sensor": "TEROS12"})
        )
        green = registry.resolve(
            ParsedEvent("d2", (Measurement("x", 1),), {"sensor": "greenstick"})
        )
        unknown = registry.resolve(
            ParsedEvent("d3", (Measurement("x", 1),), {"sensor": "custom-sensor"})
        )
        self.assertEqual(teros.key, "smarter-adapter-teros12")
        self.assertEqual(green.key, "smarter-adapter-greenstick")
        self.assertIsNone(unknown)

    def test_control_plane_migrates_only_sma_owned_device_in_place(self):
        atom = _ProfileAtom()
        control = ControlPlane(atom)
        base = control.ensure_base(
            workspace_name="Irrigap",
            workspace_alias="irrigap",
            channel_name="Telemetry",
            channel_alias="telemetry",
        )
        generic = control.ensure_device_type(base.workspace.id)
        original = control.ensure_device(
            base.workspace.id,
            base.channel.id,
            "teros12-sector1.3",
            device_type=generic,
            attributes={"sensor": "teros12"},
        )
        typed = control.ensure_device_type(
            base.workspace.id,
            key="smarter-adapter-teros12",
            name="Teros 12 Soil Sensor",
        )

        migrated = control.ensure_device(
            base.workspace.id,
            base.channel.id,
            "teros12-sector1.3",
            device_type=typed,
            allow_profile_migration=True,
        )

        self.assertEqual(migrated.id, original.id)
        self.assertTrue(migrated.profile_migrated)
        self.assertEqual(migrated.profile_id, typed.id)
        self.assertEqual(migrated.profile_version_id, typed.version_id)
        self.assertEqual(len(atom.profile_updates), 1)

    def test_foreign_device_is_not_migrated_even_when_migration_requested(self):
        atom = _ProfileAtom()
        control = ControlPlane(atom)
        base = control.ensure_base(
            workspace_name="Irrigap",
            workspace_alias="irrigap",
            channel_name="Telemetry",
            channel_alias="telemetry",
        )
        typed = control.ensure_device_type(
            base.workspace.id,
            key="smarter-adapter-teros12",
            name="Teros 12 Soil Sensor",
        )
        atom.devices.append(
            {
                "id": "foreign",
                "tenantId": base.workspace.id,
                "kind": "device",
                "profileId": "foreign-profile",
                "profileVersionId": "foreign-version",
                "externalId": "foreign-node",
                "name": "foreign-node",
                "attributes": {"managed_by": "someone-else"},
            }
        )
        with self.assertRaises(RuntimeError):
            control.ensure_device(
                base.workspace.id,
                base.channel.id,
                "foreign-node",
                device_type=typed,
                allow_profile_migration=True,
            )
        self.assertEqual(atom.profile_updates, [])

    def test_runtime_creates_typed_profile_from_parser_metadata(self):
        atom = _ProfileAtom()
        event = ParsedEvent(
            external_device_id="teros12-sector9",
            measurements=(Measurement("soil.temperature", 24.0, "Cel", 100.0),),
            metadata={"sensor": "teros12", "node_id": "2399", "bt": 100.0},
        )
        runtime = SmarterAdapterRuntime(
            pipeline=_Pipeline(event),
            control=ControlPlane(atom),
            rules=_Rules(),
            publisher=_Publisher(),
        )
        result = runtime.process(RawEvent(source="mqtt:test", payload=b"{}", received_at=100.0))

        self.assertEqual(result.profile_key, "smarter-adapter-teros12")
        self.assertEqual(result.device.profile_id, "profile-smarter-adapter-teros12")
        keys = {item["key"] for item in atom.device_types}
        self.assertIn("smarter-adapter-sensor", keys)
        self.assertIn("smarter-adapter-teros12", keys)


if __name__ == "__main__":
    unittest.main()
