import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from smarter_adapter.magistrala.atom import TokenManager
from smarter_adapter.magistrala.control_plane import ControlPlane


class _FakeAtom:
    def __init__(self):
        self.workspaces = []
        self.channels = []
        self.device_types = []
        self.device_type_versions = {}
        self.devices = []
        self.publish_policies = set()
        self.workspace_creates = 0
        self.channel_creates = 0
        self.device_type_creates = 0
        self.device_type_version_creates = 0
        self.device_creates = 0
        self.publish_policy_creates = 0

    def list_workspaces(self, limit=100):
        return list(self.workspaces)

    def create_workspace(self, name, *, alias="", attributes=None):
        self.workspace_creates += 1
        workspace = {
            "id": f"ws-{self.workspace_creates}",
            "name": name,
            "alias": alias,
            "attributes": attributes or {},
        }
        self.workspaces.append(workspace)
        return dict(workspace)

    def list_channels(self, tenant_id, *, kind="channel", limit=100):
        return [
            item
            for item in self.channels
            if item["tenantId"] == tenant_id and item["kind"] == kind
        ]

    def create_channel(self, tenant_id, name, *, alias="", attributes=None):
        self.channel_creates += 1
        channel = {
            "id": f"ch-{self.channel_creates}",
            "tenantId": tenant_id,
            "kind": "channel",
            "name": name,
            "alias": alias,
            "attributes": attributes or {},
        }
        self.channels.append(channel)
        return dict(channel)

    def list_device_types(self, tenant_id, *, status="", limit=100):
        result = [
            item for item in self.device_types if item["tenantId"] == tenant_id
        ]
        if status:
            result = [item for item in result if item.get("status") == status]
        return list(result)

    def create_device_type(
        self,
        tenant_id,
        key,
        name,
        *,
        description="",
        status="active",
    ):
        self.device_type_creates += 1
        device_type = {
            "id": f"profile-{self.device_type_creates}",
            "tenantId": tenant_id,
            "key": key,
            "name": name,
            "description": description,
            "status": status,
        }
        self.device_types.append(device_type)
        return dict(device_type)

    def list_device_type_versions(self, profile_id):
        return list(self.device_type_versions.get(profile_id, []))

    def create_device_type_version(
        self,
        profile_id,
        *,
        version,
        json_schema,
        ui_schema=None,
        status="active",
    ):
        self.device_type_version_creates += 1
        item = {
            "id": f"profile-version-{self.device_type_version_creates}",
            "profileId": profile_id,
            "version": version,
            "jsonSchema": json_schema,
            "uiSchema": ui_schema or {},
            "status": status,
        }
        self.device_type_versions.setdefault(profile_id, []).append(item)
        return dict(item)

    def list_devices(self, tenant_id, *, external_id="", limit=100):
        result = [
            item
            for item in self.devices
            if item["tenantId"] == tenant_id and item["kind"] == "device"
        ]
        if external_id:
            result = [
                item for item in result if item.get("externalId") == external_id
            ]
        return list(result)

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
        self.device_creates += 1
        device = {
            "id": f"device-{self.device_creates}",
            "tenantId": tenant_id,
            "kind": "device",
            "profileId": profile_id,
            "profileVersionId": profile_version_id,
            "externalId": external_id,
            "name": name or external_id,
            "alias": alias,
            "attributes": attributes or {},
        }
        self.devices.append(device)
        return dict(device)

    def ensure_publish_policy(self, tenant_id, device_id, channel_id):
        key = (tenant_id, device_id, channel_id)
        if key in self.publish_policies:
            return False
        self.publish_policies.add(key)
        self.publish_policy_creates += 1
        return True


class ControlPlaneTests(unittest.TestCase):
    def test_empty_atom_is_bootstrapped_once(self):
        atom = _FakeAtom()
        control = ControlPlane(atom)

        first = control.ensure_base(
            workspace_name="SmartAdapter",
            workspace_alias="smart-adapter",
            channel_name="Telemetry",
            channel_alias="telemetry",
        )
        second = control.ensure_base(
            workspace_name="SmartAdapter",
            workspace_alias="smart-adapter",
            channel_name="Telemetry",
            channel_alias="telemetry",
        )

        self.assertTrue(first.workspace.created)
        self.assertTrue(first.channel.created)
        self.assertFalse(second.workspace.created)
        self.assertFalse(second.channel.created)
        self.assertEqual(first.workspace.id, second.workspace.id)
        self.assertEqual(first.channel.id, second.channel.id)
        self.assertEqual(atom.workspace_creates, 1)
        self.assertEqual(atom.channel_creates, 1)

    def test_channel_is_scoped_to_workspace(self):
        atom = _FakeAtom()
        control = ControlPlane(atom)
        ws1 = control.ensure_workspace("one", alias="one")
        ws2 = control.ensure_workspace("two", alias="two")
        ch1 = control.ensure_channel(ws1.id, "Telemetry", alias="telemetry")
        ch2 = control.ensure_channel(ws2.id, "Telemetry", alias="telemetry")
        self.assertNotEqual(ch1.id, ch2.id)

    def test_device_type_and_active_version_are_created_once(self):
        atom = _FakeAtom()
        control = ControlPlane(atom)
        workspace = control.ensure_workspace("SmartAdapter", alias="smart-adapter")

        first = control.ensure_device_type(workspace.id)
        second = control.ensure_device_type(workspace.id)

        self.assertTrue(first.created)
        self.assertTrue(first.version_created)
        self.assertFalse(second.created)
        self.assertFalse(second.version_created)
        self.assertEqual(first.id, second.id)
        self.assertEqual(first.version_id, second.version_id)
        self.assertEqual(atom.device_type_creates, 1)
        self.assertEqual(atom.device_type_version_creates, 1)

    def test_managed_device_and_publish_policy_are_created_once(self):
        atom = _FakeAtom()
        control = ControlPlane(atom)

        kwargs = dict(
            workspace_name="SmartAdapter",
            workspace_alias="smart-adapter",
            channel_name="Telemetry",
            channel_alias="telemetry",
            external_id="NODE-3303",
            device_name="Node 3303",
            attributes={"sensor": "greenstick"},
        )
        first = control.ensure_managed_device(**kwargs)
        second = control.ensure_managed_device(**kwargs)

        self.assertTrue(first.device.created)
        self.assertTrue(first.device.publish_policy_created)
        self.assertFalse(second.device.created)
        self.assertFalse(second.device.publish_policy_created)
        self.assertEqual(first.device.id, second.device.id)
        self.assertEqual(atom.device_creates, 1)
        self.assertEqual(atom.publish_policy_creates, 1)

    def test_existing_device_with_foreign_profile_is_not_rebound_silently(self):
        atom = _FakeAtom()
        control = ControlPlane(atom)
        base = control.ensure_base(
            workspace_name="SmartAdapter",
            workspace_alias="smart-adapter",
            channel_name="Telemetry",
            channel_alias="telemetry",
        )
        managed_type = control.ensure_device_type(base.workspace.id)
        atom.devices.append(
            {
                "id": "foreign-device",
                "tenantId": base.workspace.id,
                "kind": "device",
                "profileId": "foreign-profile",
                "profileVersionId": "foreign-version",
                "externalId": "NODE-1",
                "name": "Node 1",
            }
        )

        with self.assertRaises(RuntimeError):
            control.ensure_device(
                base.workspace.id,
                base.channel.id,
                "NODE-1",
                device_type=managed_type,
            )


class TokenManagerTests(unittest.TestCase):
    def test_static_token_is_reused(self):
        manager = TokenManager(token="service-token")
        self.assertEqual(manager.cached_token(), "service-token")
        self.assertFalse(manager.can_login)

    def test_login_token_is_invalidated_before_expiry(self):
        manager = TokenManager(
            username="admin",
            password="secret",
            refresh_skew_seconds=30,
        )
        manager.record_login("jwt", time.time() + 10)
        self.assertEqual(manager.cached_token(), "")
        self.assertTrue(manager.can_login)

    def test_login_token_is_kept_when_safely_inside_validity_window(self):
        manager = TokenManager(
            username="admin",
            password="secret",
            refresh_skew_seconds=30,
        )
        manager.record_login("jwt", time.time() + 120)
        self.assertEqual(manager.cached_token(), "jwt")


if __name__ == "__main__":
    unittest.main()
