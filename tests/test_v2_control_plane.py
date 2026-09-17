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
        self.workspace_creates = 0
        self.channel_creates = 0

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
