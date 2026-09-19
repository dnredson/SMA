from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import sys

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from smarter_adapter.gateway_monitor import GatewayRegistry, GatewayTopologySQLiteManagementStore
from smarter_adapter.magistrala.control_plane import DeviceTypeRef


class _Atom:
    def __init__(self):
        self.created = []
        self.publish_policy_calls = 0

    def list_devices(self, tenant_id, *, external_id="", limit=100):
        return []

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
        self.created.append(
            {
                "tenant_id": tenant_id,
                "external_id": external_id,
                "profile_id": profile_id,
                "profile_version_id": profile_version_id,
                "name": name,
                "alias": alias,
                "attributes": dict(attributes or {}),
            }
        )
        return {
            "id": "gateway-entity-1",
            "externalId": external_id,
            "profileId": profile_id,
            "profileVersionId": profile_version_id,
            "name": name,
            "attributes": dict(attributes or {}),
        }

    def ensure_publish_policy(self, *args, **kwargs):
        self.publish_policy_calls += 1
        raise AssertionError("gateway entities must not receive sensor publish policy")


class _Control:
    def __init__(self, atom):
        self.atom = atom
        self.profile_calls = 0

    def ensure_device_type(self, workspace_id, **kwargs):
        self.profile_calls += 1
        return DeviceTypeRef(
            id="gateway-profile-1",
            workspace_id=workspace_id,
            key=kwargs["key"],
            name=kwargs["name"],
            version_id="gateway-version-1",
            version=1,
        )


class _Runtime:
    def __init__(self):
        self.base = SimpleNamespace(
            workspace=SimpleNamespace(id="ws-1"),
            channel=SimpleNamespace(id="ch-1"),
        )
        self.bootstrap_calls = 0

    def bootstrap(self):
        self.bootstrap_calls += 1


class GatewayAtomTests(unittest.TestCase):
    def test_stats_discovery_creates_gateway_entity_without_publish_permission(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = GatewayTopologySQLiteManagementStore(Path(tmp) / "state.sqlite3")
            atom = _Atom()
            control = _Control(atom)
            runtime = _Runtime()
            registry = GatewayRegistry(runtime=runtime, control=control, store=store)

            registry.observe(
                gateway_id="000000ffff001002",
                event_kind="stats",
                topic="au915_1/gateway/000000ffff001002/event/stats",
                topic_root="au915_1",
                received_at=1000.0,
                retained=False,
            )

            item = store.find_gateway("ws-1", "000000ffff001002")
            self.assertIsNotNone(item)
            assert item is not None
            self.assertEqual(item["atom_entity_id"], "gateway-entity-1")
            self.assertEqual(item["topic_root"], "au915_1")
            self.assertEqual(len(atom.created), 1)
            self.assertEqual(
                atom.created[0]["external_id"],
                "lorawan-gateway-000000ffff001002",
            )
            self.assertEqual(atom.created[0]["attributes"]["role"], "lorawan-gateway")
            self.assertEqual(atom.publish_policy_calls, 0)
            store.close()


if __name__ == "__main__":
    unittest.main()
