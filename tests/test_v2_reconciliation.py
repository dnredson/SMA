from __future__ import annotations

import unittest
from types import SimpleNamespace

from smarter_adapter.device_profiles import DeviceProfileRegistry
from smarter_adapter.irrigap_config import IrrigapNode
from smarter_adapter.magistrala.control_plane import ControlPlane, DeviceRef, DeviceTypeRef
from smarter_adapter.reconciliation import ControlPlaneReconciler


class _Atom:
    def __init__(self):
        self.workspaces = [{"id": "ws-1", "name": "Irrigap", "alias": "irrigap"}]
        self.channels = [
            {
                "id": "ch-1",
                "tenantId": "ws-1",
                "kind": "channel",
                "name": "Telemetry",
                "alias": "telemetry",
            }
        ]
        self.types = []
        self.versions = {}
        self.devices = []
        self.policies = set()
        self.revocations = []

    def list_workspaces(self, limit=100):
        return list(self.workspaces)

    def create_workspace(self, name, *, alias="", attributes=None):
        item = {"id": "ws-new", "name": name, "alias": alias, "attributes": attributes or {}}
        self.workspaces.append(item)
        return dict(item)

    def list_channels(self, tenant_id, *, kind="channel", limit=100):
        return [item for item in self.channels if item["tenantId"] == tenant_id]

    def create_channel(self, tenant_id, name, *, alias="", attributes=None):
        item = {
            "id": "ch-new",
            "tenantId": tenant_id,
            "kind": "channel",
            "name": name,
            "alias": alias,
            "attributes": attributes or {},
        }
        self.channels.append(item)
        return dict(item)

    def list_device_types(self, tenant_id, *, status="", limit=100):
        items = [item for item in self.types if item["tenantId"] == tenant_id]
        if status:
            items = [item for item in items if item.get("status") == status]
        return [dict(item) for item in items]

    def create_device_type(self, tenant_id, key, name, *, description="", status="active"):
        item = {
            "id": f"profile-{key}",
            "tenantId": tenant_id,
            "key": key,
            "name": name,
            "description": description,
            "status": status,
        }
        self.types.append(item)
        return dict(item)

    def list_device_type_versions(self, profile_id):
        return [dict(item) for item in self.versions.get(profile_id, [])]

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
            "id": f"atom-{len(self.devices) + 1}",
            "tenantId": tenant_id,
            "kind": "device",
            "externalId": external_id,
            "name": name or external_id,
            "profileId": profile_id,
            "profileVersionId": profile_version_id,
            "attributes": attributes or {},
        }
        self.devices.append(item)
        return dict(item)

    def update_device_profile(self, device_id, *, profile_id, profile_version_id):
        for item in self.devices:
            if item["id"] == device_id:
                item["profileId"] = profile_id
                item["profileVersionId"] = profile_version_id
                return dict(item)
        raise AssertionError("device not found")

    def ensure_publish_policy(self, tenant_id, device_id, channel_id):
        key = (tenant_id, device_id, channel_id)
        created = key not in self.policies
        self.policies.add(key)
        return created

    def has_publish_policy(self, tenant_id, device_id, channel_id):
        return (tenant_id, device_id, channel_id) in self.policies

    def revoke_publish_policy(self, tenant_id, device_id, channel_id):
        key = (tenant_id, device_id, channel_id)
        if key not in self.policies:
            return 0
        self.policies.remove(key)
        self.revocations.append(key)
        return 1


class _Rules:
    def __init__(self):
        self.rules = [
            {
                "id": "rule-1",
                "name": "smarter-adapter-save-senml",
                "input_channel": "ch-1",
                "input_topic": "",
                "status": "enabled",
                "outputs": [{"type": "save_senml"}],
            }
        ]

    def list_rules(self, workspace_id, *, input_channel="", status="all", limit=100):
        return [
            dict(item)
            for item in self.rules
            if not input_channel or item["input_channel"] == input_channel
        ]

    def ensure_senml_persistence(self, workspace_id, channel_id, *, name):
        if not self.rules:
            self.rules.append(
                {
                    "id": "rule-new",
                    "name": name,
                    "input_channel": channel_id,
                    "input_topic": "",
                    "status": "enabled",
                    "outputs": [{"type": "save_senml"}],
                }
            )
            return SimpleNamespace(
                id="rule-new",
                created=True,
                enabled=False,
                status="enabled",
            )
        self.rules[0]["status"] = "enabled"
        return SimpleNamespace(
            id=self.rules[0]["id"],
            created=False,
            enabled=True,
            status="enabled",
        )


class _Catalog:
    def list_nodes(self):
        return (
            IrrigapNode(
                id="2313",
                device="teros12",
                location="Test_3",
                sub_location="mz_1",
                depths={31: "15cm"},
            ),
        )


class _Store:
    def __init__(self, row, *, decommissioned=False):
        self.row = dict(row)
        self.decommissioned = decommissioned
        self.upserts = []

    def list_devices(self, *, workspace_id="", channel_id="", limit=100):
        return [dict(self.row)]

    def get_node_lifecycle(self, workspace_id, channel_id, node_id):
        if not self.decommissioned:
            return None
        return {"administrative_state": "decommissioned"}

    def upsert_device(self, device, *, channel_id, seen_at=None):
        self.upserts.append((device, channel_id, seen_at))
        self.row.update(
            {
                "atom_device_id": device.id,
                "profile_id": device.profile_id,
                "profile_version_id": device.profile_version_id,
            }
        )


class _Runtime:
    def __init__(self, atom, rules):
        self.control = ControlPlane(atom)
        self.rules = rules
        self.profile_registry = DeviceProfileRegistry()
        self.config = SimpleNamespace(
            persistence_rule_name="smarter-adapter-save-senml"
        )
        self.base = SimpleNamespace(
            workspace=SimpleNamespace(id="ws-1"),
            channel=SimpleNamespace(id="ch-1"),
        )
        self.device_type = DeviceTypeRef(
            id="profile-smarter-adapter-sensor",
            workspace_id="ws-1",
            key="smarter-adapter-sensor",
            name="Smarter Adapter Sensor",
            version_id="version-generic",
            version=1,
        )
        self.persistence_rule = SimpleNamespace(id="rule-1")
        self._device_types = {self.device_type.key: self.device_type}
        self._devices = {}

    def bootstrap(self):
        return None

    def clear_device_cache(self):
        count = len(self._devices)
        self._devices.clear()
        return count

    def evict_device(self, external_id):
        return self._devices.pop(external_id, None) is not None


def _seed_profile(atom, key, *, profile_id, version_id):
    atom.types.append(
        {
            "id": profile_id,
            "tenantId": "ws-1",
            "key": key,
            "name": key,
            "status": "active",
        }
    )
    atom.versions[profile_id] = [
        {
            "id": version_id,
            "profileId": profile_id,
            "version": 1,
            "status": "active",
        }
    ]


def _local_row(profile_id, version_id):
    return {
        "workspace_id": "ws-1",
        "channel_id": "ch-1",
        "external_id": "teros12-sector1.3",
        "atom_device_id": "atom-1",
        "name": "teros12-sector1.3",
        "profile_id": profile_id,
        "profile_version_id": version_id,
        "first_seen": 10.0,
        "last_seen": 100.0,
        "last_sync": 100.0,
        "node_id": "2313",
        "observed_sensor": "teros12",
        "observation_metadata": {
            "sensor": "teros12",
            "node_id": "2313",
            "location": "Test_3",
            "depth": "15cm",
        },
    }


class ReconciliationTests(unittest.TestCase):
    def _base(self):
        atom = _Atom()
        _seed_profile(
            atom,
            "smarter-adapter-sensor",
            profile_id="profile-smarter-adapter-sensor",
            version_id="version-generic",
        )
        _seed_profile(
            atom,
            "smarter-adapter-teros12",
            profile_id="profile-teros",
            version_id="version-teros",
        )
        atom.devices.append(
            {
                "id": "atom-1",
                "tenantId": "ws-1",
                "kind": "device",
                "externalId": "teros12-sector1.3",
                "name": "teros12-sector1.3",
                "profileId": "profile-teros",
                "profileVersionId": "version-teros",
                "attributes": {"managed_by": "smarter-adapter"},
            }
        )
        atom.policies.add(("ws-1", "atom-1", "ch-1"))
        rules = _Rules()
        runtime = _Runtime(atom, rules)
        store = _Store(_local_row("profile-teros", "version-teros"))
        reconciler = ControlPlaneReconciler(
            runtime=runtime,
            store=store,
            catalog=_Catalog(),
            atom=atom,
        )
        return atom, rules, runtime, store, reconciler

    def test_audit_reports_healthy_control_plane_without_mutation(self):
        atom, _, _, store, reconciler = self._base()
        report = reconciler.reconcile(repair=False)
        self.assertEqual(report["status"], "ok")
        self.assertEqual(report["summary"]["drift_detected"], 0)
        self.assertEqual(report["summary"]["devices_checked"], 1)
        self.assertEqual(store.upserts, [])
        self.assertEqual(len(atom.devices), 1)

    def test_repair_migrates_profile_and_restores_publish_policy_in_place(self):
        atom, _, _, store, reconciler = self._base()
        remote = atom.devices[0]
        remote["profileId"] = "profile-smarter-adapter-sensor"
        remote["profileVersionId"] = "version-generic"
        store.row["profile_id"] = "profile-smarter-adapter-sensor"
        store.row["profile_version_id"] = "version-generic"
        atom.policies.clear()

        report = reconciler.reconcile(repair=True)

        self.assertEqual(report["status"], "repaired")
        self.assertEqual(atom.devices[0]["id"], "atom-1")
        self.assertEqual(atom.devices[0]["profileId"], "profile-teros")
        self.assertIn(("ws-1", "atom-1", "ch-1"), atom.policies)
        self.assertEqual(store.row["atom_device_id"], "atom-1")
        self.assertEqual(store.row["profile_id"], "profile-teros")
        actions = report["devices"][0]["actions"]
        self.assertIn("profile_migrated", actions)
        self.assertIn("publish_policy_created", actions)

    def test_decommissioned_policy_drift_is_revoked_not_restored(self):
        atom, _, runtime, _, _ = self._base()
        store = _Store(_local_row("profile-teros", "version-teros"), decommissioned=True)
        reconciler = ControlPlaneReconciler(
            runtime=runtime,
            store=store,
            catalog=_Catalog(),
            atom=atom,
        )

        report = reconciler.reconcile(repair=True)

        self.assertEqual(report["status"], "repaired")
        self.assertNotIn(("ws-1", "atom-1", "ch-1"), atom.policies)
        self.assertEqual(atom.revocations, [("ws-1", "atom-1", "ch-1")])
        self.assertIn("revoked_publish_policy:1", report["devices"][0]["actions"])

    def test_missing_workspace_blocks_automatic_recreation(self):
        atom, _, _, _, reconciler = self._base()
        atom.workspaces.clear()

        report = reconciler.reconcile(repair=True)

        self.assertEqual(report["status"], "blocked")
        self.assertEqual(report["issues"][0]["kind"], "workspace_missing")
        self.assertFalse(report["issues"][0]["repairable"])
        self.assertEqual(atom.workspaces, [])


if __name__ == "__main__":
    unittest.main()
