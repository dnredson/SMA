from __future__ import annotations

import json
import tempfile
import time
import unittest
from pathlib import Path

from smarter_adapter.device_lifecycle import (
    DecommissionedEventSuppressed,
    DeviceLifecycleController,
    LifecycleBindingSQLiteManagementStore,
    LifecycleIrrigapCatalogManager,
    LifecycleSmarterAdapterRuntime,
)
from smarter_adapter.inputs import MQTTInputConfig
from smarter_adapter.irrigap_config import load_irrigap_catalog
from smarter_adapter.lifecycle_service import LifecycleSmarterAdapterService
from smarter_adapter.magistrala.atom import AtomConfig
from smarter_adapter.magistrala.control_plane import (
    BaseResources,
    ChannelRef,
    DeviceRef,
    DeviceTypeRef,
    WorkspaceRef,
)
from smarter_adapter.magistrala.lifecycle import LifecycleAtomClient
from smarter_adapter.magistrala.publisher import PublishResult
from smarter_adapter.magistrala.rules import PersistenceRuleRef
from smarter_adapter.models import Measurement, ParsedEvent, RawEvent
from smarter_adapter.pipeline import ParseOutcome


class _Pipeline:
    def process(self, raw):
        return ParseOutcome(
            parser="lifecycle-test",
            event=ParsedEvent(
                external_device_id="teros12-sector1.3",
                measurements=(Measurement("soil.temperature", 25.0, "Cel"),),
                metadata={"node_id": "2313", "sensor": "teros12"},
            ),
        )


class _Control:
    def __init__(self):
        self.base = BaseResources(
            workspace=WorkspaceRef(id="ws-1", name="Workspace"),
            channel=ChannelRef(id="ch-1", workspace_id="ws-1", name="Telemetry"),
        )
        self.device_type = DeviceTypeRef(
            id="profile-1",
            workspace_id="ws-1",
            key="sensor",
            name="Sensor",
            version_id="version-1",
            version=1,
        )

    def ensure_base(self, **kwargs):
        return self.base

    def ensure_device_type(self, workspace_id, **kwargs):
        return self.device_type

    def ensure_device(self, workspace_id, channel_id, external_id, **kwargs):
        return DeviceRef(
            id="atom-2313",
            workspace_id=workspace_id,
            external_id=external_id,
            name=external_id,
            profile_id="profile-1",
            profile_version_id="version-1",
        )


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
    def __init__(self):
        self.calls = 0

    def publish(self, **kwargs):
        self.calls += 1
        return PublishResult(status=202, body={"status": "accepted"})


class _Atom:
    def __init__(self):
        self.revoked = []
        self.ensured = []

    def revoke_publish_policy(self, tenant_id, device_id, channel_id):
        self.revoked.append((tenant_id, device_id, channel_id))
        return 1

    def ensure_publish_policy(self, tenant_id, device_id, channel_id):
        self.ensured.append((tenant_id, device_id, channel_id))
        return True


class _Input:
    def __init__(self, config, callback):
        self.config = config
        self.callback = callback
        self.connected = False
        self.last_error = None

    def start(self):
        self.connected = True

    def stop(self):
        self.connected = False


class _SuppressedRuntime:
    def process(self, raw):
        raise DecommissionedEventSuppressed(
            node_id="2313",
            external_id="teros12-sector1.3",
        )

    def bootstrap(self):
        return None


class DecommissionLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.db = self.root / "state.sqlite3"
        self.catalog_path = self.root / "nodes.json"
        self.catalog_path.write_text(
            json.dumps(
                {
                    "nodes": [
                        {
                            "id": "2313",
                            "device": "teros12",
                            "location": "Test_3",
                            "sub_location": "mz_1",
                            "depths": {"31": "15cm"},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        self.store = LifecycleBindingSQLiteManagementStore(self.db)
        self.manager = LifecycleIrrigapCatalogManager(
            load_irrigap_catalog(file_path=str(self.catalog_path)),
            file_path=str(self.catalog_path),
        )
        self.manager.set_binding_resolver(
            lambda node_id: self.store.find_latest_device_by_node("ws-1", "ch-1", node_id)
        )
        self.manager.set_observation_resolver(
            lambda node_id: self.store.find_latest_catalog_observation_by_node(
                "ws-1", "ch-1", node_id
            )
        )
        self.manager.set_lifecycle_resolver(
            lambda node_id: self.store.get_node_lifecycle("ws-1", "ch-1", node_id)
        )

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def _managed_device(self):
        device = DeviceRef(
            id="atom-2313",
            workspace_id="ws-1",
            external_id="teros12-sector1.3",
            name="teros12-sector1.3",
            profile_id="profile-1",
            profile_version_id="version-1",
        )
        self.store.upsert_device(device, channel_id="ch-1", seen_at=100.0)
        self.store.set_device_observation(
            "ws-1",
            "ch-1",
            device.external_id,
            node_id="2313",
            sensor="teros12",
            observed_at=100.0,
        )
        return device

    def test_decommission_state_persists_and_overrides_catalog_lifecycle(self):
        self._managed_device()
        self.store.decommission_node(
            "ws-1", "ch-1", "2313", reason="maintenance", at=200.0
        )
        self.store.record_decommission_policy_result(
            "ws-1", "ch-1", "2313", revoked=True
        )

        item = self.manager.public_item("2313")
        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["lifecycle_state"], "decommissioned")
        self.assertEqual(item["historical_lifecycle_state"], "managed")
        self.assertFalse(item["managed"])
        self.assertTrue(item["was_managed"])
        self.assertTrue(item["atom_publish_policy_revoked"])
        self.assertEqual(item["decommission_reason"], "maintenance")

        self.store.close()
        self.store = LifecycleBindingSQLiteManagementStore(self.db)
        state = self.store.get_node_lifecycle("ws-1", "ch-1", "2313")
        self.assertIsNotNone(state)
        assert state is not None
        self.assertEqual(state["administrative_state"], "decommissioned")
        self.assertEqual(state["decommissioned_at"], 200.0)

    def test_runtime_suppresses_decommissioned_before_publish(self):
        publisher = _Publisher()
        runtime = LifecycleSmarterAdapterRuntime(
            pipeline=_Pipeline(),
            control=_Control(),
            rules=_Rules(),
            publisher=publisher,
            state_store=self.store,
        )
        runtime.bootstrap()
        self.store.decommission_node("ws-1", "ch-1", "2313", at=200.0)

        with self.assertRaises(DecommissionedEventSuppressed):
            runtime.process(RawEvent(source="mqtt:test", payload=b"{}", received_at=300.0))
        self.assertEqual(publisher.calls, 0)
        self.assertIsNone(
            self.store.find_latest_catalog_observation_by_node("ws-1", "ch-1", "2313")
        )

    def test_controller_revokes_then_reactivates_same_atom_device(self):
        self._managed_device()
        atom = _Atom()
        runtime = LifecycleSmarterAdapterRuntime(
            pipeline=_Pipeline(),
            control=_Control(),
            rules=_Rules(),
            publisher=_Publisher(),
            state_store=self.store,
        )
        runtime.bootstrap()
        controller = DeviceLifecycleController(runtime, self.store, self.manager, atom)

        decommissioned = controller.decommission("2313", reason="field replacement")
        self.assertEqual(decommissioned["status"], "decommissioned")
        self.assertEqual(decommissioned["atom_policies_revoked"], 1)
        self.assertEqual(atom.revoked, [("ws-1", "atom-2313", "ch-1")])
        self.assertEqual(decommissioned["item"]["lifecycle_state"], "decommissioned")

        reactivated = controller.reactivate("2313")
        self.assertEqual(reactivated["status"], "reactivated")
        self.assertTrue(reactivated["atom_publish_policy_created"])
        self.assertEqual(atom.ensured, [("ws-1", "atom-2313", "ch-1")])
        self.assertEqual(reactivated["item"]["lifecycle_state"], "managed")
        self.assertEqual(reactivated["item"]["atom_device_id"], "atom-2313")

    def test_service_suppression_does_not_create_dlq(self):
        config = MQTTInputConfig(host="localhost", port=1883, topic="#")
        service = LifecycleSmarterAdapterService(
            _SuppressedRuntime(),
            [config],
            input_factory=_Input,
            reliability_store=self.store,
            ingress_poll_interval_seconds=0.01,
        )
        service.start()
        try:
            service._handle_event(
                RawEvent(source="mqtt:test", payload=b"{}", received_at=100.0)
            )
            deadline = time.time() + 1.0
            while service.stats.suppressed < 1 and time.time() < deadline:
                time.sleep(0.01)
            self.assertEqual(service.stats.received, 1)
            self.assertEqual(service.stats.ingressed, 1)
            self.assertEqual(service.stats.suppressed, 1)
            self.assertEqual(service.stats.failed, 0)
            self.assertEqual(service.stats.dead_lettered, 0)
            self.assertEqual(self.store.count_ingress(), 0)
            self.assertEqual(self.store.count_dlq(), 0)
        finally:
            service.stop()

    def test_atom_revoke_deletes_only_matching_publish_policy(self):
        client = LifecycleAtomClient(AtomConfig(base_url="http://atom", token="token"))
        calls = []

        def fake_graphql(query, variables=None, **kwargs):
            variables = variables or {}
            if "directPolicies" in query:
                return {
                    "directPolicies": {
                        "total": 2,
                        "items": [
                            {
                                "id": "policy-publish",
                                "permissionBlock": {
                                    "objectKind": "resource",
                                    "objectType": "resource:channel",
                                    "objectId": "ch-1",
                                    "effect": "allow",
                                    "actions": [{"id": "a1", "name": "publish"}],
                                },
                            },
                            {
                                "id": "policy-read",
                                "permissionBlock": {
                                    "objectKind": "resource",
                                    "objectType": "resource:channel",
                                    "objectId": "ch-1",
                                    "effect": "allow",
                                    "actions": [{"id": "a2", "name": "read"}],
                                },
                            },
                        ],
                    }
                }
            if "deleteDirectPolicy" in query:
                calls.append(variables["id"])
                return {"deleteDirectPolicy": True}
            raise AssertionError(query)

        client._graphql = fake_graphql  # type: ignore[method-assign]
        deleted = client.revoke_publish_policy("ws-1", "atom-2313", "ch-1")
        self.assertEqual(deleted, 1)
        self.assertEqual(calls, ["policy-publish"])


if __name__ == "__main__":
    unittest.main()
