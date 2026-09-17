from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from smarter_adapter.catalog_binding import (
    BindingSQLiteManagementStore,
    BoundIrrigapCatalogManager,
)
from smarter_adapter.irrigap_config import load_irrigap_catalog
from smarter_adapter.magistrala.control_plane import (
    BaseResources,
    ChannelRef,
    DeviceRef,
    DeviceTypeRef,
    WorkspaceRef,
)
from smarter_adapter.magistrala.rules import PersistenceRuleRef
from smarter_adapter.models import Measurement, ParsedEvent, RawEvent
from smarter_adapter.pipeline import ParseOutcome
from smarter_adapter.runtime import SmarterAdapterRuntime


class _Pipeline:
    def __init__(self, event: ParsedEvent):
        self.event = event

    def process(self, raw):
        return ParseOutcome(parser="lifecycle-parser", event=self.event)


class _FailingControl:
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
        raise RuntimeError("simulated Atom device failure")


class _Rules:
    def ensure_senml_persistence(self, workspace_id, channel_id, *, name):
        return PersistenceRuleRef(
            id="rule-1",
            workspace_id=workspace_id,
            channel_id=channel_id,
            name=name,
            status="enabled",
        )


class _NeverPublisher:
    def publish(self, **kwargs):  # pragma: no cover - must not be reached
        raise AssertionError("publisher must not run when device reconciliation fails")


class CatalogLifecycleTests(unittest.TestCase):
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
                        },
                        {
                            "id": "2305",
                            "device": "teros12",
                            "location": "Sector_5",
                            "sub_location": "mz_1",
                            "depths": {"31": "15cm"},
                        },
                    ]
                }
            ),
            encoding="utf-8",
        )
        self.store = BindingSQLiteManagementStore(self.db)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def _manager(self):
        manager = BoundIrrigapCatalogManager(
            load_irrigap_catalog(file_path=str(self.catalog_path)),
            file_path=str(self.catalog_path),
        )
        manager.set_observation_resolver(
            lambda node_id: self.store.find_latest_catalog_observation_by_node(
                "ws-1", "ch-1", node_id
            )
        )
        manager.set_binding_resolver(
            lambda node_id: self.store.find_latest_device_by_node(
                "ws-1", "ch-1", node_id
            )
        )
        return manager

    def test_catalog_distinguishes_planned_and_observed_without_atom_device(self):
        self.store.observe_catalog_node(
            "ws-1",
            "ch-1",
            "teros12-sector1.3",
            node_id="2313",
            sensor="teros12",
            metadata={"location": "Test_3", "f_port": 31},
            observed_at=100.0,
        )

        payload = self._manager().public_payload()
        self.assertEqual(payload["managed"], 0)
        self.assertEqual(payload["observed"], 1)
        self.assertEqual(payload["planned"], 1)
        self.assertEqual(payload["unbound"], 2)

        by_id = {item["id"]: item for item in payload["items"]}
        observed = by_id["2313"]
        self.assertEqual(observed["lifecycle_state"], "observed")
        self.assertTrue(observed["observed"])
        self.assertFalse(observed["managed"])
        self.assertEqual(observed["external_id"], "teros12-sector1.3")
        self.assertIsNone(observed["atom_device_id"])
        self.assertEqual(observed["first_observed_at"], 100.0)
        self.assertEqual(observed["last_observed_at"], 100.0)

        planned = by_id["2305"]
        self.assertEqual(planned["lifecycle_state"], "planned")
        self.assertFalse(planned["observed"])
        self.assertFalse(planned["managed"])

    def test_observation_keeps_first_seen_and_advances_last_seen_across_reopen(self):
        self.store.observe_catalog_node(
            "ws-1", "ch-1", "sensor-1", node_id="2313", observed_at=100.0
        )
        self.store.observe_catalog_node(
            "ws-1", "ch-1", "sensor-1", node_id="2313", observed_at=200.0
        )
        self.store.close()
        self.store = BindingSQLiteManagementStore(self.db)

        item = self.store.find_latest_catalog_observation_by_node("ws-1", "ch-1", "2313")
        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["observed_external_id"], "sensor-1")
        self.assertEqual(item["first_observed_at"], 100.0)
        self.assertEqual(item["last_observed_at"], 200.0)

    def test_runtime_records_observed_before_atom_device_reconciliation(self):
        event = ParsedEvent(
            external_device_id="teros12-sector1.3",
            measurements=(Measurement("soil.temperature", 25.0, "Cel"),),
            metadata={
                "node_id": "2313",
                "sensor": "teros12",
                "location": "Test_3",
                "sub_location": "mz_1",
                "depth": "15cm",
                "f_port": 31,
            },
        )
        runtime = SmarterAdapterRuntime(
            pipeline=_Pipeline(event),
            control=_FailingControl(),
            rules=_Rules(),
            publisher=_NeverPublisher(),
            state_store=self.store,
        )
        raw = RawEvent(source="mqtt:test", payload=b"{}", received_at=321.0)

        with self.assertRaisesRegex(RuntimeError, "simulated Atom device failure"):
            runtime.process(raw)

        observed = self.store.find_latest_catalog_observation_by_node(
            "ws-1", "ch-1", "2313"
        )
        self.assertIsNotNone(observed)
        assert observed is not None
        self.assertEqual(observed["observed_external_id"], "teros12-sector1.3")
        self.assertEqual(observed["last_observed_at"], 321.0)
        self.assertIsNone(
            self.store.find_latest_device_by_node("ws-1", "ch-1", "2313")
        )


if __name__ == "__main__":
    unittest.main()
