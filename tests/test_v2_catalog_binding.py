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
from smarter_adapter.magistrala.control_plane import DeviceRef


class CatalogBindingTests(unittest.TestCase):
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

    @staticmethod
    def _device(external_id: str, atom_id: str) -> DeviceRef:
        return DeviceRef(
            id=atom_id,
            workspace_id="ws-1",
            external_id=external_id,
            name=external_id,
            profile_id="profile-1",
            profile_version_id="version-1",
        )

    def test_binding_is_persisted_and_decorates_managed_device(self):
        device = self._device("teros12-sector1.3", "atom-2313")
        self.store.upsert_device(device, channel_id="ch-1", seen_at=100.0)
        self.store.set_device_quality(
            "ws-1",
            "ch-1",
            device.external_id,
            quality_status="invalid",
            invalid_fields=("moisture",),
            evaluated_at=101.0,
            source_received_at=100.0,
        )
        self.store.set_device_observation(
            "ws-1",
            "ch-1",
            device.external_id,
            node_id="2313",
            sensor="teros12",
            metadata={"location": "Test_3", "f_port": 31},
            observed_at=100.0,
        )

        item = self.store.find_device("ws-1", "ch-1", device.external_id)
        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["node_id"], "2313")
        self.assertEqual(item["observed_sensor"], "teros12")
        self.assertEqual(item["binding_observed_at"], 100.0)
        self.assertEqual(item["observation_metadata"]["location"], "Test_3")
        self.assertEqual(item["data_quality"], "invalid")

    def test_binding_survives_store_reopen(self):
        device = self._device("teros12-sector1.3", "atom-2313")
        self.store.upsert_device(device, channel_id="ch-1", seen_at=100.0)
        self.store.set_device_observation(
            "ws-1",
            "ch-1",
            device.external_id,
            node_id="2313",
            sensor="teros12",
            observed_at=100.0,
        )
        self.store.close()
        self.store = BindingSQLiteManagementStore(self.db)

        item = self.store.find_latest_device_by_node("ws-1", "ch-1", "2313")
        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["external_id"], "teros12-sector1.3")
        self.assertEqual(item["atom_device_id"], "atom-2313")

    def test_latest_device_wins_when_node_has_multiple_external_ids(self):
        old = self._device("teros12-old-name", "atom-old")
        new = self._device("teros12-sector1.3", "atom-new")
        self.store.upsert_device(old, channel_id="ch-1", seen_at=100.0)
        self.store.set_device_observation(
            "ws-1", "ch-1", old.external_id, node_id="2313", observed_at=100.0
        )
        self.store.upsert_device(new, channel_id="ch-1", seen_at=200.0)
        self.store.set_device_observation(
            "ws-1", "ch-1", new.external_id, node_id="2313", observed_at=200.0
        )

        item = self.store.find_latest_device_by_node("ws-1", "ch-1", "2313")
        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["external_id"], "teros12-sector1.3")
        self.assertEqual(item["atom_device_id"], "atom-new")

    def test_catalog_payload_distinguishes_bound_and_unbound_nodes(self):
        manager = BoundIrrigapCatalogManager(
            load_irrigap_catalog(file_path=str(self.catalog_path)),
            file_path=str(self.catalog_path),
        )
        manager.set_binding_resolver(
            lambda node_id: (
                {
                    "external_id": "teros12-sector1.3",
                    "atom_device_id": "atom-2313",
                    "operational_status": "online",
                    "last_seen": 123.0,
                    "last_seen_age_seconds": 2.0,
                    "data_quality": "invalid",
                    "invalid_fields": ["moisture"],
                    "binding_observed_at": 123.0,
                }
                if node_id == "2313"
                else None
            )
        )

        payload = manager.public_payload()
        self.assertEqual(payload["total"], 2)
        self.assertEqual(payload["managed"], 1)
        self.assertEqual(payload["unbound"], 1)
        by_id = {item["id"]: item for item in payload["items"]}
        self.assertTrue(by_id["2313"]["managed"])
        self.assertEqual(by_id["2313"]["external_id"], "teros12-sector1.3")
        self.assertEqual(by_id["2313"]["data_quality"], "invalid")
        self.assertFalse(by_id["2305"]["managed"])
        self.assertIsNone(by_id["2305"]["external_id"])
        self.assertEqual(by_id["2305"]["data_quality"], "unknown")


if __name__ == "__main__":
    unittest.main()
