from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from smarter_adapter.magistrala.control_plane import DeviceRef
from smarter_adapter.storage import SQLiteManagementStore


class DeviceQualityStateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = SQLiteManagementStore(Path(self.tmp.name) / "state.sqlite3")
        self.device = DeviceRef(
            id="atom-device-1",
            workspace_id="ws-1",
            external_id="sensor-1",
            name="sensor-1",
            profile_id="profile-1",
            profile_version_id="version-1",
        )
        self.store.upsert_device(self.device, channel_id="ch-1", seen_at=100.0)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def test_quality_defaults_to_unknown_until_first_evaluation(self):
        item = self.store.find_device("ws-1", "ch-1", "sensor-1")
        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["data_quality"], "unknown")
        self.assertEqual(item["invalid_fields"], [])
        self.assertIsNone(item["quality_evaluated_at"])

    def test_latest_quality_state_is_upserted_and_fields_are_deduplicated(self):
        self.store.set_device_quality(
            "ws-1",
            "ch-1",
            "sensor-1",
            quality_status="invalid",
            invalid_fields=("moisture", "temperature", "moisture"),
            evaluated_at=120.0,
            source_received_at=119.0,
        )
        item = self.store.find_device("ws-1", "ch-1", "sensor-1")
        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["data_quality"], "invalid")
        self.assertEqual(item["invalid_fields"], ["moisture", "temperature"])
        self.assertEqual(item["quality_evaluated_at"], 120.0)
        self.assertEqual(item["quality_source_received_at"], 119.0)

        self.store.set_device_quality(
            "ws-1",
            "ch-1",
            "sensor-1",
            quality_status="valid",
            invalid_fields=(),
            evaluated_at=130.0,
            source_received_at=129.0,
        )
        item = self.store.list_devices(workspace_id="ws-1", channel_id="ch-1")[0]
        self.assertEqual(item["data_quality"], "valid")
        self.assertEqual(item["invalid_fields"], [])
        self.assertEqual(item["quality_evaluated_at"], 130.0)


if __name__ == "__main__":
    unittest.main()
