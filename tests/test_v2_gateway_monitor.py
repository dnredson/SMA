from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from smarter_adapter.gateway_monitor import (
    GatewayMqttObserver,
    GatewayPresencePolicy,
    GatewayTopologySQLiteManagementStore,
)
from smarter_adapter.magistrala.control_plane import DeviceRef


class GatewayMonitorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = GatewayTopologySQLiteManagementStore(
            Path(self.tmp.name) / "state.sqlite3"
        )
        self.workspace = "ws-1"
        self.channel = "ch-1"

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def test_presence_policy_uses_observed_gateway_activity(self):
        policy = GatewayPresencePolicy(
            expected_interval_seconds=30,
            stale_after_seconds=90,
            offline_after_seconds=180,
        )
        self.assertEqual(policy.classify(1000, now=1050), "online")
        self.assertEqual(policy.classify(1000, now=1100), "stale")
        self.assertEqual(policy.classify(1000, now=1180), "offline")

    def test_retained_conn_discovers_gateway_without_false_online_state(self):
        self.store.record_gateway_event(
            self.workspace,
            "000000ffff001002",
            event_kind="conn",
            received_at=1000,
            topic="gateway/000000ffff001002/state/conn",
            retained=True,
        )
        item = self.store.find_gateway(self.workspace, "000000ffff001002")
        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["last_seen"], 0.0)
        self.assertEqual(item["conn_count"], 1)
        policy = GatewayPresencePolicy()
        self.assertEqual(policy.decorate(item, now=1001)["operational_status"], "offline")

        self.store.record_gateway_event(
            self.workspace,
            "000000ffff001002",
            event_kind="stats",
            received_at=1010,
            topic="au915_1/gateway/000000ffff001002/event/stats",
            topic_root="au915_1",
        )
        item = self.store.find_gateway(self.workspace, "000000ffff001002")
        assert item is not None
        self.assertEqual(item["last_stats_at"], 1010.0)
        self.assertEqual(item["stats_count"], 1)
        self.assertEqual(item["topic_root"], "au915_1")
        self.assertEqual(policy.decorate(item, now=1020)["operational_status"], "online")

    def test_device_gateway_link_preserves_last_radio_observation(self):
        self.store.record_device_gateway(
            self.workspace,
            self.channel,
            "teros12-sector1.3",
            {
                "gateway_id": "000000ffff001002",
                "rssi": -42,
                "snr": 12.5,
                "channel": 6,
                "rf_chain": 1,
                "crc_status": "CRC_OK",
            },
            observed_at=2000,
        )
        links = self.store.list_device_gateways(
            self.workspace,
            self.channel,
            "teros12-sector1.3",
        )
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0]["gateway_id"], "000000ffff001002")
        self.assertEqual(links[0]["rssi"], -42.0)
        self.assertEqual(links[0]["snr"], 12.5)
        self.assertEqual(links[0]["channel"], 6)
        self.assertEqual(links[0]["packet_count"], 1)

    def test_role_quality_does_not_let_battery_hide_invalid_soil(self):
        device = DeviceRef(
            id="atom-1",
            workspace_id=self.workspace,
            external_id="teros12-sector1.3",
            name="teros12-sector1.3",
            profile_id="profile-1",
            profile_version_id="version-1",
        )
        self.store.upsert_device(device, channel_id=self.channel, seen_at=1000)
        self.store.set_device_quality(
            self.workspace,
            self.channel,
            device.external_id,
            quality_status="invalid",
            invalid_fields=("moisture", "temperature", "electrical_conductivity"),
            source_received_at=1000,
            evaluated_at=1000,
            role="soil",
        )
        self.store.set_device_quality(
            self.workspace,
            self.channel,
            device.external_id,
            quality_status="valid",
            invalid_fields=(),
            source_received_at=1010,
            evaluated_at=1010,
            role="battery",
        )
        item = self.store.find_device(self.workspace, self.channel, device.external_id)
        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["data_quality"], "invalid")
        self.assertEqual(item["quality_by_role"]["soil"]["data_quality"], "invalid")
        self.assertEqual(item["quality_by_role"]["battery"]["data_quality"], "valid")
        self.assertIn("moisture", item["invalid_fields"])

    def test_gateway_topic_parser_accepts_stats_and_conn_variants(self):
        self.assertEqual(
            GatewayMqttObserver.parse_topic(
                "au915_1/gateway/000000ffff001002/event/stats"
            ),
            ("000000ffff001002", "stats", "au915_1"),
        )
        self.assertEqual(
            GatewayMqttObserver.parse_topic(
                "gateway/000000ffff007001/state/conn"
            ),
            ("000000ffff007001", "conn", ""),
        )
        self.assertIsNone(
            GatewayMqttObserver.parse_topic(
                "au915_1/gateway/000000ffff001002/event/up"
            )
        )


if __name__ == "__main__":
    unittest.main()
