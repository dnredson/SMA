from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from smarter_adapter.device_context import DeviceContextProvider
from smarter_adapter.gateway_monitor import (
    GatewayPresencePolicy,
    GatewayTopologySQLiteManagementStore,
)


class DeviceContextGatewayPresenceTests(unittest.TestCase):
    def test_gateway_link_is_enriched_with_current_gateway_presence(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = GatewayTopologySQLiteManagementStore(Path(tmp) / "state.sqlite3")
            try:
                store.record_gateway_event(
                    "ws",
                    "000000ffff001002",
                    event_kind="stats",
                    received_at=1000,
                    topic="au915_1/gateway/000000ffff001002/event/stats",
                    topic_root="au915_1",
                )
                store.record_device_gateway(
                    "ws",
                    "ch",
                    "teros12-sector1.3",
                    {
                        "gateway_id": "000000ffff001002",
                        "rssi": -42,
                        "snr": 12.5,
                    },
                    observed_at=995,
                )

                provider = DeviceContextProvider(
                    reader=None,
                    store=store,
                    presence_policy=None,
                    history_provider=None,
                    gateway_presence_policy=GatewayPresencePolicy(
                        expected_interval_seconds=30,
                        stale_after_seconds=90,
                        offline_after_seconds=180,
                    ),
                )
                links = provider._gateway_links("ws", "ch", "teros12-sector1.3")
                self.assertEqual(len(links), 1)
                self.assertEqual(links[0]["gateway_id"], "000000ffff001002")
                self.assertEqual(links[0]["gateway_operational_status"], "online")
                self.assertEqual(links[0]["rssi"], -42.0)
                self.assertEqual(links[0]["snr"], 12.5)
                self.assertEqual(links[0]["gateway_last_stats_at"], 1000.0)
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
