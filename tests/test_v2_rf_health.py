from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from smarter_adapter.rf_health import RFHealthTopologySQLiteManagementStore


class RFHealthTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = RFHealthTopologySQLiteManagementStore(
            Path(self.tmp.name) / "state.sqlite3",
            rf_retention_days=90,
            rf_summary_window_hours=24,
            rf_stable_slope_db_per_hour=0.5,
        )
        self.workspace = "ws-1"
        self.channel = "ch-1"
        self.external_id = "teros12-sector1.1"
        self.gateway_id = "000000ffff001002"

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def _record(
        self,
        at: float,
        rssi: float,
        snr: float,
        *,
        channel: int = 5,
        radio: bool = False,
    ):
        sample = {
            "gateway_id": self.gateway_id,
            "rssi": rssi,
            "snr": snr,
            "channel": channel,
            "rf_chain": 1,
            "crc_status": "CRC_OK",
            "f_port": 31,
            "message_role": "soil",
            "mqtt_topic": "application/app/device/eui/event/up",
        }
        if radio:
            sample.update(
                {
                    "frequency_hz": 903300000,
                    "modulation": "lora",
                    "spreading_factor": 10,
                    "bandwidth_hz": 125000,
                    "code_rate": "CR_4_5",
                }
            )
        self.store.record_device_gateway(
            self.workspace,
            self.channel,
            self.external_id,
            sample,
            observed_at=at,
        )

    def test_samples_are_persistent_and_duplicate_safe(self):
        self._record(1000.0, -61.0, 9.8)
        self._record(1000.0, -61.0, 9.8)
        self.assertEqual(
            self.store.count_rf_samples(
                workspace_id=self.workspace,
                channel_id=self.channel,
                external_id=self.external_id,
            ),
            1,
        )
        self.store.close()
        self.store = RFHealthTopologySQLiteManagementStore(
            Path(self.tmp.name) / "state.sqlite3",
            rf_retention_days=90,
            rf_summary_window_hours=24,
            rf_stable_slope_db_per_hour=0.5,
        )
        samples = self.store.list_rf_samples(
            self.workspace,
            self.channel,
            self.external_id,
            limit=10,
        )
        self.assertEqual(len(samples), 1)
        self.assertEqual(samples[0]["rssi"], -61.0)
        self.assertEqual(samples[0]["snr"], 9.8)
        self.assertEqual(samples[0]["f_port"], 31)
        self.assertEqual(samples[0]["message_role"], "soil")

    def test_report_detects_degrading_rssi_and_snr_trends(self):
        self._record(1000.0, -60.0, 10.0)
        self._record(4600.0, -63.0, 8.0)
        self._record(8200.0, -66.0, 6.0)
        report = self.store.rf_health_report(
            self.workspace,
            self.channel,
            self.external_id,
            window_hours=3,
            now=8200.0,
        )
        self.assertEqual(report["summary"]["samples"], 3)
        self.assertEqual(report["summary"]["gateways"], 1)
        gateway = report["gateways"][0]
        self.assertEqual(gateway["gateway_id"], self.gateway_id)
        self.assertEqual(gateway["rssi"]["trend"], "degrading")
        self.assertAlmostEqual(gateway["rssi"]["slope_db_per_hour"], -3.0)
        self.assertEqual(gateway["snr"]["trend"], "degrading")
        self.assertAlmostEqual(gateway["snr"]["slope_db_per_hour"], -2.0)
        self.assertEqual(gateway["channels"], [5])
        self.assertEqual(gateway["message_roles"], ["soil"])
        self.assertEqual(gateway["assessment"]["status"], "comparable")

    def test_latest_topology_link_is_enriched_with_rf_summary(self):
        now = time.time()
        self._record(now - 1200, -70.0, 4.0)
        self._record(now - 600, -68.0, 5.0)
        self._record(now, -66.0, 6.0)
        links = self.store.list_device_gateways(
            self.workspace,
            self.channel,
            self.external_id,
        )
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0]["rssi"], -66.0)
        health = links[0]["rf_health"]
        self.assertEqual(health["samples"], 3)
        self.assertEqual(health["rssi"]["trend"], "improving")
        self.assertEqual(health["snr"]["trend"], "improving")

    def test_lora_radio_profile_is_persisted_and_reported(self):
        self._record(1000.0, -61.0, 9.8, radio=True)
        samples = self.store.list_rf_samples(
            self.workspace,
            self.channel,
            self.external_id,
            limit=10,
        )
        self.assertEqual(samples[0]["frequency_hz"], 903300000)
        self.assertEqual(samples[0]["modulation"], "lora")
        self.assertEqual(samples[0]["spreading_factor"], 10)
        self.assertEqual(samples[0]["bandwidth_hz"], 125000)
        self.assertEqual(samples[0]["code_rate"], "CR_4_5")

        report = self.store.rf_health_report(
            self.workspace,
            self.channel,
            self.external_id,
            window_hours=1,
            now=1000.0,
        )
        profile = report["gateways"][0]["by_radio_profile"][0]["profile"]
        self.assertEqual(profile["frequency_hz"], 903300000)
        self.assertEqual(profile["spreading_factor"], 10)

    def test_mixed_channels_without_radio_profile_do_not_become_health_verdict(self):
        self._record(1000.0, -60.0, 10.0, channel=1)
        self._record(4600.0, -70.0, 8.0, channel=2)
        self._record(8200.0, -80.0, 6.0, channel=3)
        report = self.store.rf_health_report(
            self.workspace,
            self.channel,
            self.external_id,
            window_hours=3,
            now=8200.0,
        )
        gateway = report["gateways"][0]
        # The raw aggregate regression remains useful diagnostically...
        self.assertEqual(gateway["rssi"]["trend"], "degrading")
        # ...but the health assessment refuses to compare different channels
        # as if they were one identical radio condition.
        self.assertEqual(gateway["assessment"]["status"], "mixed_conditions")
        self.assertFalse(gateway["assessment"]["comparable"])
        self.assertEqual(
            gateway["assessment"]["reason"],
            "multiple_channels_without_radio_profile",
        )
        self.assertEqual(len(gateway["by_channel"]), 3)


if __name__ == "__main__":
    unittest.main()
