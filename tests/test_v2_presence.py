from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from smarter_adapter.presence import (
    DevicePresencePolicy,
    PresenceThresholds,
    parse_presence_profiles,
)


class DevicePresencePolicyTests(unittest.TestCase):
    def setUp(self):
        self.policy = DevicePresencePolicy(
            stale_after_seconds=300,
            offline_after_seconds=1800,
        )

    def test_online_before_stale_threshold(self):
        self.assertEqual(self.policy.classify(900.0, now=1000.0), "online")

    def test_stale_between_thresholds(self):
        self.assertEqual(self.policy.classify(600.0, now=1000.0), "stale")

    def test_offline_after_offline_threshold(self):
        self.assertEqual(self.policy.classify(100.0, now=2000.0), "offline")

    def test_decorate_adds_status_and_age_without_mutating_source(self):
        source = {"external_id": "sensor-1", "last_seen": 950.0}
        decorated = self.policy.decorate(source, now=1000.0)
        self.assertEqual(decorated["operational_status"], "online")
        self.assertEqual(decorated["last_seen_age_seconds"], 50.0)
        self.assertEqual(decorated["presence_profile"], "default")
        self.assertEqual(decorated["presence_stale_after_seconds"], 300.0)
        self.assertNotIn("operational_status", source)

    def test_family_profile_uses_real_reporting_cadence(self):
        policy = DevicePresencePolicy(
            stale_after_seconds=300,
            offline_after_seconds=1800,
            family_thresholds={
                "teros12": PresenceThresholds(
                    expected_interval_seconds=600,
                    stale_after_seconds=900,
                    offline_after_seconds=1800,
                )
            },
        )
        device = {
            "external_id": "teros12-sector1.1",
            "last_seen": 1000.0,
            "observed_sensor": "teros12",
        }
        # 10 minute nominal cadence: 700 seconds old is still healthy.
        decorated = policy.decorate(device, now=1700.0)
        self.assertEqual(decorated["operational_status"], "online")
        self.assertEqual(decorated["presence_profile"], "teros12")
        self.assertEqual(decorated["presence_expected_interval_seconds"], 600.0)
        self.assertEqual(decorated["presence_stale_after_seconds"], 900.0)

        self.assertEqual(
            policy.classify(1000.0, now=1950.0, family="teros12"),
            "stale",
        )
        self.assertEqual(
            policy.classify(1000.0, now=2800.0, family="teros12"),
            "offline",
        )

    def test_parse_presence_profiles(self):
        profiles = parse_presence_profiles(
            '{"teros12":{"expected_interval_seconds":600,'
            '"stale_after_seconds":900,"offline_after_seconds":1800}}'
        )
        self.assertIn("teros12", profiles)
        self.assertEqual(profiles["teros12"].expected_interval_seconds, 600.0)
        self.assertEqual(profiles["teros12"].stale_after_seconds, 900.0)
        self.assertEqual(profiles["teros12"].offline_after_seconds, 1800.0)

    def test_invalid_thresholds_are_rejected(self):
        with self.assertRaises(ValueError):
            DevicePresencePolicy(stale_after_seconds=0, offline_after_seconds=10)
        with self.assertRaises(ValueError):
            DevicePresencePolicy(stale_after_seconds=10, offline_after_seconds=10)
        with self.assertRaises(ValueError):
            PresenceThresholds(
                expected_interval_seconds=600,
                stale_after_seconds=300,
                offline_after_seconds=1800,
            )


if __name__ == "__main__":
    unittest.main()
