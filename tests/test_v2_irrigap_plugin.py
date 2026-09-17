import base64
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from smarter_adapter.models import RawEvent
from smarter_adapter.parsers import IrrigapChirpStackParser


class IrrigapChirpStackPluginTests(unittest.TestCase):
    def _event(self, *, node_id="3303", f_port=31, device_name="greenstick-3303"):
        ul = f"S|2509170900|I|{node_id}|M1|1261|T1|22.1|C1|640"
        envelope = {
            "data": base64.b64encode(ul.encode()).decode(),
            "time": "2026-09-17T12:00:00Z",
            "fPort": f_port,
            "deviceInfo": {"deviceName": device_name},
        }
        return RawEvent(
            source="mqtt:irrigap",
            topic="application/bf9286b1-b02c-4e86-976f-f7d66b75aeb7/device/3303/event/up",
            payload=json.dumps(envelope).encode(),
        )

    def test_supports_collaborator_chirpstack_envelope(self):
        parser = IrrigapChirpStackParser()
        self.assertTrue(parser.supports(self._event()))

    def test_greenstick_payload_is_normalized_with_site_metadata(self):
        parser = IrrigapChirpStackParser()
        parsed = parser.parse(self._event())
        self.assertIsNotNone(parsed)
        assert parsed is not None

        self.assertEqual(parsed.external_device_id, "greenstick-3303")
        self.assertEqual(parsed.metadata["node_id"], "3303")
        self.assertEqual(parsed.metadata["location"], "Sector_3")
        self.assertEqual(parsed.metadata["sub_location"], "mz_1")
        self.assertEqual(parsed.metadata["depth"], "15cm")
        self.assertEqual(parsed.metadata["f_port"], 31)
        self.assertEqual(
            parsed.metadata["application_id"],
            "bf9286b1-b02c-4e86-976f-f7d66b75aeb7",
        )

        values = {measurement.name: measurement for measurement in parsed.measurements}
        self.assertEqual(values["soil.raw.moisture_m1"].value, 1261.0)
        self.assertEqual(values["soil.temperature"].value, 22.1)
        self.assertEqual(values["soil.electrical_conductivity"].value, 640.0)
        self.assertGreater(values["soil.moisture"].value, 0.0)
        self.assertLess(values["soil.moisture"].value, 100.0)

    def test_teros12_catalog_and_depth_are_supported(self):
        parser = IrrigapChirpStackParser()
        parsed = parser.parse(
            self._event(node_id="2303", f_port=31, device_name="teros12-2303")
        )
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.metadata["sensor"], "teros12")
        self.assertEqual(parsed.metadata["location"], "Sector_3")
        self.assertEqual(parsed.metadata["depth"], "15cm")

    def test_unknown_node_still_parses_without_site_metadata(self):
        parser = IrrigapChirpStackParser()
        parsed = parser.parse(
            self._event(node_id="3999", f_port=31, device_name="unknown-3999")
        )
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.metadata["sensor"], "irrigap")
        self.assertNotIn("location", parsed.metadata)
        self.assertNotIn("depth", parsed.metadata)


if __name__ == "__main__":
    unittest.main()
