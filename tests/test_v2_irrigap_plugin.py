import base64
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from smarter_adapter.models import RawEvent
from smarter_adapter.parsers import IrrigapChirpStackParser, IrrigapNode
from smarter_adapter.pipeline import ParsePipeline
from smarter_adapter.plugins import ParserRegistry


TEST_NODES = (
    IrrigapNode("3303", "greenstick", "Sector_3", "mz_1", {31: "15cm", 32: "35cm", 33: "55cm"}),
    IrrigapNode("2303", "teros12", "Sector_3", "mz_1", {31: "15cm"}),
    IrrigapNode("2311", "teros12", "Test_1", "mz_1", {31: "15cm"}),
    IrrigapNode("2313", "teros12", "Test_3", "mz_1", {31: "15cm"}),
)


class IrrigapChirpStackPluginTests(unittest.TestCase):
    @staticmethod
    def _parser():
        return IrrigapChirpStackParser(TEST_NODES)

    def _event(
        self,
        *,
        node_id="3303",
        f_port=31,
        device_name="greenstick-3303",
        moisture=1261,
        temperature=22.1,
        ec=640,
    ):
        ul = f"S|2509170900|I|{node_id}|M1|{moisture}|T1|{temperature}|C1|{ec}"
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
        parser = self._parser()
        self.assertTrue(parser.supports(self._event()))

    def test_greenstick_payload_is_normalized_with_site_metadata(self):
        parser = self._parser()
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
        parser = self._parser()
        parsed = parser.parse(
            self._event(node_id="2303", f_port=31, device_name="teros12-2303")
        )
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.metadata["sensor"], "teros12")
        self.assertEqual(parsed.metadata["location"], "Sector_3")
        self.assertEqual(parsed.metadata["depth"], "15cm")

    def test_unknown_node_still_parses_without_site_metadata(self):
        parser = self._parser()
        parsed = parser.parse(
            self._event(node_id="3999", f_port=31, device_name="unknown-3999")
        )
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.metadata["sensor"], "irrigap")
        self.assertNotIn("location", parsed.metadata)
        self.assertNotIn("depth", parsed.metadata)

    def test_parser_has_no_embedded_deployment_catalog(self):
        parser = IrrigapChirpStackParser()
        parsed = parser.parse(
            self._event(node_id="3303", f_port=31, device_name="greenstick-3303")
        )
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.metadata["sensor"], "irrigap")
        self.assertNotIn("location", parsed.metadata)
        self.assertNotIn("sub_location", parsed.metadata)
        self.assertNotIn("depth", parsed.metadata)

    def test_full_minus_one_sentinel_becomes_quality_diagnostics(self):
        pipeline = ParsePipeline(ParserRegistry([self._parser()]))
        outcome = pipeline.process(
            self._event(
                node_id="2313",
                device_name="teros12-sector1.3",
                moisture=-1,
                temperature=-1,
                ec=-1,
            )
        )
        values = {measurement.name: measurement.value for measurement in outcome.event.measurements}

        self.assertEqual(outcome.quality_status, "invalid")
        self.assertNotIn("soil.moisture", values)
        self.assertNotIn("soil.raw.moisture_m1", values)
        self.assertNotIn("soil.temperature", values)
        self.assertNotIn("soil.electrical_conductivity", values)
        self.assertNotIn("soil.raw.ec_c1", values)
        self.assertEqual(values["sensor.data_quality"], "invalid")
        self.assertEqual(
            values["sensor.invalid_fields"],
            "moisture,temperature,electrical_conductivity",
        )

    def test_minus_one_temperature_alone_is_not_assumed_to_be_sentinel(self):
        pipeline = ParsePipeline(ParserRegistry([self._parser()]))
        outcome = pipeline.process(
            self._event(
                node_id="2311",
                device_name="teros12-cold-test",
                moisture=2355.1,
                temperature=-1,
                ec=67,
            )
        )
        values = {measurement.name: measurement.value for measurement in outcome.event.measurements}

        self.assertEqual(outcome.quality_status, "valid")
        self.assertEqual(values["soil.temperature"], -1.0)
        self.assertNotIn("sensor.data_quality", values)


if __name__ == "__main__":
    unittest.main()
