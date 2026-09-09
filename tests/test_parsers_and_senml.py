import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from core.senml import build_senml
from parsers import detect_and_parse


class ParserContractTests(unittest.TestCase):
    def test_existing_sensor_families_still_normalize(self):
        samples = [
            ("WXT520_WS_TEST", b"0R0,Dm=12D,Sm=1.2M,Ta=25.0C,Ua=50P,Pa=1013.2H", "wind.speed"),
            ("ATMOS41_WS_TEST", b"0+100+0+1+2+3+180+4+25+1+90+50", "air.temperature"),
            ("TEROS12_S1_TEST", b"0+1200+23.5+1.2", "soil.temperature"),
            ("GREENSTICK_1_TEST", b"S|2509091140|M1|1261|T1|22.1|VB|3.9", "soil.raw.moisture_m1"),
            ("TTN_GREENSTICK", json.dumps({
                "end_device_ids": {"device_id": "greenstick-1-test"},
                "uplink_message": {"decoded_payload": {"ultralight": "S|2509091140|M1|1261"}},
            }).encode(), "soil.raw.moisture_m1"),
        ]
        for topic, payload, expected_name in samples:
            with self.subTest(topic=topic):
                result = detect_and_parse(topic, payload)
                self.assertTrue(result.accept)
                self.assertIn(expected_name, {entry.get("n") for entry in result.entries or []})

    def test_senml_boundary_owns_base_name_and_time(self):
        result = build_senml(
            "ATMOS41_WS_TEST",
            [
                {"n": "air.temperature", "u": "Cel", "v": 25, "bn": "wrong:", "bt": 1},
                {"n": "rel.humidity", "u": "1", "v": 0.5},
            ],
            1725882000,
        )
        self.assertEqual(result[0]["bn"], "ATMOS41_WS_TEST:")
        self.assertEqual(result[0]["bt"], 1725882000)
        self.assertNotIn("bn", result[1])
        self.assertNotIn("bt", result[1])

    def test_empty_measurement_is_valid_base_record(self):
        self.assertEqual(
            build_senml("DEVICE_1", [{"n": "ignored"}], 10),
            [{"bn": "DEVICE_1:", "bt": 10}],
        )


if __name__ == "__main__":
    unittest.main()
