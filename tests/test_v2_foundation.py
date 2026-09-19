import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from smarter_adapter.legacy_parser import LegacySensorParser
from smarter_adapter.models import Measurement, ParsedEvent, RawEvent
from smarter_adapter.pipeline import ParsePipeline
from smarter_adapter.plugins import ParserNotFound, ParserRegistry


class _SyntheticParser:
    name = "synthetic"

    def supports(self, event: RawEvent) -> bool:
        return event.topic == "synthetic"

    def parse(self, event: RawEvent):
        if event.payload != b"42":
            return None
        return ParsedEvent(
            external_device_id="SYNTHETIC_1",
            measurements=(Measurement("answer", 42),),
        )


class V2FoundationTests(unittest.TestCase):
    def test_registry_is_deterministic(self):
        registry = ParserRegistry([_SyntheticParser()])
        outcome = ParsePipeline(registry).process(
            RawEvent(source="test", topic="synthetic", payload=b"42")
        )
        self.assertEqual(outcome.parser, "synthetic")
        self.assertEqual(outcome.event.external_device_id, "SYNTHETIC_1")
        self.assertEqual(outcome.event.measurements[0].value, 42)

    def test_unknown_event_is_rejected(self):
        registry = ParserRegistry([_SyntheticParser()])
        with self.assertRaises(ParserNotFound):
            registry.parse(RawEvent(source="test", topic="other", payload=b"?"))

    def test_v1_sensor_parser_is_preserved_through_v2_contract(self):
        registry = ParserRegistry([LegacySensorParser()])
        outcome = ParsePipeline(registry).process(
            RawEvent(
                source="mqtt:testbed",
                topic="ATMOS41_WS_TEST",
                payload=b"0+100+0+1+2+3+180+4+25+1+90+50",
            )
        )
        self.assertEqual(outcome.parser, "legacy-sensor-parsers")
        self.assertEqual(outcome.event.external_device_id, "ATMOS41_WS_TEST")
        names = {measurement.name for measurement in outcome.event.measurements}
        self.assertIn("air.temperature", names)

    def test_duplicate_parser_names_are_rejected(self):
        registry = ParserRegistry([_SyntheticParser()])
        with self.assertRaises(ValueError):
            registry.register(_SyntheticParser())


if __name__ == "__main__":
    unittest.main()
