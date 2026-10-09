import json
import sys
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from smarter_adapter.models import RawEvent
from smarter_adapter.parsers import PitayaSaciParser
from smarter_adapter.pipeline import ParsePipeline
from smarter_adapter.plugins import ParserRegistry


class PitayaSaciParserTests(unittest.TestCase):
    @staticmethod
    def _raw(payload, topic="pitaya/NSAAB_DATA"):
        return RawEvent(
            source="mqtt:pitaya-test",
            topic=topic,
            payload=json.dumps(payload).encode("utf-8"),
            received_at=1.0,
        )

    def test_topic_extracts_location_and_future_farms_are_dynamic(self):
        parser = PitayaSaciParser()
        self.assertTrue(parser.supports(self._raw({}, "pitaya/NSAAB_DATA")))
        self.assertTrue(parser.supports(self._raw({}, "pitaya/LANAPRE_DATA")))
        self.assertFalse(parser.supports(self._raw({}, "ATMOS41_WS_NSAAB")))

    def test_operational_fields_are_normalized_and_99_sentinels_are_omitted(self):
        payload = {
            "date": "09/10/2026",
            "hour": "12:53:04",
            "s1": (
                '"ID":01,"S1":03,"S2":09,"S3":08,"S4":00,"S5":00,"S6":00,'
                '"BAT":99,"T1":-7,"T2":99,"T3":99'
            ),
            "s2": (
                '"ID":02,"S1":99,"S2":99,"S3":99,"S4":99,"S5":99,"S6":99,'
                '"BAT":99,"T1":0,"T2":0,"T3":0'
            ),
            "rele1": (
                '"ID":51,"R1":1,"R2":0,"R3":0,"R4":0,'
                '"R5":0,"R6":0,"R7":0,"R8":0'
            ),
        }
        event = PitayaSaciParser().parse(self._raw(payload))
        self.assertIsNotNone(event)
        assert event is not None

        self.assertEqual(event.external_device_id, "PITAYA_NSAAB")
        self.assertEqual(event.metadata["sensor"], "pitaya")
        self.assertEqual(event.metadata["location"], "NSAAB")
        self.assertEqual(event.metadata["timestamp_source"], "payload")

        values = {item.name: item.value for item in event.measurements}
        self.assertEqual(values["sensorboard.s01.id01.s1.raw"], 3)
        self.assertEqual(values["sensorboard.s01.id01.s2.raw"], 9)
        self.assertEqual(values["sensorboard.s01.id01.battery.level"], 99)
        self.assertEqual(values["sensorboard.s01.id01.soil.tension.t1"], -7)
        self.assertNotIn("sensorboard.s02.id02.s1.raw", values)
        self.assertNotIn("sensorboard.s02.id02.soil.tension.t1", values)
        self.assertIs(values["relayboard.rele1.id51.r1.state"], True)
        self.assertIs(values["relayboard.rele1.id51.r2.state"], False)
        self.assertEqual(values["bus.sensor_boards.reported"], 2)
        self.assertEqual(values["bus.sensor_channels.operational"], 6)
        self.assertEqual(values["bus.sensor_channels.unavailable"], 6)

        expected = datetime(
            2026, 10, 9, 12, 53, 4, tzinfo=ZoneInfo("America/Sao_Paulo")
        ).timestamp()
        first = event.measurements[0]
        self.assertEqual(first.timestamp, expected)

    def test_duplicate_and_slot_mismatch_are_quality_diagnostics(self):
        payload = {
            "date": "09/10/2026",
            "hour": "12:53:04",
            "s14": (
                '"ID":14,"S1":10,"S2":99,"S3":99,"S4":99,"S5":99,"S6":99,'
                '"BAT":80,"T1":-10,"T2":99,"T3":99'
            ),
            "s24": (
                '"ID":14,"S1":11,"S2":99,"S3":99,"S4":99,"S5":99,"S6":99,'
                '"BAT":81,"T1":-9,"T2":99,"T3":99'
            ),
        }
        outcome = ParsePipeline(
            ParserRegistry([PitayaSaciParser()])
        ).process(self._raw(payload))

        self.assertEqual(outcome.parser, "pitaya-saci-v1")
        self.assertEqual(outcome.quality_status, "degraded")
        self.assertTrue(
            any(issue.reason.startswith("duplicate_board_id") for issue in outcome.quality_issues)
        )
        values = {item.name: item.value for item in outcome.event.measurements}
        self.assertEqual(values["bus.identity.duplicate_id_count"], 1)
        self.assertEqual(values["bus.identity.mismatch_count"], 1)
        self.assertEqual(values["bus.identity.duplicate_ids"], "14")
        self.assertEqual(values["bus.identity.slot_mismatches"], "s24->ID14")
        self.assertEqual(values["sensor.data_quality"], "degraded")
        self.assertNotIn("bus.payload_quality_marker", values)

    def test_malformed_source_time_falls_back_to_receive_time(self):
        payload = {
            "date": "bad",
            "hour": "bad",
            "s1": (
                '"ID":01,"S1":99,"S2":99,"S3":99,"S4":99,"S5":99,"S6":99,'
                '"BAT":99,"T1":99,"T2":99,"T3":99'
            ),
        }
        event = PitayaSaciParser().parse(self._raw(payload))
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.metadata["timestamp_source"], "received_at")
        self.assertTrue(all(item.timestamp == 1.0 for item in event.measurements))


if __name__ == "__main__":
    unittest.main()
