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

    def test_only_validated_sensor_fields_are_emitted(self):
        payload = {
            "date": "09/10/2026",
            "hour": "12:53:04",
            "s1": (
                '"ID":01,"S1":03,"S2":09,"S3":99,"S4":00,"S5":99,"S6":00,'
                '"BAT":87,"T1":-7,"T2":12,"T3":99'
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

        # S and T are forwarded independently (except sentinel 99).
        self.assertEqual(values["sensorboard.s01.id01.s1.raw"], 3)
        self.assertEqual(values["sensorboard.s01.id01.s2.raw"], 9)
        self.assertEqual(values["sensorboard.s01.id01.s4.raw"], 0)
        self.assertEqual(values["sensorboard.s01.id01.s6.raw"], 0)
        self.assertNotIn("sensorboard.s01.id01.s3.raw", values)
        self.assertEqual(values["sensorboard.s01.id01.t1.raw"], -7)
        self.assertEqual(values["sensorboard.s01.id01.t2.raw"], 12)

        # BAT and relay registers remain out of scope.
        self.assertFalse(any("battery" in name for name in values))
        self.assertFalse(any(name.startswith("relayboard.") for name in values))
        self.assertFalse(any(name.startswith("bus.") for name in values))

        expected = datetime(
            2026, 10, 9, 12, 53, 4, tzinfo=ZoneInfo("America/Sao_Paulo")
        ).timestamp()
        self.assertTrue(all(m.timestamp == expected for m in event.measurements))

    def test_all_s_and_t_fields_forwarded_from_real_sample(self):
        payload = {
            "date": "09/10/2026",
            "hour": "15:43:24",
            "s1": '"ID":01,"S1":00,"S2":17,"S3":02,"S4":16,"S5":19,"S6":17,"BAT":99,"T1":-20,"T2":-3,"T3":-18',
            "s2": '"ID":02,"S1":00,"S2":12,"S3":16,"S4":16,"S5":17,"S6":19,"BAT":99,"T1":-20,"T2":99,"T3":99',
            "s6": '"ID":06,"S1":99,"S2":99,"S3":99,"S4":99,"S5":99,"S6":99,"BAT":99,"T1":0,"T2":0,"T3":0',
        }
        parsed = PitayaSaciParser().parse(self._raw(payload))
        self.assertIsNotNone(parsed)
        values = {m.name: m.value for m in parsed.measurements}
        self.assertEqual(len(values), 21)
        self.assertEqual(values["sensorboard.s01.id01.s3.raw"], 2)
        self.assertEqual(values["sensorboard.s01.id01.t1.raw"], -20)
        self.assertEqual(values["sensorboard.s01.id01.t2.raw"], -3)
        self.assertEqual(values["sensorboard.s01.id01.t3.raw"], -18)
        self.assertEqual(values["sensorboard.s02.id02.t1.raw"], -20)
        self.assertEqual(values["sensorboard.s01.id01.s1.raw"], 0)
        self.assertEqual(values["sensorboard.s01.id01.s2.raw"], 17)
        # Do not mistake inactive-board T zeros for confirmed measurements:
        # preserve them as raw and delegate validity to the platform.
        self.assertEqual(values["sensorboard.s06.id06.t1.raw"], 0)
        self.assertEqual(values["sensorboard.s06.id06.t2.raw"], 0)
        self.assertEqual(values["sensorboard.s06.id06.t3.raw"], 0)

    def test_t_is_independent_of_s_even_without_s_readings(self):
        payload = {"s3": '"ID":03,"S1":99,"S2":99,"S3":99,"S4":99,"S5":99,"S6":99,"T1":-18,"T2":99,"T3":0'}
        parsed = PitayaSaciParser().parse(self._raw(payload))
        values = {m.name: m.value for m in parsed.measurements}
        self.assertEqual(values, {
            "sensorboard.s03.id03.t1.raw": -18,
            "sensorboard.s03.id03.t3.raw": 0,
        })

    def test_all_99_sensor_snapshot_emits_no_telemetry(self):
        payload = {
            "date": "09/10/2026",
            "hour": "12:53:04",
            "s1": (
                '"ID":01,"S1":99,"S2":99,"S3":99,"S4":99,"S5":99,"S6":99,'
                '"BAT":99,"T1":99,"T2":99,"T3":99'
            ),
            "rele1": (
                '"ID":51,"R1":0,"R2":0,"R3":0,"R4":0,'
                '"R5":0,"R6":0,"R7":0,"R8":0'
            ),
        }
        event = PitayaSaciParser().parse(self._raw(payload))
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.measurements, ())

    def test_identity_anomalies_stay_in_metadata_only(self):
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
        self.assertEqual(outcome.quality_status, "valid")
        self.assertEqual(outcome.quality_issues, ())

        pitaya = outcome.event.metadata["pitaya"]
        self.assertEqual(pitaya["duplicate_board_ids"], [14])
        self.assertEqual(pitaya["slot_mismatches"], ["s24->ID14"])

        values = {item.name: item.value for item in outcome.event.measurements}
        self.assertEqual(values["sensorboard.s14.id14.s1.raw"], 10)
        self.assertEqual(values["sensorboard.s24.id14.s1.raw"], 11)
        self.assertEqual(values["sensorboard.s14.id14.t1.raw"], -10)
        self.assertEqual(values["sensorboard.s24.id14.t1.raw"], -9)
        self.assertFalse(any(name.startswith("bus.") for name in values))

    def test_malformed_source_time_falls_back_to_receive_time(self):
        payload = {
            "date": "bad",
            "hour": "bad",
            "s1": (
                '"ID":01,"S1":10,"S2":99,"S3":11,"S4":99,"S5":99,"S6":99,'
                '"BAT":80,"T1":-10,"T2":99,"T3":99'
            ),
        }
        event = PitayaSaciParser().parse(self._raw(payload))
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.metadata["timestamp_source"], "received_at")
        self.assertTrue(event.measurements)
        self.assertTrue(all(item.timestamp == 1.0 for item in event.measurements))


if __name__ == "__main__":
    unittest.main()
