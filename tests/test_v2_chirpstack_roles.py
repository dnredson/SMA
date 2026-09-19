from __future__ import annotations

import base64
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from smarter_adapter.models import RawEvent
from smarter_adapter.parsers import IrrigapChirpStackParser, IrrigapNode


def _raw(
    text: str,
    *,
    device: str,
    f_port: int,
    gateway_id: str = "",
    with_tx_info: bool = False,
) -> RawEvent:
    envelope = {
        "time": "2026-09-18T01:33:08+00:00",
        "fPort": f_port,
        "deviceInfo": {
            "deviceName": device,
            "applicationId": "app-1",
        },
        "data": base64.b64encode(text.encode("utf-8")).decode("ascii"),
    }
    if gateway_id:
        envelope["rxInfo"] = [
            {
                "gatewayId": gateway_id,
                "rssi": -42,
                "snr": 12.5,
                "channel": 6,
                "rfChain": 1,
                "crcStatus": "CRC_OK",
            }
        ]
    if with_tx_info:
        envelope["txInfo"] = {
            "frequency": 903300000,
            "modulation": {
                "lora": {
                    "bandwidth": 125000,
                    "codeRate": "CR_4_5",
                    "spreadingFactor": 10,
                }
            },
        }
    return RawEvent(
        source="mqtt:test",
        topic="application/app-1/device/eui/event/up",
        payload=json.dumps(envelope).encode("utf-8"),
        received_at=1.0,
    )


class ChirpStackRoleParsingTests(unittest.TestCase):
    def setUp(self):
        self.teros = IrrigapNode(
            id="2313",
            device="teros12",
            location="Test_3",
            sub_location="mz_1",
            depths={31: "15cm"},
        )
        self.greenstick = IrrigapNode(
            id="3303",
            device="greenstick",
            location="Sector_3",
            sub_location="mz_1",
            depths={31: "15cm", 32: "35cm", 33: "55cm"},
        )
        self.parser = IrrigapChirpStackParser(nodes=(self.teros, self.greenstick))

    def test_battery_frame_is_not_positionally_misread_as_soil(self):
        event = self.parser.parse(
            _raw(
                "S|2609180100|I|2313|VB|4.2|BT|100",
                device="teros12-sector1.3",
                f_port=1,
            )
        )
        self.assertIsNotNone(event)
        assert event is not None
        values = {item.name: item.value for item in event.measurements}
        self.assertEqual(values, {"battery.voltage": 4.2, "battery.level": 100.0})
        self.assertEqual(event.metadata["message_role"], "battery")
        self.assertEqual(event.metadata["port_role"], "battery")
        self.assertEqual(event.metadata["transport"]["f_port"], 1)
        self.assertNotIn("soil.moisture", values)
        self.assertNotIn("soil.temperature", values)

    def test_teros_soil_preserves_raw_without_greenstick_calibration(self):
        event = self.parser.parse(
            _raw(
                "S|2609180110|I|2313|M|2342.7|T|18.2|C|65",
                device="teros12-sector1.3",
                f_port=31,
            )
        )
        self.assertIsNotNone(event)
        assert event is not None
        values = {item.name: item.value for item in event.measurements}
        self.assertEqual(values["soil.raw.moisture_m1"], 2342.7)
        self.assertEqual(values["soil.temperature"], 18.2)
        self.assertEqual(values["soil.electrical_conductivity"], 65.0)
        self.assertNotIn("soil.moisture", values)
        self.assertEqual(event.metadata["message_role"], "soil")
        self.assertEqual(event.metadata["port_role"], "soil")
        self.assertEqual(event.metadata["depth"], "15cm")

    def test_greenstick_keeps_calibrated_moisture(self):
        event = self.parser.parse(
            _raw(
                "S|2509170900|I|3303|M1|1261|T1|22.1|C1|640",
                device="TEST-GREENSTICK-3303",
                f_port=31,
            )
        )
        self.assertIsNotNone(event)
        assert event is not None
        values = {item.name: item.value for item in event.measurements}
        self.assertAlmostEqual(values["soil.moisture"], 45.3429637)
        self.assertEqual(values["soil.raw.moisture_m1"], 1261.0)

    def test_topic_and_gateway_reception_are_preserved(self):
        event = self.parser.parse(
            _raw(
                "S|2609180110|I|2313|M|2342.7|T|18.2|C|65",
                device="teros12-sector1.3",
                f_port=31,
                gateway_id="000000ffff001002",
            )
        )
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(
            event.metadata["transport"]["mqtt_topic"],
            "application/app-1/device/eui/event/up",
        )
        self.assertEqual(event.metadata["gateway_rx"][0]["gateway_id"], "000000ffff001002")
        self.assertEqual(event.metadata["gateway_rx"][0]["rssi"], -42)
        self.assertEqual(event.metadata["gateway_rx"][0]["snr"], 12.5)

    def test_lora_tx_info_is_normalized_separately_from_sensor_semantics(self):
        event = self.parser.parse(
            _raw(
                "S|2609180110|I|2313|M|2342.7|T|18.2|C|65",
                device="teros12-sector1.3",
                f_port=31,
                gateway_id="000000ffff001002",
                with_tx_info=True,
            )
        )
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(
            event.metadata["rf_tx"],
            {
                "frequency_hz": 903300000,
                "modulation": "lora",
                "spreading_factor": 10,
                "bandwidth_hz": 125000,
                "code_rate": "CR_4_5",
            },
        )
        self.assertEqual(event.metadata["gateway_rx"][0]["rssi"], -42)


if __name__ == "__main__":
    unittest.main()
