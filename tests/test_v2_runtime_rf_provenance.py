from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from smarter_adapter.magistrala.control_plane import (
    BaseResources,
    ChannelRef,
    WorkspaceRef,
)
from smarter_adapter.models import Measurement, ParsedEvent, RawEvent
from smarter_adapter.runtime import SmarterAdapterRuntime


class _Store:
    def __init__(self) -> None:
        self.calls = []

    def record_device_gateway(
        self,
        workspace_id,
        channel_id,
        external_id,
        gateway,
        *,
        observed_at=None,
    ):
        self.calls.append(
            {
                "workspace_id": workspace_id,
                "channel_id": channel_id,
                "external_id": external_id,
                "gateway": dict(gateway),
                "observed_at": observed_at,
            }
        )


class RuntimeRFProvenanceTests(unittest.TestCase):
    def test_runtime_enriches_rxinfo_with_frame_provenance(self):
        store = _Store()
        runtime = object.__new__(SmarterAdapterRuntime)
        runtime.state_store = store

        base = BaseResources(
            workspace=WorkspaceRef("ws-1", "Workspace", "workspace"),
            channel=ChannelRef("ch-1", "ws-1", "Telemetry", "telemetry"),
        )
        parsed = ParsedEvent(
            external_device_id="teros12-sector5",
            measurements=(Measurement("soil.temperature", 20.0, "Cel", 1000.0),),
            metadata={
                "f_port": 31,
                "message_role": "soil",
                "topic": "application/app-1/device/dev-eui/event/up",
                "transport": {
                    "type": "chirpstack",
                    "f_port": 31,
                    "mqtt_topic": "application/app-1/device/dev-eui/event/up",
                },
                "rf_tx": {
                    "frequency_hz": 903300000,
                    "modulation": "lora",
                    "spreading_factor": 10,
                    "bandwidth_hz": 125000,
                    "code_rate": "CR_4_5",
                },
                "gateway_rx": [
                    {
                        "gateway_id": "000000ffff001002",
                        "rssi": -21,
                        "snr": 9.5,
                        "channel": 4,
                        "rf_chain": 1,
                        "crc_status": "CRC_OK",
                    }
                ],
            },
        )
        raw = RawEvent(
            source="mqtt:test",
            topic="application/app-1/device/dev-eui/event/up",
            payload=b"payload",
            received_at=1234.5,
        )

        runtime._record_gateway_observations(base, parsed, raw)

        self.assertEqual(len(store.calls), 1)
        call = store.calls[0]
        self.assertEqual(call["workspace_id"], "ws-1")
        self.assertEqual(call["channel_id"], "ch-1")
        self.assertEqual(call["external_id"], "teros12-sector5")
        self.assertEqual(call["observed_at"], 1234.5)
        gateway = call["gateway"]
        self.assertEqual(gateway["gateway_id"], "000000ffff001002")
        self.assertEqual(gateway["rssi"], -21)
        self.assertEqual(gateway["snr"], 9.5)
        self.assertEqual(gateway["f_port"], 31)
        self.assertEqual(gateway["message_role"], "soil")
        self.assertEqual(
            gateway["mqtt_topic"],
            "application/app-1/device/dev-eui/event/up",
        )
        self.assertEqual(gateway["frequency_hz"], 903300000)
        self.assertEqual(gateway["modulation"], "lora")
        self.assertEqual(gateway["spreading_factor"], 10)
        self.assertEqual(gateway["bandwidth_hz"], 125000)
        self.assertEqual(gateway["code_rate"], "CR_4_5")

    def test_runtime_uses_transport_and_raw_topic_fallbacks(self):
        store = _Store()
        runtime = object.__new__(SmarterAdapterRuntime)
        runtime.state_store = store

        base = BaseResources(
            workspace=WorkspaceRef("ws-1", "Workspace", "workspace"),
            channel=ChannelRef("ch-1", "ws-1", "Telemetry", "telemetry"),
        )
        parsed = ParsedEvent(
            external_device_id="device-1",
            measurements=(Measurement("battery.voltage", 4.2, "V", 1000.0),),
            metadata={
                "message_role": "battery",
                "transport": {"type": "chirpstack", "f_port": 1},
                "gateway_rx": [{"gateway_id": "gw-1", "rssi": -70}],
            },
        )
        raw = RawEvent(
            source="mqtt:test",
            topic="application/app/device/eui/event/up",
            payload=b"payload",
            received_at=1001.0,
        )

        runtime._record_gateway_observations(base, parsed, raw)

        gateway = store.calls[0]["gateway"]
        self.assertEqual(gateway["f_port"], 1)
        self.assertEqual(gateway["message_role"], "battery")
        self.assertEqual(gateway["mqtt_topic"], raw.topic)


if __name__ == "__main__":
    unittest.main()
