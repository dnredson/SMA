from __future__ import annotations

import struct
import tempfile
import unittest
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from smarter_adapter.gateway_stats import (
    ChirpStackGatewayStatsDecoder,
    GatewayStatsTopologySQLiteManagementStore,
)


def _varint(value: int) -> bytes:
    result = bytearray()
    number = int(value)
    while True:
        byte = number & 0x7F
        number >>= 7
        if number:
            result.append(byte | 0x80)
        else:
            result.append(byte)
            return bytes(result)


def _field_varint(field: int, value: int) -> bytes:
    return _varint((field << 3) | 0) + _varint(value)


def _field_bytes(field: int, value: bytes) -> bytes:
    return _varint((field << 3) | 2) + _varint(len(value)) + value


def _field_fixed64(field: int, value: float) -> bytes:
    return _varint((field << 3) | 1) + struct.pack("<d", value)


def _map_ss(key: str, value: str) -> bytes:
    body = _field_bytes(1, key.encode()) + _field_bytes(2, value.encode())
    return _field_bytes(10, body)


def _gateway_stats_payload() -> bytes:
    timestamp = _field_varint(1, 1789773984) + _field_varint(2, 250_000_000)
    location = (
        _field_fixed64(1, -22.81)
        + _field_fixed64(2, -47.06)
        + _field_fixed64(3, 650.5)
    )
    return b"".join(
        [
            _field_bytes(17, b"000000ffff001002"),
            _field_bytes(2, timestamp),
            _field_bytes(3, location),
            _field_bytes(4, b"cfg-42"),
            _field_varint(5, 1000),
            _field_varint(6, 995),
            _field_varint(7, 100),
            _field_varint(8, 98),
            _map_ss("model", "seeed_wm1302"),
            _map_ss("mqtt_forwarder_version", "4.6.1"),
            _map_ss("concentrator_temp", "43.375"),
        ]
    )


class GatewayStatsDecoderTests(unittest.TestCase):
    def test_decode_gateway_stats_protobuf(self):
        stats = ChirpStackGatewayStatsDecoder.decode(_gateway_stats_payload())
        self.assertEqual(stats["gateway_id"], "000000ffff001002")
        self.assertAlmostEqual(stats["gateway_time"], 1789773984.25)
        self.assertEqual(stats["config_version"], "cfg-42")
        self.assertEqual(stats["counters"]["rx_packets_received"], 1000)
        self.assertEqual(stats["counters"]["rx_packets_received_ok"], 995)
        self.assertEqual(stats["counters"]["tx_packets_received"], 100)
        self.assertEqual(stats["counters"]["tx_packets_emitted"], 98)
        self.assertEqual(stats["metadata"]["model"], "seeed_wm1302")
        self.assertEqual(stats["health"]["model"], "seeed_wm1302")
        self.assertEqual(stats["health"]["forwarder_version"], "4.6.1")
        self.assertAlmostEqual(stats["health"]["concentrator_temperature_c"], 43.375)
        self.assertAlmostEqual(stats["location"]["latitude"], -22.81)
        self.assertAlmostEqual(stats["location"]["longitude"], -47.06)

    def test_decode_gateway_stats_json(self):
        payload = (
            b'{"gatewayId":"abc","rxPacketsReceived":4,'
            b'"rxPacketsReceivedOk":3,"metadata":{"model":"gw-x"}}'
        )
        stats = ChirpStackGatewayStatsDecoder.decode(payload)
        self.assertEqual(stats["gateway_id"], "abc")
        self.assertEqual(stats["counters"]["rx_packets_received"], 4)
        self.assertEqual(stats["health"]["model"], "gw-x")

    def test_stats_store_enriches_gateway_without_affecting_presence(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = GatewayStatsTopologySQLiteManagementStore(
                Path(tmp) / "state.sqlite3"
            )
            try:
                store.record_gateway_event(
                    "ws",
                    "000000ffff001002",
                    event_kind="stats",
                    received_at=1000,
                    topic="au915_1/gateway/000000ffff001002/event/stats",
                    topic_root="au915_1",
                )
                stats = ChirpStackGatewayStatsDecoder.decode(_gateway_stats_payload())
                store.record_gateway_stats(
                    "ws",
                    "000000ffff001002",
                    stats=stats,
                    decoder=ChirpStackGatewayStatsDecoder.name,
                    observed_at=1000,
                )
                item = store.find_gateway("ws", "000000ffff001002")
                self.assertIsNotNone(item)
                assert item is not None
                self.assertEqual(item["last_stats_at"], 1000.0)
                self.assertEqual(item["stats"]["health"]["model"], "seeed_wm1302")
                self.assertEqual(
                    item["stats_decoder"],
                    "chirpstack-gateway-stats-v1",
                )
                self.assertEqual(item["stats_decode_error"], "")
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
