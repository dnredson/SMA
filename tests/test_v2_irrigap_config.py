from __future__ import annotations

import base64
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from smarter_adapter.irrigap_config import (
    IrrigapCatalogConflict,
    IrrigapCatalogManager,
    IrrigapCatalogReadOnly,
    load_irrigap_catalog,
    parse_irrigap_catalog_json,
)
from smarter_adapter.models import RawEvent
from smarter_adapter.parsers import IrrigapChirpStackParser


class IrrigapCatalogTests(unittest.TestCase):
    @staticmethod
    def _write_seed(path: Path) -> None:
        path.write_text(
            json.dumps(
                {
                    "nodes": [
                        {
                            "id": "2313",
                            "device": "teros12",
                            "location": "Test_3",
                            "sub_location": "mz_1",
                            "depths": {"31": "15cm"},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )

    @staticmethod
    def _event(node_id: str, *, device_name: str) -> RawEvent:
        raw_sensor = f"S|2609171340|I|{node_id}|M|2355.1|T|25.3|C|67"
        envelope = {
            "data": base64.b64encode(raw_sensor.encode()).decode(),
            "time": "2026-09-17T16:30:00Z",
            "fPort": 31,
            "deviceInfo": {"deviceName": device_name},
        }
        return RawEvent(
            source="mqtt:test",
            topic="application/app-1/device/dev-1/event/up",
            payload=json.dumps(envelope).encode(),
        )

    def test_inline_catalog_normalizes_ids_and_depth_ports(self):
        catalog = parse_irrigap_catalog_json(
            json.dumps(
                {
                    "nodes": [
                        {
                            "id": "2313",
                            "device": "teros12",
                            "location": "Test_3",
                            "sub_location": "mz_1",
                            "depths": {"31": "15cm"},
                        }
                    ]
                }
            ),
            source="test",
        )
        self.assertEqual(catalog.source, "test")
        self.assertEqual(len(catalog.nodes), 1)
        self.assertEqual(catalog.nodes[0].id, "2313")
        self.assertEqual(catalog.nodes[0].depths, {31: "15cm"})

    def test_catalog_can_be_loaded_from_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "nodes.json"
            path.write_text(
                json.dumps(
                    [
                        {
                            "id": "3303",
                            "device": "greenstick",
                            "location": "Sector_3",
                            "sub_location": "mz_1",
                            "depths": {"31": "15cm", "32": "35cm"},
                        }
                    ]
                ),
                encoding="utf-8",
            )
            catalog = load_irrigap_catalog(file_path=str(path))
            self.assertEqual(len(catalog.nodes), 1)
            self.assertTrue(catalog.source.startswith("file:"))
            self.assertEqual(catalog.nodes[0].depths[32], "35cm")

    def test_duplicate_node_ids_are_rejected_case_insensitively(self):
        with self.assertRaisesRegex(RuntimeError, "duplicate node id"):
            parse_irrigap_catalog_json(
                json.dumps(
                    {
                        "nodes": [
                            {"id": "ab12", "device": "x"},
                            {"id": "AB12", "device": "y"},
                        ]
                    }
                )
            )

    def test_file_and_inline_sources_are_mutually_exclusive(self):
        with self.assertRaisesRegex(RuntimeError, "set only one"):
            load_irrigap_catalog(
                file_path="/tmp/does-not-matter.json",
                inline_json='{"nodes":[{"id":"1","device":"x"}]}',
            )

    def test_file_backed_manager_crud_is_persistent(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "nodes.json"
            self._write_seed(path)
            manager = IrrigapCatalogManager(
                load_irrigap_catalog(file_path=str(path)),
                file_path=str(path),
            )

            created = manager.create_node(
                {
                    "id": "2314",
                    "device": "teros12",
                    "location": "Sector_8",
                    "sub_location": "mz_2",
                    "depths": {"31": "15cm"},
                }
            )
            self.assertEqual(created.id, "2314")
            self.assertEqual(len(load_irrigap_catalog(file_path=str(path)).nodes), 2)

            updated = manager.replace_node(
                "2314",
                {
                    "device": "teros12",
                    "location": "Sector_8B",
                    "sub_location": "mz_2",
                    "depths": {"31": "20cm"},
                },
            )
            self.assertEqual(updated.location, "Sector_8B")
            reloaded = load_irrigap_catalog(file_path=str(path))
            by_id = {node.id: node for node in reloaded.nodes}
            self.assertEqual(by_id["2314"].depths[31], "20cm")

            deleted = manager.delete_node("2314")
            self.assertEqual(deleted.id, "2314")
            self.assertEqual(len(load_irrigap_catalog(file_path=str(path)).nodes), 1)

    def test_inline_manager_is_read_only(self):
        catalog = parse_irrigap_catalog_json(
            '{"nodes":[{"id":"2313","device":"teros12"}]}',
            source="env:SMA_IRRIGAP_NODES_JSON",
        )
        manager = IrrigapCatalogManager(catalog)
        self.assertFalse(manager.writable)
        with self.assertRaises(IrrigapCatalogReadOnly):
            manager.create_node({"id": "2314", "device": "teros12"})

    def test_duplicate_create_is_rejected_without_changing_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "nodes.json"
            self._write_seed(path)
            before = path.read_text(encoding="utf-8")
            manager = IrrigapCatalogManager(
                load_irrigap_catalog(file_path=str(path)),
                file_path=str(path),
            )
            with self.assertRaises(IrrigapCatalogConflict):
                manager.create_node({"id": "2313", "device": "teros12"})
            self.assertEqual(path.read_text(encoding="utf-8"), before)

    def test_live_parser_resolver_sees_new_node_without_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "nodes.json"
            self._write_seed(path)
            manager = IrrigapCatalogManager(
                load_irrigap_catalog(file_path=str(path)),
                file_path=str(path),
            )
            parser = IrrigapChirpStackParser(node_resolver=manager.get_node)

            before = parser.parse(self._event("2314", device_name="teros12-sector8"))
            self.assertIsNotNone(before)
            assert before is not None
            self.assertEqual(before.metadata["sensor"], "irrigap")
            self.assertNotIn("location", before.metadata)

            manager.create_node(
                {
                    "id": "2314",
                    "device": "teros12",
                    "location": "Sector_8",
                    "sub_location": "mz_2",
                    "depths": {"31": "15cm"},
                }
            )

            after = parser.parse(self._event("2314", device_name="teros12-sector8"))
            self.assertIsNotNone(after)
            assert after is not None
            self.assertEqual(after.metadata["sensor"], "teros12")
            self.assertEqual(after.metadata["location"], "Sector_8")
            self.assertEqual(after.metadata["sub_location"], "mz_2")
            self.assertEqual(after.metadata["depth"], "15cm")


if __name__ == "__main__":
    unittest.main()
