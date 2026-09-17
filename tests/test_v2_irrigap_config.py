from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from smarter_adapter.irrigap_config import (
    load_irrigap_catalog,
    parse_irrigap_catalog_json,
)


class IrrigapCatalogTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
