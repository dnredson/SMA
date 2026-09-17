from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from urllib import error, request

from smarter_adapter.device_lifecycle import (
    LifecycleBindingSQLiteManagementStore,
    LifecycleIrrigapCatalogManager,
)
from smarter_adapter.irrigap_config import load_irrigap_catalog
from smarter_adapter.lifecycle_management import start_lifecycle_management_server
from smarter_adapter.lifecycle_service import LifecycleServiceStats
from smarter_adapter.presence import DevicePresencePolicy


def _http(method: str, url: str, token: str = "", json_body=None):
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    data = None
    if json_body is not None:
        data = json.dumps(json_body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = request.Request(url, headers=headers, data=data, method=method)
    with request.urlopen(req, timeout=3) as response:
        raw = response.read() or b"{}"
        return response.status, json.loads(raw.decode("utf-8"))


class _Runtime:
    def __init__(self):
        self.base = SimpleNamespace(
            workspace=SimpleNamespace(id="ws-1"),
            channel=SimpleNamespace(id="ch-1"),
        )
        self.device_type = SimpleNamespace(id="profile-1", version_id="version-1")
        self.persistence_rule = SimpleNamespace(id="rule-1")
        self.device_cache_size = 0


class _Service:
    def __init__(self):
        self.stats = LifecycleServiceStats(received=0, processed=0, failed=0)
        self.input_configs = (
            SimpleNamespace(host="broker", port=1883, topic="#", source="mqtt:test"),
        )
        self.inputs = (SimpleNamespace(connected=True, last_error=None),)


class _Controller:
    def __init__(self, catalog):
        self.catalog = catalog
        self.calls = []

    def decommission(self, node_id, *, reason=""):
        self.calls.append(("decommission", node_id, reason))
        return {
            "status": "decommissioned",
            "node_id": node_id,
            "atom_policies_revoked": 1,
            "item": self.catalog.public_item(node_id),
        }

    def reactivate(self, node_id):
        self.calls.append(("reactivate", node_id, ""))
        return {
            "status": "reactivated",
            "node_id": node_id,
            "atom_publish_policy_created": True,
            "item": self.catalog.public_item(node_id),
        }


class LifecycleManagementAPITests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        path = root / "nodes.json"
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
        self.store = LifecycleBindingSQLiteManagementStore(root / "state.sqlite3")
        self.catalog = LifecycleIrrigapCatalogManager(
            load_irrigap_catalog(file_path=str(path)),
            file_path=str(path),
        )
        self.catalog.set_observation_resolver(
            lambda node_id: self.store.find_latest_catalog_observation_by_node(
                "ws-1", "ch-1", node_id
            )
        )
        self.catalog.set_lifecycle_resolver(
            lambda node_id: self.store.get_node_lifecycle("ws-1", "ch-1", node_id)
        )
        self.controller = _Controller(self.catalog)
        self.server = start_lifecycle_management_server(
            host="127.0.0.1",
            port=0,
            service=_Service(),
            runtime=_Runtime(),
            store=self.store,
            lifecycle_controller=self.controller,
            presence_policy=DevicePresencePolicy(),
            catalog_manager=self.catalog,
            api_token="secret",
        )
        host, port = self.server.server_address[:2]
        self.base = f"http://{host}:{port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.store.close()
        self.tmp.cleanup()

    def test_lifecycle_actions_require_token_and_forward_reason(self):
        with self.assertRaises(error.HTTPError) as ctx:
            _http(
                "POST",
                self.base + "/api/v2/catalog/devices/2313/decommission",
                json_body={"reason": "maintenance"},
            )
        self.assertEqual(ctx.exception.code, 401)

        status, body = _http(
            "POST",
            self.base + "/api/v2/catalog/devices/2313/decommission",
            token="secret",
            json_body={"reason": "maintenance"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "decommissioned")
        self.assertEqual(
            self.controller.calls[-1],
            ("decommission", "2313", "maintenance"),
        )

        status, body = _http(
            "POST",
            self.base + "/api/v2/catalog/devices/2313/reactivate",
            token="secret",
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "reactivated")
        self.assertEqual(self.controller.calls[-1], ("reactivate", "2313", ""))

    def test_catalog_single_node_get_uses_enriched_lifecycle_view(self):
        status, body = _http(
            "GET",
            self.base + "/api/v2/catalog/devices/2313",
            token="secret",
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["id"], "2313")
        self.assertEqual(body["lifecycle_state"], "planned")
        self.assertEqual(body["administrative_state"], "active")
        self.assertFalse(body["decommissioned"])

    def test_observed_catalog_node_cannot_be_deleted_destructively(self):
        self.store.observe_catalog_node(
            "ws-1",
            "ch-1",
            "teros12-sector1.3",
            node_id="2313",
            sensor="teros12",
            observed_at=100.0,
        )
        with self.assertRaises(error.HTTPError) as ctx:
            _http(
                "DELETE",
                self.base + "/api/v2/catalog/devices/2313",
                token="secret",
            )
        self.assertEqual(ctx.exception.code, 409)
        self.assertIsNotNone(self.catalog.get_node("2313"))


if __name__ == "__main__":
    unittest.main()
