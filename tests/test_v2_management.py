from __future__ import annotations

import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from urllib import error, request

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from smarter_adapter.irrigap_config import IrrigapCatalogManager, load_irrigap_catalog
from smarter_adapter.magistrala.control_plane import DeviceRef
from smarter_adapter.magistrala.reader import MessagesPage
from smarter_adapter.management import start_management_server
from smarter_adapter.models import RawEvent
from smarter_adapter.plugins import ParserNotFound
from smarter_adapter.presence import DevicePresencePolicy
from smarter_adapter.service import ServiceStats
from smarter_adapter.storage import SQLiteManagementStore


class _Runtime:
    def __init__(self):
        self.base = SimpleNamespace(
            workspace=SimpleNamespace(id="ws-1"),
            channel=SimpleNamespace(id="ch-1"),
        )
        self.device_type = SimpleNamespace(id="profile-1", version_id="version-1")
        self.persistence_rule = SimpleNamespace(id="rule-1")
        self.device_cache_size = 2
        self.bootstrap_calls = 0
        self.bootstrap_force = None
        self.clear_calls = 0

    def bootstrap(self, *, force=False):
        self.bootstrap_calls += 1
        self.bootstrap_force = force

    def clear_device_cache(self):
        self.clear_calls += 1
        return 2


class _Service:
    def __init__(self):
        self.stats = ServiceStats(received=3, processed=2, failed=1, dead_lettered=1)
        self.input_configs = (
            SimpleNamespace(host="broker", port=1883, topic="application/#", source="mqtt:test"),
        )
        self.inputs = (SimpleNamespace(connected=True, last_error=None),)


class _Reader:
    def __init__(self):
        self.calls = []

    def list_device_messages(self, workspace_id, channel_id, device_id, **kwargs):
        self.calls.append((workspace_id, channel_id, device_id, kwargs))
        return MessagesPage(
            offset=int(kwargs.get("offset", 0)),
            limit=int(kwargs.get("limit", 100)),
            total=1,
            messages=(
                {
                    "device_id": device_id,
                    "publisher": "atom-device-1",
                    "name": f"{device_id}:soil.temperature",
                    "unit": "Cel",
                    "value": 25.3,
                },
            ),
            order=str(kwargs.get("order", "time")),
            direction=str(kwargs.get("direction", "desc")),
        )


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


class ManagementAPITests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        tmp_path = Path(self.tmp.name)
        self.store = SQLiteManagementStore(tmp_path / "state.sqlite3")
        self.catalog_path = tmp_path / "irrigap.nodes.json"
        self.catalog_path.write_text(
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
        self.catalog = IrrigapCatalogManager(
            load_irrigap_catalog(file_path=str(self.catalog_path)),
            file_path=str(self.catalog_path),
        )
        self.runtime = _Runtime()
        self.service = _Service()
        self.reader = _Reader()
        self.server = start_management_server(
            host="127.0.0.1",
            port=0,
            service=self.service,
            runtime=self.runtime,
            store=self.store,
            reader=self.reader,
            presence_policy=DevicePresencePolicy(
                stale_after_seconds=60,
                offline_after_seconds=120,
            ),
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

    def test_health_and_ready_are_probe_friendly(self):
        status, body = _http("GET", self.base + "/health")
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "ok")

        status, body = _http("GET", self.base + "/ready")
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "ready")
        self.assertTrue(body["inputs_ready"])
        self.assertTrue(body["reader_configured"])

    def test_admin_routes_require_token(self):
        with self.assertRaises(error.HTTPError) as ctx:
            _http("GET", self.base + "/api/v2/status")
        self.assertEqual(ctx.exception.code, 401)

        status, body = _http("GET", self.base + "/api/v2/status", token="secret")
        self.assertEqual(status, 200)
        self.assertEqual(body["queues"], {"retry": 0, "dlq": 0})
        self.assertEqual(body["runtime"]["workspace_id"], "ws-1")
        self.assertEqual(body["service"]["received"], 3)
        self.assertTrue(body["runtime"]["reader_configured"])
        self.assertEqual(body["catalog"]["total"], 1)
        self.assertTrue(body["catalog"]["writable"])
        self.assertEqual(
            body["devices"],
            {
                "total": 0,
                "online": 0,
                "stale": 0,
                "offline": 0,
                "stale_after_seconds": 60,
                "offline_after_seconds": 120,
            },
        )

    def test_catalog_crud_is_exposed_and_persisted(self):
        status, body = _http(
            "GET",
            self.base + "/api/v2/catalog/devices",
            token="secret",
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["total"], 1)
        self.assertTrue(body["writable"])

        status, created = _http(
            "POST",
            self.base + "/api/v2/catalog/devices",
            token="secret",
            json_body={
                "id": "2314",
                "device": "teros12",
                "location": "Sector_8",
                "sub_location": "mz_2",
                "depths": {"31": "15cm"},
            },
        )
        self.assertEqual(status, 201)
        self.assertEqual(created["item"]["id"], "2314")
        self.assertEqual(created["catalog_total"], 2)

        status, item = _http(
            "GET",
            self.base + "/api/v2/catalog/devices/2314",
            token="secret",
        )
        self.assertEqual(status, 200)
        self.assertEqual(item["location"], "Sector_8")

        status, updated = _http(
            "PUT",
            self.base + "/api/v2/catalog/devices/2314",
            token="secret",
            json_body={
                "device": "teros12",
                "location": "Sector_8B",
                "sub_location": "mz_2",
                "depths": {"31": "20cm"},
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(updated["item"]["location"], "Sector_8B")
        self.assertEqual(updated["item"]["depths"]["31"], "20cm")

        status, deleted = _http(
            "DELETE",
            self.base + "/api/v2/catalog/devices/2314",
            token="secret",
        )
        self.assertEqual(status, 200)
        self.assertEqual(deleted["status"], "deleted")
        self.assertEqual(deleted["catalog_total"], 1)

        reloaded = load_irrigap_catalog(file_path=str(self.catalog_path))
        self.assertEqual([node.id for node in reloaded.nodes], ["2313"])

    def test_dlq_can_be_requeued_through_http(self):
        raw = RawEvent(
            source="mqtt:test",
            topic="application/test/device/bad/event/up",
            payload=b"bad-payload",
            received_at=time.time(),
        )
        dlq_id = self.store.add_dlq(raw, ParserNotFound("bad payload"), attempts=1)

        status, body = _http("GET", self.base + "/api/v2/dlq", token="secret")
        self.assertEqual(status, 200)
        self.assertEqual(body["total"], 1)
        self.assertEqual(body["items"][0]["id"], dlq_id)
        self.assertEqual(body["items"][0]["raw"]["payload_utf8"], "bad-payload")

        status, body = _http(
            "POST",
            self.base + f"/api/v2/dlq/{dlq_id}/retry",
            token="secret",
        )
        self.assertEqual(status, 202)
        self.assertEqual(body["status"], "queued")
        self.assertEqual(self.store.count_dlq(), 0)
        self.assertEqual(self.store.count_retries(), 1)

        status, body = _http("GET", self.base + "/api/v2/retry", token="secret")
        self.assertEqual(status, 200)
        self.assertEqual(body["total"], 1)
        self.assertEqual(body["items"][0]["attempts"], 0)

    def test_reconcile_forces_control_plane_and_clears_fast_path(self):
        status, body = _http("POST", self.base + "/api/v2/reconcile", token="secret")
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "reconciled")
        self.assertEqual(body["cleared_device_cache"], 2)
        self.assertEqual(self.runtime.bootstrap_calls, 1)
        self.assertTrue(self.runtime.bootstrap_force)
        self.assertEqual(self.runtime.clear_calls, 1)

    def test_managed_device_telemetry_and_presence_are_exposed(self):
        device = DeviceRef(
            id="atom-device-1",
            workspace_id="ws-1",
            external_id="teros12-sector1.1",
            name="teros12-sector1.1",
            profile_id="profile-1",
            profile_version_id="version-1",
        )
        seen_at = time.time() - 90.0
        self.store.upsert_device(device, channel_id="ch-1", seen_at=seen_at)

        status, devices = _http(
            "GET",
            self.base + "/api/v2/devices",
            token="secret",
        )
        self.assertEqual(status, 200)
        self.assertEqual(devices["total"], 1)
        self.assertEqual(devices["items"][0]["operational_status"], "stale")
        self.assertGreaterEqual(devices["items"][0]["last_seen_age_seconds"], 90.0)

        status, body = _http(
            "GET",
            self.base
            + "/api/v2/devices/teros12-sector1.1/messages?limit=20&dir=desc&name=teros12-sector1.1%3Asoil.temperature",
            token="secret",
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["external_id"], "teros12-sector1.1")
        self.assertEqual(body["atom_device_id"], "atom-device-1")
        self.assertEqual(body["operational_status"], "stale")
        self.assertEqual(body["total"], 1)
        self.assertEqual(body["messages"][0]["value"], 25.3)
        self.assertEqual(self.reader.calls[0][0:3], ("ws-1", "ch-1", "teros12-sector1.1"))
        self.assertEqual(self.reader.calls[0][3]["limit"], 20)
        self.assertEqual(
            self.reader.calls[0][3]["name"],
            "teros12-sector1.1:soil.temperature",
        )

        status, summary = _http("GET", self.base + "/api/v2/status", token="secret")
        self.assertEqual(status, 200)
        self.assertEqual(summary["devices"]["total"], 1)
        self.assertEqual(summary["devices"]["stale"], 1)

        with self.assertRaises(error.HTTPError) as ctx:
            _http(
                "GET",
                self.base + "/api/v2/devices/not-managed/messages",
                token="secret",
            )
        self.assertEqual(ctx.exception.code, 404)


if __name__ == "__main__":
    unittest.main()
