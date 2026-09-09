import json
import sys
import threading
import unittest
from http.client import HTTPConnection
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from core.api import AdapterAPIServer


class _FakeRegistry:
    def __init__(self):
        self.devices = {
            "device-1": {
                "id": "device-1",
                "kind": "device",
                "name": "D1",
                "externalId": "D1",
                "tenantId": "tenant-1",
                "status": "active",
                "attributes": {},
            }
        }

    def list_devices(self, tenant_id=None):
        return list(self.devices.values())

    def get_device(self, device_id):
        return self.devices[device_id]

    def create_device(self, external_id, **kwargs):
        device = {"id": "device-2", "kind": "device", "name": kwargs.get("name") or external_id,
                  "externalId": external_id, "tenantId": "tenant-1", "status": "active",
                  "attributes": kwargs.get("attributes") or {}}
        self.devices[device["id"]] = device
        return device

    def update_device(self, device_id, payload):
        self.devices[device_id].update({key: value for key, value in payload.items() if key in {"name", "status", "attributes"}})
        return self.devices[device_id]

    def delete_device(self, device_id):
        del self.devices[device_id]


class ApiContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = AdapterAPIServer(("127.0.0.1", 0), _FakeRegistry(), "api-test")
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def request(self, method, path, body=None, token="api-test"):
        conn = HTTPConnection("127.0.0.1", self.server.server_port)
        headers = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}
        conn.request(method, path, body=json.dumps(body) if body is not None else None, headers=headers)
        response = conn.getresponse()
        raw = response.read()
        conn.close()
        return response.status, json.loads(raw) if raw else None

    def test_crud_routes_are_exposed_and_protected(self):
        status, _ = self.request("GET", "/api/v1/devices", token="wrong")
        self.assertEqual(status, 401)
        status, payload = self.request("GET", "/api/v1/devices")
        self.assertEqual(status, 200)
        self.assertEqual(payload["items"][0]["id"], "device-1")
        status, payload = self.request("POST", "/api/v1/devices", {"external_id": "D2"})
        self.assertEqual(status, 201)
        self.assertEqual(payload["externalId"], "D2")
        status, payload = self.request("PATCH", "/api/v1/devices/device-2", {"status": "inactive"})
        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "inactive")
        status, _ = self.request("DELETE", "/api/v1/devices/device-2")
        self.assertEqual(status, 204)


if __name__ == "__main__":
    unittest.main()
