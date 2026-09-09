import json
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from core.atom_client import AtomClient, AtomConfig
from core.publisher import HttpPublisher


class _Handler(BaseHTTPRequestHandler):
    requests = []

    def log_message(self, *_args):
        pass

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        self.__class__.requests.append((self.path, dict(self.headers), body))
        if self.path == "/graphql":
            payload = json.loads(body)
            query = payload["query"]
            if "createEntity" in query:
                data = {"createEntity": {"id": "atom-device-1", "kind": "device", "name": "D1", "externalId": "D1", "tenantId": "tenant-1", "status": "active", "attributes": {}}}
            else:
                data = {"entity": {"id": "atom-device-1", "kind": "device", "name": "D1", "externalId": "D1", "tenantId": "tenant-1", "status": "active", "attributes": {}}}
            response = {"data": data}
        else:
            response = {"status": "accepted"}
        raw = json.dumps(response).encode()
        self.send_response(202 if self.path != "/graphql" else 200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


class TransportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _Handler.requests = []
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def test_atom_graphql_uses_bearer_and_atom_entity_fields(self):
        client = AtomClient(AtomConfig(url=self.base, graphql_url=self.base + "/graphql", token="atom_test"))
        device = client.create_device("D1", tenant_id="tenant-1")
        self.assertEqual(device["id"], "atom-device-1")
        path, headers, body = _Handler.requests[-1]
        self.assertEqual(path, "/graphql")
        self.assertEqual(headers["Authorization"], "Bearer atom_test")
        self.assertEqual(json.loads(body)["variables"]["input"]["externalId"], "D1")

    def test_publish_uses_modern_fluxmq_route_and_json_envelope(self):
        publisher = HttpPublisher({"http_adapter_url": self.base})
        ok, error = publisher.publish(
            tenant_id="tenant-1",
            channel_id="channel-1",
            device_id="device-1",
            atom_token="atom_test",
            senml=[{"bn": "D1:", "bt": 10, "n": "air.temperature", "u": "Cel", "v": 25}],
        )
        self.assertTrue(ok, error)
        path, headers, body = _Handler.requests[-1]
        self.assertEqual(path, "/tenant-1/channels/channel-1/messages")
        self.assertEqual(headers["Authorization"], "Bearer atom_test")
        self.assertEqual(headers["Content-Type"], "application/json")
        payload = json.loads(body)
        self.assertEqual(payload["device_id"], "device-1")
        self.assertEqual(payload["payload"][0]["n"], "air.temperature")


if __name__ == "__main__":
    unittest.main()
