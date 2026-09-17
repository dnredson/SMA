from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from urllib import error, request

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from smarter_adapter.management import start_management_server
from smarter_adapter.metrics import render_prometheus_metrics
from smarter_adapter.presence import DevicePresencePolicy
from smarter_adapter.service import ServiceStats
from smarter_adapter.storage import SQLiteManagementStore


class MetricsTests(unittest.TestCase):
    def test_renderer_exports_counters_gauges_and_escaped_input_labels(self):
        text = render_prometheus_metrics(
            {
                "service": {
                    "received": 10,
                    "processed": 8,
                    "failed": 2,
                    "queued": 1,
                    "retried": 3,
                    "recovered": 1,
                    "dead_lettered": 1,
                },
                "queues": {"retry": 2, "dlq": 1},
                "devices": {"online": 2, "stale": 1, "offline": 0},
                "quality": {"valid": 1, "degraded": 0, "invalid": 1, "unknown": 1},
                "runtime": {"device_cache_size": 2, "reader_configured": True},
                "inputs": [
                    {
                        "source": 'mqtt:test"quoted',
                        "host": "broker\\lab",
                        "port": 1883,
                        "connected": True,
                    }
                ],
            },
            ready=True,
        )
        self.assertIn("sma_ready 1", text)
        self.assertIn("sma_events_received_total 10", text)
        self.assertIn('sma_devices{status="stale"} 1', text)
        self.assertIn('sma_device_quality{status="invalid"} 1', text)
        self.assertIn('source="mqtt:test\\"quoted"', text)
        self.assertIn('host="broker\\\\lab"', text)

    def test_metrics_endpoint_requires_admin_token_and_exports_live_snapshot(self):
        tmp = tempfile.TemporaryDirectory()
        store = SQLiteManagementStore(Path(tmp.name) / "state.sqlite3")
        runtime = SimpleNamespace(
            base=SimpleNamespace(
                workspace=SimpleNamespace(id="ws-1"),
                channel=SimpleNamespace(id="ch-1"),
            ),
            device_type=SimpleNamespace(id="profile-1", version_id="version-1"),
            persistence_rule=SimpleNamespace(id="rule-1"),
            device_cache_size=4,
        )
        service = SimpleNamespace(
            stats=ServiceStats(received=7, processed=6, failed=1),
            input_configs=(
                SimpleNamespace(host="broker", port=1883, topic="#", source="mqtt:test"),
            ),
            inputs=(SimpleNamespace(connected=True, last_error=None),),
        )
        server = start_management_server(
            host="127.0.0.1",
            port=0,
            service=service,
            runtime=runtime,
            store=store,
            reader=object(),
            presence_policy=DevicePresencePolicy(
                stale_after_seconds=60,
                offline_after_seconds=120,
            ),
            api_token="secret",
        )
        host, port = server.server_address[:2]
        url = f"http://{host}:{port}/metrics"
        try:
            with self.assertRaises(error.HTTPError) as ctx:
                request.urlopen(url, timeout=3)
            self.assertEqual(ctx.exception.code, 401)

            req = request.Request(
                url,
                headers={"Authorization": "Bearer secret"},
            )
            with request.urlopen(req, timeout=3) as response:
                body = response.read().decode("utf-8")
                self.assertEqual(response.status, 200)
                self.assertTrue(
                    response.headers.get("Content-Type", "").startswith("text/plain")
                )
            self.assertIn("sma_up 1", body)
            self.assertIn("sma_ready 1", body)
            self.assertIn("sma_events_received_total 7", body)
            self.assertIn("sma_device_cache_size 4", body)
            self.assertIn('sma_input_connected{source="mqtt:test",host="broker",port="1883"} 1', body)
        finally:
            server.shutdown()
            server.server_close()
            store.close()
            tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
