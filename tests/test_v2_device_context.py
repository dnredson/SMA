from __future__ import annotations

import json
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from urllib import error, request

from smarter_adapter.device_context import DeviceContextProvider
from smarter_adapter.device_lifecycle import LifecycleBindingSQLiteManagementStore
from smarter_adapter.historical_intelligence import TimescaleHistoryProvider
from smarter_adapter.lifecycle_management import start_lifecycle_management_server
from smarter_adapter.magistrala.control_plane import DeviceRef
from smarter_adapter.magistrala.reader import MessagesPage, ReaderError
from smarter_adapter.presence import DevicePresencePolicy


class _Reader:
    def __init__(self, messages=(), *, error_value=None):
        self.messages = tuple(messages)
        self.error_value = error_value
        self.calls = []

    def list_device_messages(self, workspace_id, channel_id, device_id, **kwargs):
        self.calls.append((workspace_id, channel_id, device_id, kwargs))
        if self.error_value is not None:
            raise self.error_value
        return MessagesPage(
            offset=0,
            limit=int(kwargs.get("limit", 120)),
            total=len(self.messages),
            messages=self.messages,
            direction="desc",
        )


class _Runtime:
    def __init__(self):
        self.base = SimpleNamespace(
            workspace=SimpleNamespace(id="ws-1"),
            channel=SimpleNamespace(id="ch-1"),
        )
        self.device_type = SimpleNamespace(id="profile-generic", version_id="version-generic")
        self.persistence_rule = SimpleNamespace(id="rule-1")
        self.device_cache_size = 0


class _Service:
    def __init__(self):
        self.stats = SimpleNamespace()
        self.input_configs = ()
        self.inputs = ()


def _http(url: str, token: str = ""):
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    req = request.Request(url, headers=headers, method="GET")
    with request.urlopen(req, timeout=3) as response:
        return response.status, json.loads((response.read() or b"{}").decode("utf-8"))


class DeviceContextTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = LifecycleBindingSQLiteManagementStore(
            Path(self.tmp.name) / "state.sqlite3"
        )
        self.external_id = "teros12-sector1.3"
        self.device = DeviceRef(
            id="atom-device-1",
            workspace_id="ws-1",
            external_id=self.external_id,
            name=self.external_id,
            profile_id="profile-teros",
            profile_version_id="version-teros",
        )
        seen = time.time() - 90.0
        self.store.upsert_device(self.device, channel_id="ch-1", seen_at=seen)
        self.store.set_device_observation(
            "ws-1",
            "ch-1",
            self.external_id,
            node_id="2313",
            sensor="teros12",
            metadata={
                "sensor": "teros12",
                "node_id": "2313",
                "location": "Test_3",
                "sub_location": "mz_1",
                "depth": "15cm",
                "application_id": "app-1",
                "f_port": 31,
            },
            observed_at=seen,
        )
        self.store.set_device_quality(
            "ws-1",
            "ch-1",
            self.external_id,
            quality_status="invalid",
            invalid_fields=("moisture",),
            evaluated_at=seen,
            source_received_at=seen,
        )
        self.presence = DevicePresencePolicy(
            stale_after_seconds=60,
            offline_after_seconds=120,
        )

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def _messages(self):
        first = 1_800_000_000_000_000_000
        last = first + 3_600_000_000_000
        return (
            {
                "name": f"{self.external_id}:soil.moisture",
                "value": 24.0,
                "unit": "%",
                "time": last,
            },
            {
                "name": f"{self.external_id}:soil.temperature",
                "value": 22.0,
                "unit": "Cel",
                "time": last,
            },
            {
                "name": f"{self.external_id}:soil.moisture",
                "value": 20.0,
                "unit": "%",
                "time": first,
            },
            {
                "name": f"{self.external_id}:soil.temperature",
                "value": 21.0,
                "unit": "Cel",
                "time": first,
            },
        )

    def test_context_combines_durable_state_latest_observation_and_trends(self):
        reader = _Reader(self._messages())
        history = TimescaleHistoryProvider(reader)
        provider = DeviceContextProvider(
            reader=reader,
            store=self.store,
            presence_policy=self.presence,
            history_provider=history,
        )

        context = provider.build(
            workspace_id="ws-1",
            channel_id="ch-1",
            external_id=self.external_id,
        )

        self.assertEqual(context["schema"], "smarter-adapter.device-context/1")
        self.assertEqual(context["device"]["atom_device_id"], "atom-device-1")
        self.assertEqual(context["device"]["sensor_family"], "teros12")
        self.assertEqual(context["deployment"]["location"], "Test_3")
        self.assertEqual(context["state"]["operational_status"], "stale")
        self.assertEqual(context["state"]["data_quality"], "invalid")
        self.assertEqual(context["state"]["invalid_fields"], ["moisture"])

        latest = context["latest_observation"]
        self.assertEqual(latest["status"], "available")
        self.assertEqual(latest["at"], 1_800_003_600.0)
        self.assertEqual(
            [item["name"] for item in latest["measurements"]],
            ["soil.moisture", "soil.temperature"],
        )

        moisture = next(
            item for item in context["history"]["series"]
            if item["name"] == "soil.moisture"
        )
        self.assertEqual(moisture["direction"], "increasing")
        self.assertAlmostEqual(moisture["slope_per_hour"], 4.0, places=6)
        self.assertIn("treat them as unavailable, not zero", context["text"])

    def test_reader_failure_returns_partial_context_from_durable_state(self):
        reader = _Reader(error_value=ReaderError("reader down"))
        provider = DeviceContextProvider(
            reader=reader,
            store=self.store,
            presence_policy=self.presence,
            history_provider=TimescaleHistoryProvider(reader),
        )
        context = provider.build(
            workspace_id="ws-1",
            channel_id="ch-1",
            external_id=self.external_id,
        )
        self.assertEqual(context["state"]["data_quality"], "invalid")
        self.assertEqual(context["latest_observation"]["status"], "unavailable")
        self.assertEqual(context["history"]["status"], "unavailable")
        self.assertIn("durable adapter state", context["history"]["interpretation"])

    def test_unknown_managed_device_is_rejected(self):
        reader = _Reader(self._messages())
        provider = DeviceContextProvider(
            reader=reader,
            store=self.store,
            presence_policy=self.presence,
            history_provider=TimescaleHistoryProvider(reader),
        )
        with self.assertRaises(KeyError):
            provider.build(
                workspace_id="ws-1",
                channel_id="ch-1",
                external_id="missing-device",
            )

    def test_lifecycle_management_exposes_protected_context_route(self):
        reader = _Reader(self._messages())
        server = start_lifecycle_management_server(
            host="127.0.0.1",
            port=0,
            service=_Service(),
            runtime=_Runtime(),
            store=self.store,
            lifecycle_controller=SimpleNamespace(),
            reader=reader,
            presence_policy=self.presence,
            api_token="secret",
        )
        host, port = server.server_address[:2]
        base = f"http://{host}:{port}"
        try:
            with self.assertRaises(error.HTTPError) as unauthorized:
                _http(base + f"/api/v2/devices/{self.external_id}/context")
            self.assertEqual(unauthorized.exception.code, 401)

            status, body = _http(
                base + f"/api/v2/devices/{self.external_id}/context",
                token="secret",
            )
            self.assertEqual(status, 200)
            self.assertEqual(body["schema"], "smarter-adapter.device-context/1")
            self.assertEqual(body["latest_observation"]["status"], "available")

            with self.assertRaises(error.HTTPError) as missing:
                _http(
                    base + "/api/v2/devices/not-managed/context",
                    token="secret",
                )
            self.assertEqual(missing.exception.code, 404)
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
