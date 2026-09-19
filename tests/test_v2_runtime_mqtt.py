from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from smarter_adapter.inputs import MQTTInput, MQTTInputConfig
from smarter_adapter.magistrala.control_plane import (
    BaseResources,
    ChannelRef,
    DeviceRef,
    DeviceTypeRef,
    WorkspaceRef,
)
from smarter_adapter.magistrala.publisher import PublishResult
from smarter_adapter.magistrala.rules import PersistenceRuleRef
from smarter_adapter.models import Measurement, ParsedEvent, RawEvent
from smarter_adapter.pipeline import ParsePipeline
from smarter_adapter.plugins import ParserRegistry
from smarter_adapter.runtime import RuntimeConfig, SmarterAdapterRuntime


class _FakeMQTTClient:
    def __init__(self):
        self.on_connect = None
        self.on_disconnect = None
        self.on_message = None
        self.subscriptions = []
        self.credentials = None

    def username_pw_set(self, username, password=None):
        self.credentials = (username, password)

    def subscribe(self, topic, qos=0):
        self.subscriptions.append((topic, qos))
        return (0, 1)

    def connect(self, host, port, keepalive=60):
        self.connection = (host, port, keepalive)
        return 0

    def loop_start(self):
        pass

    def loop_stop(self):
        pass

    def disconnect(self):
        pass


class MQTTInputTests(unittest.TestCase):
    def test_message_is_emitted_as_transport_neutral_raw_event(self):
        seen = []
        client = _FakeMQTTClient()
        inp = MQTTInput(
            MQTTInputConfig(
                host="broker",
                port=1884,
                topic="application/#",
                qos=1,
                source="mqtt:test",
            ),
            seen.append,
            client_factory=lambda _client_id: client,
        )

        inp._on_connect(client, None, None, 0)
        self.assertTrue(inp.connected)
        self.assertEqual(client.subscriptions, [("application/#", 1)])

        inp._on_message(
            client,
            None,
            SimpleNamespace(
                topic="application/app/device/dev/event/up",
                payload=b'{"hello":"world"}',
                qos=1,
                retain=False,
            ),
        )

        self.assertEqual(len(seen), 1)
        self.assertEqual(seen[0].source, "mqtt:test")
        self.assertEqual(seen[0].topic, "application/app/device/dev/event/up")
        self.assertEqual(seen[0].payload, b'{"hello":"world"}')
        self.assertEqual(seen[0].metadata["qos"], 1)


class _Parser:
    name = "test-parser"

    def supports(self, event):
        return True

    def parse(self, event):
        return ParsedEvent(
            external_device_id="device-external-1",
            measurements=(Measurement("temperature", 22.5, "Cel", 100.0),),
            metadata={"sensor": "test", "location": "lab"},
        )


class _Control:
    def __init__(self):
        self.base_calls = 0
        self.type_calls = 0
        self.device_calls = 0

    def ensure_base(self, **kwargs):
        self.base_calls += 1
        return BaseResources(
            workspace=WorkspaceRef("ws-1", "Workspace", "workspace"),
            channel=ChannelRef("ch-1", "ws-1", "Telemetry", "telemetry"),
        )

    def ensure_device_type(self, workspace_id):
        self.type_calls += 1
        return DeviceTypeRef(
            id="profile-1",
            workspace_id=workspace_id,
            key="sensor",
            name="Sensor",
            version_id="profile-version-1",
            version=1,
        )

    def ensure_device(self, workspace_id, channel_id, external_id, **kwargs):
        self.device_calls += 1
        return DeviceRef(
            id="device-uuid-1",
            workspace_id=workspace_id,
            external_id=external_id,
            name=external_id,
            profile_id="profile-1",
            profile_version_id="profile-version-1",
        )


class _Rules:
    def __init__(self):
        self.calls = 0

    def ensure_senml_persistence(self, workspace_id, channel_id, *, name):
        self.calls += 1
        return PersistenceRuleRef(
            id="rule-1",
            workspace_id=workspace_id,
            channel_id=channel_id,
            name=name,
            status="enabled",
        )


class _Publisher:
    def __init__(self):
        self.calls = []

    def publish(self, **kwargs):
        self.calls.append(kwargs)
        return PublishResult(status=202, body={"status": "accepted"})


class ManagedRuntimeTests(unittest.TestCase):
    def test_bootstrap_is_once_and_device_uses_fast_path_after_first_message(self):
        control = _Control()
        rules = _Rules()
        publisher = _Publisher()
        runtime = SmarterAdapterRuntime(
            pipeline=ParsePipeline(ParserRegistry([_Parser()])),
            control=control,
            rules=rules,
            publisher=publisher,
            config=RuntimeConfig(
                workspace_name="Workspace",
                workspace_alias="workspace",
                channel_name="Telemetry",
                channel_alias="telemetry",
            ),
        )
        raw = RawEvent(source="mqtt:test", topic="a/b", payload=b"payload")

        first = runtime.process(raw)
        second = runtime.process(raw)

        self.assertFalse(first.device_cache_hit)
        self.assertTrue(second.device_cache_hit)
        self.assertEqual(runtime.device_cache_size, 1)
        self.assertEqual(control.base_calls, 1)
        self.assertEqual(control.type_calls, 1)
        self.assertEqual(control.device_calls, 1)
        self.assertEqual(rules.calls, 1)
        self.assertEqual(len(publisher.calls), 2)
        self.assertEqual(publisher.calls[0]["device_id"], "device-uuid-1")
        self.assertEqual(publisher.calls[0]["senml"][0]["bn"], "device-external-1")


if __name__ == "__main__":
    unittest.main()
