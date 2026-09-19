from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

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
from smarter_adapter.runtime import SmarterAdapterRuntime
from smarter_adapter.storage import SQLiteStateStore


class _Parser:
    name = "state-test-parser"

    def supports(self, event):
        return True

    def parse(self, event):
        return ParsedEvent(
            external_device_id="persistent-device-1",
            measurements=(Measurement("temperature", 23.0, "Cel", 100.0),),
            metadata={"sensor": "test"},
        )


class _Control:
    def __init__(self, device_id="atom-device-1"):
        self.device_id = device_id
        self.device_calls = 0

    def ensure_base(self, **kwargs):
        return BaseResources(
            workspace=WorkspaceRef("ws-1", "Workspace", "workspace"),
            channel=ChannelRef("ch-1", "ws-1", "Telemetry", "telemetry"),
        )

    def ensure_device_type(self, workspace_id):
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
            id=self.device_id,
            workspace_id=workspace_id,
            external_id=external_id,
            name=external_id,
            profile_id="profile-1",
            profile_version_id="profile-version-1",
        )


class _Rules:
    def ensure_senml_persistence(self, workspace_id, channel_id, *, name):
        return PersistenceRuleRef(
            id="rule-1",
            workspace_id=workspace_id,
            channel_id=channel_id,
            name=name,
            status="enabled",
        )


class _Publisher:
    def publish(self, **kwargs):
        return PublishResult(status=202, body={"status": "accepted"})


class SQLiteStateStoreTests(unittest.TestCase):
    def test_device_mapping_survives_store_reopen(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.db"
            device = DeviceRef(
                id="atom-uuid-1",
                workspace_id="ws-1",
                external_id="external-1",
                name="External 1",
                profile_id="profile-1",
                profile_version_id="version-1",
            )

            with SQLiteStateStore(path) as store:
                store.upsert_device(device, channel_id="ch-1", seen_at=10.0)
                self.assertEqual(store.count_devices("ws-1", "ch-1"), 1)

            with SQLiteStateStore(path) as reopened:
                restored = reopened.get_device("ws-1", "ch-1", "external-1")

            self.assertIsNotNone(restored)
            assert restored is not None
            self.assertEqual(restored.id, "atom-uuid-1")
            self.assertEqual(restored.external_id, "external-1")
            self.assertEqual(restored.profile_id, "profile-1")
            self.assertFalse(restored.created)

    def test_runtime_uses_persistent_mapping_after_restart(self):
        raw = RawEvent(source="mqtt:test", topic="application/test", payload=b"payload")

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.db"

            first_control = _Control()
            with SQLiteStateStore(path) as first_store:
                first_runtime = SmarterAdapterRuntime(
                    pipeline=ParsePipeline(ParserRegistry([_Parser()])),
                    control=first_control,
                    rules=_Rules(),
                    publisher=_Publisher(),
                    state_store=first_store,
                )
                first = first_runtime.process(raw)

            self.assertEqual(first.device_cache_source, "remote")
            self.assertFalse(first.device_cache_hit)
            self.assertEqual(first_control.device_calls, 1)

            second_control = _Control(device_id="should-not-be-created")
            with SQLiteStateStore(path) as second_store:
                second_runtime = SmarterAdapterRuntime(
                    pipeline=ParsePipeline(ParserRegistry([_Parser()])),
                    control=second_control,
                    rules=_Rules(),
                    publisher=_Publisher(),
                    state_store=second_store,
                )
                second = second_runtime.process(raw)

            self.assertTrue(second.device_cache_hit)
            self.assertEqual(second.device_cache_source, "persistent")
            self.assertEqual(second.device.id, "atom-device-1")
            self.assertEqual(second_control.device_calls, 0)


if __name__ == "__main__":
    unittest.main()
