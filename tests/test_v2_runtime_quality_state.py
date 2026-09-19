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
from smarter_adapter.magistrala.publisher import PublishError, PublishResult
from smarter_adapter.magistrala.rules import PersistenceRuleRef
from smarter_adapter.models import Measurement, ParsedEvent, RawEvent
from smarter_adapter.pipeline import ParseOutcome
from smarter_adapter.quality import QualityIssue
from smarter_adapter.runtime import SmarterAdapterRuntime
from smarter_adapter.storage import SQLiteManagementStore


class _Pipeline:
    def __init__(self, outcome: ParseOutcome):
        self.outcome = outcome

    def process(self, raw):
        return self.outcome


class _Control:
    def __init__(self):
        self.base = BaseResources(
            workspace=WorkspaceRef(id="ws-1", name="Workspace"),
            channel=ChannelRef(id="ch-1", workspace_id="ws-1", name="Telemetry"),
        )
        self.device_type = DeviceTypeRef(
            id="profile-1",
            workspace_id="ws-1",
            key="sensor",
            name="Sensor",
            version_id="version-1",
            version=1,
        )

    def ensure_base(self, **kwargs):
        return self.base

    def ensure_device_type(self, workspace_id):
        return self.device_type

    def ensure_device(self, workspace_id, channel_id, external_id, **kwargs):
        return DeviceRef(
            id="atom-device-1",
            workspace_id=workspace_id,
            external_id=external_id,
            name=external_id,
            profile_id="profile-1",
            profile_version_id="version-1",
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
    def __init__(self, *, fail=False):
        self.fail = fail

    def publish(self, **kwargs):
        if self.fail:
            raise PublishError("boom", status=500)
        return PublishResult(status=202, body={"status": "accepted"})


class RuntimeQualityStateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = SQLiteManagementStore(Path(self.tmp.name) / "state.sqlite3")

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def _runtime(self, outcome: ParseOutcome, *, fail_publish=False):
        return SmarterAdapterRuntime(
            pipeline=_Pipeline(outcome),
            control=_Control(),
            rules=_Rules(),
            publisher=_Publisher(fail=fail_publish),
            state_store=self.store,
        )

    def test_successful_process_persists_latest_quality_snapshot(self):
        event = ParsedEvent(
            external_device_id="sensor-1",
            measurements=(Measurement("sensor.data_quality", "invalid", timestamp=123.0),),
        )
        outcome = ParseOutcome(
            parser="test-parser",
            event=event,
            quality_status="invalid",
            quality_issues=(
                QualityIssue(
                    measurement="soil.raw.moisture_m1",
                    reason="source_sentinel",
                    source_field="moisture",
                ),
                QualityIssue(
                    measurement="soil.temperature",
                    reason="source_sentinel",
                    source_field="temperature",
                ),
            ),
        )
        runtime = self._runtime(outcome)
        raw = RawEvent(source="mqtt:test", payload=b"{}", received_at=120.0)

        result = runtime.process(raw)

        self.assertEqual(result.publish.status, 202)
        item = self.store.find_device("ws-1", "ch-1", "sensor-1")
        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["data_quality"], "invalid")
        self.assertEqual(item["invalid_fields"], ["moisture", "temperature"])
        self.assertEqual(item["quality_source_received_at"], 120.0)
        self.assertIsNotNone(item["quality_evaluated_at"])

    def test_failed_publish_does_not_advance_quality_snapshot(self):
        event = ParsedEvent(
            external_device_id="sensor-1",
            measurements=(Measurement("soil.temperature", 22.0, timestamp=123.0),),
        )
        outcome = ParseOutcome(
            parser="test-parser",
            event=event,
            quality_status="valid",
        )
        runtime = self._runtime(outcome, fail_publish=True)
        raw = RawEvent(source="mqtt:test", payload=b"{}", received_at=120.0)

        with self.assertRaises(PublishError):
            runtime.process(raw)

        item = self.store.find_device("ws-1", "ch-1", "sensor-1")
        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item["data_quality"], "unknown")
        self.assertIsNone(item["quality_evaluated_at"])


if __name__ == "__main__":
    unittest.main()
