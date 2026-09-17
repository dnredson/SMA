from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from smarter_adapter.catalog_binding import BindingSQLiteManagementStore
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


class _Pipeline:
    def process(self, raw):
        event = ParsedEvent(
            external_device_id="teros12-sector1.3",
            measurements=(Measurement("soil.temperature", 25.0, "Cel"),),
            metadata={
                "node_id": "2313",
                "sensor": "teros12",
                "location": "Test_3",
                "sub_location": "mz_1",
                "depth": "15cm",
                "f_port": 31,
            },
        )
        if raw.received_at == 200.0:
            return ParseOutcome(
                parser="lifecycle-parser",
                event=event,
                quality_status="invalid",
                quality_issues=(
                    QualityIssue(
                        measurement="soil.temperature",
                        reason="source_sentinel",
                        source_field="temperature",
                    ),
                ),
            )
        return ParseOutcome(
            parser="lifecycle-parser",
            event=event,
            quality_status="valid",
        )


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
            id="atom-2313",
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
            raise PublishError("simulated downstream failure", status=500)
        return PublishResult(status=202, body={"status": "accepted"})


class RuntimeLifecycleStateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = BindingSQLiteManagementStore(Path(self.tmp.name) / "state.sqlite3")

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def _runtime(self, *, fail_publish=False):
        return SmarterAdapterRuntime(
            pipeline=_Pipeline(),
            control=_Control(),
            rules=_Rules(),
            publisher=_Publisher(fail=fail_publish),
            state_store=self.store,
        )

    def test_successful_publish_owns_binding_and_replays_do_not_regress_state(self):
        runtime = self._runtime()

        runtime.process(RawEvent(source="mqtt:test", payload=b"{}", received_at=100.0))
        runtime.process(RawEvent(source="mqtt:test", payload=b"{}", received_at=200.0))
        # Simulate recovery/replay of an older raw event after newer traffic.
        runtime.process(RawEvent(source="mqtt:test", payload=b"{}", received_at=150.0))

        observed = self.store.find_latest_catalog_observation_by_node(
            "ws-1", "ch-1", "2313"
        )
        self.assertIsNotNone(observed)
        assert observed is not None
        self.assertEqual(observed["first_observed_at"], 100.0)
        self.assertEqual(observed["last_observed_at"], 200.0)

        managed = self.store.find_latest_device_by_node("ws-1", "ch-1", "2313")
        self.assertIsNotNone(managed)
        assert managed is not None
        self.assertEqual(managed["binding_observed_at"], 200.0)
        self.assertEqual(managed["last_seen"], 200.0)
        self.assertEqual(managed["data_quality"], "invalid")
        self.assertEqual(managed["invalid_fields"], ["temperature"])
        self.assertEqual(managed["quality_source_received_at"], 200.0)

    def test_failed_publish_stays_observed_without_managed_binding(self):
        runtime = self._runtime(fail_publish=True)
        raw = RawEvent(source="mqtt:test", payload=b"{}", received_at=321.0)

        with self.assertRaises(PublishError):
            runtime.process(raw)

        observed = self.store.find_latest_catalog_observation_by_node(
            "ws-1", "ch-1", "2313"
        )
        self.assertIsNotNone(observed)
        assert observed is not None
        self.assertEqual(observed["last_observed_at"], 321.0)

        self.assertIsNone(
            self.store.find_latest_device_by_node("ws-1", "ch-1", "2313")
        )
        device = self.store.find_device("ws-1", "ch-1", "teros12-sector1.3")
        self.assertIsNotNone(device)
        assert device is not None
        self.assertEqual(device["data_quality"], "unknown")
        self.assertIsNone(device["quality_source_received_at"])


if __name__ == "__main__":
    unittest.main()
