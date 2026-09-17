from __future__ import annotations

import io
import json
import sys
import unittest
from pathlib import Path
from urllib import error

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from smarter_adapter.magistrala.rules import RulesClient, RulesError


class _FakeResponse:
    def __init__(self, status=200, payload=None):
        self.status = status
        self._body = json.dumps(payload or {}).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return self._body

    def getcode(self):
        return self.status


class RulesClientTests(unittest.TestCase):
    def test_null_rules_page_is_normalized_and_persistence_is_created(self):
        calls = []

        def opener(req, timeout):
            calls.append((req.method, req.full_url, req.data))
            if req.method == "GET":
                return _FakeResponse(200, {"total": 0, "rules": None})
            if req.method == "POST" and req.full_url.endswith("/rules"):
                body = json.loads(req.data.decode("utf-8"))
                self.assertEqual(body["input_channel"], "ch-1")
                self.assertEqual(body["outputs"], [{"type": "save_senml"}])
                return _FakeResponse(
                    201,
                    {
                        "id": "rule-1",
                        "name": "smarter-adapter-save-senml",
                        "input_channel": "ch-1",
                        "input_topic": "",
                        "outputs": [{"type": "save_senml"}],
                        "status": "disabled",
                    },
                )
            if req.method == "POST" and req.full_url.endswith("/rules/rule-1/enable"):
                return _FakeResponse(
                    200,
                    {
                        "id": "rule-1",
                        "name": "smarter-adapter-save-senml",
                        "input_channel": "ch-1",
                        "input_topic": "",
                        "outputs": [{"type": "save_senml"}],
                        "status": "enabled",
                    },
                )
            raise AssertionError(req.full_url)

        client = RulesClient("http://magistrala", lambda: "token", opener=opener)
        ref = client.ensure_senml_persistence("ws-1", "ch-1")

        self.assertEqual(ref.id, "rule-1")
        self.assertTrue(ref.created)
        self.assertTrue(ref.enabled)
        self.assertEqual(ref.status, "enabled")
        self.assertEqual([method for method, _, _ in calls], ["GET", "POST", "POST"])

    def test_existing_enabled_rule_is_reused(self):
        def opener(req, timeout):
            if req.method != "GET":
                raise AssertionError("existing enabled rule must not be modified")
            return _FakeResponse(
                200,
                {
                    "total": 1,
                    "rules": [
                        {
                            "id": "rule-1",
                            "name": "smarter-adapter-save-senml",
                            "input_channel": "ch-1",
                            "input_topic": "",
                            "outputs": [{"type": "save_senml"}],
                            "status": "enabled",
                        }
                    ],
                },
            )

        client = RulesClient("http://magistrala", lambda: "token", opener=opener)
        ref = client.ensure_senml_persistence("ws-1", "ch-1")

        self.assertFalse(ref.created)
        self.assertFalse(ref.enabled)
        self.assertEqual(ref.id, "rule-1")

    def test_existing_wrong_rule_is_reported_as_drift(self):
        def opener(req, timeout):
            return _FakeResponse(
                200,
                {
                    "rules": [
                        {
                            "id": "rule-1",
                            "name": "smarter-adapter-save-senml",
                            "input_channel": "ch-1",
                            "input_topic": "",
                            "outputs": [{"type": "alarms"}],
                            "status": "enabled",
                        }
                    ]
                },
            )

        client = RulesClient("http://magistrala", lambda: "token", opener=opener)
        with self.assertRaises(RulesError):
            client.ensure_senml_persistence("ws-1", "ch-1")

    def test_401_invalidates_token_and_retries_once(self):
        calls = {"count": 0, "invalidations": 0}
        tokens = iter(["old", "new"])

        def invalidate():
            calls["invalidations"] += 1

        def opener(req, timeout):
            calls["count"] += 1
            if calls["count"] == 1:
                raise error.HTTPError(
                    req.full_url,
                    401,
                    "Unauthorized",
                    {},
                    io.BytesIO(b'{"message":"expired"}'),
                )
            self.assertEqual(req.get_header("Authorization"), "Bearer new")
            return _FakeResponse(200, {"rules": []})

        client = RulesClient(
            "http://magistrala",
            lambda: next(tokens),
            invalidate_token=invalidate,
            opener=opener,
        )
        self.assertEqual(client.list_rules("ws-1"), [])
        self.assertEqual(calls["count"], 2)
        self.assertEqual(calls["invalidations"], 1)


if __name__ == "__main__":
    unittest.main()
