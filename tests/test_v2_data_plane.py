from __future__ import annotations

import io
import json
import sys
import unittest
from pathlib import Path
from urllib import error

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from smarter_adapter.magistrala.publisher import FluxMQPublisher, PublishError
from smarter_adapter.models import Measurement, ParsedEvent
from smarter_adapter.senml import event_to_senml


class _FakeResponse:
    def __init__(self, status=202, body=b'{"status":"accepted"}'):
        self.status = status
        self._body = body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return self._body

    def getcode(self):
        return self.status


class SenMLV2Tests(unittest.TestCase):
    def test_device_serial_is_preserved_in_bn_without_separator(self):
        event = ParsedEvent(
            external_device_id="sensor-01",
            measurements=(Measurement("soil.moisture", 42.5, "%", 1700000000.0),),
        )

        pack = event_to_senml(event)

        self.assertEqual(pack[0]["bn"], "sensor-01")
        self.assertEqual(pack[0]["n"], ":soil.moisture")
        self.assertEqual(pack[0]["bt"], 1700000000.0)
        self.assertEqual(pack[0]["v"], 42.5)
        self.assertEqual(pack[0]["u"], "%")

    def test_value_types_map_to_senml_fields(self):
        event = ParsedEvent(
            external_device_id="sensor-01",
            measurements=(
                Measurement("numeric", 7),
                Measurement("text", "online"),
                Measurement("flag", True),
                Measurement("blob", b"abc"),
            ),
        )

        pack = event_to_senml(event, default_timestamp=100.0)

        self.assertEqual(pack[0]["v"], 7)
        self.assertEqual(pack[1]["vs"], "online")
        self.assertIs(pack[2]["vb"], True)
        self.assertEqual(pack[3]["vd"], "YWJj")


class FluxMQPublisherTests(unittest.TestCase):
    def test_publish_uses_current_http_route_and_envelope(self):
        seen = {}

        def opener(req, timeout):
            seen["url"] = req.full_url
            seen["headers"] = dict(req.header_items())
            seen["body"] = json.loads(req.data.decode("utf-8"))
            seen["timeout"] = timeout
            return _FakeResponse()

        publisher = FluxMQPublisher(
            "http://magistrala.local",
            lambda: "token-1",
            opener=opener,
        )
        result = publisher.publish(
            workspace_id="ws-1",
            channel_id="ch-1",
            device_id="dev-uuid-1",
            senml=[{"bn": "serial-1", "n": ":temperature", "v": 22.1}],
        )

        self.assertEqual(result.status, 202)
        self.assertEqual(
            seen["url"],
            "http://magistrala.local/ws-1/channels/ch-1/messages",
        )
        self.assertEqual(seen["headers"]["Authorization"], "Bearer token-1")
        self.assertEqual(seen["body"]["device_id"], "dev-uuid-1")
        self.assertEqual(seen["body"]["subtopic"], "")
        self.assertEqual(seen["body"]["payload"][0]["bn"], "serial-1")

    def test_401_invalidates_token_and_retries_once(self):
        calls = {"count": 0, "invalidations": 0}
        tokens = iter(["expired-token", "fresh-token"])

        def token_provider():
            return next(tokens)

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
                    io.BytesIO(b'{"error":"invalid bearer token"}'),
                )
            self.assertEqual(req.get_header("Authorization"), "Bearer fresh-token")
            return _FakeResponse()

        publisher = FluxMQPublisher(
            "http://magistrala.local",
            token_provider,
            invalidate_token=invalidate,
            opener=opener,
        )
        result = publisher.publish(
            workspace_id="ws",
            channel_id="ch",
            device_id="dev",
            senml=[{"bn": "serial", "n": ":x", "v": 1}],
        )

        self.assertEqual(result.status, 202)
        self.assertEqual(calls["count"], 2)
        self.assertEqual(calls["invalidations"], 1)

    def test_403_is_not_retried(self):
        calls = {"count": 0}

        def opener(req, timeout):
            calls["count"] += 1
            raise error.HTTPError(
                req.full_url,
                403,
                "Forbidden",
                {},
                io.BytesIO(b'{"error":"not allowed"}'),
            )

        publisher = FluxMQPublisher(
            "http://magistrala.local",
            lambda: "token",
            invalidate_token=lambda: None,
            opener=opener,
        )

        with self.assertRaises(PublishError) as ctx:
            publisher.publish(
                workspace_id="ws",
                channel_id="ch",
                device_id="dev",
                senml=[{"bn": "serial", "n": ":x", "v": 1}],
            )

        self.assertEqual(ctx.exception.status, 403)
        self.assertEqual(calls["count"], 1)


if __name__ == "__main__":
    unittest.main()
