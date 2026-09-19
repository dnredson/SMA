from __future__ import annotations

import io
import json
import sys
import unittest
from pathlib import Path
from urllib import error, parse

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from smarter_adapter.magistrala.reader import TimescaleReaderClient


class _Response:
    def __init__(self, payload, status=200):
        self.status = status
        self._raw = json.dumps(payload).encode("utf-8")

    def read(self):
        return self._raw

    def getcode(self):
        return self.status

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class TimescaleReaderClientTests(unittest.TestCase):
    def test_device_messages_use_plural_device_ids_filter(self):
        seen = []

        def opener(req, timeout=0):
            seen.append(req)
            return _Response(
                {
                    "offset": 0,
                    "limit": 20,
                    "order": "time",
                    "dir": "desc",
                    "format": "messages",
                    "total": 1,
                    "messages": [
                        {
                            "device_id": "teros12-sector1.1",
                            "name": "teros12-sector1.1:soil.temperature",
                            "value": 25.3,
                        }
                    ],
                }
            )

        client = TimescaleReaderClient(
            "http://reader:9011",
            lambda: "token-1",
            opener=opener,
        )
        page = client.list_device_messages(
            "ws-1",
            "ch-1",
            "teros12-sector1.1",
            limit=20,
        )

        self.assertEqual(page.total, 1)
        self.assertEqual(page.messages[0]["value"], 25.3)
        self.assertEqual(len(seen), 1)
        query = parse.parse_qs(parse.urlparse(seen[0].full_url).query)
        self.assertEqual(query["device_ids"], ["teros12-sector1.1"])
        self.assertNotIn("device_id", query)
        self.assertEqual(seen[0].get_header("Authorization"), "Bearer token-1")

    def test_401_invalidates_token_and_retries_once(self):
        calls = []
        state = {"token": "old", "invalidated": 0}

        def token_provider():
            return state["token"]

        def invalidate():
            state["invalidated"] += 1
            state["token"] = "new"

        def opener(req, timeout=0):
            calls.append(req.get_header("Authorization"))
            if len(calls) == 1:
                raise error.HTTPError(
                    req.full_url,
                    401,
                    "unauthorized",
                    hdrs=None,
                    fp=io.BytesIO(b'{"error":"expired"}'),
                )
            return _Response({"offset": 0, "limit": 10, "total": 0, "messages": []})

        client = TimescaleReaderClient(
            "http://reader:9011",
            token_provider,
            invalidate_token=invalidate,
            opener=opener,
        )
        page = client.list_device_messages("ws", "ch", "device", limit=10)

        self.assertEqual(page.total, 0)
        self.assertEqual(state["invalidated"], 1)
        self.assertEqual(calls, ["Bearer old", "Bearer new"])


if __name__ == "__main__":
    unittest.main()
