from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Any, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse

from .magistrala.reader import ReaderError, TimescaleReaderClient
from .presence import DevicePresencePolicy
from .reliability import DeliveryQueueStore
from .runtime import SmarterAdapterRuntime
from .service import SmarterAdapterService

logger = logging.getLogger("smarter_adapter.management")


class _Handler(BaseHTTPRequestHandler):
    server_version = "SmarterAdapter/2"

    def log_message(self, fmt: str, *args: Any) -> None:
        logger.info("%s - %s", self.address_string(), fmt % args)

    @property
    def service(self) -> SmarterAdapterService:
        return self.server.service  # type: ignore[attr-defined]

    @property
    def runtime(self) -> SmarterAdapterRuntime:
        return self.server.runtime  # type: ignore[attr-defined]

    @property
    def store(self) -> DeliveryQueueStore:
        return self.server.store  # type: ignore[attr-defined]

    @property
    def reader(self) -> Optional[TimescaleReaderClient]:
        return self.server.reader  # type: ignore[attr-defined]

    @property
    def presence(self) -> DevicePresencePolicy:
        return self.server.presence_policy  # type: ignore[attr-defined]

    def _authorized(self) -> bool:
        expected = self.server.api_token  # type: ignore[attr-defined]
        if not expected:
            return True
        return self.headers.get("Authorization", "") == "Bearer " + expected

    def _send(self, status: int, value: Any = None) -> None:
        body = b"" if value is None else json.dumps(value, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        if value is not None:
            self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def _error(self, status: int, message: str) -> None:
        self._send(status, {"error": message})

    @staticmethod
    def _raw_public(raw) -> dict:
        payload = raw.payload
        try:
            payload_text = payload.decode("utf-8")
        except UnicodeDecodeError:
            payload_text = None
        return {
            "source": raw.source,
            "topic": raw.topic,
            "received_at": raw.received_at,
            "metadata": raw.metadata,
            "payload_utf8": payload_text,
            "payload_bytes": len(payload),
        }

    def _ready_payload(self) -> Tuple[bool, dict]:
        runtime_ready = (
            self.runtime.base is not None
            and self.runtime.device_type is not None
            and self.runtime.persistence_rule is not None
        )
        inputs = [
            {
                "connected": bool(getattr(item, "connected", False)),
                "last_error": getattr(item, "last_error", None),
            }
            for item in self.service.inputs
        ]
        inputs_ready = bool(inputs) and all(item["connected"] for item in inputs)
        ready = runtime_ready and inputs_ready
        return ready, {
            "status": "ready" if ready else "not_ready",
            "runtime_ready": runtime_ready,
            "inputs_ready": inputs_ready,
            "reader_configured": self.reader is not None,
            "inputs": inputs,
        }

    def _managed_devices(self, *, limit: int, now: Optional[float] = None) -> list[dict]:
        base = self.runtime.base
        workspace_id = base.workspace.id if base else ""
        channel_id = base.channel.id if base else ""
        rows = self.store.list_devices(  # type: ignore[attr-defined]
            workspace_id=workspace_id,
            channel_id=channel_id,
            limit=limit,
        )
        current = time.time() if now is None else float(now)
        return [self.presence.decorate(item, now=current) for item in rows]

    def _presence_summary(self) -> dict:
        items = self._managed_devices(limit=100000)
        counts = {"online": 0, "stale": 0, "offline": 0}
        for item in items:
            state = str(item.get("operational_status") or "offline")
            if state in counts:
                counts[state] += 1
        return {
            "total": len(items),
            **counts,
            "stale_after_seconds": self.presence.stale_after_seconds,
            "offline_after_seconds": self.presence.offline_after_seconds,
        }

    def _status_payload(self) -> dict:
        base = self.runtime.base
        rule = self.runtime.persistence_rule
        device_type = self.runtime.device_type
        return {
            "service": asdict(self.service.stats),
            "queues": {
                "retry": self.store.count_retries(),
                "dlq": self.store.count_dlq(),
            },
            "devices": self._presence_summary(),
            "runtime": {
                "workspace_id": base.workspace.id if base else None,
                "channel_id": base.channel.id if base else None,
                "device_type_id": device_type.id if device_type else None,
                "device_type_version_id": device_type.version_id if device_type else None,
                "persistence_rule_id": rule.id if rule else None,
                "device_cache_size": self.runtime.device_cache_size,
                "reader_configured": self.reader is not None,
            },
            "inputs": [
                {
                    "host": cfg.host,
                    "port": cfg.port,
                    "topic": cfg.topic,
                    "source": cfg.source,
                    "connected": bool(getattr(inp, "connected", False)),
                    "last_error": getattr(inp, "last_error", None),
                }
                for cfg, inp in zip(self.service.input_configs, self.service.inputs)
            ],
        }

    @staticmethod
    def _device_messages_external_id(path: str) -> Optional[str]:
        prefix = "/api/v2/devices/"
        suffix = "/messages"
        if not path.startswith(prefix) or not path.endswith(suffix):
            return None
        raw = path[len(prefix) : -len(suffix)].strip("/")
        if not raw or "/" in raw:
            return None
        return unquote(raw)

    def _device_messages(self, parsed, path: str) -> bool:
        external_id = self._device_messages_external_id(path)
        if external_id is None:
            return False
        if self.reader is None:
            self._error(503, "Timescale reader is not configured")
            return True
        base = self.runtime.base
        if base is None:
            self._error(503, "runtime is not bootstrapped")
            return True

        query = parse_qs(parsed.query)
        limit = min(max(int((query.get("limit") or [100])[0]), 1), 1000)
        offset = max(int((query.get("offset") or [0])[0]), 0)
        order = str((query.get("order") or ["time"])[0])
        direction = str((query.get("dir") or ["desc"])[0])
        name = str((query.get("name") or [""])[0])

        device = self.store.find_device(  # type: ignore[attr-defined]
            base.workspace.id,
            base.channel.id,
            external_id,
        )
        if device is None:
            self._error(404, "managed device not found")
            return True

        page = self.reader.list_device_messages(
            base.workspace.id,
            base.channel.id,
            external_id,
            limit=limit,
            offset=offset,
            order=order,
            direction=direction,
            name=name,
        )
        payload = page.as_dict()
        payload["external_id"] = external_id
        payload["atom_device_id"] = device["atom_device_id"]
        payload["operational_status"] = self.presence.classify(float(device["last_seen"]))
        payload["last_seen"] = device["last_seen"]
        payload["last_seen_age_seconds"] = round(
            self.presence.age_seconds(float(device["last_seen"])), 3
        )
        self._send(200, payload)
        return True

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"

        if path == "/health":
            self._send(200, {"status": "ok", "service": "smarter-adapter-v2"})
            return
        if path == "/ready":
            ready, payload = self._ready_payload()
            self._send(200 if ready else 503, payload)
            return

        if not self._authorized():
            self._error(401, "unauthorized")
            return

        try:
            if self._device_messages(parsed, path):
                return

            query = parse_qs(parsed.query)
            limit = min(max(int((query.get("limit") or [100])[0]), 1), 1000)

            if path == "/api/v2/status":
                self._send(200, self._status_payload())
                return
            if path == "/api/v2/devices":
                items = self._managed_devices(limit=limit)
                self._send(200, {"total": len(items), "items": items})
                return
            if path == "/api/v2/retry":
                items = [
                    {
                        "id": item.id,
                        "attempts": item.attempts,
                        "next_attempt_at": item.next_attempt_at,
                        "first_failed_at": item.first_failed_at,
                        "last_error": item.last_error,
                        "error_type": item.error_type,
                        "raw": self._raw_public(item.raw),
                    }
                    for item in self.store.list_retries(limit=limit)  # type: ignore[attr-defined]
                ]
                self._send(200, {"total": self.store.count_retries(), "items": items})
                return
            if path == "/api/v2/dlq":
                items = [
                    {
                        "id": item.id,
                        "attempts": item.attempts,
                        "failed_at": item.failed_at,
                        "last_error": item.last_error,
                        "error_type": item.error_type,
                        "raw": self._raw_public(item.raw),
                    }
                    for item in self.store.list_dlq(limit=limit)  # type: ignore[attr-defined]
                ]
                self._send(200, {"total": self.store.count_dlq(), "items": items})
                return
            self._error(404, "route not found")
        except ValueError as exc:
            self._error(400, str(exc))
        except ReaderError as exc:
            logger.exception("Timescale reader request failed")
            self._error(502, str(exc))
        except Exception as exc:
            logger.exception("management GET failed")
            self._error(500, str(exc))

    def do_POST(self) -> None:  # noqa: N802
        if not self._authorized():
            self._error(401, "unauthorized")
            return
        path = urlparse(self.path).path.rstrip("/")
        try:
            if path == "/api/v2/reconcile":
                self.runtime.bootstrap(force=True)
                cleared = self.runtime.clear_device_cache()
                self._send(200, {"status": "reconciled", "cleared_device_cache": cleared})
                return

            prefix = "/api/v2/dlq/"
            suffix = "/retry"
            if path.startswith(prefix) and path.endswith(suffix):
                raw_id = unquote(path[len(prefix) : -len(suffix)]).strip("/")
                item_id = int(raw_id)
                retry_id = self.store.requeue_dlq(item_id)  # type: ignore[attr-defined]
                if retry_id == 0:
                    self._error(404, "DLQ item not found")
                    return
                self._send(202, {"status": "queued", "retry_id": retry_id})
                return

            self._error(404, "route not found")
        except ValueError as exc:
            self._error(400, str(exc))
        except Exception as exc:
            logger.exception("management POST failed")
            self._error(500, str(exc))


class ManagementServer(ThreadingHTTPServer):
    allow_reuse_address = True

    def __init__(
        self,
        address: Tuple[str, int],
        *,
        service: SmarterAdapterService,
        runtime: SmarterAdapterRuntime,
        store: DeliveryQueueStore,
        reader: Optional[TimescaleReaderClient] = None,
        presence_policy: DevicePresencePolicy = DevicePresencePolicy(),
        api_token: str = "",
    ) -> None:
        super().__init__(address, _Handler)
        self.service = service
        self.runtime = runtime
        self.store = store
        self.reader = reader
        self.presence_policy = presence_policy
        self.api_token = api_token


def start_management_server(
    *,
    host: str,
    port: int,
    service: SmarterAdapterService,
    runtime: SmarterAdapterRuntime,
    store: DeliveryQueueStore,
    reader: Optional[TimescaleReaderClient] = None,
    presence_policy: DevicePresencePolicy = DevicePresencePolicy(),
    api_token: str = "",
) -> ManagementServer:
    server = ManagementServer(
        (host, int(port)),
        service=service,
        runtime=runtime,
        store=store,
        reader=reader,
        presence_policy=presence_policy,
        api_token=api_token,
    )
    thread = Thread(target=server.serve_forever, name="smarter-adapter-management", daemon=True)
    thread.start()
    return server


__all__ = ["ManagementServer", "start_management_server"]
