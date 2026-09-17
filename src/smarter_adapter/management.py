from __future__ import annotations

import json
import logging
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Any, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse

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
            "inputs": inputs,
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
            "runtime": {
                "workspace_id": base.workspace.id if base else None,
                "channel_id": base.channel.id if base else None,
                "device_type_id": device_type.id if device_type else None,
                "device_type_version_id": device_type.version_id if device_type else None,
                "persistence_rule_id": rule.id if rule else None,
                "device_cache_size": self.runtime.device_cache_size,
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

        query = parse_qs(parsed.query)
        limit = min(max(int((query.get("limit") or [100])[0]), 1), 1000)

        try:
            if path == "/api/v2/status":
                self._send(200, self._status_payload())
                return
            if path == "/api/v2/devices":
                workspace_id = self.runtime.base.workspace.id if self.runtime.base else ""
                channel_id = self.runtime.base.channel.id if self.runtime.base else ""
                items = self.store.list_devices(
                    workspace_id=workspace_id,
                    channel_id=channel_id,
                    limit=limit,
                )
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
                    for item in self.store.list_retries(limit=limit)
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
                    for item in self.store.list_dlq(limit=limit)
                ]
                self._send(200, {"total": self.store.count_dlq(), "items": items})
                return
            self._error(404, "route not found")
        except ValueError as exc:
            self._error(400, str(exc))
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
                self.runtime.bootstrap()
                self.runtime.clear_device_cache()
                self._send(200, {"status": "reconciled"})
                return

            prefix = "/api/v2/dlq/"
            suffix = "/retry"
            if path.startswith(prefix) and path.endswith(suffix):
                raw_id = unquote(path[len(prefix) : -len(suffix)]).strip("/")
                item_id = int(raw_id)
                retry_id = self.store.requeue_dlq(item_id)
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
        api_token: str = "",
    ) -> None:
        super().__init__(address, _Handler)
        self.service = service
        self.runtime = runtime
        self.store = store
        self.api_token = api_token


def start_management_server(
    *,
    host: str,
    port: int,
    service: SmarterAdapterService,
    runtime: SmarterAdapterRuntime,
    store: DeliveryQueueStore,
    api_token: str = "",
) -> ManagementServer:
    server = ManagementServer(
        (host, int(port)),
        service=service,
        runtime=runtime,
        store=store,
        api_token=api_token,
    )
    thread = Thread(target=server.serve_forever, name="smarter-adapter-management", daemon=True)
    thread.start()
    return server


__all__ = ["ManagementServer", "start_management_server"]
