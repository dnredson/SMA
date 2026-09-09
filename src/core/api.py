from __future__ import annotations

import json
import logging
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Any, Dict, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse

from .atom_client import AtomError

logger = logging.getLogger("api")


def _error_status(exc: Exception) -> int:
    if isinstance(exc, AtomError) and exc.status and 400 <= exc.status < 500:
        return exc.status
    return 502


def _public_device(device: Dict[str, Any]) -> Dict[str, Any]:
    """Return an Atom entity without credentials or internal fields."""
    allowed = (
        "id",
        "kind",
        "name",
        "externalId",
        "external_id",
        "tenantId",
        "tenant_id",
        "status",
        "attributes",
        "createdAt",
        "created_at",
        "updatedAt",
        "updated_at",
    )
    return {key: device[key] for key in allowed if key in device}


class _Handler(BaseHTTPRequestHandler):
    server_version = "SmartAdapter/2"

    def log_message(self, fmt: str, *args: Any) -> None:
        logger.info("%s - %s", self.address_string(), fmt % args)

    @property
    def adapter(self) -> Any:
        return self.server.adapter  # type: ignore[attr-defined]

    def _authorized(self) -> bool:
        expected = self.server.api_token  # type: ignore[attr-defined]
        if not expected:
            return True
        supplied = self.headers.get("Authorization", "")
        return supplied == "Bearer " + expected

    def _send(self, status: int, value: Any = None) -> None:
        body = b"" if value is None else json.dumps(value, ensure_ascii=False).encode()
        self.send_response(status)
        if value is not None:
            self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def _error(self, status: int, message: str) -> None:
        self._send(status, {"error": message})

    def _body(self) -> Dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length else b"{}"
            value = json.loads(raw.decode("utf-8"))
            if not isinstance(value, dict):
                raise ValueError("JSON body must be an object")
            return value
        except (ValueError, json.JSONDecodeError) as exc:
            raise ValueError(f"invalid JSON body: {exc}") from exc

    def _route(self) -> Tuple[str, Optional[str]]:
        parsed = urlparse(self.path)
        prefix = "/api/v1/devices"
        if parsed.path == prefix or parsed.path == prefix + "/":
            return "collection", None
        if parsed.path.startswith(prefix + "/"):
            device_id = unquote(parsed.path[len(prefix) + 1 :]).strip("/")
            if device_id and "/" not in device_id:
                return "item", device_id
        return "unknown", None

    def do_GET(self) -> None:  # noqa: N802
        if urlparse(self.path).path == "/health":
            self._send(200, {"status": "ok", "service": "smartadapter"})
            return
        if not self._authorized():
            self._error(401, "unauthorized")
            return
        route, device_id = self._route()
        try:
            if route == "collection":
                query = parse_qs(urlparse(self.path).query)
                tenant = (query.get("tenant_id") or [None])[0]
                devices = self.adapter.list_devices(tenant_id=tenant)
                self._send(200, {"items": [_public_device(d) for d in devices]})
            elif route == "item" and device_id:
                self._send(200, _public_device(self.adapter.get_device(device_id)))
            else:
                self._error(404, "route not found")
        except Exception as exc:
            logger.exception("GET API failed")
            self._error(_error_status(exc), str(exc))

    def do_POST(self) -> None:  # noqa: N802
        if not self._authorized():
            self._error(401, "unauthorized")
            return
        route, _ = self._route()
        if route != "collection":
            self._error(404, "route not found")
            return
        try:
            body = self._body()
            external_id = str(body.get("external_id") or body.get("externalId") or "").strip()
            if not external_id:
                self._error(400, "external_id is required")
                return
            device = self.adapter.create_device(
                external_id,
                name=body.get("name"),
                tenant_id=body.get("tenant_id") or body.get("tenantId"),
                attributes=body.get("attributes"),
                ensure_publish=bool(body.get("ensure_publish", True)),
            )
            self._send(201, _public_device(device))
        except ValueError as exc:
            self._error(400, str(exc))
        except Exception as exc:
            logger.exception("POST API failed")
            self._error(_error_status(exc), str(exc))

    def _update(self) -> None:
        if not self._authorized():
            self._error(401, "unauthorized")
            return
        route, device_id = self._route()
        if route != "item" or not device_id:
            self._error(404, "route not found")
            return
        try:
            device = self.adapter.update_device(device_id, self._body())
            self._send(200, _public_device(device))
        except ValueError as exc:
            self._error(400, str(exc))
        except Exception as exc:
            logger.exception("UPDATE API failed")
            self._error(_error_status(exc), str(exc))

    def do_PUT(self) -> None:  # noqa: N802
        self._update()

    def do_PATCH(self) -> None:  # noqa: N802
        self._update()

    def do_DELETE(self) -> None:  # noqa: N802
        if not self._authorized():
            self._error(401, "unauthorized")
            return
        route, device_id = self._route()
        if route != "item" or not device_id:
            self._error(404, "route not found")
            return
        try:
            self.adapter.delete_device(device_id)
            self._send(204)
        except Exception as exc:
            logger.exception("DELETE API failed")
            self._error(_error_status(exc), str(exc))


class AdapterAPIServer(ThreadingHTTPServer):
    allow_reuse_address = True

    def __init__(self, address: Tuple[str, int], adapter: Any, api_token: str = "") -> None:
        super().__init__(address, _Handler)
        self.adapter = adapter
        self.api_token = api_token


def start_api_server(adapter: Any, cfg: Dict[str, Any]) -> AdapterAPIServer:
    host = str(cfg.get("api_host") or os.getenv("ADAPTER_API_HOST") or "127.0.0.1")
    port = int(cfg.get("api_port", os.getenv("ADAPTER_API_PORT", "8081")))
    token = str(cfg.get("api_token") or os.getenv("ADAPTER_API_TOKEN") or "")
    server = AdapterAPIServer((host, port), adapter, token)
    thread = Thread(target=server.serve_forever, name="smartadapter-api", daemon=True)
    thread.start()
    logger.info("API do SmartAdapter ouvindo em http://%s:%d", host, port)
    return server


__all__ = ["AdapterAPIServer", "start_api_server"]
