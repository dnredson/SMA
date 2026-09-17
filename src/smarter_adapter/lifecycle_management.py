from __future__ import annotations

import json
from http.server import ThreadingHTTPServer
from threading import Thread
from typing import Any, Optional, Tuple
from urllib.parse import unquote, urlparse

from .device_lifecycle import DeviceLifecycleController, LifecycleRemoteError
from .management import _Handler


class _LifecycleHandler(_Handler):
    @property
    def lifecycle(self) -> DeviceLifecycleController:
        return self.server.lifecycle_controller  # type: ignore[attr-defined]

    @staticmethod
    def _lifecycle_action(path: str) -> Optional[tuple[str, str]]:
        prefix = "/api/v2/catalog/devices/"
        if not path.startswith(prefix):
            return None
        rest = path[len(prefix) :].strip("/")
        parts = rest.split("/")
        if len(parts) != 2 or parts[1] not in {"decommission", "reactivate"}:
            return None
        node_id = unquote(parts[0]).strip().upper()
        if not node_id:
            return None
        return node_id, parts[1]

    def _optional_json_object(self) -> dict[str, Any]:
        raw_length = self.headers.get("Content-Length", "")
        if not raw_length:
            return {}
        try:
            length = int(raw_length)
        except ValueError as exc:
            raise ValueError("invalid Content-Length") from exc
        if length <= 0:
            return {}
        if length > 65536:
            raise ValueError("JSON request body exceeds 65536 bytes")
        raw = self.rfile.read(length)
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"invalid JSON request body: {exc}") from exc
        if not isinstance(value, dict):
            raise ValueError("JSON request body must be an object")
        return value

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        prefix = "/api/v2/catalog/devices/"
        if path.startswith(prefix) and "/" not in path[len(prefix) :]:
            if not self._authorized():
                self._error(401, "unauthorized")
                return
            node_id = unquote(path[len(prefix) :]).strip().upper()
            catalog = self.catalog
            public_item = getattr(catalog, "public_item", None) if catalog is not None else None
            if callable(public_item):
                item = public_item(node_id)
                if item is None:
                    self._error(404, f"Irrigap node {node_id!r} not found")
                else:
                    self._send(200, item)
                return
        super().do_GET()

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path.rstrip("/")
        action = self._lifecycle_action(path)
        if action is None:
            super().do_POST()
            return
        if not self._authorized():
            self._error(401, "unauthorized")
            return

        node_id, operation = action
        try:
            if operation == "decommission":
                body = self._optional_json_object()
                result = self.lifecycle.decommission(
                    node_id,
                    reason=str(body.get("reason") or "").strip(),
                )
            else:
                result = self.lifecycle.reactivate(node_id)
            self._send(200, result)
        except KeyError as exc:
            self._error(404, str(exc))
        except LifecycleRemoteError as exc:
            self._send(
                502,
                {
                    "error": str(exc),
                    "node_id": exc.node_id,
                    "local_state": exc.local_state,
                },
            )
        except ValueError as exc:
            self._error(400, str(exc))
        except Exception as exc:
            self._error(500, str(exc))


class LifecycleManagementServer(ThreadingHTTPServer):
    allow_reuse_address = True

    def __init__(
        self,
        address: Tuple[str, int],
        *,
        service,
        runtime,
        store,
        lifecycle_controller: DeviceLifecycleController,
        reader=None,
        presence_policy=None,
        catalog_manager=None,
        api_token: str = "",
    ) -> None:
        super().__init__(address, _LifecycleHandler)
        self.service = service
        self.runtime = runtime
        self.store = store
        self.reader = reader
        self.presence_policy = presence_policy
        self.catalog_manager = catalog_manager
        self.api_token = api_token
        self.lifecycle_controller = lifecycle_controller


def start_lifecycle_management_server(
    *,
    host: str,
    port: int,
    service,
    runtime,
    store,
    lifecycle_controller: DeviceLifecycleController,
    reader=None,
    presence_policy=None,
    catalog_manager=None,
    api_token: str = "",
) -> LifecycleManagementServer:
    server = LifecycleManagementServer(
        (host, int(port)),
        service=service,
        runtime=runtime,
        store=store,
        lifecycle_controller=lifecycle_controller,
        reader=reader,
        presence_policy=presence_policy,
        catalog_manager=catalog_manager,
        api_token=api_token,
    )
    thread = Thread(
        target=server.serve_forever,
        name="smarter-adapter-management",
        daemon=True,
    )
    thread.start()
    return server


__all__ = ["LifecycleManagementServer", "start_lifecycle_management_server"]
