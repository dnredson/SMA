from __future__ import annotations

import json
from http.server import ThreadingHTTPServer
from threading import Thread
from typing import Any, Optional, Tuple
from urllib.parse import unquote, urlparse

from .device_context import DeviceContextProvider
from .device_lifecycle import DeviceLifecycleController, LifecycleRemoteError
from .gateway_monitor import GatewayPresencePolicy
from .historical_intelligence import TimescaleHistoryProvider
from .management import _Handler
from .reconciliation import ControlPlaneReconciler


class _LifecycleHandler(_Handler):
    @property
    def lifecycle(self) -> DeviceLifecycleController:
        return self.server.lifecycle_controller  # type: ignore[attr-defined]

    @property
    def reconciler(self) -> Optional[ControlPlaneReconciler]:
        return self.server.reconciler  # type: ignore[attr-defined]

    @property
    def context_provider(self) -> Optional[DeviceContextProvider]:
        return self.server.context_provider  # type: ignore[attr-defined]

    @property
    def gateway_presence(self) -> GatewayPresencePolicy:
        return self.server.gateway_presence_policy  # type: ignore[attr-defined]

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

    @staticmethod
    def _device_context_external_id(path: str) -> Optional[str]:
        prefix = "/api/v2/devices/"
        suffix = "/context"
        if not path.startswith(prefix) or not path.endswith(suffix):
            return None
        raw = path[len(prefix) : -len(suffix)].strip("/")
        if not raw or "/" in raw:
            return None
        return unquote(raw)

    @staticmethod
    def _gateway_id(path: str) -> Optional[str]:
        prefix = "/api/v2/gateways/"
        if not path.startswith(prefix):
            return None
        raw = path[len(prefix) :].strip("/")
        if not raw or "/" in raw:
            return None
        return unquote(raw).strip().lower()

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

    def _catalog_summary(self) -> dict:
        catalog = self.catalog
        if catalog is None:
            return {
                "configured": False,
                "source": "none",
                "writable": False,
                "total": 0,
                "planned": 0,
                "observed": 0,
                "managed": 0,
                "decommissioned": 0,
                "active": 0,
            }
        public_payload = getattr(catalog, "public_payload", None)
        if not callable(public_payload):
            return super()._catalog_summary()
        payload = public_payload()
        return {
            "configured": True,
            "source": payload.get("source", ""),
            "writable": bool(payload.get("writable", False)),
            "total": int(payload.get("total", 0)),
            "planned": int(payload.get("planned", 0)),
            "observed": int(payload.get("observed", 0)),
            "managed": int(payload.get("managed", 0)),
            "decommissioned": int(payload.get("decommissioned", 0)),
            "active": int(payload.get("active", payload.get("total", 0))),
        }

    def _gateway_items(self, *, limit: int = 1000) -> list[dict[str, Any]]:
        base = self.runtime.base
        lister = getattr(self.store, "list_gateways", None)
        if base is None or not callable(lister):
            return []
        return [
            self.gateway_presence.decorate(item)
            for item in lister(base.workspace.id, limit=limit)
        ]

    def _gateway_summary(self, items: Optional[list[dict[str, Any]]] = None) -> dict[str, Any]:
        if items is None:
            items = self._gateway_items(limit=100000)
        counts = {"online": 0, "stale": 0, "offline": 0}
        for item in items:
            state = str(item.get("operational_status") or "offline")
            if state in counts:
                counts[state] += 1
        return {
            "total": len(items),
            **counts,
            "expected_interval_seconds": self.gateway_presence.expected_interval_seconds,
            "stale_after_seconds": self.gateway_presence.stale_after_seconds,
            "offline_after_seconds": self.gateway_presence.offline_after_seconds,
        }

    def _status_payload(self) -> dict:
        payload = super()._status_payload()
        payload["gateways"] = self._gateway_summary()
        return payload

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"

        external_id = self._device_context_external_id(path)
        if external_id is not None:
            if not self._authorized():
                self._error(401, "unauthorized")
                return
            if self.context_provider is None:
                self._error(503, "device context provider is not configured")
                return
            base = self.runtime.base
            if base is None:
                self._error(503, "runtime is not bootstrapped")
                return
            try:
                context = self.context_provider.build(
                    workspace_id=base.workspace.id,
                    channel_id=base.channel.id,
                    external_id=external_id,
                )
            except KeyError:
                self._error(404, "managed device not found")
                return
            except Exception as exc:
                self._error(500, str(exc))
                return
            self._send(200, context)
            return

        if path == "/api/v2/gateways":
            if not self._authorized():
                self._error(401, "unauthorized")
                return
            query = dict()
            try:
                from urllib.parse import parse_qs

                query = parse_qs(parsed.query)
                limit = min(max(int((query.get("limit") or [100])[0]), 1), 1000)
            except ValueError:
                self._error(400, "limit must be an integer")
                return
            items = self._gateway_items(limit=limit)
            self._send(200, {"summary": self._gateway_summary(items), "items": items})
            return

        gateway_id = self._gateway_id(path)
        if gateway_id is not None:
            if not self._authorized():
                self._error(401, "unauthorized")
                return
            base = self.runtime.base
            finder = getattr(self.store, "find_gateway", None)
            if base is None or not callable(finder):
                self._error(503, "gateway registry is not configured")
                return
            item = finder(base.workspace.id, gateway_id)
            if item is None:
                self._error(404, "gateway not found")
                return
            self._send(200, self.gateway_presence.decorate(item))
            return

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

        if path == "/api/v2/reconcile" and self.reconciler is not None:
            if not self._authorized():
                self._error(401, "unauthorized")
                return
            try:
                body = self._optional_json_object()
                repair = body.get("repair", True)
                include_devices = body.get("include_devices", True)
                if not isinstance(repair, bool):
                    raise ValueError("repair must be a boolean")
                if not isinstance(include_devices, bool):
                    raise ValueError("include_devices must be a boolean")
                result = self.reconciler.reconcile(
                    repair=repair,
                    include_devices=include_devices,
                )
                self._send(200, result)
            except ValueError as exc:
                self._error(400, str(exc))
            except Exception as exc:
                self._error(500, str(exc))
            return

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

    def do_DELETE(self) -> None:  # noqa: N802
        if not self._authorized():
            self._error(401, "unauthorized")
            return
        path = urlparse(self.path).path.rstrip("/")
        node_id = self._catalog_node_id(path)
        catalog = self.catalog
        public_item = getattr(catalog, "public_item", None) if catalog is not None else None
        if node_id is not None and callable(public_item):
            item = public_item(node_id)
            if item is not None and item.get("lifecycle_state") != "planned":
                self._error(
                    409,
                    "catalog nodes with observation/management history cannot be deleted; "
                    "decommission the node instead",
                )
                return
        super().do_DELETE()


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
        reconciler: Optional[ControlPlaneReconciler] = None,
        context_provider: Optional[DeviceContextProvider] = None,
        gateway_presence_policy: Optional[GatewayPresencePolicy] = None,
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
        self.gateway_presence_policy = gateway_presence_policy or GatewayPresencePolicy()

        # Backward-compatible construction: older component tests and custom
        # embeddings can use lifecycle management without exposing an Atom
        # client on the controller. The production runner does expose it, so
        # the richer drift reconciler is created automatically there.
        if reconciler is not None:
            self.reconciler = reconciler
        else:
            atom = getattr(lifecycle_controller, "atom", None)
            self.reconciler = (
                ControlPlaneReconciler(
                    runtime=runtime,
                    store=store,
                    catalog=catalog_manager,
                    atom=atom,
                )
                if atom is not None
                else None
            )

        if context_provider is not None:
            self.context_provider = context_provider
        elif reader is not None and presence_policy is not None:
            self.context_provider = DeviceContextProvider(
                reader=reader,
                store=store,
                presence_policy=presence_policy,
                history_provider=TimescaleHistoryProvider(reader),
                gateway_presence_policy=self.gateway_presence_policy,
            )
        else:
            self.context_provider = None


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
    reconciler: Optional[ControlPlaneReconciler] = None,
    context_provider: Optional[DeviceContextProvider] = None,
    gateway_presence_policy: Optional[GatewayPresencePolicy] = None,
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
        reconciler=reconciler,
        context_provider=context_provider,
        gateway_presence_policy=gateway_presence_policy,
    )
    thread = Thread(
        target=server.serve_forever,
        name="smarter-adapter-management",
        daemon=True,
    )
    thread.start()
    return server


__all__ = ["LifecycleManagementServer", "start_lifecycle_management_server"]
