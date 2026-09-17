from __future__ import annotations

import json
import os
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional, Tuple

from .parsers.chirpstack_irrigap import IrrigapNode


@dataclass(frozen=True)
class IrrigapCatalog:
    nodes: Tuple[IrrigapNode, ...]
    source: str


class IrrigapCatalogError(ValueError):
    """Base error for management-time catalog mutations."""


class IrrigapCatalogConflict(IrrigapCatalogError):
    pass


class IrrigapCatalogNotFound(IrrigapCatalogError):
    pass


class IrrigapCatalogReadOnly(IrrigapCatalogError):
    pass


def _nodes_document(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        nodes = value.get("nodes")
        if isinstance(nodes, list):
            return nodes
    raise RuntimeError("Irrigap catalog JSON must be an array or an object with a 'nodes' array")


def _node_from_mapping(value: Mapping[str, Any], *, index: int) -> IrrigapNode:
    node_id = str(value.get("id") or "").strip().upper()
    device = str(value.get("device") or "").strip()
    location = str(value.get("location") or "").strip()
    sub_location = str(value.get("sub_location") or "").strip()

    if not node_id:
        raise RuntimeError(f"Irrigap catalog node #{index} is missing id")
    if not device:
        raise RuntimeError(f"Irrigap catalog node {node_id!r} is missing device")

    raw_depths = value.get("depths") or {}
    if not isinstance(raw_depths, dict):
        raise RuntimeError(f"Irrigap catalog node {node_id!r} depths must be an object")

    depths: dict[int, str] = {}
    for raw_port, raw_depth in raw_depths.items():
        try:
            port = int(raw_port)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Irrigap catalog node {node_id!r} has invalid fPort {raw_port!r}"
            ) from exc
        if not 1 <= port <= 255:
            raise RuntimeError(
                f"Irrigap catalog node {node_id!r} fPort must be between 1 and 255"
            )
        depth = str(raw_depth or "").strip()
        if not depth:
            raise RuntimeError(
                f"Irrigap catalog node {node_id!r} depth for fPort {port} must not be empty"
            )
        depths[port] = depth

    return IrrigapNode(
        id=node_id,
        device=device,
        location=location,
        sub_location=sub_location,
        depths=depths,
    )


def irrigap_node_to_dict(node: IrrigapNode) -> dict[str, Any]:
    return {
        "id": node.id,
        "device": node.device,
        "location": node.location,
        "sub_location": node.sub_location,
        "depths": {str(port): depth for port, depth in sorted(node.depths.items())},
    }


def parse_irrigap_catalog_json(raw: str, *, source: str = "inline") -> IrrigapCatalog:
    try:
        document = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid Irrigap catalog JSON from {source}: {exc}") from exc

    values = _nodes_document(document)
    if not values:
        raise RuntimeError(f"Irrigap catalog from {source} must contain at least one node")

    nodes = []
    seen: set[str] = set()
    for index, value in enumerate(values, start=1):
        if not isinstance(value, dict):
            raise RuntimeError(f"Irrigap catalog node #{index} must be an object")
        node = _node_from_mapping(value, index=index)
        key = node.id.upper()
        if key in seen:
            raise RuntimeError(f"Irrigap catalog contains duplicate node id {node.id!r}")
        seen.add(key)
        nodes.append(node)

    return IrrigapCatalog(nodes=tuple(nodes), source=source)


def load_irrigap_catalog(*, file_path: str = "", inline_json: str = "") -> IrrigapCatalog:
    path_text = str(file_path or "").strip()
    inline = str(inline_json or "").strip()
    if path_text and inline:
        raise RuntimeError(
            "set only one of SMA_IRRIGAP_NODES_FILE or SMA_IRRIGAP_NODES_JSON"
        )

    if inline:
        return parse_irrigap_catalog_json(
            inline,
            source="env:SMA_IRRIGAP_NODES_JSON",
        )

    if path_text:
        path = Path(path_text).expanduser()
        if not path.is_file():
            raise RuntimeError(f"Irrigap catalog file not found: {path}")
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise RuntimeError(f"cannot read Irrigap catalog file {path}: {exc}") from exc
        return parse_irrigap_catalog_json(raw, source=f"file:{path}")

    return IrrigapCatalog(nodes=(), source="none")


class IrrigapCatalogManager:
    """Thread-safe live Irrigap deployment catalog.

    File-backed catalogs are mutable and every mutation is persisted atomically
    before becoming visible to parser lookups. Inline/env catalogs are kept
    read-only so a management write can never silently disappear on restart.
    """

    def __init__(
        self,
        catalog: IrrigapCatalog,
        *,
        file_path: str = "",
    ) -> None:
        self._lock = threading.RLock()
        self._nodes = tuple(catalog.nodes)
        self._source = str(catalog.source)
        self._file_path: Optional[Path] = None
        path_text = str(file_path or "").strip()
        if path_text:
            self._file_path = Path(path_text).expanduser()

    @property
    def source(self) -> str:
        return self._source

    @property
    def writable(self) -> bool:
        return self._file_path is not None

    def snapshot(self) -> IrrigapCatalog:
        with self._lock:
            return IrrigapCatalog(nodes=tuple(self._nodes), source=self._source)

    def list_nodes(self) -> Tuple[IrrigapNode, ...]:
        with self._lock:
            return tuple(self._nodes)

    def get_node(self, node_id: str) -> Optional[IrrigapNode]:
        key = str(node_id or "").strip().upper()
        if not key:
            return None
        with self._lock:
            for node in self._nodes:
                if node.id.upper() == key:
                    return node
        return None

    def public_payload(self) -> dict[str, Any]:
        nodes = self.list_nodes()
        return {
            "source": self.source,
            "writable": self.writable,
            "total": len(nodes),
            "items": [irrigap_node_to_dict(node) for node in nodes],
        }

    def _ensure_writable(self) -> Path:
        if self._file_path is None:
            raise IrrigapCatalogReadOnly(
                "Irrigap catalog is read-only; configure SMA_IRRIGAP_NODES_FILE for CRUD writes"
            )
        return self._file_path

    @staticmethod
    def _validated_node(value: Mapping[str, Any], *, index: int = 1) -> IrrigapNode:
        try:
            return _node_from_mapping(value, index=index)
        except RuntimeError as exc:
            raise IrrigapCatalogError(str(exc)) from exc

    def _persist_candidate(self, nodes: Tuple[IrrigapNode, ...]) -> None:
        path = self._ensure_writable()
        path.parent.mkdir(parents=True, exist_ok=True)
        document = {
            "nodes": [irrigap_node_to_dict(node) for node in nodes],
        }
        tmp_name = ""
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=path.parent,
                prefix=path.name + ".",
                suffix=".tmp",
                delete=False,
            ) as handle:
                tmp_name = handle.name
                json.dump(document, handle, ensure_ascii=False, indent=2)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, path)
        except OSError as exc:
            if tmp_name:
                try:
                    Path(tmp_name).unlink(missing_ok=True)
                except OSError:
                    pass
            raise IrrigapCatalogError(f"cannot persist Irrigap catalog {path}: {exc}") from exc

    def create_node(self, value: Mapping[str, Any]) -> IrrigapNode:
        node = self._validated_node(value)
        with self._lock:
            if any(existing.id.upper() == node.id.upper() for existing in self._nodes):
                raise IrrigapCatalogConflict(f"Irrigap node {node.id!r} already exists")
            candidate = tuple(self._nodes) + (node,)
            self._persist_candidate(candidate)
            self._nodes = candidate
            return node

    def replace_node(self, node_id: str, value: Mapping[str, Any]) -> IrrigapNode:
        key = str(node_id or "").strip().upper()
        if not key:
            raise IrrigapCatalogError("node id must not be empty")
        payload = dict(value)
        payload_id = str(payload.get("id") or "").strip().upper()
        if payload_id and payload_id != key:
            raise IrrigapCatalogError("payload id must match node id in URL")
        payload["id"] = key
        node = self._validated_node(payload)

        with self._lock:
            index = next(
                (idx for idx, existing in enumerate(self._nodes) if existing.id.upper() == key),
                None,
            )
            if index is None:
                raise IrrigapCatalogNotFound(f"Irrigap node {key!r} not found")
            candidate_list = list(self._nodes)
            candidate_list[index] = node
            candidate = tuple(candidate_list)
            self._persist_candidate(candidate)
            self._nodes = candidate
            return node

    def delete_node(self, node_id: str) -> IrrigapNode:
        key = str(node_id or "").strip().upper()
        if not key:
            raise IrrigapCatalogError("node id must not be empty")
        with self._lock:
            index = next(
                (idx for idx, existing in enumerate(self._nodes) if existing.id.upper() == key),
                None,
            )
            if index is None:
                raise IrrigapCatalogNotFound(f"Irrigap node {key!r} not found")
            deleted = self._nodes[index]
            candidate = tuple(
                node for idx, node in enumerate(self._nodes) if idx != index
            )
            if not candidate:
                raise IrrigapCatalogConflict("Irrigap catalog must contain at least one node")
            self._persist_candidate(candidate)
            self._nodes = candidate
            return deleted


__all__ = [
    "IrrigapCatalog",
    "IrrigapCatalogConflict",
    "IrrigapCatalogError",
    "IrrigapCatalogManager",
    "IrrigapCatalogNotFound",
    "IrrigapCatalogReadOnly",
    "irrigap_node_to_dict",
    "load_irrigap_catalog",
    "parse_irrigap_catalog_json",
]
