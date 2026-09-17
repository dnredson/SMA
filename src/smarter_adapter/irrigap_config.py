from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Tuple

from .parsers.chirpstack_irrigap import IrrigapNode


@dataclass(frozen=True)
class IrrigapCatalog:
    nodes: Tuple[IrrigapNode, ...]
    source: str


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


__all__ = [
    "IrrigapCatalog",
    "load_irrigap_catalog",
    "parse_irrigap_catalog_json",
]
