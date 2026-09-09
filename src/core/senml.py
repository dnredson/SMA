from __future__ import annotations

from typing import Any, Dict, Iterable, List


_VALUE_FIELDS = ("v", "vs", "vb", "vd")


def build_senml(external_id: str, entries: Iterable[Dict[str, Any]], bt: int) -> List[Dict[str, Any]]:
    """Build one canonical SenML JSON batch from any current parser output.

    Parsers only describe measurements. This boundary owns the common base
    name/time fields and prevents a parser-specific header from leaking into
    the Atom/FluxMQ transport.
    """
    clean: List[Dict[str, Any]] = []
    for raw in entries:
        item = dict(raw)
        if not item.get("n"):
            continue
        if not any(field in item for field in _VALUE_FIELDS):
            continue
        for field in ("bn", "bt"):
            item.pop(field, None)
        clean.append(item)

    if not clean:
        return [{"bn": external_id + ":", "bt": int(bt)}]
    clean[0]["bn"] = external_id + ":"
    clean[0]["bt"] = int(bt)
    return clean


__all__ = ["build_senml"]
