from __future__ import annotations
import re, json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from . import wxt520_csv, wxt520_sdi, atmos41, teros12, greenstick_ul, greenstick_common


@dataclass
class NormalizeResult:
    accept: bool
    external_id: str = ""
    entries: List[Dict[str, Any]] | None = None
    meta: Dict[str, Any] | None = None


TOPIC_RE = re.compile(r"^([A-Za-z0-9]+)_([A-Za-z0-9]+)_([A-Za-z0-9-]+)$")


def _normalize_topic(raw: str) -> str:
    t = raw.strip().replace(" ", "_")
    t = re.sub(r"[^A-Za-z0-9_-]", "_", t).upper()
    return t[:80]


def _route_by_topic(topic: str) -> Optional[str]:
    t = topic.upper()
    if t.startswith("WXT520_"):
        return "wxt520"
    if t.startswith("ATMOS41_"):
        return "atmos41"
    if t.startswith("TEROS12_"):
        return "teros12"

    # aceita variações de tipagem e com/sem sufixo
    if (
        t.startswith("GREENSTICK")
        or t.startswith("GEENSTICK")
        or t.startswith("GREENSTICKS")
        or t.startswith("GEENSTICKS")
    ):
        return "greenstick"

    if t.startswith("TTN_"):
        return "ttn"
    return None


def detect_and_parse(topic: str, payload: bytes) -> NormalizeResult:
    norm = _normalize_topic(topic)  # NORMALIZA PRIMEIRO
    external_id = norm  # já está normalizado
    s = payload.decode("utf-8", errors="ignore").strip()

    # if not TOPIC_RE.match(norm):
    #    return NormalizeResult(accept=False)

    fam = _route_by_topic(norm)  # ROTEIA NO TOPIC NORMALIZADO
    if fam is None:
        if s.startswith("{"):
            try:
                js = json.loads(s)
                if (
                    isinstance(js, dict)
                    and "end_device_ids" in js
                    and "uplink_message" in js
                ):
                    ext2, entries, meta = greenstick_common.parse_chirpstack_json(js)
                    return NormalizeResult(True, ext2, entries, meta)
            except Exception:
                pass
        return NormalizeResult(accept=False)

    external_id = norm
    # --- WXT520 CSV & SDI-12 ---
    if fam == "wxt520":
        if s.startswith("0R0,") or s.startswith("0R3,"):
            entries, meta = wxt520_csv.parse(s)
            return NormalizeResult(True, external_id, entries, meta)
        if "+" in s and re.match(r"^\d\+", s):
            entries, meta = wxt520_sdi.parse(s)
            return NormalizeResult(True, external_id, entries, meta)
        return NormalizeResult(False)

    # --- ATMOS41 SDI-12 ---
    if fam == "atmos41" and "+" in s and re.match(r"^\d\+", s):
        entries, meta = atmos41.parse(s)
        return NormalizeResult(True, external_id, entries, meta)

    # --- TEROS12 SDI-12 ---
    if fam == "teros12" and "+" in s and re.match(r"^\d\+", s):
        entries, meta = teros12.parse(s)
        return NormalizeResult(True, external_id, entries, meta)

    # --- GREENSTICK UL (direto) ---
    # --- GREENSTICK UL/JSON ---
    # --- GREENSTICK (UL ou JSON do ChirpStack/TTN) ---
    if fam == "greenstick":
        if s.startswith("{"):
            try:
                js = json.loads(s)
                ext2, entries, meta = greenstick_common.parse_chirpstack_json(js)
                return NormalizeResult(True, ext2, entries, meta)
            except Exception:
                return NormalizeResult(False)
        if s.startswith("S|") or s.startswith("|"):
            entries, meta = greenstick_ul.parse(s)
            return NormalizeResult(True, external_id, entries, meta)
        return NormalizeResult(False)

    if fam == "geensticks":
        if s.startswith("{"):
            try:
                js = json.loads(s)
                ext2, entries, meta = greenstick_common.parse_chirpstack_json(js)
                return NormalizeResult(True, ext2, entries, meta)
            except Exception:
                return NormalizeResult(False)
        if s.startswith("S|") or s.startswith("|"):
            entries, meta = greenstick_ul.parse(s)
            return NormalizeResult(True, external_id, entries, meta)
        return NormalizeResult(False)
    # --- TTN JSON envelope (ultralight dentro) ---
    if fam == "ttn" and s.startswith("{"):
        try:
            js = json.loads(s)
            ext2, entries, meta = greenstick_ul.parse_ttn_json(js)
            return NormalizeResult(True, ext2, entries, meta)
        except Exception:
            return NormalizeResult(False)

    return NormalizeResult(accept=False)
