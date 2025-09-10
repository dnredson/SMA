from __future__ import annotations
import re, json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from . import wxt520_csv, wxt520_sdi, atmos41, teros12, greenstick_ul


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
    if topic.startswith("WXT520_"):
        return "wxt520"
    if topic.startswith("ATMOS41_"):
        return "atmos41"
    if topic.startswith("TEROS12_"):
        return "teros12"
    if topic.startswith("GREENSTICK_"):
        return "greenstick"
    if topic.startswith("TTN_"):
        return "ttn"
    return None


def detect_and_parse(topic: str, payload: bytes) -> NormalizeResult:
    if not TOPIC_RE.match(topic):
        return NormalizeResult(accept=False)

    fam = _route_by_topic(topic)
    if fam is None:
        return NormalizeResult(accept=False)

    external_id = _normalize_topic(topic)
    s = payload.decode("utf-8", errors="ignore").strip()

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
    if fam == "greenstick":
        if s.startswith("{"):  # pode ser JSON (ex.: debug manual), tenta TTN json
            try:
                js = json.loads(s)
                ext2, entries, meta = greenstick_ul.parse_ttn_json(js)
                return NormalizeResult(True, ext2, entries, meta)
            except Exception:
                return NormalizeResult(False)
        # ultralight: começa com 'S|' ou '|' (linhas subsequentes)
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
