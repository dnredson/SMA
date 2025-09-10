from __future__ import annotations
import json
import re
import time
from typing import Any, Dict, List, Tuple
from .greenstick_common import parse_ultralight_line


def _norm_external_from_device_id(dev_id: str) -> str:
    # 'greenstick-8-nsaab' -> 'GREENSTICK_8_NSAAB'
    t = re.sub(r"[^A-Za-z0-9_-]", "_", dev_id).upper()
    t = t.replace("-", "_")
    if not t.startswith("GREENSTICK_"):
        t = "GREENSTICK_" + t
    return t[:80]


def _iso_to_epoch(iso: str) -> int:
    try:
        import datetime as dt

        return int(dt.datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp())
    except Exception:
        return int(time.time())


def parse(payload_json: str) -> Tuple[str, List[Dict[str, Any]], Dict[str, Any]]:
    """
    Recebe o JSON do TTN (string), extrai:
      - end_device_ids.device_id  -> external_id normalizado
      - uplink_message.decoded_payload.ultralight -> linha UL
      - opcionalmente decoded_payload.unix (ms) ou received_at -> bt se o 'S|' não vier
    Retorna (external_id, entries, meta)
    """
    data = json.loads(payload_json)

    # device_id
    dev_id = (
        data.get("end_device_ids", {}).get("device_id")
        or data.get("end_device_ids", {}).get("dev_id")
        or "greenstick_unknown"
    )
    external_id = _norm_external_from_device_id(dev_id)

    # achar UL
    ul = None
    up = data.get("uplink_message", {})
    dp = up.get("decoded_payload") or data.get("decoded_payload") or {}
    ul = dp.get("ultralight")
    if not ul and isinstance(dp, dict):
        # às vezes vem em outra chave (defensivo)
        for k, v in dp.items():
            if isinstance(v, str) and "|" in v and v.startswith(("S|", "|")):
                ul = v
                break
    if not ul:
        # não é payload esperado
        return external_id, [], {"sensor": "greenstick"}

    entries, meta = parse_ultralight_line(ul)

    # se o 'S|' não veio ou falhou, tenta bt por 'unix' (ms) ou 'received_at'
    if "bt" not in meta:
        unix = dp.get("unix")
        if isinstance(unix, (int, float)):
            bt = int(int(unix) / 1000)
        else:
            bt = _iso_to_epoch(up.get("received_at") or data.get("received_at") or "")
        meta["bt"] = bt

    # marcações extras úteis
    meta.setdefault("sensor", "greenstick")
    app_id = (
        data.get("end_device_ids", {}).get("application_ids", {}).get("application_id")
    )
    if app_id:
        meta["app_id"] = app_id

    return external_id, entries, meta
