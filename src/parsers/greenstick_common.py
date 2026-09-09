from __future__ import annotations
import re
import time
from typing import Any, Dict, List, Tuple
import json, base64, datetime as dt

_UL_KV = re.compile(r"\|")  # split por '|'
_UL_MAP = {
    "VB": ("battery.voltage", "V"),
    "BT": ("battery.level", "%"),
    "I": ("index", "1"),
    "S": ("serial", None),
}

_num_re = re.compile(r"^[-+]?\d+(?:\.\d+)?$")


def _parse_ts_yymmddhhmm(syy: str) -> int:
    # syy = '2509091140' -> 2025-09-09 11:40:00 UTC (assumindo século 2000+)
    if len(syy) != 10 or not syy.isdigit():
        return int(time.time())
    yy = int(syy[0:2])
    mm = int(syy[2:4])
    dd = int(syy[4:6])
    hh = int(syy[6:8])
    mi = int(syy[8:10])
    year = 2000 + yy
    import datetime as dt

    try:
        return int(
            dt.datetime(year, mm, dd, hh, mi, tzinfo=dt.timezone.utc).timestamp()
        )
    except Exception:
        return int(time.time())


def parse_ultralight_line(ul: str):
    """
    Retorna (entries, meta). 'entries' é uma lista de dicts SenML (sem bn/bt aqui),
    'meta' contém pelo menos 'bt' quando S|timestamp existir (ou será definido por quem chama).
    """
    entries = []
    meta = {}

    if not ul:
        return entries, meta

    parts = [p.strip() for p in ul.strip().split("|") if p.strip() != ""]
    # se vier como uma string única com vírgulas, tenta split por vírgula
    if len(parts) == 1 and "," in parts[0]:
        parts = [p.strip() for p in parts[0].split(",") if p.strip()]

    it = iter(parts)
    for k in it:
        v = next(it, None)
        if v is None:
            break

        key = k.upper()
        name, unit = _UL_MAP.get(key, (f"ul.{key}", None))

        # regra especial: se a key S costuma carregar serial ou timestamp; aqui deixamos como string
        rec = {"n": name}
        if _num_re.match(v) and key not in ("S",):  # S guardamos como string
            try:
                rec["v"] = float(v)
            except Exception:
                rec["vs"] = v
        else:
            rec["vs"] = v

        if unit:
            rec["u"] = unit

        entries.append(rec)

    # tenta inferir bt a partir de S quando S for carimbo epoch (nem sempre é o caso)
    # seu UL usa 'S' como serial, então não definimos bt aqui; quem chama define
    return entries, meta


def _norm_external_from_device_id(dev_id: str) -> str:
    # 'greenstick-10-lanapre' -> 'GREENSTICK_10_LANAPRE'
    t = re.sub(r"[^A-Za-z0-9_-]", "_", (dev_id or "greenstick_unknown")).upper()
    t = t.replace("-", "_")
    if not t.startswith("GREENSTICK_"):
        t = "GREENSTICK_" + t
    return t[:80]


def _iso_to_epoch(iso: str) -> int:
    try:
        return int(dt.datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp())
    except Exception:
        return int(time.time())


def _b64_to_text(b64s: str) -> str:
    try:
        raw = base64.b64decode(b64s)
        return raw.decode("utf-8", errors="ignore").strip()
    except Exception:
        return ""


def parse_chirpstack_json(js: dict) -> tuple[str, list[dict], dict]:
    """
    Extrai ultralight de um JSON de uplink do ChirpStack/TTN e devolve:
      (external_id, entries_senml, meta_enriquecido)
    Requer helpers já existentes no módulo:
      - _norm_external_from_device_id(str) -> str
      - _iso_to_epoch(str) -> int
      - _b64_to_text(str) -> str
      - parse_ultralight_line(str) -> tuple[list[dict], dict]
    """
    import re, time

    # Mapa local para fallback via decoded_payload.ngsi
    _UL_MAP = {
        "VB": ("battery.voltage", "V"),
        "BT": ("battery.level", "%"),
        "I": ("index", "1"),
        "S": ("serial", None),
    }
    _num_re = re.compile(r"^[-+]?\d+(?:\.\d+)?$")

    ed = js.get("end_device_ids") or {}
    up = js.get("uplink_message") or {}

    dev_id = ed.get("device_id") or ed.get("dev_id") or "greenstick_unknown"
    app_id = (ed.get("application_ids") or {}).get("application_id")
    dev_eui = ed.get("dev_eui")
    join_eui = ed.get("join_eui")
    dev_addr = ed.get("dev_addr")

    external_id = _norm_external_from_device_id(dev_id)

    # 1) Tenta pegar o ultralight já decodificado
    dp = up.get("decoded_payload") or js.get("decoded_payload") or {}
    ul = dp.get("ultralight")
    if not isinstance(ul, str) or "|" not in ul:
        # 2) Fallback: decodifica frm_payload base64
        frm = up.get("frm_payload")
        if isinstance(frm, str):
            ul = _b64_to_text(frm)
        # 3) Fallback extra: procurar string com '|' no decoded_payload
        if (not ul) and isinstance(dp, dict):
            for _, v in dp.items():
                if isinstance(v, str) and "|" in v:
                    ul = v
                    break

    # 2.x) Converte UL → SenML
    entries, meta = ([], {})
    if ul:
        try:
            # Use the typed Greenstick parser when available. The generic UL
            # parser is retained for unknown keys, but M#/T#/C# must keep the
            # canonical soil names used by the quality rules and SenML output.
            from .greenstick_ul import parse as parse_greenstick_ul

            entries, meta = parse_greenstick_ul(ul)
        except Exception:
            entries, meta = [], {}

    # 3) bt: se o UL não trouxe, usa unix(ms) do decoded_payload
    if "bt" not in meta:
        unix = dp.get("unix")
        if isinstance(unix, (int, float)):
            # unix em ms -> s
            meta["bt"] = int(int(unix) / 1000)
        else:
            # received_at do uplink ou do root como fallback
            meta["bt"] = _iso_to_epoch(
                up.get("received_at") or js.get("received_at") or ""
            )

    # 4) Enriquecimento de metadados
    meta.setdefault("sensor", "greenstick")
    if app_id:
        meta["app_id"] = app_id
    if dev_eui:
        meta["dev_eui"] = dev_eui
    if join_eui:
        meta["join_eui"] = join_eui
    if dev_addr:
        meta["dev_addr"] = dev_addr

    # uplink details
    if "f_port" in up:
        meta["f_port"] = up.get("f_port")
    if "f_cnt" in up:
        meta["f_cnt"] = up.get("f_cnt")
    if "consumed_airtime" in up:
        meta["airtime"] = up.get("consumed_airtime")
    if "packet_error_rate" in up:
        meta["per"] = up.get("packet_error_rate")

    # RF / gateway (pega o primeiro gateway)
    rxm = up.get("rx_metadata") or []
    if rxm:
        gw = rxm[0]
        gw_ids = gw.get("gateway_ids") or {}
        if gw_ids.get("gateway_id"):
            meta["gateway_id"] = gw_ids["gateway_id"]
        if "rssi" in gw:
            meta["rssi"] = gw["rssi"]  # dBm
        if "snr" in gw:
            meta["snr"] = gw["snr"]  # dB
        loc = gw.get("location") or {}
        if "latitude" in loc:
            meta["lat"] = loc["latitude"]
        if "longitude" in loc:
            meta["lon"] = loc["longitude"]
        if "altitude" in loc:
            meta["alt"] = loc["altitude"]

    # PHY settings
    st = up.get("settings") or {}
    if "frequency" in st:
        meta["freq_hz"] = st["frequency"]
    lora = (st.get("data_rate") or {}).get("lora") or {}
    if "spreading_factor" in lora:
        meta["sf"] = lora["spreading_factor"]
    if "bandwidth" in lora:
        meta["bw_hz"] = lora["bandwidth"]
    if "coding_rate" in lora:
        meta["coding_rate"] = lora["coding_rate"]

    # Bateria do NS (quando houver)
    lbp = (up.get("last_battery_percentage") or {}).get("value")
    if isinstance(lbp, (int, float)):
        meta["battery.level.ns"] = float(
            lbp
        )  # mantida separada do valor reportado pelo UL

    # 5) Fallback se o UL não gerou entradas: usa decoded_payload.ngsi
    if not entries:
        ngsi = dp.get("ngsi") or {}
        for k, v in ngsi.items():
            key = str(k).upper()
            name, unit = _UL_MAP.get(key, (f"ul.{key}", None))
            rec = {"n": name}
            # decide v (numérico) vs vs (string)
            if isinstance(v, (int, float)) or (
                _num_re.match(str(v)) and key not in ("S",)
            ):
                try:
                    rec["v"] = float(v)
                except Exception:
                    rec["vs"] = str(v)
            else:
                rec["vs"] = str(v)
            if unit:
                rec["u"] = unit
            entries.append(rec)

    # 6) Se ainda vazio, retorna só meta (caller poderá publicar apenas frame-base)
    if not entries:
        return external_id, [], meta

    # 7) Injeção de bn/bt no primeiro registro (modelo do seu publisher)
    bt = int(meta.get("bt", int(time.time())))
    bn = f"{external_id}:"
    if entries:
        if "bn" not in entries[0]:
            entries[0]["bn"] = bn
        if "bt" not in entries[0]:
            entries[0]["bt"] = bt

    return external_id, entries, meta
