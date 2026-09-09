from __future__ import annotations
import json
import logging
import os
import re
import signal
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from core.config import load_config
from core.storage import EntitiesStore
from core.registry import Registry, EnsureResult
from core.publisher import HttpPublisher
from core.api import start_api_server
from core.senml import build_senml
from core.mqtt import run_mqtt_loop
from parsers import detect_and_parse, NormalizeResult

logger = logging.getLogger("adapter")


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def setup_logging_from_cfg(cfg: Dict[str, Any]) -> None:
    import logging
    from logging.handlers import RotatingFileHandler
    from pathlib import Path

    level_name = str(cfg.get("log_level", "info")).upper()
    level = getattr(logging, level_name, logging.INFO)

    log_file = cfg.get("log_file")
    max_bytes = int(cfg.get("log_max_bytes", 0))  # 0 = sem rotação por tamanho
    backup_count = int(cfg.get("log_backup_count", 3))
    rotate_on_boot = bool(cfg.get("rotate_on_boot", False))

    # Limpa handlers existentes
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)

    fmt = "%(asctime)s %(levelname)s %(name)s: %(message)s"
    handlers = []

    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        if max_bytes > 0:
            fh = RotatingFileHandler(
                log_file, maxBytes=max_bytes, backupCount=backup_count
            )
        else:
            fh = logging.FileHandler(log_file)
        handlers.append(fh)
    else:
        # fallback para console (stdout/stderr)
        handlers.append(logging.StreamHandler())

    logging.basicConfig(level=level, format=fmt, handlers=handlers)

    # Se pediu rotação no boot e o handler suporta, faz o rollover agora
    if log_file and rotate_on_boot:
        for h in handlers:
            if isinstance(h, RotatingFileHandler):
                try:
                    h.doRollover()
                except Exception:
                    pass
                break


# -------------------- Quality/Alerts helpers --------------------


def _parse_mqtt_url(url: str) -> Tuple[str, int, bool]:
    """
    Suporta:
      - mqtt://host:1883
      - tcp://host:1883
      - ssl://host:8883
      - tls://host:8883
      - host:port
    Retorna (host, port, use_tls)
    """
    if not url:
        return "127.0.0.1", 1883, False
    s = url.strip()
    use_tls = False
    if "://" in s:
        scheme, rest = s.split("://", 1)
        use_tls = scheme.lower() in ("ssl", "tls", "mqtts")
        s = rest
    if ":" in s:
        host, p = s.rsplit(":", 1)
        try:
            port = int(p)
        except Exception:
            port = 8883 if use_tls else 1883
        return host, port, use_tls
    return s, (8883 if use_tls else 1883), use_tls


def _load_thresholds_from_json(
    quality_thresholds_json: Optional[str],
) -> Tuple[Dict[str, Dict[str, float]], List[Tuple[re.Pattern, Dict[str, float], str]]]:
    """
    Lê o JSON de limites (name -> {min,max}), separa exatos e com wildcard.
    Retorna (exact_map, patterns)
    """
    exact: Dict[str, Dict[str, float]] = {}
    pats: List[Tuple[re.Pattern, Dict[str, float], str]] = []

    if not quality_thresholds_json:
        return exact, pats

    try:
        data = json.loads(quality_thresholds_json)
        if not isinstance(data, dict):
            logger.warning(
                "quality_thresholds_json inválido (esperado objeto), ignorando."
            )
            return exact, pats
        for key, lim in data.items():
            if not isinstance(lim, dict):
                continue
            if "min" not in lim or "max" not in lim:
                continue
            try:
                vmin = float(lim["min"])
                vmax = float(lim["max"])
            except Exception:
                continue

            if "*" in key or "?" in key:
                rx = "^" + re.escape(key).replace("\\*", ".*").replace("\\?", ".") + "$"
                try:
                    pats.append((re.compile(rx), {"min": vmin, "max": vmax}, key))
                except re.error:
                    logger.warning(
                        "regex inválido para threshold '%s', ignorando.", key
                    )
            else:
                exact[key] = {"min": vmin, "max": vmax}
    except json.JSONDecodeError as e:
        logger.warning("quality_thresholds_json não é JSON válido: %s", e)

    return exact, pats


def _find_threshold(
    th_exact: Dict[str, Dict[str, float]],
    th_pats: List[Tuple[re.Pattern, Dict[str, float], str]],
    name: str,
) -> Optional[Tuple[float, float, str]]:
    """Procura limite pelo nome, priorizando match exato, senão primeiro wildcard que bater."""
    if name in th_exact:
        lm = th_exact[name]
        return float(lm["min"]), float(lm["max"]), name
    for rx, lm, key in th_pats:
        if rx.match(name):
            return float(lm["min"]), float(lm["max"]), key
    return None


def _publish_alert_once(
    addr: str, topic: str, payload: str, qos: int = 0, timeout: float = 3.0
) -> bool:
    """Publica um alerta MQTT 'one-shot' e aguarda a confirmação do publish."""
    try:
        import paho.mqtt.client as mqtt
    except Exception:
        logger.error("paho-mqtt não está instalado; alerta MQTT não enviado.")
        return False

    host, port, use_tls = _parse_mqtt_url(addr)
    client_id = f"adapter-alerts-{os.getpid()}-{int(time.time()*1000)%100000}"

    cli = mqtt.Client(client_id=client_id, clean_session=True, protocol=mqtt.MQTTv311)
    if use_tls:
        try:
            cli.tls_set()  # usa CA do sistema
        except Exception as e:
            logger.debug("tls_set falhou (prosseguindo sem custom CA): %s", e)

    try:
        logger.debug(
            "MQTT alert connect %s:%s tls=%s topic=%s", host, port, use_tls, topic
        )
        cli.connect(host, port, keepalive=10)
        cli.loop_start()

        info = cli.publish(topic, payload=payload, qos=qos, retain=False)
        ok = info.wait_for_publish(timeout=timeout) and (
            info.rc == mqtt.MQTT_ERR_SUCCESS
        )

        cli.loop_stop()
        cli.disconnect()

        if ok:
            logger.debug(
                "alerta MQTT publicado com sucesso em %s:%s → %s", host, port, topic
            )
        else:
            logger.warning(
                "falha ao publicar alerta MQTT (timeout/rc) em %s:%s → %s (rc=%s)",
                host,
                port,
                topic,
                getattr(info, "rc", "?"),
            )
        return ok
    except Exception as e:
        logger.warning("falha ao publicar alerta MQTT em %s (%s): %s", addr, topic, e)
        try:
            cli.loop_stop()
            cli.disconnect()
        except Exception:
            pass
        return False


def main() -> None:
    cfg_path = Path(os.environ.get("ADAPTER_CONFIG", "./config.toml"))
    cfg: Dict[str, Any] = load_config(cfg_path)

    setup_logging_from_cfg(cfg)
    logger.info("iniciando adapter…")

    # 1) serviços
    store = EntitiesStore(
        Path(cfg.get("entities_json_path", "./entities.json")), key_manager=None
    )
    registry = Registry(cfg=cfg, key_manager=None, store=store, cfg_path=cfg_path)
    publisher = HttpPublisher(cfg=cfg)
    start_api_server(registry, cfg)

    # --------- Quality/Alerts state (mutável p/ hot-reload) ----------
    qstate: Dict[str, Any] = {}
    qstate["quality_enable"] = bool(cfg.get("quality_enable", True))
    qstate["quality_alerts_enable"] = bool(cfg.get("quality_alerts_enable", True))
    qstate["alerts_addr"] = cfg.get("alerts_mqtt_address") or cfg.get(
        "mqtt_address", "tcp://127.0.0.1:1883"
    )
    qstate["alerts_topic_base"] = (
        (cfg.get("mqtt_alert_topic_base", "adapter/alerts") or "adapter/alerts")
        .strip()
        .rstrip("/")
    )
    qstate["alerts_qos"] = int(cfg.get("mqtt_alert_qos", 0))
    th_exact, th_pats = _load_thresholds_from_json(cfg.get("quality_thresholds_json"))
    qstate["th_exact"] = th_exact
    qstate["th_pats"] = th_pats

    # 2) SIGHUP → hot-reload
    _last_reload = 0.0

    def on_sighup(signum, frame):
        nonlocal _last_reload, cfg
        now = time.time()
        debounce = cfg.get("reload_debounce_ms", 3000) / 1000.0
        if now - _last_reload < debounce:
            return
        _last_reload = now
        new_cfg = load_config(cfg_path)

        # campos gerais
        for k in (
            "log_level",
            "mqtt_topics",
            "senml_max_batch",
            "batch_flush_ms",
            "publish_timeout_ms",
            "max_retries",
            "retry_backoff_ms",
            "ul_bt_accept_seconds",
        ):
            if k in new_cfg:
                cfg[k] = new_cfg[k]
        setup_logging_from_cfg(cfg)

        # quality/alerts
        qstate["quality_enable"] = bool(
            new_cfg.get("quality_enable", qstate["quality_enable"])
        )
        qstate["quality_alerts_enable"] = bool(
            new_cfg.get("quality_alerts_enable", qstate["quality_alerts_enable"])
        )
        qstate["alerts_addr"] = new_cfg.get(
            "alerts_mqtt_address", qstate["alerts_addr"]
        ) or new_cfg.get("mqtt_address", qstate["alerts_addr"])
        qstate["alerts_topic_base"] = (
            (
                new_cfg.get("mqtt_alert_topic_base", qstate["alerts_topic_base"])
                or qstate["alerts_topic_base"]
            )
            .strip()
            .rstrip("/")
        )
        qstate["alerts_qos"] = int(new_cfg.get("mqtt_alert_qos", qstate["alerts_qos"]))
        th_exact2, th_pats2 = _load_thresholds_from_json(
            new_cfg.get("quality_thresholds_json")
        )
        qstate["th_exact"], qstate["th_pats"] = th_exact2, th_pats2

        logger.info(
            "config recarregada (parcial) via SIGHUP; quality=%s alerts=%s",
            qstate["quality_enable"],
            qstate["quality_alerts_enable"],
        )

    signal.signal(signal.SIGHUP, on_sighup)

    # 3) callback de mensagem MQTT
    def handle_message(topic: str, payload_bytes: bytes, recv_ts: float) -> None:
        try:
            norm: NormalizeResult = detect_and_parse(topic, payload_bytes)
            if not norm.accept:
                logger.debug("ignorado tópico fora do padrão: %s", topic)
                return

            external_id = norm.external_id
            entries = norm.entries or []
            meta = norm.meta or {}

            # 3.1 garantir client/secret/channel/domain
            ensured: EnsureResult = registry.ensure_client(external_id, meta=meta)
            if not ensured.ok:
                logger.warning("ensure_client falhou: %s", ensured.error)
                return

            # 3.2 montar bt (segundos) com janela de aceitação
            now = int(recv_ts)
            bt = now
            accept_skew = int(cfg.get("ul_bt_accept_seconds", 600))
            bt_meta = (norm.meta or {}).get("bt") if norm.meta else None
            if isinstance(bt_meta, (int, float)):
                cand = int(bt_meta)
                if abs(cand - now) <= accept_skew:
                    bt = cand
                else:
                    logger.debug(
                        "bt fora da janela (%s vs now=%s, skew=%ss) — usando now",
                        cand,
                        now,
                        accept_skew,
                    )

            # 3.3 Quality: verificar limites e emitir alerta mqtt quando necessário
            if qstate.get("quality_enable", True) and entries:
                th_e: Dict[str, Dict[str, float]] = qstate.get("th_exact", {})
                th_p: List[Tuple[re.Pattern, Dict[str, float], str]] = qstate.get(
                    "th_pats", []
                )
                for it in entries:
                    try:
                        name = it.get("n")
                        val = it.get("v")
                        if name is None or not isinstance(val, (int, float)):
                            continue
                        found = _find_threshold(th_e, th_p, name)
                        if not found:
                            continue
                        vmin, vmax, _src = found
                        if (val < vmin) or (val > vmax):
                            # Log legível p/ LLMs / monitoramento:
                            logger.warning(
                                "Parameter out of range for entity %s: %s=%s (min=%s max=%s) sensor=%s",
                                external_id,
                                name,
                                val,
                                vmin,
                                vmax,
                                meta.get("sensor", "-"),
                            )
                            # Publicar alerta via MQTT (se habilitado)
                            if qstate.get("quality_alerts_enable", True):
                                alert = {
                                    "type": "threshold_violation",
                                    "external_id": external_id,
                                    "sensor": meta.get("sensor", "unknown"),
                                    "name": name,
                                    "value": float(val),
                                    "min": float(vmin),
                                    "max": float(vmax),
                                    "bt": bt,
                                    "ts": int(time.time()),
                                }
                                topic = f"{qstate['alerts_topic_base']}"
                                ok_pub = _publish_alert_once(
                                    qstate.get("alerts_addr", "tcp://127.0.0.1:1883"),
                                    topic,
                                    json.dumps(
                                        alert, separators=(",", ":"), ensure_ascii=False
                                    ),
                                    qos=int(qstate.get("alerts_qos", 0)),
                                )
                                if ok_pub:
                                    logger.debug(
                                        "alerta MQTT publicado: %s %s", topic, alert
                                    )
                                else:
                                    logger.debug(
                                        "falha ao publicar alerta MQTT: %s %s",
                                        topic,
                                        alert,
                                    )
                    except Exception as e:
                        logger.debug(
                            "falha ao avaliar thresholds para item %s: %s", it, e
                        )

            # 3.4 construir SenML canônico (primeiro item leva bn/bt)
            senml = build_senml(external_id, entries, bt)

            # 3.5 publicar no HTTP Adapter
            ok, err = publisher.publish(
                tenant_id=ensured.tenant_id,
                channel_id=ensured.channel_id,
                device_id=ensured.device_id,
                atom_token=registry.atom.token,
                senml=senml,
                subtopic="",  # sem subtopic
            )
            if ok:
                store.touch_last_seen(external_id)
                logger.debug(
                    "publicado com sucesso: %s (%d itens)", external_id, len(entries)
                )
            else:
                logger.warning("publicação falhou: %s", err)

        except Exception as e:
            logger.exception("erro ao processar mensagem MQTT: %s", e)

    # 4) loop MQTT
    run_mqtt_loop(
        broker_url=cfg.get("mqtt_address", "tcp://127.0.0.1:1883"),
        topics=cfg.get("mqtt_topics", ["#"]),
        on_message=handle_message,
        log=logger,
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
