# src/core/alerts.py
from __future__ import annotations
import json
import logging
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

log = logging.getLogger("alerts")

try:
    import paho.mqtt.client as mqtt  # type: ignore
except Exception:  # paho não instalado
    mqtt = None  # type: ignore


def _parse_broker_url(url: str) -> Tuple[str, int]:
    # espera "tcp://host:port" (igual ao resto do projeto)
    parsed = urlparse(url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 1883
    return host, port


class MqttNotifier:
    def __init__(
        self,
        *,
        broker_url: str,
        topic_base: str = "ALERTS",
        qos: int = 0,
        enabled: bool = True,
    ):
        self.enabled = enabled and (mqtt is not None)
        self.topic_base = (topic_base or "ALERTS").strip().strip("/")
        self.qos = int(qos)
        self._client: Optional["mqtt.Client"] = None  # type: ignore
        self._host, self._port = _parse_broker_url(broker_url)

    def _ensure_conn(self) -> None:
        if not self.enabled:
            return
        if self._client is not None:
            return
        try:
            cli = mqtt.Client(client_id="adapter-alerts", clean_session=True)  # type: ignore
            cli.connect(self._host, self._port, keepalive=30)
            cli.loop_start()
            self._client = cli
            log.debug("MqttNotifier conectado a %s:%d", self._host, self._port)
        except Exception as e:
            log.warning("MqttNotifier: falha ao conectar: %s", e)
            self.enabled = False

    def publish_threshold_violations(
        self, external_id: str, violations: List[Dict[str, Any]], ts: int
    ) -> None:
        if not self.enabled or not violations:
            return
        self._ensure_conn()
        if self._client is None:
            return
        topic = f"{self.topic_base}/{external_id}"
        payload = {
            "type": "threshold_violation",
            "external_id": external_id,
            "at": ts,
            "count": len(violations),
            "violations": violations,
        }
        try:
            j = json.dumps(payload, ensure_ascii=False)
            self._client.publish(topic, j, qos=self.qos, retain=False)
            log.debug("MQTT alert publicado em %s: %s", topic, j)
        except Exception as e:
            log.warning("MqttNotifier publish falhou: %s", e)
