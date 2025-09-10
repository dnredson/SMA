# src/core/mqtt.py
from __future__ import annotations
from typing import Callable, List
import time
import ssl
import random
import string
from urllib.parse import urlparse, unquote, parse_qs

try:
    import paho.mqtt.client as mqtt
except Exception as e:
    raise SystemExit("paho-mqtt não encontrado. Instale com: pip install paho-mqtt")

# Assinatura esperada pelo main
OnMessage = Callable[[str, bytes, float], None]


def _gen_client_id(prefix: str = "adapter-") -> str:
    return prefix + "".join(random.choices(string.ascii_lowercase + string.digits, k=8))


def run_mqtt_loop(
    broker_url: str,
    topics: List[str],
    on_message: OnMessage,
    log,
    qos: int = 1,
) -> None:
    """Conecta ao broker via paho-mqtt e fica em loop com reconexão automática.

    broker_url exemplos:
      - tcp://127.0.0.1:1883
      - tcp://user:pass@192.168.0.10:1883?client_id=mag-adapter&keepalive=60&clean=true
      - ssl://broker.example.com:8883
      - ws://host:9001/mqtt  |  wss://host:443/mqtt
    """
    parsed = urlparse(broker_url)
    scheme = (parsed.scheme or "tcp").lower()
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or (
        8883 if scheme in ("ssl", "mqtts", "wss") else (80 if scheme == "ws" else 1883)
    )
    user = unquote(parsed.username) if parsed.username else None
    password = unquote(parsed.password) if parsed.password else None
    params = parse_qs(parsed.query or "")

    client_id = params.get("client_id", [None])[0] or _gen_client_id()
    keepalive = int(params.get("keepalive", [60])[0])
    clean_flag = params.get("clean", ["true"])[0].lower() != "false"

    # Paho v1.x aceita clean_session; v2 usa clean_start. Tentamos ser compatíveis.
    transport = "websockets" if scheme in ("ws", "wss") else "tcp"
    try:
        client = mqtt.Client(
            client_id=client_id, clean_session=clean_flag, transport=transport
        )
    except TypeError:
        # Provável Paho v2
        client = mqtt.Client(client_id=client_id, transport=transport)

    # Integra logs do Paho ao logger do app
    client.enable_logger(log)

    # Credenciais (se passadas no URL)
    if user:
        client.username_pw_set(user, password=password or None)

    # TLS para ssl/mqtts/wss
    if scheme in ("ssl", "mqtts", "wss"):
        ctx = ssl.create_default_context()
        client.tls_set_context(ctx)

    # Reconnect com backoff
    try:
        client.reconnect_delay_set(min_delay=1, max_delay=30)
    except Exception:
        pass

    # Callbacks
    def _on_connect(cli, userdata, flags, rc, properties=None):
        if rc == 0:
            log.info("MQTT conectado em %s:%s (client_id=%s)", host, port, client_id)
            for t in topics:
                cli.subscribe(t, qos=qos)
                log.info("subscribed: %s (qos=%d)", t, qos)
        else:
            log.warning("falha de conexão MQTT rc=%s", rc)

    def _on_message(cli, userdata, msg):
        try:
            on_message(msg.topic, msg.payload, time.time())
        except Exception as e:
            log.exception("erro no handler de mensagem: %s", e)

    def _on_disconnect(cli, userdata, rc, properties=None):
        if rc != 0:
            log.warning(
                "MQTT desconectado inesperadamente (rc=%s); tentando reconectar…", rc
            )

    client.on_connect = _on_connect
    client.on_message = _on_message
    client.on_disconnect = _on_disconnect

    # WebSocket path (se houver)
    if scheme in ("ws", "wss") and parsed.path:
        try:
            client.ws_set_options(path=parsed.path)
        except Exception:
            pass

    # Conecta
    try:
        client.connect(host, port, keepalive=keepalive)
    except Exception as e:
        log.error("erro conectando ao broker MQTT %s: %s", broker_url, e)
        raise

    # Loop infinito com reconexão automática
    try:
        client.loop_forever(retry_first_connection=True)
    except TypeError:
        # Paho mais antigo
        client.loop_forever()
