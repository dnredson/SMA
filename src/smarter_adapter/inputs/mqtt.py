from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Callable, Optional

from ..models import RawEvent

try:
    import paho.mqtt.client as mqtt
except ImportError:  # pragma: no cover - exercised only without optional dependency
    mqtt = None


@dataclass(frozen=True)
class MQTTInputConfig:
    host: str
    port: int = 1883
    topic: str = "#"
    qos: int = 0
    username: str = ""
    password: str = ""
    client_id: str = "smarter-adapter-v2"
    keepalive: int = 60
    source: str = "mqtt"

    def __post_init__(self) -> None:
        if not self.host.strip():
            raise ValueError("MQTT host must not be empty")
        if not self.topic.strip():
            raise ValueError("MQTT topic must not be empty")
        if self.port <= 0 or self.port > 65535:
            raise ValueError("MQTT port must be in 1..65535")
        if self.qos not in (0, 1, 2):
            raise ValueError("MQTT qos must be 0, 1, or 2")


class MQTTInput:
    """Transport-only MQTT input.

    This class knows nothing about ChirpStack, SenML, Atom or Magistrala. Every
    received MQTT message is converted to a transport-neutral ``RawEvent`` and
    delivered to the callback supplied by the runtime.
    """

    def __init__(
        self,
        config: MQTTInputConfig,
        on_event: Callable[[RawEvent], None],
        *,
        client_factory=None,
    ) -> None:
        self.config = config
        self.on_event = on_event
        self._connected = threading.Event()
        self._stopped = threading.Event()
        self._last_error: Optional[str] = None

        if client_factory is not None:
            self._client = client_factory(config.client_id)
        else:
            if mqtt is None:
                raise RuntimeError(
                    "paho-mqtt is required for MQTTInput; install requirements.txt"
                )
            self._client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id=config.client_id,
                protocol=mqtt.MQTTv311,
            )

        if config.username:
            self._client.username_pw_set(config.username, config.password or None)

        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

    @property
    def connected(self) -> bool:
        return self._connected.is_set()

    @property
    def last_error(self) -> Optional[str]:
        return self._last_error

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        code = int(reason_code)
        if code != 0:
            self._last_error = f"MQTT connect failed with reason code {code}"
            self._connected.clear()
            return
        result, _mid = client.subscribe(self.config.topic, qos=self.config.qos)
        if result != 0:
            self._last_error = f"MQTT subscribe failed with result {result}"
            self._connected.clear()
            return
        self._last_error = None
        self._connected.set()

    def _on_disconnect(
        self,
        client,
        userdata,
        disconnect_flags,
        reason_code,
        properties=None,
    ):
        self._connected.clear()
        code = int(reason_code)
        if code != 0 and not self._stopped.is_set():
            self._last_error = f"MQTT disconnected with reason code {code}"

    def _on_message(self, client, userdata, message):
        event = RawEvent(
            source=self.config.source,
            topic=str(message.topic or ""),
            payload=bytes(message.payload or b""),
            metadata={
                "qos": int(message.qos),
                "retain": bool(message.retain),
            },
        )
        self.on_event(event)

    def connect(self) -> None:
        self._stopped.clear()
        self._client.connect(
            self.config.host,
            self.config.port,
            keepalive=self.config.keepalive,
        )

    def loop_forever(self) -> None:
        self.connect()
        try:
            self._client.loop_forever(retry_first_connection=True)
        finally:
            self._connected.clear()

    def start(self) -> None:
        self.connect()
        self._client.loop_start()

    def stop(self) -> None:
        self._stopped.set()
        try:
            self._client.disconnect()
        finally:
            self._client.loop_stop()
            self._connected.clear()


__all__ = ["MQTTInput", "MQTTInputConfig"]
