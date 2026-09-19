#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import os
import signal
import sys
import threading
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from smarter_adapter.envfile import EnvFileError, load_default_env

try:
    import paho.mqtt.client as mqtt
except ImportError as exc:  # pragma: no cover - operator-facing dependency error
    raise SystemExit("paho-mqtt is required; activate SMA's .venv first") from exc


DEFAULT_APP_ID = "bf9286b1-b02c-4e86-976f-f7d66b75aeb7"


def _env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default)).strip()


def _safe_decode(data_b64: Any) -> tuple[str | None, str | None, str | None]:
    if not isinstance(data_b64, str) or not data_b64:
        return None, None, None
    try:
        raw = base64.b64decode(data_b64, validate=True)
    except (ValueError, TypeError):
        return None, None, None
    try:
        text = raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        text = None
    return text, raw.hex(), base64.b64encode(raw).decode("ascii")


def _ultralight_pairs(text: str | None) -> dict[str, str]:
    if not text:
        return {}
    parts = [part.strip() for part in text.split("|")]
    result: dict[str, str] = {}
    index = 0
    while index + 1 < len(parts):
        key = parts[index]
        value = parts[index + 1]
        if key and key[0].isalpha():
            result[key] = value
            index += 2
        else:
            index += 1
    return result


def _client(client_id: str):
    kwargs = {
        "client_id": client_id,
        "protocol": mqtt.MQTTv311,
        "transport": "tcp",
    }
    callback_api = getattr(mqtt, "CallbackAPIVersion", None)
    if callback_api is not None:
        kwargs["callback_api_version"] = callback_api.VERSION2
    return mqtt.Client(**kwargs)


def _connect_failed(reason_code: Any) -> bool:
    """Handle both Paho 1 integer rc and Paho 2 ReasonCode objects."""
    is_failure = getattr(reason_code, "is_failure", None)
    if isinstance(is_failure, bool):
        return is_failure
    value = getattr(reason_code, "value", reason_code)
    try:
        return int(value) != 0
    except (TypeError, ValueError):
        return str(reason_code).strip().lower() not in {"0", "success"}


def _topic_parts(topic: str) -> dict[str, Any]:
    """Extract common ChirpStack topic coordinates without assuming event/up."""
    parts = [part for part in str(topic).split("/") if part]
    result: dict[str, Any] = {
        "topic_root": parts[0] if parts else None,
        "topic_event": None,
        "topic_application_id": None,
        "topic_device_id": None,
    }
    if len(parts) >= 2 and parts[0] == "application":
        result["topic_application_id"] = parts[1]
    if len(parts) >= 4 and parts[2] == "device":
        result["topic_device_id"] = parts[3]
    if "event" in parts:
        index = parts.index("event")
        if index + 1 < len(parts):
            result["topic_event"] = parts[index + 1]
    return result


def parse_args() -> argparse.Namespace:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    parser = argparse.ArgumentParser(
        description=(
            "Passively capture MQTT/ChirpStack samples as JSONL for parser, calibration, "
            "inventory and traffic analysis."
        )
    )
    parser.add_argument("--broker", default=_env("SMA_MQTT_HOST", "189.18.9.13"))
    parser.add_argument("--port", type=int, default=int(_env("SMA_MQTT_PORT", "1883")))
    parser.add_argument("--app-id", default=DEFAULT_APP_ID)
    parser.add_argument(
        "--topic",
        default="",
        help="MQTT topic override. Default: application/<app-id>/device/+/event/up",
    )
    parser.add_argument(
        "--all-topics",
        action="store_true",
        help="Subscribe to # for passive broker inventory/overnight capture.",
    )
    parser.add_argument("--qos", type=int, choices=(0, 1, 2), default=0)
    parser.add_argument("--username", default=_env("SMA_MQTT_USERNAME"))
    parser.add_argument("--password", default=_env("SMA_MQTT_PASSWORD"))
    parser.add_argument(
        "--device",
        action="append",
        default=[],
        help="Capture only this deviceName. Repeat for multiple devices.",
    )
    parser.add_argument(
        "--output",
        default=str(ROOT / "captures" / f"irrigap-{stamp}.jsonl"),
        help="JSONL output path.",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Stop after N matching messages; 0 means unlimited.",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=0.0,
        help="Stop after N seconds; 0 means unlimited.",
    )
    parser.add_argument(
        "--client-id",
        default=f"sma-sample-capture-{os.getpid()}",
    )
    return parser.parse_args()


def main() -> int:
    try:
        load_default_env(ROOT)
    except EnvFileError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    args = parse_args()
    if args.all_topics and args.topic.strip():
        print("ERROR: use either --all-topics or --topic, not both", file=sys.stderr)
        return 2

    if args.all_topics:
        topic = "#"
    else:
        topic = args.topic.strip() or f"application/{args.app_id}/device/+/event/up"

    output = Path(args.output).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)

    wanted_devices = {str(item).strip() for item in args.device if str(item).strip()}
    stop = threading.Event()
    connected = threading.Event()
    counter = 0
    devices: Counter[str] = Counter()
    ports: Counter[str] = Counter()
    topics: Counter[str] = Counter()
    events: Counter[str] = Counter()
    decode_failures = 0
    json_failures = 0
    started = time.time()

    client = _client(args.client_id)
    if args.username:
        client.username_pw_set(args.username, args.password or None)

    handle = output.open("a", encoding="utf-8", buffering=1)

    def request_stop(*_args) -> None:
        stop.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    def on_connect(client, userdata, flags, reason_code, properties=None):
        if _connect_failed(reason_code):
            print(
                f"ERROR MQTT connect reason={reason_code}",
                file=sys.stderr,
                flush=True,
            )
            stop.set()
            return
        result, _mid = client.subscribe(topic, qos=args.qos)
        if result != mqtt.MQTT_ERR_SUCCESS:
            print(f"ERROR MQTT subscribe rc={result}", file=sys.stderr, flush=True)
            stop.set()
            return
        connected.set()
        print(
            f"CAPTURING broker={args.broker}:{args.port} topic={topic} qos={args.qos}",
            flush=True,
        )
        print(f"OUTPUT    {output}", flush=True)
        if wanted_devices:
            print("FILTER    devices=" + ",".join(sorted(wanted_devices)), flush=True)
        if args.all_topics:
            print("MODE      passive all-topic broker inventory", flush=True)
        print("STOP      Ctrl+C", flush=True)

    def on_disconnect(client, userdata, *callback_args):
        if not stop.is_set():
            print("WARN MQTT disconnected; paho will attempt reconnect", file=sys.stderr, flush=True)

    def on_message(client, userdata, msg):
        nonlocal counter, decode_failures, json_failures
        received_at = time.time()
        topic_meta = _topic_parts(msg.topic)
        try:
            envelope = json.loads(msg.payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            json_failures += 1
            envelope = None

        if isinstance(envelope, dict):
            device_info = envelope.get("deviceInfo")
            if not isinstance(device_info, dict):
                device_info = {}
            device_name = str(device_info.get("deviceName") or "")
            if wanted_devices and device_name not in wanted_devices:
                return
            data_b64 = envelope.get("data")
            decoded_utf8, decoded_hex, normalized_b64 = _safe_decode(data_b64)
            if data_b64 and decoded_hex is None:
                decode_failures += 1
            f_port = envelope.get("fPort")
            record = {
                "capture_schema": "smarter-adapter.mqtt-sample/2",
                "received_at": received_at,
                "received_at_utc": datetime.fromtimestamp(received_at, timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "topic": msg.topic,
                **topic_meta,
                "mqtt_qos": int(msg.qos),
                "mqtt_retain": bool(msg.retain),
                "chirpstack_time": envelope.get("time"),
                "device_name": device_name or None,
                "dev_eui": device_info.get("devEui"),
                "device_profile_name": device_info.get("deviceProfileName"),
                "application_id": device_info.get("applicationId"),
                "application_name": device_info.get("applicationName"),
                "f_port": f_port,
                "data_base64": normalized_b64 or data_b64,
                "data_decoded_utf8": decoded_utf8,
                "data_decoded_hex": decoded_hex,
                "decoded_pairs": _ultralight_pairs(decoded_utf8),
                "envelope": envelope,
            }
        else:
            if wanted_devices:
                return
            device_name = ""
            f_port = None
            record = {
                "capture_schema": "smarter-adapter.mqtt-sample/2",
                "received_at": received_at,
                "received_at_utc": datetime.fromtimestamp(received_at, timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "topic": msg.topic,
                **topic_meta,
                "mqtt_qos": int(msg.qos),
                "mqtt_retain": bool(msg.retain),
                "payload_utf8": msg.payload.decode("utf-8", errors="replace"),
                "payload_base64": base64.b64encode(msg.payload).decode("ascii"),
                "parse_error": "invalid_json",
            }

        handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        handle.flush()
        counter += 1
        devices[device_name or "<unknown>"] += 1
        ports[str(f_port) if f_port is not None else "<none>"] += 1
        topics[msg.topic] += 1
        events[str(topic_meta.get("topic_event") or "<other>")] += 1

        decoded_preview = record.get("data_decoded_utf8")
        if decoded_preview is None:
            decoded_preview = record.get("parse_error") or record.get("topic_event") or "json"
        print(
            f"[{counter:05d}] event={topic_meta.get('topic_event') or '<other>'} "
            f"device={device_name or '<unknown>'} fPort={f_port} "
            f"topic={msg.topic} data={str(decoded_preview)[:120]}",
            flush=True,
        )

        if args.max_messages > 0 and counter >= args.max_messages:
            stop.set()

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    try:
        client.connect(args.broker, args.port, keepalive=60)
        client.loop_start()

        deadline = started + args.duration if args.duration > 0 else None
        while not stop.wait(0.25):
            if deadline is not None and time.time() >= deadline:
                stop.set()
    except Exception as exc:
        print(f"ERROR {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    finally:
        stop.set()
        try:
            client.disconnect()
        except Exception:
            pass
        try:
            client.loop_stop()
        except Exception:
            pass
        handle.flush()
        handle.close()

    elapsed = max(time.time() - started, 0.0)
    print(
        f"STOPPED messages={counter} elapsed={elapsed:.1f}s "
        f"json_failures={json_failures} decode_failures={decode_failures}",
        flush=True,
    )
    if devices:
        print("DEVICES   " + " ".join(f"{name}={count}" for name, count in devices.most_common()))
    if ports:
        print("PORTS     " + " ".join(f"{port}={count}" for port, count in ports.most_common()))
    if events:
        print("EVENTS    " + " ".join(f"{event}={count}" for event, count in events.most_common()))
    if topics:
        print(f"TOPICS    unique={len(topics)}")
        for name, count in topics.most_common(20):
            print(f"          {count:6d} {name}")
    print(f"SAVED     {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
