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


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


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


def parse_args() -> argparse.Namespace:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    parser = argparse.ArgumentParser(
        description=(
            "Passively capture ChirpStack telemetry samples as JSONL for parser/calibration analysis."
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
    topic = args.topic.strip() or f"application/{args.app_id}/device/+/event/up"
    output = Path(args.output).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)

    wanted_devices = {str(item).strip() for item in args.device if str(item).strip()}
    stop = threading.Event()
    connected = threading.Event()
    counter = 0
    devices: Counter[str] = Counter()
    ports: Counter[str] = Counter()
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
        rc = int(reason_code)
        if rc != 0:
            print(f"ERROR MQTT connect rc={rc}", file=sys.stderr, flush=True)
            stop.set()
            return
        result, mid = client.subscribe(topic, qos=args.qos)
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
        print("STOP      Ctrl+C", flush=True)

    def on_disconnect(client, userdata, *callback_args):
        if not stop.is_set():
            print("WARN MQTT disconnected; paho will attempt reconnect", file=sys.stderr, flush=True)

    def on_message(client, userdata, msg):
        nonlocal counter, decode_failures, json_failures
        received_at = time.time()
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
                "capture_schema": "smarter-adapter.chirpstack-sample/1",
                "received_at": received_at,
                "received_at_utc": datetime.fromtimestamp(received_at, timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "topic": msg.topic,
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
                "capture_schema": "smarter-adapter.chirpstack-sample/1",
                "received_at": received_at,
                "received_at_utc": datetime.fromtimestamp(received_at, timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "topic": msg.topic,
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

        decoded_preview = record.get("data_decoded_utf8")
        if decoded_preview is None:
            decoded_preview = record.get("parse_error", "binary")
        print(
            f"[{counter:05d}] device={device_name or '<unknown>'} "
            f"fPort={f_port} data={str(decoded_preview)[:140]}",
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
    print(f"SAVED     {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
