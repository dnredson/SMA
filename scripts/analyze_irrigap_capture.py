#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize passive SMA MQTT JSONL captures for parser/topology analysis."
    )
    parser.add_argument("capture", nargs="+", help="JSONL capture file(s)")
    parser.add_argument(
        "--catalog",
        default=str(ROOT / "config" / "irrigap.nodes.json"),
        help="Optional Irrigap catalog used to flag unknown node IDs.",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    return parser.parse_args()


def _load_catalog(path: str) -> set[str]:
    target = Path(path)
    if not target.exists():
        return set()
    try:
        value = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()
    nodes = value.get("nodes") if isinstance(value, dict) else None
    if not isinstance(nodes, list):
        return set()
    return {
        str(item.get("id") or "").strip().upper()
        for item in nodes
        if isinstance(item, dict) and str(item.get("id") or "").strip()
    }


def _pattern(pairs: Any) -> str:
    if not isinstance(pairs, dict) or not pairs:
        return "<none>"
    return "/".join(sorted(str(key) for key in pairs))


def _topic_event(topic: str) -> str:
    parts = [part for part in topic.split("/") if part]
    if "event" in parts:
        index = parts.index("event")
        if index + 1 < len(parts):
            return "event/" + parts[index + 1]
    if "state" in parts:
        index = parts.index("state")
        if index + 1 < len(parts):
            return "state/" + parts[index + 1]
    if "command" in parts:
        index = parts.index("command")
        if index + 1 < len(parts):
            return "command/" + parts[index + 1]
    return "direct" if "/" not in topic else "other"


def analyze(paths: list[str], catalog_ids: set[str]) -> dict[str, Any]:
    topics: Counter[str] = Counter()
    events: Counter[str] = Counter()
    devices: Counter[str] = Counter()
    direct_topics: Counter[str] = Counter()
    device_ports: dict[str, Counter[str]] = defaultdict(Counter)
    device_patterns: dict[str, Counter[str]] = defaultdict(Counter)
    node_counts: Counter[str] = Counter()
    node_sentinels: Counter[str] = Counter()
    gateway_events: dict[str, Counter[str]] = defaultdict(Counter)
    gateway_times: dict[str, list[float]] = defaultdict(list)
    gateway_roots: dict[str, Counter[str]] = defaultdict(Counter)
    failures = 0
    rows = 0

    for raw_path in paths:
        path = Path(raw_path)
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    failures += 1
                    continue
                if not isinstance(record, dict):
                    failures += 1
                    continue
                rows += 1
                topic = str(record.get("topic") or "")
                topics[topic] += 1
                event = str(record.get("topic_event") or _topic_event(topic))
                events[event] += 1

                device = str(record.get("device_name") or "")
                if device:
                    devices[device] += 1
                    port = record.get("f_port")
                    device_ports[device][str(port) if port is not None else "<none>"] += 1
                    device_patterns[device][_pattern(record.get("decoded_pairs"))] += 1

                pairs = record.get("decoded_pairs")
                if isinstance(pairs, dict):
                    node = str(pairs.get("I") or "").strip().upper()
                    if node:
                        node_counts[node] += 1
                        if all(str(pairs.get(key) or "") == "-1" for key in ("M", "T", "C")):
                            node_sentinels[node] += 1

                if event == "direct" and topic:
                    direct_topics[topic] += 1

                parts = [part for part in topic.split("/") if part]
                if "gateway" in parts:
                    index = parts.index("gateway")
                    if index + 1 < len(parts):
                        gateway_id = parts[index + 1].lower()
                        gateway_events[gateway_id][event] += 1
                        root = parts[0] if index > 0 else ""
                        if root:
                            gateway_roots[gateway_id][root] += 1
                        if event == "event/stats":
                            try:
                                gateway_times[gateway_id].append(float(record.get("received_at")))
                            except (TypeError, ValueError):
                                pass

    gateways = []
    for gateway_id in sorted(gateway_events):
        times = sorted(gateway_times.get(gateway_id, []))
        intervals = [b - a for a, b in zip(times, times[1:]) if b >= a]
        gateways.append(
            {
                "gateway_id": gateway_id,
                "events": dict(gateway_events[gateway_id]),
                "topic_roots": dict(gateway_roots[gateway_id]),
                "stats_interval": {
                    "samples": len(intervals),
                    "median_seconds": statistics.median(intervals) if intervals else None,
                    "min_seconds": min(intervals) if intervals else None,
                    "max_seconds": max(intervals) if intervals else None,
                },
            }
        )

    nodes = []
    all_nodes = sorted(set(node_counts) | catalog_ids)
    for node in all_nodes:
        nodes.append(
            {
                "node_id": node,
                "captured": int(node_counts.get(node, 0)),
                "sentinel_packets": int(node_sentinels.get(node, 0)),
                "in_catalog": node in catalog_ids if catalog_ids else None,
            }
        )

    return {
        "rows": rows,
        "json_failures": failures,
        "topics": [{"topic": name, "count": count} for name, count in topics.most_common()],
        "events": dict(events.most_common()),
        "devices": [
            {
                "device_name": name,
                "count": count,
                "f_ports": dict(device_ports[name].most_common()),
                "payload_key_patterns": dict(device_patterns[name].most_common()),
            }
            for name, count in devices.most_common()
        ],
        "nodes": nodes,
        "gateways": gateways,
        "direct_topics": [
            {"topic": name, "count": count} for name, count in direct_topics.most_common()
        ],
    }


def print_text(report: dict[str, Any]) -> None:
    print(f"ROWS      {report['rows']} (json failures={report['json_failures']})")
    print("\nTOP EVENTS")
    for event, count in report["events"].items():
        print(f"  {count:6d}  {event}")

    print("\nCHIRPSTACK DEVICES")
    for item in report["devices"]:
        ports = ", ".join(f"{key}:{value}" for key, value in item["f_ports"].items())
        patterns = ", ".join(
            f"{key}:{value}" for key, value in item["payload_key_patterns"].items()
        )
        print(f"  {item['device_name']}: n={item['count']} ports=[{ports}] keys=[{patterns}]")

    print("\nNODES")
    for item in report["nodes"]:
        catalog = "" if item["in_catalog"] is None else (" catalog" if item["in_catalog"] else " UNKNOWN")
        print(
            f"  {item['node_id']}: captured={item['captured']} "
            f"sentinel={item['sentinel_packets']}{catalog}"
        )

    print("\nGATEWAYS")
    for item in report["gateways"]:
        interval = item["stats_interval"]
        median = interval["median_seconds"]
        interval_text = "n/a" if median is None else f"{median:.3f}s"
        print(
            f"  {item['gateway_id']}: events={item['events']} roots={item['topic_roots']} "
            f"stats_median={interval_text}"
        )

    print("\nDIRECT MQTT TOPICS")
    for item in report["direct_topics"]:
        print(f"  {item['count']:6d}  {item['topic']}")

    print("\nTOPICS")
    for item in report["topics"]:
        print(f"  {item['count']:6d}  {item['topic']}")


def main() -> int:
    args = parse_args()
    report = analyze(args.capture, _load_catalog(args.catalog))
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print_text(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
