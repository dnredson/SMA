from __future__ import annotations

import json
import logging
import urllib.parse
import http.client
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("publisher")


class HttpPublisher:
    """Publishes normalized SenML through the current FluxMQ HTTP API."""

    def __init__(self, cfg: Dict[str, Any]) -> None:
        self.base = str(cfg.get("http_adapter_url", "http://localhost:8008")).rstrip("/")
        self.timeout = float(cfg.get("publish_timeout_ms", 5000)) / 1000.0
        self.log_full_body = bool(cfg.get("publisher_log_full_body", False))

    def _modern_path(self, tenant_id: str, channel_id: str) -> str:
        return (
            "/"
            + "/".join(
                urllib.parse.quote(value, safe="")
                for value in (tenant_id, "channels", channel_id, "messages")
            )
        )

    def _request(
        self,
        path: str,
        headers: Dict[str, str],
        body: bytes,
        method: str = "POST",
    ) -> Tuple[int, str, bytes]:
        parsed = urllib.parse.urlparse(self.base)
        conn_cls = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
        conn = conn_cls(
            parsed.hostname,
            parsed.port or (443 if parsed.scheme == "https" else 80),
            timeout=self.timeout,
        )
        base_path = parsed.path.rstrip("/")
        full_path = (base_path + path) or "/"
        try:
            if log.isEnabledFor(logging.DEBUG):
                log.debug("HTTP %s %s%s", method, self.base, path)
                if self.log_full_body or len(body) <= 8192:
                    log.debug("body: %s", body.decode("utf-8", "ignore"))
                else:
                    log.debug("body.len=%d", len(body))
            conn.request(method, full_path, body=body, headers=headers)
            response = conn.getresponse()
            response_body = response.read() or b""
            return response.status, response.reason, response_body
        finally:
            conn.close()

    def publish(
        self,
        *,
        tenant_id: str,
        channel_id: str,
        device_id: str,
        atom_token: str,
        senml: List[Dict[str, Any]],
        subtopic: str = "",
    ) -> Tuple[bool, Optional[str]]:
        """Send a SenML batch using Atom's bearer-authenticated publish route.

        FluxMQ's current HTTP endpoint authenticates the adapter as a bearer
        token and receives the Atom device identity in the JSON envelope.
        """
        if not tenant_id or not device_id or not atom_token:
            return False, "tenant_id, device_id and atom_token are required for Atom publish"
        payload: Any = {"device_id": device_id, "subtopic": subtopic, "payload": senml}
        path = self._modern_path(tenant_id, channel_id)
        headers = {
            "Authorization": "Bearer " + atom_token,
            "Content-Type": "application/json",
        }
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        try:
            status, reason, response_body = self._request(path, headers, body)
        except OSError as exc:
            return False, f"HTTP transport error: {exc}"
        if 200 <= status < 300:
            return True, None
        return False, f"HTTP {status} {reason}; body: {response_body.decode('utf-8', 'ignore')}"


__all__ = ["HttpPublisher"]
