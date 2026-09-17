from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, Optional
from urllib import error, parse, request


class PublishError(RuntimeError):
    """A FluxMQ HTTP publish failed."""

    def __init__(self, message: str, *, status: Optional[int] = None, body: str = "") -> None:
        super().__init__(message)
        self.status = status
        self.body = body


@dataclass(frozen=True)
class PublishResult:
    status: int
    body: Dict[str, Any]
    raw_body: str = ""


class FluxMQPublisher:
    """Bearer-authenticated publisher for Magistrala's current FluxMQ route."""

    def __init__(
        self,
        base_url: str,
        token_provider: Callable[[], str],
        *,
        invalidate_token: Optional[Callable[[], None]] = None,
        timeout: float = 10.0,
        opener: Any = None,
    ) -> None:
        self.base_url = str(base_url or "").rstrip("/")
        if not self.base_url:
            raise ValueError("FluxMQPublisher.base_url must not be empty")
        self.token_provider = token_provider
        self.invalidate_token = invalidate_token
        self.timeout = max(float(timeout), 0.1)
        self._opener = opener or request.urlopen

    def _url(self, workspace_id: str, channel_id: str) -> str:
        workspace = parse.quote(str(workspace_id), safe="")
        channel = parse.quote(str(channel_id), safe="")
        return f"{self.base_url}/{workspace}/channels/{channel}/messages"

    def _once(
        self,
        *,
        workspace_id: str,
        channel_id: str,
        device_id: str,
        senml: Iterable[Dict[str, Any]],
        subtopic: str,
    ) -> PublishResult:
        token = str(self.token_provider() or "")
        if not token:
            raise PublishError("Atom token provider returned an empty token")

        envelope = {
            "device_id": str(device_id),
            "subtopic": str(subtopic or ""),
            "payload": list(senml),
        }
        body = json.dumps(envelope, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        req = request.Request(
            self._url(workspace_id, channel_id),
            data=body,
            headers={
                "Accept": "application/json",
                "Authorization": "Bearer " + token,
                "Content-Type": "application/json",
            },
            method="POST",
        )

        try:
            response_ctx = self._opener(req, timeout=self.timeout)
            with response_ctx as response:
                raw = response.read() or b""
                status = int(getattr(response, "status", response.getcode()))
        except error.HTTPError as exc:
            raw = exc.read() or b""
            detail = raw.decode("utf-8", errors="replace")
            raise PublishError(
                f"FluxMQ HTTP {exc.code}: {detail[:500]}",
                status=exc.code,
                body=detail,
            ) from exc
        except (error.URLError, TimeoutError, OSError) as exc:
            raise PublishError(f"FluxMQ request failed: {exc}") from exc

        text = raw.decode("utf-8", errors="replace")
        try:
            parsed = json.loads(text) if text else {}
        except json.JSONDecodeError:
            parsed = {}

        if status < 200 or status >= 300:
            raise PublishError(
                f"FluxMQ HTTP {status}: {text[:500]}",
                status=status,
                body=text,
            )
        return PublishResult(status=status, body=parsed, raw_body=text)

    def publish(
        self,
        *,
        workspace_id: str,
        channel_id: str,
        device_id: str,
        senml: Iterable[Dict[str, Any]],
        subtopic: str = "",
    ) -> PublishResult:
        if not workspace_id or not channel_id or not device_id:
            raise ValueError("workspace_id, channel_id and device_id are required")

        payload = list(senml)
        if not payload:
            raise ValueError("senml payload must not be empty")

        try:
            return self._once(
                workspace_id=workspace_id,
                channel_id=channel_id,
                device_id=device_id,
                senml=payload,
                subtopic=subtopic,
            )
        except PublishError as exc:
            if exc.status != 401 or self.invalidate_token is None:
                raise
            self.invalidate_token()
            return self._once(
                workspace_id=workspace_id,
                channel_id=channel_id,
                device_id=device_id,
                senml=payload,
                subtopic=subtopic,
            )


__all__ = ["FluxMQPublisher", "PublishError", "PublishResult"]
