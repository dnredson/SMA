from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional
from urllib import error, parse, request


class ReaderError(RuntimeError):
    """A Magistrala Timescale reader request failed."""

    def __init__(self, message: str, *, status: Optional[int] = None, body: str = "") -> None:
        super().__init__(message)
        self.status = status
        self.body = body


@dataclass(frozen=True)
class MessagesPage:
    offset: int
    limit: int
    total: int
    messages: tuple[Dict[str, Any], ...]
    order: str = "time"
    direction: str = "desc"
    format: str = "messages"

    def as_dict(self) -> Dict[str, Any]:
        return {
            "offset": self.offset,
            "limit": self.limit,
            "order": self.order,
            "dir": self.direction,
            "format": self.format,
            "total": self.total,
            "messages": list(self.messages),
        }


class TimescaleReaderClient:
    """Bearer-authenticated client for Magistrala's Timescale HTTP reader."""

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
            raise ValueError("TimescaleReaderClient.base_url must not be empty")
        self.token_provider = token_provider
        self.invalidate_token = invalidate_token
        self.timeout = max(float(timeout), 0.1)
        self._opener = opener or request.urlopen

    def _url(
        self,
        workspace_id: str,
        channel_id: str,
        *,
        device_id: str,
        limit: int,
        offset: int,
        order: str,
        direction: str,
        name: str = "",
    ) -> str:
        workspace = parse.quote(str(workspace_id), safe="")
        channel = parse.quote(str(channel_id), safe="")
        query = {
            # Current Magistrala reader exposes the plural filter key.
            "device_ids": str(device_id),
            "limit": str(int(limit)),
            "offset": str(int(offset)),
            "order": str(order),
            "dir": str(direction),
        }
        if name:
            query["name"] = str(name)
        return (
            f"{self.base_url}/{workspace}/channels/{channel}/messages?"
            + parse.urlencode(query)
        )

    @staticmethod
    def _page(payload: Any) -> MessagesPage:
        if not isinstance(payload, dict):
            raise ReaderError("Timescale reader returned a non-object JSON response")
        messages = payload.get("messages") or []
        if not isinstance(messages, list):
            raise ReaderError("Timescale reader response field 'messages' is not a list")
        return MessagesPage(
            offset=int(payload.get("offset", 0) or 0),
            limit=int(payload.get("limit", 0) or 0),
            total=int(payload.get("total", 0) or 0),
            messages=tuple(item for item in messages if isinstance(item, dict)),
            order=str(payload.get("order") or "time"),
            direction=str(payload.get("dir") or "desc"),
            format=str(payload.get("format") or "messages"),
        )

    def _once(
        self,
        workspace_id: str,
        channel_id: str,
        *,
        device_id: str,
        limit: int,
        offset: int,
        order: str,
        direction: str,
        name: str,
    ) -> MessagesPage:
        token = str(self.token_provider() or "")
        if not token:
            raise ReaderError("Atom token provider returned an empty token")
        req = request.Request(
            self._url(
                workspace_id,
                channel_id,
                device_id=device_id,
                limit=limit,
                offset=offset,
                order=order,
                direction=direction,
                name=name,
            ),
            headers={
                "Accept": "application/json",
                "Authorization": "Bearer " + token,
            },
            method="GET",
        )
        try:
            response_ctx = self._opener(req, timeout=self.timeout)
            with response_ctx as response:
                raw = response.read() or b"{}"
                status = int(getattr(response, "status", response.getcode()))
        except error.HTTPError as exc:
            raw = exc.read() or b""
            detail = raw.decode("utf-8", errors="replace")
            raise ReaderError(
                f"Timescale reader HTTP {exc.code}: {detail[:500]}",
                status=exc.code,
                body=detail,
            ) from exc
        except (error.URLError, TimeoutError, OSError) as exc:
            raise ReaderError(f"Timescale reader request failed: {exc}") from exc

        text = raw.decode("utf-8", errors="replace")
        if status < 200 or status >= 300:
            raise ReaderError(
                f"Timescale reader HTTP {status}: {text[:500]}",
                status=status,
                body=text,
            )
        try:
            payload = json.loads(text) if text else {}
        except json.JSONDecodeError as exc:
            raise ReaderError("Timescale reader returned invalid JSON", status=status, body=text) from exc
        return self._page(payload)

    def list_device_messages(
        self,
        workspace_id: str,
        channel_id: str,
        device_id: str,
        *,
        limit: int = 100,
        offset: int = 0,
        order: str = "time",
        direction: str = "desc",
        name: str = "",
    ) -> MessagesPage:
        if not workspace_id or not channel_id or not device_id:
            raise ValueError("workspace_id, channel_id and device_id are required")
        limit = min(max(int(limit), 1), 1000)
        offset = max(int(offset), 0)
        if order not in ("time", "value", "publisher"):
            raise ValueError("order must be time, value or publisher")
        if direction not in ("asc", "desc"):
            raise ValueError("dir must be asc or desc")

        kwargs = dict(
            device_id=device_id,
            limit=limit,
            offset=offset,
            order=order,
            direction=direction,
            name=name,
        )
        try:
            return self._once(workspace_id, channel_id, **kwargs)
        except ReaderError as exc:
            if exc.status != 401 or self.invalidate_token is None:
                raise
            self.invalidate_token()
            return self._once(workspace_id, channel_id, **kwargs)


__all__ = ["MessagesPage", "ReaderError", "TimescaleReaderClient"]
