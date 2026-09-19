from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional
from urllib import error, parse, request


class RulesError(RuntimeError):
    """A Magistrala Rules Engine HTTP operation failed."""

    def __init__(self, message: str, status: Optional[int] = None) -> None:
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class PersistenceRuleRef:
    id: str
    workspace_id: str
    channel_id: str
    name: str
    status: str
    created: bool = False
    enabled: bool = False


class RulesClient:
    """Small dependency-free client for the Magistrala Rules Engine.

    The Smarter Adapter uses Rules as part of its base data-plane bootstrap:
    messages published to a channel are not persisted by the Timescale writer
    until a rule emits them through the ``save_senml`` output.
    """

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
            raise ValueError("RulesClient base_url must not be empty")
        self.token_provider = token_provider
        self.invalidate_token = invalidate_token
        self.timeout = max(float(timeout), 0.1)
        self._opener = opener or request.urlopen

    def _request(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
        *,
        retry_auth: bool = True,
    ) -> tuple[int, Dict[str, Any]]:
        data = None
        headers = {
            "Accept": "application/json",
            "Authorization": "Bearer " + self.token_provider(),
        }
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = request.Request(
            self.base_url + path,
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with self._opener(req, timeout=self.timeout) as response:
                raw = response.read() or b"{}"
                status = int(getattr(response, "status", response.getcode()))
        except error.HTTPError as exc:
            detail = (exc.read() or b"").decode("utf-8", errors="replace")
            if exc.code == 401 and retry_auth and self.invalidate_token is not None:
                self.invalidate_token()
                return self._request(method, path, payload, retry_auth=False)
            raise RulesError(
                f"Rules HTTP {exc.code}: {detail[:500]}",
                exc.code,
            ) from exc
        except (error.URLError, TimeoutError, OSError) as exc:
            raise RulesError(f"Rules request failed: {exc}") from exc

        if not raw:
            return status, {}
        try:
            return status, json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise RulesError("Rules Engine returned invalid JSON", status) from exc

    def list_rules(
        self,
        workspace_id: str,
        *,
        input_channel: str = "",
        status: str = "all",
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        query: Dict[str, Any] = {"status": status, "limit": int(limit)}
        if input_channel:
            query["input_channel"] = input_channel
        path = (
            "/"
            + parse.quote(workspace_id, safe="")
            + "/rules?"
            + parse.urlencode(query)
        )
        _, body = self._request("GET", path)
        return list(body.get("rules") or [])

    def get_rule(self, workspace_id: str, rule_id: str) -> Dict[str, Any]:
        _, body = self._request(
            "GET",
            "/"
            + parse.quote(workspace_id, safe="")
            + "/rules/"
            + parse.quote(rule_id, safe=""),
        )
        return body

    def delete_rule(self, workspace_id: str, rule_id: str) -> None:
        self._request(
            "DELETE",
            "/"
            + parse.quote(workspace_id, safe="")
            + "/rules/"
            + parse.quote(rule_id, safe=""),
        )

    @staticmethod
    def is_managed_persistence_rule(rule: Dict[str, Any]) -> bool:
        metadata = rule.get("metadata") or {}
        tags = set(str(value) for value in (rule.get("tags") or []))
        outputs = rule.get("outputs") or []
        has_save = any(
            isinstance(item, dict) and str(item.get("type") or "") == "save_senml"
            for item in outputs
        )
        return (
            isinstance(metadata, dict)
            and metadata.get("managed_by") == "smarter-adapter"
            and metadata.get("purpose") == "senml-persistence"
            and has_save
            and "smarter-adapter" in tags
            and "persistence" in tags
        )

    def create_rule(
        self,
        workspace_id: str,
        *,
        name: str,
        input_channel: str,
        input_topic: str = "",
        metadata: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
        logic: Optional[Dict[str, Any]] = None,
        outputs: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "name": name,
            "input_channel": input_channel,
            "input_topic": input_topic,
            "logic": logic
            or {
                "type": 0,
                "value": "return message.payload",
                "mode": "sandboxed",
            },
            "outputs": outputs or [{"type": "save_senml"}],
        }
        if metadata is not None:
            payload["metadata"] = metadata
        if tags is not None:
            payload["tags"] = tags
        _, body = self._request(
            "POST",
            "/" + parse.quote(workspace_id, safe="") + "/rules",
            payload,
        )
        return body

    def enable_rule(self, workspace_id: str, rule_id: str) -> Dict[str, Any]:
        _, body = self._request(
            "POST",
            "/"
            + parse.quote(workspace_id, safe="")
            + "/rules/"
            + parse.quote(rule_id, safe="")
            + "/enable",
        )
        return body

    @staticmethod
    def _has_save_senml(rule: Dict[str, Any]) -> bool:
        return any(
            isinstance(item, dict) and str(item.get("type") or "") == "save_senml"
            for item in (rule.get("outputs") or [])
        )

    def ensure_senml_persistence(
        self,
        workspace_id: str,
        channel_id: str,
        *,
        name: str = "smarter-adapter-save-senml",
        input_topic: str = "",
    ) -> PersistenceRuleRef:
        matches = [
            rule
            for rule in self.list_rules(
                workspace_id,
                input_channel=channel_id,
                status="all",
                limit=100,
            )
            if str(rule.get("name") or "") == name
            and str(rule.get("input_channel") or "") == channel_id
        ]
        if len(matches) > 1:
            raise RulesError(
                f"multiple persistence rules share name {name!r} on channel {channel_id!r}"
            )

        created = False
        if matches:
            rule = matches[0]
            if str(rule.get("input_topic") or "") != input_topic:
                raise RulesError(
                    f"persistence rule {name!r} has unexpected input_topic "
                    f"{rule.get('input_topic')!r}; expected {input_topic!r}"
                )
            if not self._has_save_senml(rule):
                raise RulesError(
                    f"persistence rule {name!r} exists but has no save_senml output"
                )
        else:
            try:
                rule = self.create_rule(
                    workspace_id,
                    name=name,
                    input_channel=channel_id,
                    input_topic=input_topic,
                    metadata={
                        "managed_by": "smarter-adapter",
                        "purpose": "senml-persistence",
                    },
                    tags=["smarter-adapter", "senml", "persistence"],
                    outputs=[{"type": "save_senml"}],
                )
            except RulesError as exc:
                if exc.status == 422 and "rule limit for this edition reached" in str(exc).lower():
                    raise RulesError(
                        "Magistrala refused the persistence rule because the rule limit for this edition "
                        "is already consumed. Release an existing Smarter Adapter-managed persistence "
                        "rule before bootstrapping another workspace.",
                        exc.status,
                    ) from exc
                raise
            created = True

        rule_id = str(rule.get("id") or "")
        if not rule_id:
            raise RulesError("Rules persistence response is missing id")

        enabled = False
        status = str(rule.get("status") or "")
        if status != "enabled":
            rule = self.enable_rule(workspace_id, rule_id)
            status = str(rule.get("status") or "enabled")
            enabled = True

        if status != "enabled":
            raise RulesError(
                f"persistence rule {name!r} did not become enabled (status={status!r})"
            )

        return PersistenceRuleRef(
            id=rule_id,
            workspace_id=workspace_id,
            channel_id=channel_id,
            name=str(rule.get("name") or name),
            status=status,
            created=created,
            enabled=enabled,
        )


__all__ = ["PersistenceRuleRef", "RulesClient", "RulesError"]
