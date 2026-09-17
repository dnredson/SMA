from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib import error, request


class AtomError(RuntimeError):
    """An Atom HTTP or GraphQL operation failed."""

    def __init__(self, message: str, status: Optional[int] = None) -> None:
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class AtomConfig:
    base_url: str
    graphql_url: str = ""
    token: str = ""
    username: str = ""
    password: str = ""
    timeout: float = 10.0
    refresh_skew_seconds: float = 30.0

    def __post_init__(self) -> None:
        if not self.base_url.strip():
            raise ValueError("AtomConfig.base_url must not be empty")

    @property
    def graphql_endpoint(self) -> str:
        if self.graphql_url:
            return self.graphql_url.rstrip("/")
        return self.base_url.rstrip("/") + "/graphql"


class TokenManager:
    """Owns Atom authentication state independently from devices/plugins.

    Static service tokens remain valid until Atom rejects them. When username
    and password are configured, login tokens are renewed before their
    expiration (when `expiresAt` is parseable) and once after an authentication
    failure.
    """

    def __init__(
        self,
        *,
        token: str = "",
        username: str = "",
        password: str = "",
        refresh_skew_seconds: float = 30.0,
    ) -> None:
        self.username = username
        self.password = password
        self.refresh_skew_seconds = max(float(refresh_skew_seconds), 0.0)
        self._token = token
        self._expires_at: Optional[float] = None

    @property
    def can_login(self) -> bool:
        return bool(self.username and self.password)

    def cached_token(self) -> str:
        if not self._token:
            return ""
        if self._expires_at is None:
            return self._token
        if time.time() + self.refresh_skew_seconds < self._expires_at:
            return self._token
        self._token = ""
        self._expires_at = None
        return ""

    def record_login(self, token: str, expires_at: Any = None) -> None:
        token = str(token or "")
        if not token:
            raise AtomError("Atom login response did not contain a token")
        self._token = token
        self._expires_at = _parse_expiry(expires_at)

    def invalidate(self) -> None:
        self._token = ""
        self._expires_at = None


def _parse_expiry(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if text.isdigit():
        return float(text)
    try:
        normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    except ValueError:
        return None


class AtomClient:
    """Small dependency-free client for the Atom GraphQL control plane."""

    tenant_fields = "id name alias status tags attributes createdAt updatedAt"
    resource_fields = (
        "id kind name alias tenantId ownerId objectGroupIds attributes createdAt updatedAt"
    )

    _login_mutation = """
    mutation Login($input: LoginInput!) {
      login(input: $input) {
        token entityId sessionId expiresAt emailVerified verificationRequired
      }
    }
    """

    def __init__(self, cfg: AtomConfig, opener: Any = None) -> None:
        self.cfg = cfg
        self.tokens = TokenManager(
            token=cfg.token,
            username=cfg.username,
            password=cfg.password,
            refresh_skew_seconds=cfg.refresh_skew_seconds,
        )
        self._opener = opener or request.urlopen

    def token(self) -> str:
        cached = self.tokens.cached_token()
        if cached:
            return cached
        if not self.tokens.can_login:
            raise AtomError(
                "Atom token unavailable; configure a service token or username/password"
            )
        result = self._graphql(
            self._login_mutation,
            {
                "input": {
                    "identifier": self.tokens.username,
                    "secret": self.tokens.password,
                    "kind": "password",
                }
            },
            auth=False,
            retry_auth=False,
        ).get("login") or {}
        self.tokens.record_login(result.get("token"), result.get("expiresAt"))
        return self.tokens.cached_token()

    def _post_graphql(
        self,
        query: str,
        variables: Dict[str, Any],
        *,
        auth: bool,
    ) -> Dict[str, Any]:
        body = json.dumps({"query": query, "variables": variables}).encode("utf-8")
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if auth:
            headers["Authorization"] = "Bearer " + self.token()
        req = request.Request(
            self.cfg.graphql_endpoint,
            data=body,
            headers=headers,
            method="POST",
        )
        try:
            response_ctx = self._opener(req, timeout=max(self.cfg.timeout, 0.1))
            with response_ctx as response:
                raw = response.read() or b"{}"
        except error.HTTPError as exc:
            detail = (exc.read() or b"").decode("utf-8", errors="replace")
            raise AtomError(f"Atom HTTP {exc.code}: {detail[:500]}", exc.code) from exc
        except (error.URLError, TimeoutError, OSError) as exc:
            raise AtomError(f"Atom request failed: {exc}") from exc
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise AtomError("Atom returned invalid JSON") from exc

    @staticmethod
    def _auth_graphql_error(errors: Any) -> bool:
        for item in errors or []:
            if not isinstance(item, dict):
                continue
            extensions = item.get("extensions") or {}
            code = str(extensions.get("code") or "").upper()
            message = str(item.get("message") or "").lower()
            if code in {"UNAUTHENTICATED", "UNAUTHORIZED"}:
                return True
            if "unauthenticated" in message or "invalid token" in message:
                return True
        return False

    def _graphql(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
        *,
        auth: bool = True,
        retry_auth: bool = True,
    ) -> Dict[str, Any]:
        variables = variables or {}
        try:
            response = self._post_graphql(query, variables, auth=auth)
        except AtomError as exc:
            if auth and retry_auth and exc.status == 401 and self.tokens.can_login:
                self.tokens.invalidate()
                return self._graphql(query, variables, auth=True, retry_auth=False)
            raise

        errors = response.get("errors")
        if errors:
            if auth and retry_auth and self._auth_graphql_error(errors) and self.tokens.can_login:
                self.tokens.invalidate()
                return self._graphql(query, variables, auth=True, retry_auth=False)
            messages = "; ".join(
                str(item.get("message", item)) if isinstance(item, dict) else str(item)
                for item in errors
            )
            raise AtomError("Atom GraphQL error: " + messages)

        data = response.get("data")
        if not isinstance(data, dict):
            raise AtomError("Atom GraphQL response did not contain data")
        return data

    def list_workspaces(self, limit: int = 100) -> List[Dict[str, Any]]:
        query = f"""
        query ListWorkspaces($limit: Int, $offset: Int) {{
          tenants(limit: $limit, offset: $offset) {{
            total items {{ {self.tenant_fields} }}
          }}
        }}
        """
        return self._paged(query, "tenants", {}, limit)

    def create_workspace(
        self,
        name: str,
        *,
        alias: str = "",
        attributes: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        mutation = f"""
        mutation CreateWorkspace($input: CreateTenantInput!) {{
          createTenant(input: $input) {{ {self.tenant_fields} }}
        }}
        """
        inp: Dict[str, Any] = {"name": name}
        if alias:
            inp["alias"] = alias
        if attributes:
            inp["attributes"] = attributes
        return dict(self._graphql(mutation, {"input": inp}).get("createTenant") or {})

    def list_channels(
        self,
        tenant_id: str,
        *,
        kind: str = "channel",
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        query = f"""
        query ListChannels($tenantId: ID, $kind: String, $limit: Int, $offset: Int) {{
          resources(tenantId: $tenantId, kind: $kind, limit: $limit, offset: $offset) {{
            total items {{ {self.resource_fields} }}
          }}
        }}
        """
        return self._paged(
            query,
            "resources",
            {"tenantId": tenant_id, "kind": kind},
            limit,
        )

    def create_channel(
        self,
        tenant_id: str,
        name: str,
        *,
        alias: str = "",
        attributes: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        mutation = f"""
        mutation CreateChannel($input: CreateResourceInput!) {{
          createResource(input: $input) {{ {self.resource_fields} }}
        }}
        """
        inp: Dict[str, Any] = {"tenantId": tenant_id, "name": name, "kind": "channel"}
        if alias:
            inp["alias"] = alias
        if attributes:
            inp["attributes"] = attributes
        return dict(self._graphql(mutation, {"input": inp}).get("createResource") or {})

    def _paged(
        self,
        query: str,
        field: str,
        variables: Dict[str, Any],
        limit: int,
    ) -> List[Dict[str, Any]]:
        page_size = max(1, min(int(limit), 500))
        offset = 0
        result: List[Dict[str, Any]] = []
        while True:
            page_vars = dict(variables)
            page_vars.update({"limit": page_size, "offset": offset})
            page = self._graphql(query, page_vars).get(field) or {}
            items = list(page.get("items") or [])
            result.extend(items)
            total = int(page.get("total") or len(result))
            if not items or len(result) >= total:
                return result
            offset += len(items)


__all__ = ["AtomClient", "AtomConfig", "AtomError", "TokenManager"]
