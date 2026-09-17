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
    device_type_fields = (
        "id tenantId key name: displayName description status createdAt updatedAt"
    )
    device_type_version_fields = (
        "id profileId version jsonSchema uiSchema status createdAt"
    )
    entity_fields = (
        "id kind profileId profileVersionId name alias externalId tenantId "
        "objectGroupIds status attributes createdAt updatedAt"
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

    def list_device_types(
        self,
        tenant_id: str,
        *,
        status: str = "",
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """List Atom profiles that Magistrala exposes as device types."""
        query = f"""
        query DeviceTypes(
          $objectKind: String,
          $kind: String,
          $tenantId: ID,
          $status: String,
          $limit: Int,
          $offset: Int
        ) {{
          profiles(
            objectKind: $objectKind,
            kind: $kind,
            tenantId: $tenantId,
            status: $status,
            limit: $limit,
            offset: $offset
          ) {{
            total
            items {{ {self.device_type_fields} }}
          }}
        }}
        """
        variables: Dict[str, Any] = {
            "objectKind": "entity",
            "kind": "device",
            "tenantId": tenant_id,
        }
        if status:
            variables["status"] = status
        return self._paged(query, "profiles", variables, limit)

    def create_device_type(
        self,
        tenant_id: str,
        key: str,
        name: str,
        *,
        description: str = "",
        status: str = "active",
    ) -> Dict[str, Any]:
        mutation = f"""
        mutation CreateDeviceType($input: CreateProfileInput!) {{
          createProfile(input: $input) {{ {self.device_type_fields} }}
        }}
        """
        inp: Dict[str, Any] = {
            "tenantId": tenant_id,
            "objectKind": "entity",
            "kind": "device",
            "key": key,
            "displayName": name,
            "status": status,
        }
        if description:
            inp["description"] = description
        return dict(self._graphql(mutation, {"input": inp}).get("createProfile") or {})

    def list_device_type_versions(self, profile_id: str) -> List[Dict[str, Any]]:
        query = f"""
        query DeviceTypeVersions($profileId: ID!) {{
          profileVersions(profileId: $profileId) {{
            {self.device_type_version_fields}
          }}
        }}
        """
        versions = list(
            self._graphql(query, {"profileId": profile_id}).get("profileVersions") or []
        )
        versions.sort(key=lambda item: int(item.get("version") or 0))
        return versions

    def create_device_type_version(
        self,
        profile_id: str,
        *,
        version: int,
        json_schema: Dict[str, Any],
        ui_schema: Optional[Dict[str, Any]] = None,
        status: str = "active",
    ) -> Dict[str, Any]:
        mutation = f"""
        mutation CreateDeviceTypeVersion(
          $profileId: ID!,
          $input: CreateProfileVersionInput!
        ) {{
          createProfileVersion(profileId: $profileId, input: $input) {{
            {self.device_type_version_fields}
          }}
        }}
        """
        inp: Dict[str, Any] = {
            "version": int(version),
            "jsonSchema": json_schema,
            "status": status,
        }
        if ui_schema is not None:
            inp["uiSchema"] = ui_schema
        return dict(
            self._graphql(
                mutation,
                {"profileId": profile_id, "input": inp},
            ).get("createProfileVersion")
            or {}
        )

    def list_devices(
        self,
        tenant_id: str,
        *,
        external_id: str = "",
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        query = f"""
        query Devices(
          $tenantId: ID,
          $kind: String,
          $externalId: String,
          $limit: Int,
          $offset: Int
        ) {{
          entities(
            tenantId: $tenantId,
            kind: $kind,
            externalId: $externalId,
            limit: $limit,
            offset: $offset
          ) {{
            total
            items {{ {self.entity_fields} }}
          }}
        }}
        """
        variables: Dict[str, Any] = {
            "tenantId": tenant_id,
            "kind": "device",
        }
        if external_id:
            variables["externalId"] = external_id
        return self._paged(query, "entities", variables, limit)

    def create_device(
        self,
        tenant_id: str,
        external_id: str,
        *,
        profile_id: str,
        profile_version_id: str = "",
        name: str = "",
        alias: str = "",
        attributes: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        mutation = f"""
        mutation CreateDevice($input: CreateEntityInput!) {{
          createEntity(input: $input) {{ {self.entity_fields} }}
        }}
        """
        inp: Dict[str, Any] = {
            "tenantId": tenant_id,
            "kind": "device",
            "profileId": profile_id,
            "name": name or external_id,
            "externalId": external_id,
            "attributes": attributes or {},
        }
        if profile_version_id:
            inp["profileVersionId"] = profile_version_id
        if alias:
            inp["alias"] = alias
        return dict(self._graphql(mutation, {"input": inp}).get("createEntity") or {})

    def capability_id(self, action_name: str) -> str:
        query = """
        query Actions($limit: Int!, $offset: Int!) {
          actions(limit: $limit, offset: $offset) {
            total
            items { id name }
          }
        }
        """
        offset = 0
        while True:
            page = self._graphql(query, {"limit": 100, "offset": offset}).get("actions") or {}
            items = list(page.get("items") or [])
            for item in items:
                if str(item.get("name") or "") == action_name:
                    capability_id = str(item.get("id") or "")
                    if capability_id:
                        return capability_id
            total = int(page.get("total") or 0)
            if not items or offset + len(items) >= total:
                break
            offset += len(items)
        raise AtomError(f"Atom capability not found: {action_name}")

    def ensure_publish_policy(
        self,
        tenant_id: str,
        device_id: str,
        channel_id: str,
    ) -> bool:
        """Ensure a device has direct `publish` permission on a channel.

        Returns True only when a new direct policy was created.
        """
        query = """
        query DevicePolicies(
          $tenantId: ID,
          $subjectKind: SubjectKind,
          $subjectId: ID,
          $limit: Int,
          $offset: Int
        ) {
          directPolicies(
            tenantId: $tenantId,
            subjectKind: $subjectKind,
            subjectId: $subjectId,
            limit: $limit,
            offset: $offset
          ) {
            total
            items {
              id
              subjectKind
              subjectId
              permissionBlock {
                id
                objectKind
                objectType
                objectId
                scopeMode
                effect
                actions { id name }
              }
            }
          }
        }
        """
        offset = 0
        while True:
            page = self._graphql(
                query,
                {
                    "tenantId": tenant_id,
                    "subjectKind": "entity",
                    "subjectId": device_id,
                    "limit": 100,
                    "offset": offset,
                },
            ).get("directPolicies") or {}
            items = list(page.get("items") or [])
            for item in items:
                block = item.get("permissionBlock") or {}
                actions = {
                    str(action.get("name") or "")
                    for action in (block.get("actions") or [])
                }
                if (
                    block.get("objectKind") == "resource"
                    and block.get("objectType") == "resource:channel"
                    and block.get("objectId") == channel_id
                    and str(block.get("effect") or "allow") == "allow"
                    and "publish" in actions
                ):
                    return False
            total = int(page.get("total") or 0)
            if not items or offset + len(items) >= total:
                break
            offset += len(items)

        action_id = self.capability_id("publish")
        block_mutation = """
        mutation CreatePermissionBlock($input: CreatePermissionBlockInput!) {
          createPermissionBlock(input: $input) {
            id
          }
        }
        """
        block = self._graphql(
            block_mutation,
            {
                "input": {
                    "tenantId": tenant_id,
                    "scopeMode": "object",
                    "objectKind": "resource",
                    "objectType": "resource:channel",
                    "objectId": channel_id,
                    "effect": "allow",
                    "actionIds": [action_id],
                }
            },
        ).get("createPermissionBlock") or {}
        block_id = str(block.get("id") or "")
        if not block_id:
            raise AtomError("Atom did not return the created permission block")

        policy_mutation = """
        mutation CreateDirectPolicy($input: CreateDirectPolicyInput!) {
          createDirectPolicy(input: $input) { id }
        }
        """
        policy = self._graphql(
            policy_mutation,
            {
                "input": {
                    "tenantId": tenant_id,
                    "subjectKind": "entity",
                    "subjectId": device_id,
                    "permissionBlockId": block_id,
                }
            },
        ).get("createDirectPolicy") or {}
        if not policy.get("id"):
            raise AtomError("Atom did not return the created direct policy")
        return True

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
