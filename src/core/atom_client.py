from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib import error, request

log = logging.getLogger("atom")


class AtomError(RuntimeError):
    """An Atom HTTP or GraphQL operation failed."""

    def __init__(self, message: str, status: Optional[int] = None) -> None:
        super().__init__(message)
        self.status = status


@dataclass
class AtomConfig:
    url: str
    graphql_url: str
    token: str = ""
    username: str = ""
    password: str = ""
    timeout: float = 10.0
    tenant_id: str = ""
    channel_id: str = ""
    manage_policies: bool = True

    @classmethod
    def from_mapping(cls, cfg: Dict[str, Any]) -> "AtomConfig":
        url = str(
            cfg.get("atom_url")
            or os.getenv("ATOM_URL")
            or "http://localhost:8080"
        ).rstrip("/")
        graphql_url = str(
            cfg.get("atom_graphql_url")
            or os.getenv("ATOM_GRAPHQL_URL")
            or url + "/graphql"
        ).rstrip("/")
        token = str(
            cfg.get("atom_token")
            or os.getenv("ATOM_SERVICE_TOKEN")
            or os.getenv("ATOM_ADMIN_TOKEN")
            or os.getenv("ATOM_TOKEN")
            or ""
        )
        username = str(cfg.get("atom_username") or os.getenv("ATOM_USERNAME") or "")
        password = str(cfg.get("atom_password") or os.getenv("ATOM_PASSWORD") or "")
        timeout_ms = cfg.get("atom_timeout_ms", cfg.get("publish_timeout_ms", 5000))
        return cls(
            url=url,
            graphql_url=graphql_url,
            token=token,
            username=username,
            password=password,
            timeout=max(float(timeout_ms) / 1000.0, 0.1),
            tenant_id=str(
                cfg.get("atom_tenant_id") or os.getenv("ATOM_TENANT_ID") or ""
            ),
            channel_id=str(
                cfg.get("atom_channel_id") or os.getenv("ATOM_CHANNEL_ID") or ""
            ),
            manage_policies=bool(cfg.get("atom_manage_policies", True)),
        )


class AtomClient:
    """Small stdlib client for the Atom GraphQL and login APIs."""

    entity_fields = (
        "id kind name externalId tenantId status attributes createdAt updatedAt"
    )

    def __init__(self, cfg: AtomConfig, http_client: Any = None) -> None:
        self.cfg = cfg
        self._token = cfg.token
        self._http_client = http_client

    @property
    def token(self) -> str:
        if not self._token:
            self._login_if_configured()
        if not self._token:
            raise AtomError(
                "Atom token not configured; set ATOM_SERVICE_TOKEN/ATOM_ADMIN_TOKEN "
                "or configure atom_username and atom_password"
            )
        return self._token

    def _login_if_configured(self) -> None:
        if not self.cfg.username or not self.cfg.password:
            return
        payload = {
            "identifier": self.cfg.username,
            "secret": self.cfg.password,
            "kind": "password",
        }
        data = self._http_json("POST", self.cfg.url + "/auth/login", payload, auth=False)
        self._token = str(data.get("token") or data.get("access_token") or "")
        if not self._token:
            raise AtomError("Atom login succeeded without a token")

    def _http_json(
        self,
        method: str,
        url: str,
        payload: Optional[Dict[str, Any]] = None,
        *,
        auth: bool = True,
    ) -> Dict[str, Any]:
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        headers = {"Accept": "application/json"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        if auth:
            headers["Authorization"] = "Bearer " + self.token
        req = request.Request(url, data=body, headers=headers, method=method)
        opener = self._http_client or request.urlopen
        try:
            response_ctx = opener(req, timeout=self.cfg.timeout)
            with response_ctx as response:
                raw = response.read() or b"{}"
                return json.loads(raw.decode("utf-8")) if raw else {}
        except error.HTTPError as exc:
            raw = exc.read() or b""
            detail = raw.decode("utf-8", errors="replace")
            raise AtomError(f"Atom HTTP {exc.code}: {detail[:500]}", exc.code) from exc
        except (error.URLError, TimeoutError, OSError) as exc:
            raise AtomError(f"Atom request failed: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise AtomError("Atom returned invalid JSON") from exc

    def _graphql(
        self, query: str, variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        response = self._http_json(
            "POST",
            self.cfg.graphql_url,
            {"query": query, "variables": variables or {}},
        )
        errors = response.get("errors")
        if errors:
            messages = "; ".join(
                str(item.get("message", item)) if isinstance(item, dict) else str(item)
                for item in errors
            )
            raise AtomError("Atom GraphQL error: " + messages)
        data = response.get("data")
        if not isinstance(data, dict):
            raise AtomError("Atom GraphQL response did not contain data")
        return data

    def login(self) -> str:
        return self.token

    def list_devices(
        self,
        tenant_id: Optional[str] = None,
        external_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        query = f"""
        query ListDevices($tenantId: ID, $kind: String, $externalId: String, $limit: Int, $offset: Int) {{
          entities(tenantId: $tenantId, kind: $kind, externalId: $externalId, limit: $limit, offset: $offset) {{
            total
            items {{ {self.entity_fields} }}
          }}
        }}
        """
        variables: Dict[str, Any] = {
            "tenantId": tenant_id or self.cfg.tenant_id or None,
            "kind": "device",
            "externalId": external_id,
            "limit": limit,
            "offset": offset,
        }
        page = self._graphql(query, variables).get("entities") or {}
        return list(page.get("items") or [])

    def get_device(self, device_id: str) -> Dict[str, Any]:
        query = f"""
        query GetDevice($id: ID!) {{
          entity(id: $id) {{ {self.entity_fields} }}
        }}
        """
        return dict(self._graphql(query, {"id": device_id}).get("entity") or {})

    def create_device(
        self,
        external_id: str,
        *,
        name: Optional[str] = None,
        tenant_id: Optional[str] = None,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        mutation = f"""
        mutation CreateDevice($input: CreateEntityInput!) {{
          createEntity(input: $input) {{ {self.entity_fields} }}
        }}
        """
        inp: Dict[str, Any] = {
            "kind": "device",
            "name": name or external_id,
            "externalId": external_id,
            "tenantId": tenant_id or self.cfg.tenant_id,
            "attributes": attributes or {},
        }
        return dict(self._graphql(mutation, {"input": inp}).get("createEntity") or {})

    def update_device(
        self,
        device_id: str,
        *,
        name: Optional[str] = None,
        external_id: Optional[str] = None,
        status: Optional[str] = None,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        mutation = f"""
        mutation UpdateDevice($id: ID!, $input: UpdateEntityInput!) {{
          updateEntity(id: $id, input: $input) {{ {self.entity_fields} }}
        }}
        """
        inp: Dict[str, Any] = {}
        if name is not None:
            inp["name"] = name
        if external_id is not None:
            inp["externalId"] = external_id
        if status is not None:
            inp["status"] = status
        if attributes is not None:
            inp["attributes"] = attributes
        return dict(
            self._graphql(mutation, {"id": device_id, "input": inp}).get(
                "updateEntity"
            )
            or {}
        )

    def delete_device(self, device_id: str) -> None:
        self._graphql(
            "mutation DeleteDevice($id: ID!) { deleteEntity(id: $id) }",
            {"id": device_id},
        )

    def capability_id(self, action_name: str) -> str:
        query = """
        query Actions($limit: Int!, $offset: Int!) {
          actions(limit: $limit, offset: $offset) { total items { id name } }
        }
        """
        offset = 0
        while True:
            page = self._graphql(query, {"limit": 100, "offset": offset}).get(
                "actions"
            ) or {}
            for item in page.get("items") or []:
                if item.get("name") == action_name:
                    return str(item["id"])
            items = list(page.get("items") or [])
            if len(items) < 100 or offset + len(items) >= int(page.get("total") or 0):
                break
            offset += len(items)
        raise AtomError(f"Atom capability not found: {action_name}")

    def ensure_publish_policy(self, device_id: str, channel_id: Optional[str] = None) -> None:
        channel_id = channel_id or self.cfg.channel_id
        tenant_id = self.cfg.tenant_id
        if not channel_id:
            raise AtomError("Atom channel_id is required to publish telemetry")
        query = """
        query DevicePolicies($tenantId: ID, $subjectKind: SubjectKind, $subjectId: ID, $limit: Int, $offset: Int) {
          directPolicies(tenantId: $tenantId, subjectKind: $subjectKind, subjectId: $subjectId, limit: $limit, offset: $offset) {
            total
            items {
              id subjectKind subjectId
              permissionBlock {
                objectKind objectType objectId scopeMode effect
                actions { id name }
              }
            }
          }
        }
        """
        page = self._graphql(
            query,
            {
                "tenantId": tenant_id or None,
                "subjectKind": "entity",
                "subjectId": device_id,
                "limit": 100,
                "offset": 0,
            },
        ).get("directPolicies") or {}
        for item in page.get("items") or []:
            block = item.get("permissionBlock") or {}
            actions = {str(a.get("name")) for a in block.get("actions") or []}
            if (
                block.get("objectKind") == "resource"
                and block.get("objectType") == "resource:channel"
                and block.get("objectId") == channel_id
                and block.get("effect", "allow") == "allow"
                and "publish" in actions
            ):
                return

        action_id = self.capability_id("publish")
        block_mutation = """
        mutation CreatePermissionBlock($input: CreatePermissionBlockInput!) {
          createPermissionBlock(input: $input) {
            id objectKind objectType objectId scopeMode effect actions { id name }
          }
        }
        """
        block = self._graphql(
            block_mutation,
            {
                "input": {
                    "tenantId": tenant_id or None,
                    "scopeMode": "object",
                    "objectKind": "resource",
                    "objectType": "resource:channel",
                    "objectId": channel_id,
                    "effect": "allow",
                    "actionIds": [action_id],
                }
            },
        ).get("createPermissionBlock") or {}
        if not block.get("id"):
            raise AtomError("Atom did not return the created permission block")
        self._graphql(
            """
            mutation CreateDirectPolicy($input: CreateDirectPolicyInput!) {
              createDirectPolicy(input: $input) { id }
            }
            """,
            {
                "input": {
                    "tenantId": tenant_id or None,
                    "subjectKind": "entity",
                    "subjectId": device_id,
                    "permissionBlockId": block["id"],
                }
            },
        )


__all__ = ["AtomClient", "AtomConfig", "AtomError"]
