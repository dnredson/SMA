from __future__ import annotations

from typing import Any, Dict, List

from .atom import AtomClient, AtomError


class LifecycleAtomClient(AtomClient):
    """Atom client with explicit publish-policy revocation for device lifecycle.

    Magistrala's current Atom API exposes deletion of direct policies. We only
    remove direct allow-policies that grant the target device the ``publish``
    action on the target channel; unrelated permissions are left untouched.
    """

    def _publish_policy_ids(
        self,
        tenant_id: str,
        device_id: str,
        channel_id: str,
    ) -> List[str]:
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
        matches: List[str] = []
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
                    policy_id = str(item.get("id") or "")
                    if policy_id:
                        matches.append(policy_id)
            total = int(page.get("total") or 0)
            if not items or offset + len(items) >= total:
                break
            offset += len(items)
        return matches

    def revoke_publish_policy(
        self,
        tenant_id: str,
        device_id: str,
        channel_id: str,
    ) -> int:
        """Remove all matching direct publish grants and return delete count."""
        policy_ids = self._publish_policy_ids(tenant_id, device_id, channel_id)
        mutation = """
        mutation DeleteDirectPolicy($id: ID!) {
          deleteDirectPolicy(id: $id)
        }
        """
        deleted = 0
        for policy_id in policy_ids:
            data: Dict[str, Any] = self._graphql(mutation, {"id": policy_id})
            # Atom currently returns a scalar for deleteDirectPolicy. The
            # absence of GraphQL errors is the success signal; tolerate either
            # true/null response shapes for forward compatibility.
            if "deleteDirectPolicy" not in data:
                raise AtomError(
                    f"Atom did not acknowledge direct policy deletion {policy_id!r}"
                )
            deleted += 1
        return deleted


__all__ = ["LifecycleAtomClient"]
