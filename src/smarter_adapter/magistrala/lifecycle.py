from __future__ import annotations

from typing import Any, Dict, List

from .atom import AtomClient, AtomError


class LifecycleAtomClient(AtomClient):
    """Atom client with lifecycle and typed-profile operations used by SMA v2."""

    def update_device_profile(
        self,
        device_id: str,
        *,
        profile_id: str,
        profile_version_id: str,
    ) -> Dict[str, Any]:
        """Rebind a device to another Atom profile/version without changing ID.

        Magistrala's current Atom ``updateEntity`` mutation accepts these fields
        as a partial update, so omitted name, external ID, attributes and group
        memberships remain unchanged.
        """
        mutation = f"""
        mutation UpdateDeviceProfile($id: ID!, $input: UpdateEntityInput!) {{
          updateEntity(id: $id, input: $input) {{ {self.entity_fields} }}
        }}
        """
        updated = self._graphql(
            mutation,
            {
                "id": str(device_id),
                "input": {
                    "profileId": str(profile_id),
                    "profileVersionId": str(profile_version_id),
                },
            },
        ).get("updateEntity") or {}
        if not updated.get("id"):
            raise AtomError("Atom did not return the device after profile update")
        return dict(updated)

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

    def publish_policy_ids(
        self,
        tenant_id: str,
        device_id: str,
        channel_id: str,
    ) -> List[str]:
        """Return direct publish-policy IDs for one device/channel pair."""
        return self._publish_policy_ids(tenant_id, device_id, channel_id)

    def has_publish_policy(
        self,
        tenant_id: str,
        device_id: str,
        channel_id: str,
    ) -> bool:
        """Audit whether a direct allow/publish grant currently exists."""
        return bool(self._publish_policy_ids(tenant_id, device_id, channel_id))

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
