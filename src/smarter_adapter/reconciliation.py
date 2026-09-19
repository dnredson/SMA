from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Mapping, Optional

from .device_profiles import DeviceProfileSpec
from .magistrala.control_plane import GENERIC_DEVICE_TYPE_KEY, DeviceRef


@dataclass(frozen=True)
class ReconcileIssue:
    kind: str
    resource: str
    detail: str
    repaired: bool = False
    repairable: bool = True

    def as_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "resource": self.resource,
            "detail": self.detail,
            "repaired": self.repaired,
            "repairable": self.repairable,
        }


class ControlPlaneReconciler:
    """Audit and safely repair drift between SMA durable state and Atom.

    The reconciler intentionally refuses to auto-recreate the workspace or
    channel because those IDs scope every local mapping and persistence rule.
    Child resources are safe to repair in place: persistence rule, typed
    profiles, active devices and direct publish policies. Decommissioned nodes
    are kept blocked and any accidentally restored publish policy is revoked.
    """

    def __init__(self, *, runtime, store, catalog, atom) -> None:
        self.runtime = runtime
        self.store = store
        self.catalog = catalog
        self.atom = atom

    @staticmethod
    def _profile_version(versions) -> Optional[dict[str, Any]]:
        active = [
            item for item in versions if str(item.get("status") or "") == "active"
        ]
        if not active:
            return None
        return max(active, key=lambda item: int(item.get("version") or 0))

    @staticmethod
    def _managed_rule_ok(rule: Mapping[str, Any], *, name: str, channel_id: str) -> bool:
        outputs = rule.get("outputs") or []
        has_save = any(
            isinstance(item, Mapping) and str(item.get("type") or "") == "save_senml"
            for item in outputs
        )
        return (
            str(rule.get("name") or "") == name
            and str(rule.get("input_channel") or "") == channel_id
            and str(rule.get("input_topic") or "") == ""
            and has_save
            and str(rule.get("status") or "") == "enabled"
        )

    def _policy_present(self, workspace_id: str, device_id: str, channel_id: str) -> bool:
        checker = getattr(self.atom, "has_publish_policy", None)
        if callable(checker):
            return bool(checker(workspace_id, device_id, channel_id))
        ids = getattr(self.atom, "_publish_policy_ids", None)
        if callable(ids):
            return bool(ids(workspace_id, device_id, channel_id))
        raise RuntimeError("Atom client cannot audit direct publish policies")

    def _remote_device(self, workspace_id: str, external_id: str):
        matches = [
            item
            for item in self.atom.list_devices(
                workspace_id,
                external_id=external_id,
                limit=10,
            )
            if str(item.get("externalId") or "") == external_id
        ]
        return matches

    def _profile_specs(self) -> dict[str, DeviceProfileSpec]:
        result: dict[str, DeviceProfileSpec] = {}
        registry = self.runtime.profile_registry
        if self.catalog is not None:
            for node in self.catalog.list_nodes():
                spec = registry.get(node.device)
                if spec is not None:
                    result[spec.key] = spec
        base = self.runtime.base
        if base is not None:
            for item in self.store.list_devices(
                workspace_id=base.workspace.id,
                channel_id=base.channel.id,
                limit=100000,
            ):
                spec = registry.get(item.get("observed_sensor"))
                if spec is not None:
                    result[spec.key] = spec
        return result

    @staticmethod
    def _device_attributes(item: Mapping[str, Any]) -> dict[str, Any]:
        metadata = item.get("observation_metadata") or {}
        result: dict[str, Any] = {}
        if isinstance(metadata, Mapping):
            for key in (
                "sensor",
                "node_id",
                "location",
                "sub_location",
                "depth",
                "application_id",
            ):
                if key in metadata:
                    result[key] = metadata[key]
        if item.get("observed_sensor"):
            result["sensor"] = item["observed_sensor"]
        if item.get("node_id"):
            result["node_id"] = item["node_id"]
        return result

    def _cache_profile(self, ref) -> None:
        cache = getattr(self.runtime, "_device_types", None)
        if isinstance(cache, dict):
            cache[ref.key] = ref
        if ref.key == GENERIC_DEVICE_TYPE_KEY:
            self.runtime.device_type = ref

    def reconcile(
        self,
        *,
        repair: bool = True,
        include_devices: bool = True,
    ) -> dict[str, Any]:
        started = time.time()
        issues: list[ReconcileIssue] = []
        errors: list[dict[str, str]] = []
        devices_report: list[dict[str, Any]] = []
        profiles_report: list[dict[str, Any]] = []

        self.runtime.bootstrap()
        base = self.runtime.base
        generic = self.runtime.device_type
        if base is None or generic is None:
            raise RuntimeError("runtime is not bootstrapped")
        workspace_id = base.workspace.id
        channel_id = base.channel.id

        # Base identity is deliberately audit-only. Recreating either ID would
        # orphan local mappings keyed by workspace/channel.
        workspaces = list(self.atom.list_workspaces())
        remote_workspace = next(
            (item for item in workspaces if str(item.get("id") or "") == workspace_id),
            None,
        )
        if remote_workspace is None:
            issues.append(
                ReconcileIssue(
                    "workspace_missing",
                    f"workspace:{workspace_id}",
                    "current SMA workspace ID no longer exists in Atom; automatic recreation is unsafe",
                    repairable=False,
                )
            )
            return self._result(
                started,
                repair,
                include_devices,
                issues,
                errors,
                profiles_report,
                devices_report,
                blocked=True,
            )

        channels = list(self.atom.list_channels(workspace_id, kind="channel", limit=100))
        remote_channel = next(
            (item for item in channels if str(item.get("id") or "") == channel_id),
            None,
        )
        if remote_channel is None:
            issues.append(
                ReconcileIssue(
                    "channel_missing",
                    f"channel:{channel_id}",
                    "current SMA channel ID no longer exists in Atom; automatic recreation is unsafe",
                    repairable=False,
                )
            )
            return self._result(
                started,
                repair,
                include_devices,
                issues,
                errors,
                profiles_report,
                devices_report,
                blocked=True,
            )

        # Persistence rule drift is safe to repair because the channel identity
        # remains stable and ensure_senml_persistence is idempotent.
        try:
            matching_rules = [
                item
                for item in self.runtime.rules.list_rules(
                    workspace_id,
                    input_channel=channel_id,
                    status="all",
                    limit=100,
                )
                if str(item.get("name") or "") == self.runtime.config.persistence_rule_name
            ]
            rule_ok = (
                len(matching_rules) == 1
                and self._managed_rule_ok(
                    matching_rules[0],
                    name=self.runtime.config.persistence_rule_name,
                    channel_id=channel_id,
                )
            )
            if not rule_ok:
                repaired = False
                detail = "managed SenML persistence rule is missing, disabled or malformed"
                if repair:
                    ref = self.runtime.rules.ensure_senml_persistence(
                        workspace_id,
                        channel_id,
                        name=self.runtime.config.persistence_rule_name,
                    )
                    self.runtime.persistence_rule = ref
                    repaired = True
                    detail = (
                        f"persistence rule reconciled id={ref.id} "
                        f"created={ref.created} enabled={ref.enabled}"
                    )
                issues.append(
                    ReconcileIssue(
                        "persistence_rule_drift",
                        f"rule:{self.runtime.config.persistence_rule_name}",
                        detail,
                        repaired=repaired,
                    )
                )
        except Exception as exc:
            errors.append(
                {
                    "resource": "persistence_rule",
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )

        # Generic fallback and all known typed families are audited up front.
        expected_profiles: dict[str, dict[str, str]] = {}
        profile_inputs: list[tuple[str, Optional[DeviceProfileSpec]]] = [
            (GENERIC_DEVICE_TYPE_KEY, None)
        ]
        profile_inputs.extend(sorted(self._profile_specs().items(), key=lambda item: item[0]))

        for key, spec in profile_inputs:
            try:
                remote_types = [
                    item
                    for item in self.atom.list_device_types(workspace_id)
                    if str(item.get("key") or "") == key
                ]
                profile = remote_types[0] if len(remote_types) == 1 else None
                version = None
                if profile is not None and str(profile.get("status") or "active") == "active":
                    version = self._profile_version(
                        self.atom.list_device_type_versions(str(profile.get("id") or ""))
                    )
                healthy = profile is not None and version is not None and len(remote_types) == 1
                repaired = False
                ref = None
                if not healthy and repair:
                    kwargs: dict[str, Any] = {}
                    if spec is not None:
                        kwargs = {
                            "key": spec.key,
                            "name": spec.name,
                            "description": spec.description,
                            "json_schema": spec.json_schema,
                        }
                    ref = self.runtime.control.ensure_device_type(workspace_id, **kwargs)
                    self._cache_profile(ref)
                    profile = {"id": ref.id, "key": ref.key, "status": "active"}
                    version = {"id": ref.version_id, "version": ref.version, "status": "active"}
                    repaired = True
                    issues.append(
                        ReconcileIssue(
                            "profile_drift",
                            f"profile:{key}",
                            f"profile reconciled id={ref.id} version={ref.version}",
                            repaired=True,
                        )
                    )
                elif not healthy:
                    issues.append(
                        ReconcileIssue(
                            "profile_drift",
                            f"profile:{key}",
                            "profile is missing, duplicated, inactive or has no active version",
                        )
                    )

                if profile is not None and version is not None:
                    expected_profiles[key] = {
                        "id": str(profile.get("id") or ""),
                        "version_id": str(version.get("id") or ""),
                    }
                profiles_report.append(
                    {
                        "key": key,
                        "profile_id": expected_profiles.get(key, {}).get("id"),
                        "profile_version_id": expected_profiles.get(key, {}).get("version_id"),
                        "healthy": bool(profile is not None and version is not None),
                        "repaired": repaired,
                    }
                )
            except Exception as exc:
                errors.append(
                    {
                        "resource": f"profile:{key}",
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )

        if include_devices:
            local_devices = self.store.list_devices(
                workspace_id=workspace_id,
                channel_id=channel_id,
                limit=100000,
            )
            for item in local_devices:
                external_id = str(item.get("external_id") or "")
                node_id = str(item.get("node_id") or "").strip().upper()
                sensor = str(item.get("observed_sensor") or "")
                lifecycle = (
                    self.store.get_node_lifecycle(workspace_id, channel_id, node_id)
                    if node_id and hasattr(self.store, "get_node_lifecycle")
                    else None
                )
                decommissioned = bool(
                    lifecycle
                    and lifecycle.get("administrative_state") == "decommissioned"
                )
                entry: dict[str, Any] = {
                    "external_id": external_id,
                    "node_id": node_id or None,
                    "sensor": sensor or None,
                    "administrative_state": "decommissioned" if decommissioned else "active",
                    "local_atom_device_id": item.get("atom_device_id"),
                    "state": "healthy",
                    "actions": [],
                }
                try:
                    remote = self._remote_device(workspace_id, external_id)
                    if len(remote) > 1:
                        entry["state"] = "error"
                        errors.append(
                            {
                                "resource": f"device:{external_id}",
                                "error": "multiple remote devices share this external_id",
                            }
                        )
                        devices_report.append(entry)
                        continue

                    if decommissioned:
                        if not remote:
                            entry["state"] = "historical_only"
                            entry["actions"].append("remote_device_absent")
                            devices_report.append(entry)
                            continue
                        remote_id = str(remote[0].get("id") or "")
                        entry["remote_atom_device_id"] = remote_id
                        if self._policy_present(workspace_id, remote_id, channel_id):
                            repaired = False
                            if repair:
                                deleted = int(
                                    self.atom.revoke_publish_policy(
                                        workspace_id,
                                        remote_id,
                                        channel_id,
                                    )
                                )
                                entry["actions"].append(f"revoked_publish_policy:{deleted}")
                                repaired = True
                            entry["state"] = "repaired" if repaired else "drift"
                            issues.append(
                                ReconcileIssue(
                                    "decommission_policy_drift",
                                    f"device:{external_id}",
                                    "decommissioned device had a direct publish policy",
                                    repaired=repaired,
                                )
                            )
                        devices_report.append(entry)
                        continue

                    spec = self.runtime.profile_registry.get(sensor)
                    expected_key = spec.key if spec is not None else GENERIC_DEVICE_TYPE_KEY
                    expected = expected_profiles.get(expected_key)
                    if expected is None:
                        entry["state"] = "error"
                        errors.append(
                            {
                                "resource": f"device:{external_id}",
                                "error": f"expected profile {expected_key!r} is unavailable",
                            }
                        )
                        devices_report.append(entry)
                        continue

                    remote_item = remote[0] if remote else None
                    remote_id = str(remote_item.get("id") or "") if remote_item else ""
                    profile_ok = bool(
                        remote_item
                        and str(remote_item.get("profileId") or "") == expected["id"]
                        and str(remote_item.get("profileVersionId") or "")
                        == expected["version_id"]
                    )
                    policy_ok = bool(
                        remote_id
                        and self._policy_present(workspace_id, remote_id, channel_id)
                    )
                    identity_ok = bool(
                        remote_id
                        and remote_id == str(item.get("atom_device_id") or "")
                    )
                    healthy = bool(remote_item and profile_ok and policy_ok and identity_ok)
                    if healthy:
                        devices_report.append(entry)
                        continue

                    repaired = False
                    if repair:
                        if spec is None:
                            device_type = self.runtime.control.ensure_device_type(workspace_id)
                        else:
                            device_type = self.runtime.control.ensure_device_type(
                                workspace_id,
                                key=spec.key,
                                name=spec.name,
                                description=spec.description,
                                json_schema=spec.json_schema,
                            )
                        self._cache_profile(device_type)
                        ref = self.runtime.control.ensure_device(
                            workspace_id,
                            channel_id,
                            external_id,
                            device_type=device_type,
                            name=str(item.get("name") or external_id),
                            attributes=self._device_attributes(item),
                            allow_profile_migration=True,
                        )
                        self.store.upsert_device(
                            ref,
                            channel_id=channel_id,
                            seen_at=float(item.get("last_seen") or time.time()),
                        )
                        evict = getattr(self.runtime, "evict_device", None)
                        if callable(evict):
                            evict(external_id)
                        else:
                            cache = getattr(self.runtime, "_devices", None)
                            if isinstance(cache, dict):
                                cache.pop(external_id, None)
                        if ref.created:
                            entry["actions"].append("remote_device_recreated")
                        if ref.profile_migrated:
                            entry["actions"].append("profile_migrated")
                        if ref.publish_policy_created:
                            entry["actions"].append("publish_policy_created")
                        if ref.id != str(item.get("atom_device_id") or ""):
                            entry["actions"].append("local_atom_device_id_updated")
                        entry["remote_atom_device_id"] = ref.id
                        repaired = True

                    entry["state"] = "repaired" if repaired else "drift"
                    issues.append(
                        ReconcileIssue(
                            "managed_device_drift",
                            f"device:{external_id}",
                            (
                                f"remote_present={bool(remote_item)} profile_ok={profile_ok} "
                                f"policy_ok={policy_ok} identity_ok={identity_ok}"
                            ),
                            repaired=repaired,
                        )
                    )
                except Exception as exc:
                    entry["state"] = "error"
                    errors.append(
                        {
                            "resource": f"device:{external_id}",
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )
                devices_report.append(entry)

        if repair:
            self.runtime.clear_device_cache()

        return self._result(
            started,
            repair,
            include_devices,
            issues,
            errors,
            profiles_report,
            devices_report,
        )

    @staticmethod
    def _result(
        started: float,
        repair: bool,
        include_devices: bool,
        issues: list[ReconcileIssue],
        errors: list[dict[str, str]],
        profiles: list[dict[str, Any]],
        devices: list[dict[str, Any]],
        *,
        blocked: bool = False,
    ) -> dict[str, Any]:
        repaired = sum(1 for item in issues if item.repaired)
        unresolved = sum(1 for item in issues if not item.repaired)
        if blocked:
            status = "blocked"
        elif errors:
            status = "error"
        elif unresolved:
            status = "drift"
        elif repaired:
            status = "repaired"
        else:
            status = "ok"
        return {
            "status": status,
            "repair": bool(repair),
            "include_devices": bool(include_devices),
            "checked_at": time.time(),
            "duration_seconds": round(time.time() - started, 6),
            "summary": {
                "drift_detected": len(issues),
                "repairs_applied": repaired,
                "unresolved": unresolved,
                "errors": len(errors),
                "profiles_checked": len(profiles),
                "devices_checked": len(devices),
            },
            "issues": [item.as_dict() for item in issues],
            "errors": errors,
            "profiles": profiles,
            "devices": devices,
        }


__all__ = ["ControlPlaneReconciler", "ReconcileIssue"]
