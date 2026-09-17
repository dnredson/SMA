from __future__ import annotations

from typing import Optional, Protocol, runtime_checkable

from ..magistrala.control_plane import DeviceRef


@runtime_checkable
class DeviceStateStore(Protocol):
    """Persistent mapping between external devices and Atom devices.

    Stores are scoped by workspace and channel so the same external identifier
    can safely exist in independent Smarter Adapter deployments.
    """

    def get_device(
        self,
        workspace_id: str,
        channel_id: str,
        external_id: str,
    ) -> Optional[DeviceRef]: ...

    def upsert_device(
        self,
        device: DeviceRef,
        *,
        channel_id: str,
        seen_at: Optional[float] = None,
    ) -> None: ...

    def touch_device(
        self,
        workspace_id: str,
        channel_id: str,
        external_id: str,
        *,
        seen_at: Optional[float] = None,
    ) -> None: ...


from .sqlite import SQLiteStateStore

__all__ = ["DeviceStateStore", "SQLiteStateStore"]
