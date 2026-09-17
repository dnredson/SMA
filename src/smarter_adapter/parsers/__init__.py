"""Parser plugins shipped with Smarter Adapter 2.0."""

from .chirpstack_irrigap import (
    DEFAULT_IRRIGAP_NODES,
    IrrigapChirpStackParser,
    IrrigapNode,
)

__all__ = [
    "DEFAULT_IRRIGAP_NODES",
    "IrrigapChirpStackParser",
    "IrrigapNode",
]
