"""Parser plugins shipped with Smarter Adapter 2.0."""

from .chirpstack_irrigap import IrrigapChirpStackParser, IrrigapNode

__all__ = [
    "IrrigapChirpStackParser",
    "IrrigapNode",
]
