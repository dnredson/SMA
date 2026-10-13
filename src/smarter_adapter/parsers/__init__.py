"""Parser plugins shipped with Smarter Adapter 2.0."""

from .chirpstack_irrigap import IrrigapChirpStackParser, IrrigapNode
from .pitaya import PitayaSaciParser

__all__ = [
    "IrrigapChirpStackParser",
    "IrrigapNode",
    "PitayaSaciParser",
]
