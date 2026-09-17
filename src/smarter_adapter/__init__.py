"""Smarter Adapter 2.0 development package.

The v2 package intentionally coexists with the current SmartAdapter modules while
we migrate behavior behind explicit contracts and end-to-end tests.
"""

from .models import Measurement, ParsedEvent, RawEvent
from .plugins import ParserPlugin, ParserRegistry

__version__ = "2.0.0-dev"

__all__ = [
    "Measurement",
    "ParsedEvent",
    "ParserPlugin",
    "ParserRegistry",
    "RawEvent",
]
