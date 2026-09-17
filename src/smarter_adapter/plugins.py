from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Protocol, Sequence, runtime_checkable

from .models import ParsedEvent, RawEvent


@runtime_checkable
class ParserPlugin(Protocol):
    """Contract implemented by every v2 payload parser."""

    name: str

    def supports(self, event: RawEvent) -> bool:
        """Return True when this plugin can attempt to parse the event."""

    def parse(self, event: RawEvent) -> Optional[ParsedEvent]:
        """Parse an event, returning None when the payload is not accepted."""


class ParserNotFound(LookupError):
    """No registered parser accepted an input event."""


@dataclass(frozen=True)
class ParserMatch:
    plugin_name: str
    event: ParsedEvent


class ParserRegistry:
    """Deterministic parser registry.

    Parsers are evaluated in registration order. This keeps routing predictable
    while still allowing source-specific parsers to be registered before broad
    fallback parsers.
    """

    def __init__(self, plugins: Sequence[ParserPlugin] = ()) -> None:
        self._plugins: List[ParserPlugin] = []
        for plugin in plugins:
            self.register(plugin)

    @property
    def plugins(self) -> tuple[ParserPlugin, ...]:
        return tuple(self._plugins)

    def register(self, plugin: ParserPlugin) -> None:
        name = str(getattr(plugin, "name", "")).strip()
        if not name:
            raise ValueError("parser plugin must expose a non-empty name")
        if any(existing.name == name for existing in self._plugins):
            raise ValueError(f"parser plugin already registered: {name}")
        self._plugins.append(plugin)

    def parse(self, event: RawEvent) -> ParserMatch:
        for plugin in self._plugins:
            if not plugin.supports(event):
                continue
            parsed = plugin.parse(event)
            if parsed is not None:
                return ParserMatch(plugin_name=plugin.name, event=parsed)
        raise ParserNotFound(
            f"no parser accepted source={event.source!r} topic={event.topic!r}"
        )


__all__ = [
    "ParserMatch",
    "ParserNotFound",
    "ParserPlugin",
    "ParserRegistry",
]
