from __future__ import annotations

from dataclasses import dataclass

from .models import ParsedEvent, RawEvent
from .plugins import ParserRegistry


@dataclass(frozen=True)
class ParseOutcome:
    parser: str
    event: ParsedEvent


class ParsePipeline:
    """First v2 pipeline boundary: RawEvent -> ParsedEvent.

    Device reconciliation, SenML serialization and delivery are intentionally
    added in later milestones so parser plugins remain independent of
    Magistrala and networking concerns.
    """

    def __init__(self, parsers: ParserRegistry) -> None:
        self.parsers = parsers

    def process(self, event: RawEvent) -> ParseOutcome:
        match = self.parsers.parse(event)
        return ParseOutcome(parser=match.plugin_name, event=match.event)


__all__ = ["ParseOutcome", "ParsePipeline"]
