from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

from .models import ParsedEvent, RawEvent
from .plugins import ParserRegistry
from .quality import MeasurementQualityPolicy, QualityIssue


@dataclass(frozen=True)
class ParseOutcome:
    parser: str
    event: ParsedEvent
    quality_status: str = "valid"
    quality_issues: Tuple[QualityIssue, ...] = ()


class ParsePipeline:
    """Transport-neutral RawEvent -> ParsedEvent pipeline.

    Parser plugins normalize source formats. A separate post-parse quality
    policy may then remove invalid measurements and attach diagnostics before
    device reconciliation, SenML serialization and delivery.
    """

    def __init__(
        self,
        parsers: ParserRegistry,
        *,
        quality_policy: Optional[MeasurementQualityPolicy] = None,
    ) -> None:
        self.parsers = parsers
        self.quality_policy = quality_policy or MeasurementQualityPolicy()

    def process(self, event: RawEvent) -> ParseOutcome:
        match = self.parsers.parse(event)
        quality = self.quality_policy.apply(match.event)
        return ParseOutcome(
            parser=match.plugin_name,
            event=quality.event,
            quality_status=quality.status,
            quality_issues=quality.issues,
        )


__all__ = ["ParseOutcome", "ParsePipeline"]
