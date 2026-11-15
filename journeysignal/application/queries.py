"""Read contracts and safe cursor semantics, independent of HTTP and SQL."""

import base64
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID

from journeysignal.domain.events import SourceRef
from journeysignal.domain.findings import JourneyExposure


@dataclass(frozen=True)
class TimeWindow:
    from_time: datetime
    to_time: datetime

    def __post_init__(self) -> None:
        if any(t.tzinfo is None or t.utcoffset() is None for t in (self.from_time, self.to_time)):
            raise ValueError("Window timestamps must include a timezone")
        if self.from_time >= self.to_time:
            raise ValueError("Window start must precede end")


@dataclass(frozen=True)
class Overview:
    window: TimeWindow
    transport_records: int
    parsed_records: int
    zero_finding_parsed_records: int
    quarantined_records: int
    bad_json_count: int
    quality_pass_rate: float | None
    finding_counts_by_rule: dict[str, int]
    finding_counts_by_severity: dict[str, int]
    finding_counts_by_event_type: dict[str, int]
    event_age_p50_seconds: float | None
    event_age_p95_seconds: float | None
    pass_rate_definition: str = "zero_finding_parsed_records / parsed_records"


@dataclass(frozen=True)
class FindingItem:
    finding_id: UUID
    rule_id: str
    category: str
    severity: str
    event_id: str | None
    event_type: str | None
    source_ref: SourceRef
    detected_at: datetime
    evidence: dict[str, str | int | bool]
    message: str
    suggested_action: str
    associated_journeys: tuple[JourneyExposure, ...]


@dataclass(frozen=True)
class FindingFilter:
    rule_id: str | None = None
    severity: str | None = None
    event_id: str | None = None
    journey_id: str | None = None
    window: TimeWindow | None = None

    def scope(self) -> str:
        value = [
            self.rule_id,
            self.severity,
            self.event_id,
            self.journey_id,
            self.window.from_time.isoformat() if self.window else None,
            self.window.to_time.isoformat() if self.window else None,
        ]
        return hashlib.sha256(json.dumps(value).encode()).hexdigest()[:16]


@dataclass(frozen=True)
class FindingPage:
    items: tuple[FindingItem, ...]
    next_cursor: str | None


@dataclass(frozen=True)
class ImpactCounts:
    affected_events: int
    findings: int


class QualityQuery(Protocol):
    def ready(self) -> bool: ...

    def overview(self, window: TimeWindow) -> Overview: ...

    def findings(self, filters: FindingFilter, limit: int, cursor: str | None) -> FindingPage: ...

    def impact_counts(self, journey_id: str, window: TimeWindow) -> ImpactCounts: ...


def encode_cursor(detected_at: datetime, finding_id: UUID, filters: FindingFilter) -> str:
    return base64.urlsafe_b64encode(
        json.dumps(
            [detected_at.isoformat(), str(finding_id), filters.scope()], separators=(",", ":")
        ).encode()
    ).decode()


def decode_cursor(cursor: str, filters: FindingFilter) -> tuple[datetime, UUID]:
    try:
        if len(cursor) > 512:
            raise ValueError("Cursor too long")
        value = json.loads(base64.b64decode(cursor, altchars=b"-_", validate=True))
        if (
            not isinstance(value, list)
            or len(value) != 3
            or not all(isinstance(item, str) for item in value)
            or value[2] != filters.scope()
        ):
            raise ValueError("Cursor scope mismatch")
        timestamp = datetime.fromisoformat(value[0])
        if timestamp.tzinfo is None:
            raise ValueError("Naive cursor timestamp")
        return timestamp, UUID(value[1])
    except (ValueError, TypeError, UnicodeError, OverflowError):
        raise ValueError("Invalid cursor or filters do not match cursor") from None
