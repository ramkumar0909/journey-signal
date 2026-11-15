"""Triage values describe evidence, never measured business outcomes."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from uuid import UUID

from journeysignal.domain.events import Event, EventType, ParseFailure, SourceRef


class Severity(StrEnum):
    HIGH = "high"
    MEDIUM = "medium"


class IssueCategory(StrEnum):
    CONTRACT = "contract"
    IDENTITY = "identity"
    DUPLICATE = "duplicate"
    FRESHNESS = "freshness"
    TIME_ANOMALY = "time_anomaly"


Evidence = tuple[tuple[str, str | int | bool], ...]


@dataclass(frozen=True)
class QualityFinding:
    finding_id: UUID
    rule_id: str
    category: IssueCategory
    severity: Severity
    event_id: str | None
    event_type: EventType | None
    source_ref: SourceRef
    detected_at: datetime
    evidence: Evidence
    message: str
    suggested_action: str


@dataclass(frozen=True)
class JourneyExposure:
    journey_id: str
    finding_id: UUID
    dependency_id: str
    reason: str
    impact_type: str = "POTENTIAL_DATA_QUALITY_IMPACT"


@dataclass(frozen=True)
class QualityOutcome:
    source_ref: SourceRef
    received_at: datetime
    event: Event | None
    failure: ParseFailure | None
    findings: tuple[QualityFinding, ...]
    exposures: tuple[JourneyExposure, ...]
