"""Explicit public response models; durable SQL entities are never HTTP models."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class ResponseModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Problem(ResponseModel):
    type: str
    title: str
    status: int
    detail: str


class HealthResponse(ResponseModel):
    status: Literal["alive", "ready"]


class WindowResponse(ResponseModel):
    from_time: datetime
    to_time: datetime


class OverviewResponse(ResponseModel):
    window: WindowResponse
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
    pass_rate_definition: str


class SourceResponse(ResponseModel):
    topic: str
    partition: int
    offset: int


class ExposureResponse(ResponseModel):
    journey_id: str
    finding_id: UUID
    dependency_id: str
    reason: str
    impact_type: Literal["POTENTIAL_DATA_QUALITY_IMPACT"]


class FindingResponse(ResponseModel):
    finding_id: UUID
    rule_id: str
    category: str
    severity: Literal["high", "medium"]
    event_id: str | None
    event_type: str | None
    source_ref: SourceResponse
    detected_at: datetime
    evidence: dict[str, str | int | bool]
    message: str
    suggested_action: str
    associated_journeys: list[ExposureResponse]


class FindingPageResponse(ResponseModel):
    items: list[FindingResponse]
    next_cursor: str | None


class ImpactResponse(ResponseModel):
    journey_id: str
    window: WindowResponse
    affected_events: int
    finding_count: int
    status: Literal["potential_data_quality_impact", "no_findings_in_window"]
    impact_type: Literal["POTENTIAL_DATA_QUALITY_IMPACT"] = "POTENTIAL_DATA_QUALITY_IMPACT"
    interpretation: str = (
        "Configured dependency exposure; not confirmed journey failure or revenue loss"
    )
    findings: FindingPageResponse
