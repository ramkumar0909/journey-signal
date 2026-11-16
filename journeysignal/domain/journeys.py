from dataclasses import dataclass

from journeysignal.domain.events import EventType
from journeysignal.domain.findings import JourneyExposure, QualityFinding


@dataclass(frozen=True)
class JourneyDependency:
    journey_id: str
    dependency_id: str
    display_name: str
    dependency_description: str
    required_event_types: tuple[EventType, ...]
    required_fields: tuple[str, ...]
    relevant_rule_ids: tuple[str, ...]
    freshness_threshold_seconds: int
    owner_role: str


@dataclass(frozen=True)
class JourneyDependencyRegistry:
    journeys: tuple[JourneyDependency, ...]

    def associate(self, findings: tuple[QualityFinding, ...]) -> tuple[JourneyExposure, ...]:
        return tuple(
            JourneyExposure(
                journey.journey_id,
                finding.finding_id,
                journey.dependency_id,
                f"Configured dependency uses {finding.event_type}; rule {finding.rule_id} "
                f"is relevant; required fields: {', '.join(journey.required_fields)}.",
            )
            for finding in findings
            for journey in self.journeys
            if finding.event_type in journey.required_event_types
            and finding.rule_id in journey.relevant_rule_ids
        )
