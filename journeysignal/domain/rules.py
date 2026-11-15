"""Pure, fixed-order rule evaluation with explicit injected time and prior identity."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

from journeysignal.domain.events import Event, EventType, ParseFailure, SourceRef
from journeysignal.domain.findings import Evidence, IssueCategory, QualityFinding, Severity

RULE_IDS = frozenset({"JSQ-001", "JSQ-002", "JSQ-003", "JSQ-004", "JSQ-005"})


@dataclass(frozen=True)
class RuleContext:
    source_ref: SourceRef
    received_at: datetime
    event: Event | None
    failure: ParseFailure | None
    first_fingerprint: str | None = None


class QualityRule(Protocol):
    @property
    def rule_id(self) -> str: ...

    def evaluate(self, context: RuleContext) -> tuple[QualityFinding, ...]: ...


def finding(
    context: RuleContext,
    rule_id: str,
    category: IssueCategory,
    severity: Severity,
    evidence: Evidence,
    message: str,
    action: str,
) -> tuple[QualityFinding, ...]:
    event = context.event
    failure = context.failure
    ref = context.source_ref
    return (
        QualityFinding(
            uuid5(NAMESPACE_URL, f"{ref.topic}/{ref.partition}/{ref.offset}/{rule_id}"),
            rule_id,
            category,
            severity,
            event.event_id if event else failure.event_id if failure else None,
            event.event_type if event else failure.event_type if failure else None,
            ref,
            context.received_at,
            evidence,
            message,
            action,
        ),
    )


class ContractRule:
    rule_id = "JSQ-001"

    def evaluate(self, context: RuleContext) -> tuple[QualityFinding, ...]:
        if context.failure is None:
            return ()
        return finding(
            context,
            self.rule_id,
            IssueCategory.CONTRACT,
            Severity.HIGH,
            (
                ("error_code", context.failure.code),
                ("field_paths", ",".join(context.failure.field_paths)),
            ),
            "Event does not satisfy the supported contract",
            "Check the producer contract and supported schema version.",
        )


class MissingIdentityRule:
    rule_id = "JSQ-002"

    def evaluate(self, context: RuleContext) -> tuple[QualityFinding, ...]:
        event = context.event
        if event is None or event.event_type != EventType.ADD_TO_CART or event.customer_id:
            return ()
        return finding(
            context,
            self.rule_id,
            IssueCategory.IDENTITY,
            Severity.HIGH,
            (("required_field", "customer_id"),),
            "Missing customer identity on add_to_cart",
            "Validate storefront identity mapping before downstream audience activation.",
        )


class DuplicateEventRule:
    rule_id = "JSQ-003"

    def evaluate(self, context: RuleContext) -> tuple[QualityFinding, ...]:
        if context.event is None or context.first_fingerprint is None:
            return ()
        return finding(
            context,
            self.rule_id,
            IssueCategory.DUPLICATE,
            Severity.MEDIUM,
            (("content_matches_first", context.event.fingerprint == context.first_fingerprint),),
            "Business event ID repeated in a distinct transport receipt",
            "Inspect producer retry and event ID generation policies.",
        )


@dataclass(frozen=True)
class EventFreshnessRule:
    threshold_seconds: int = 300
    rule_id: str = "JSQ-004"

    def evaluate(self, context: RuleContext) -> tuple[QualityFinding, ...]:
        event = context.event
        if event is None:
            return ()
        age = context.received_at - event.event_time
        if age <= timedelta(seconds=self.threshold_seconds):
            return ()
        return finding(
            context,
            self.rule_id,
            IssueCategory.FRESHNESS,
            Severity.MEDIUM,
            (
                ("event_age_ms", int(age.total_seconds() * 1000)),
                ("threshold_seconds", self.threshold_seconds),
                (
                    "producer_delay_ms",
                    int((event.produced_at - event.event_time).total_seconds() * 1000),
                ),
                (
                    "transport_delay_ms",
                    int((context.received_at - event.produced_at).total_seconds() * 1000),
                ),
            ),
            "Event exceeded the configured freshness window",
            "Compare producer delay, broker lag and clock synchronisation.",
        )


@dataclass(frozen=True)
class FutureTimeRule:
    allowed_skew_seconds: int = 30
    rule_id: str = "JSQ-005"

    def evaluate(self, context: RuleContext) -> tuple[QualityFinding, ...]:
        event = context.event
        if event is None or event.event_time - context.received_at <= timedelta(
            seconds=self.allowed_skew_seconds
        ):
            return ()
        return finding(
            context,
            self.rule_id,
            IssueCategory.TIME_ANOMALY,
            Severity.HIGH,
            (("allowed_skew_seconds", self.allowed_skew_seconds),),
            "Event timestamp exceeds allowed future clock skew",
            "Check producer clock synchronisation and timestamp conversion.",
        )


def quality_rules(
    freshness_seconds: int = 300,
    future_skew_seconds: int = 30,
    disabled: frozenset[str] = frozenset(),
) -> tuple[QualityRule, ...]:
    if disabled - RULE_IDS:
        raise ValueError("Unknown disabled rule ID")
    rules: tuple[QualityRule, ...] = (
        ContractRule(),
        MissingIdentityRule(),
        DuplicateEventRule(),
        EventFreshnessRule(freshness_seconds),
        FutureTimeRule(future_skew_seconds),
    )
    return tuple(rule for rule in rules if rule.rule_id not in disabled)
