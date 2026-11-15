import json
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta

import pytest

from journeysignal.adapters.config.journey_rules import load_registry
from journeysignal.adapters.kafka.serialization import EventParser
from journeysignal.domain.events import EventType, SourceRef
from journeysignal.domain.rules import RuleContext, quality_rules


def context(payload: dict[str, object], now: datetime) -> RuleContext:
    parsed = EventParser().parse(json.dumps(payload).encode())
    return RuleContext(SourceRef("test", 0, 1), now, parsed.event, parsed.failure)


def rule_ids(ctx: RuleContext) -> list[str]:
    return [f.rule_id for r in quality_rules() for f in r.evaluate(ctx)]


def test_clean_and_immutable(payload: dict[str, object], now: datetime) -> None:
    ctx = context(payload, now)
    assert ctx.event is not None
    assert rule_ids(ctx) == []
    with pytest.raises(FrozenInstanceError):
        ctx.event.event_id = "changed"


@pytest.mark.parametrize("milliseconds,expected", [(300000, []), (300001, ["JSQ-004"])])
def test_freshness_boundary(
    payload: dict[str, object], now: datetime, milliseconds: int, expected: list[str]
) -> None:
    assert rule_ids(context(payload, now + timedelta(milliseconds=milliseconds))) == expected


@pytest.mark.parametrize("milliseconds,expected", [(30000, []), (30001, ["JSQ-005"])])
def test_future_boundary(
    payload: dict[str, object], now: datetime, milliseconds: int, expected: list[str]
) -> None:
    assert rule_ids(context(payload, now - timedelta(milliseconds=milliseconds))) == expected


def test_identity_and_duplicate(payload: dict[str, object], now: datetime) -> None:
    payload.pop("customer_id")
    ctx = context(payload, now)
    assert ctx.event is not None
    assert rule_ids(ctx) == ["JSQ-002"]
    ctx = replace(ctx, first_fingerprint=ctx.event.fingerprint)
    assert rule_ids(ctx) == ["JSQ-002", "JSQ-003"]
    duplicate = quality_rules()[2].evaluate(ctx)[0]
    assert dict(duplicate.evidence)["content_matches_first"] is True
    changed = quality_rules()[2].evaluate(replace(ctx, first_fingerprint="different"))[0]
    assert dict(changed.evidence)["content_matches_first"] is False


@pytest.mark.parametrize("raw", [b"{", b"[]", b"null", b'{"x":NaN}', b'{"x":1,"x":2}', None])
def test_safe_parse_failures(raw: bytes | None, now: datetime) -> None:
    parsed = EventParser().parse(raw)
    assert parsed.event is None and parsed.failure is not None
    ctx = RuleContext(SourceRef("test", 0, 0), now, None, parsed.failure)
    assert rule_ids(ctx) == ["JSQ-001"]
    assert load_registry().associate(quality_rules()[0].evaluate(ctx)) == ()


@pytest.mark.parametrize("value", ["2026-11-15T10:00:00", "bad", 42, "2200-01-01T00:00:00Z"])
def test_invalid_timestamps(payload: dict[str, object], now: datetime, value: object) -> None:
    payload["event_time"] = value
    assert rule_ids(context(payload, now)) == ["JSQ-001"]


def test_offsets_and_fingerprints(payload: dict[str, object], now: datetime) -> None:
    original = context(payload, now).event
    payload["event_time"] = "2026-11-15T11:00:00+01:00"
    reordered = dict(reversed(list(payload.items())))
    event = context(reordered, now).event
    assert event is not None and original is not None
    assert event.fingerprint == original.fingerprint
    assert rule_ids(context(payload, now)) == []


def test_contract_and_association(payload: dict[str, object], now: datetime) -> None:
    payload["schema_version"] = "99.0"
    ctx = context(payload, now)
    findings = quality_rules()[0].evaluate(ctx)
    exposures = load_registry().associate(findings)
    assert len(exposures) == 1
    assert exposures[0].dependency_id == "abandoned_cart_add_to_cart_v1"
    assert "JSQ-001" in exposures[0].reason
    payload["event_type"] = "product_viewed"
    assert not load_registry().associate(quality_rules()[0].evaluate(context(payload, now)))


def test_limits_and_sanitised_paths(payload: dict[str, object], now: datetime) -> None:
    assert EventParser(10).parse(b"x" * 11).failure is not None
    assert EventParser(max_depth=1).parse(json.dumps(payload).encode()).failure is not None
    payload["secret@example.com"] = "do not expose"
    failure = context(payload, now).failure
    assert failure is not None
    assert "secret" not in str(failure) and "do not expose" not in str(failure)
    payload["event_type"] = []
    assert EventParser().parse(json.dumps(payload).encode()).failure is not None


def test_rule_configuration() -> None:
    assert len(quality_rules(disabled=frozenset({"JSQ-002"}))) == 4
    with pytest.raises(ValueError):
        quality_rules(disabled=frozenset({"unknown"}))
    assert EventType.ADD_TO_CART in load_registry().journeys[0].required_event_types
