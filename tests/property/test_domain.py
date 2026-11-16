import json
from datetime import UTC, datetime, timedelta

from hypothesis import given
from hypothesis import strategies as st

from journeysignal.adapters.kafka.serialization import EventParser
from journeysignal.domain.events import SourceRef
from journeysignal.domain.rules import EventFreshnessRule, RuleContext


@given(st.binary(max_size=10000))
def test_arbitrary_bytes_never_escape_parser(raw: bytes) -> None:
    parsed = EventParser().parse(raw)
    assert (parsed.event is None) != (parsed.failure is None)


@given(st.integers(min_value=300001, max_value=86400000))
def test_every_event_beyond_freshness_is_late(age_ms: int) -> None:
    from journeysignal.application.simulator import known_scenario

    now = datetime(2026, 11, 15, 10, tzinfo=UTC)
    event = EventParser().parse(known_scenario(now, "property")[0].value).event
    assert event is not None
    ctx = RuleContext(SourceRef("test", 0, 0), now + timedelta(milliseconds=age_ms), event, None)
    assert len(EventFreshnessRule().evaluate(ctx)) == 1


@given(st.one_of(st.text(max_size=200), st.integers(), st.lists(st.text(max_size=10), max_size=3)))
def test_invalid_timestamp_values_are_controlled(value: object) -> None:
    payload = {"event_time": value, "event_type": "add_to_cart"}
    assert EventParser().parse(json.dumps(payload).encode()).failure is not None
