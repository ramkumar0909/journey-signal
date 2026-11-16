import json
from dataclasses import replace
from datetime import UTC, datetime

from hypothesis import given
from hypothesis import strategies as st

from journeysignal.adapters.config.journey_rules import load_registry
from journeysignal.adapters.kafka.serialization import EventParser
from journeysignal.application.simulator import event_payload, known_scenario
from journeysignal.domain.events import EventType, SourceRef
from journeysignal.domain.rules import RuleContext, quality_rules


@given(st.permutations(tuple(event_payload(1, datetime(2026, 11, 15, tzinfo=UTC), "keys"))))
def test_canonicalisation_stable_across_key_order(order: list[str]) -> None:
    payload = event_payload(1, datetime(2026, 11, 15, tzinfo=UTC), "keys")
    original = EventParser().parse(json.dumps(payload).encode()).event
    reordered = EventParser().parse(json.dumps({key: payload[key] for key in order}).encode()).event
    assert original is not None and reordered is not None
    assert original.fingerprint == reordered.fingerprint


@given(st.lists(st.sampled_from([t for t in EventType if t != EventType.ADD_TO_CART]), max_size=20))
def test_irrelevant_events_do_not_expand_journey_exposure(event_types: list[EventType]) -> None:
    now = datetime(2026, 11, 15, 10, tzinfo=UTC)
    parsed = EventParser().parse(known_scenario(now)[10].value)
    context = RuleContext(SourceRef("property", 0, 10), now, parsed.event, parsed.failure)
    finding = quality_rules()[1].evaluate(context)[0]
    registry = load_registry()
    extra = tuple(replace(finding, event_type=event_type) for event_type in event_types)
    assert registry.associate((finding,) + extra) == registry.associate((finding,))
