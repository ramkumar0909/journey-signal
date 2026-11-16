from datetime import UTC, datetime

from hypothesis import given
from hypothesis import strategies as st

from journeysignal.adapters.config.journey_rules import load_registry
from journeysignal.adapters.kafka.serialization import EventParser
from journeysignal.adapters.persistence.memory import MemoryStore
from journeysignal.application.ingest_event import IngestEvent
from journeysignal.domain.events import SourceRef, TransportRecord
from journeysignal.domain.rules import quality_rules


class FixedClock:
    def now(self) -> datetime:
        return datetime(2026, 11, 15, 10, tzinfo=UTC)


@given(st.binary(max_size=1000), st.integers(min_value=1, max_value=10))
def test_replay_never_duplicates_durable_findings(raw: bytes, replay_count: int) -> None:
    store = MemoryStore()
    service = IngestEvent(
        store.transaction, EventParser(), FixedClock(), quality_rules(), load_registry()
    )
    record = TransportRecord(SourceRef("property", 0, 0), raw)
    service.execute(record)
    original = store.outcomes.copy()
    for _ in range(replay_count):
        assert service.execute(record).replay
    assert store.outcomes == original
