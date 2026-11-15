from datetime import datetime
from threading import Event

import pytest
from sqlalchemy.exc import OperationalError

from journeysignal.adapters.config.journey_rules import load_registry
from journeysignal.adapters.kafka.serialization import EventParser
from journeysignal.adapters.persistence.memory import MemoryStore, MemoryTransaction
from journeysignal.adapters.telemetry.metrics import Metrics
from journeysignal.application.ingest_event import IngestEvent
from journeysignal.application.simulator import known_scenario
from journeysignal.domain.events import SourceRef, TransportRecord
from journeysignal.domain.rules import quality_rules
from journeysignal.interfaces.worker.main import run_worker


class FixedClock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def now(self) -> datetime:
        return self.value


class Source:
    def __init__(self, records: list[TransportRecord], fail_ack: bool = False) -> None:
        self.records = records.copy()
        self.acknowledged: list[TransportRecord] = []
        self.closed = False
        self.fail_ack = fail_ack

    def poll(self, timeout: float) -> TransportRecord | None:
        return self.records.pop(0) if self.records else None

    def acknowledge(self, record: TransportRecord) -> None:
        if self.fail_ack:
            raise RuntimeError("Injected acknowledgement failure")
        self.acknowledged.append(record)

    def close(self) -> None:
        self.closed = True


class FailingStore(MemoryStore):
    def __init__(self, failures: int) -> None:
        super().__init__()
        self.failures = failures

    def transaction(self) -> MemoryTransaction:
        if self.failures:
            self.failures -= 1
            raise OperationalError("injected", None, Exception("unavailable"))
        return super().transaction()


def service(store: MemoryStore, now: datetime) -> IngestEvent:
    return IngestEvent(
        store.transaction, EventParser(), FixedClock(now), quality_rules(), load_registry()
    )


def test_retry_then_commit(now: datetime) -> None:
    store = FailingStore(2)
    source = Source([TransportRecord(SourceRef("test", 0, 0), b"{")])
    delays: list[float] = []
    telemetry = Metrics()
    assert run_worker(source, service(store, now), Event(), telemetry, 1, sleep=delays.append) == 1
    assert len(delays) == 2 and len(source.acknowledged) == 1
    assert len(store.outcomes) == 1 and source.closed
    assert next(iter(store.outcomes.values())).received_at == now


def test_exhaustion_never_acknowledges_quarantine(now: datetime) -> None:
    store = FailingStore(10)
    source = Source([TransportRecord(SourceRef("test", 0, 0), b"{")])
    with pytest.raises(RuntimeError, match="offset not acknowledged"):
        run_worker(source, service(store, now), Event(), Metrics(), 1, sleep=lambda _: None)
    assert source.closed and not source.acknowledged and not store.outcomes


def test_commit_then_ack_failure_replays_safely(now: datetime) -> None:
    store = MemoryStore()
    record = TransportRecord(SourceRef("test", 0, 0), known_scenario(now)[10].value)
    source = Source([record], fail_ack=True)
    with pytest.raises(RuntimeError, match="acknowledgement"):
        run_worker(source, service(store, now), Event(), Metrics(), 1)
    assert len(store.outcomes) == 1
    assert service(store, now).execute(record).replay
    assert len(next(iter(store.outcomes.values())).findings) == 1
