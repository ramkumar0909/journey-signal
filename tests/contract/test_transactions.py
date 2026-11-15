import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime

import pytest
from sqlalchemy import Engine, func, select
from sqlalchemy.orm import Session

from journeysignal.adapters.config.journey_rules import load_registry
from journeysignal.adapters.kafka.serialization import EventParser
from journeysignal.adapters.persistence.memory import MemoryStore
from journeysignal.adapters.persistence.models import FindingRow, TransportReceipt
from journeysignal.adapters.persistence.unit_of_work import PostgresTransaction
from journeysignal.application.ingest_event import IngestEvent
from journeysignal.application.ports import TransactionFactory
from journeysignal.application.simulator import event_payload, known_scenario
from journeysignal.domain.events import SourceRef, TransportRecord
from journeysignal.domain.rules import quality_rules


@dataclass(frozen=True)
class FixedClock:
    value: datetime

    def now(self) -> datetime:
        return self.value


@pytest.fixture(params=["memory", "postgres"])
def transactions(request: pytest.FixtureRequest) -> TransactionFactory:
    if request.param == "memory":
        return MemoryStore().transaction
    engine = request.getfixturevalue("pg_engine")
    return lambda: PostgresTransaction(engine)


def ingester(factory: TransactionFactory, now: datetime) -> IngestEvent:
    return IngestEvent(factory, EventParser(), FixedClock(now), quality_rules(), load_registry())


def test_shared_replay_and_duplicate_semantics(
    transactions: TransactionFactory,
    now: datetime,
) -> None:
    service = ingester(transactions, now)
    attempts = known_scenario(now)
    findings = []
    for index, attempt in enumerate(attempts):
        record = TransportRecord(SourceRef("contract", 0, index), attempt.value)
        result = service.execute(record)
        assert not result.replay and result.outcome is not None
        assert tuple(f.rule_id for f in result.outcome.findings) == attempt.expected_rule_ids
        findings.extend(result.outcome.findings)
        assert service.execute(record).replay
    assert len(findings) == 5


def test_shared_rollback_includes_registry(
    transactions: TransactionFactory,
    now: datetime,
) -> None:
    raw = known_scenario(now)[0].value
    event = EventParser().parse(raw).event
    assert event is not None
    ref = SourceRef("rollback", 0, 0)
    with pytest.raises(RuntimeError), transactions() as tx:
        assert tx.claim_receipt(ref, now)
        assert tx.claim_business_id(event) is None
        raise RuntimeError("Injected transaction failure")
    result = ingester(transactions, now).execute(TransportRecord(ref, raw))
    assert result.outcome is not None and result.outcome.findings == ()


def test_shared_incomplete_receipt_cannot_commit(
    transactions: TransactionFactory,
    now: datetime,
) -> None:
    ref = SourceRef("incomplete", 0, 0)
    with pytest.raises(RuntimeError, match="incomplete"), transactions() as tx:
        assert tx.claim_receipt(ref, now)
        tx.commit()
    result = ingester(transactions, now).execute(TransportRecord(ref, known_scenario(now)[0].value))
    assert not result.replay


def test_postgres_concurrent_cross_partition_duplicates(pg_engine: Engine, now: datetime) -> None:
    service = ingester(lambda: PostgresTransaction(pg_engine), now)
    raw = known_scenario(now)[0].value
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                service.execute,
                [
                    TransportRecord(SourceRef("concurrent", partition, 0), raw)
                    for partition in (0, 1)
                ],
            )
        )
    assert sorted(len(r.outcome.findings) for r in results if r.outcome) == [0, 1]
    with Session(pg_engine) as session:
        assert session.scalar(select(func.count()).select_from(TransportReceipt)) == 2
        assert session.scalar(select(func.count()).select_from(FindingRow)) == 1


def test_postgres_same_transport_concurrently(pg_engine: Engine, now: datetime) -> None:
    service = ingester(lambda: PostgresTransaction(pg_engine), now)
    record = TransportRecord(SourceRef("concurrent", 0, 0), known_scenario(now)[10].value)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(service.execute, [record, record]))
    assert sorted(r.replay for r in results) == [False, True]
    with Session(pg_engine) as session:
        assert session.scalar(select(func.count()).select_from(FindingRow)) == 1


def test_shared_changed_content_collision_preserves_first(
    transactions: TransactionFactory,
    now: datetime,
) -> None:
    service = ingester(transactions, now)
    payload = event_payload(1, now, "collision")
    original = service.execute(
        TransportRecord(SourceRef("collision", 0, 0), json.dumps(payload).encode())
    )
    properties = payload["properties"]
    assert isinstance(properties, dict)
    properties["quantity"] = 2
    changed = service.execute(
        TransportRecord(SourceRef("collision", 1, 0), json.dumps(payload).encode())
    )
    assert original.outcome is not None and changed.outcome is not None
    assert original.outcome.event is not None
    assert dict(original.outcome.event.properties)["quantity"] == 1
    assert [f.rule_id for f in changed.outcome.findings] == ["JSQ-003"]
    assert dict(changed.outcome.findings[0].evidence)["content_matches_first"] is False
    assert service.execute(TransportRecord(SourceRef("collision", 0, 0), b"{")).replay


def test_shared_invalid_record_does_not_reserve_logical_identity(
    transactions: TransactionFactory,
    now: datetime,
) -> None:
    service = ingester(transactions, now)
    invalid = service.execute(
        TransportRecord(SourceRef("correction", 0, 0), known_scenario(now)[13].value)
    )
    corrected = service.execute(
        TransportRecord(
            SourceRef("correction", 0, 1), json.dumps(event_payload(13, now, "known42")).encode()
        )
    )
    assert invalid.outcome is not None and invalid.outcome.failure is not None
    assert corrected.outcome is not None and corrected.outcome.findings == ()
