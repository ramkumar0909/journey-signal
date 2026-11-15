import os
import time
from datetime import datetime
from threading import Event
from uuid import uuid4

import pytest
from sqlalchemy import Engine, func, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from journeysignal.adapters.config.journey_rules import load_registry
from journeysignal.adapters.kafka.consumer import KafkaSource
from journeysignal.adapters.kafka.publisher import ensure_topic, publish
from journeysignal.adapters.kafka.serialization import EventParser
from journeysignal.adapters.persistence.models import FindingRow, TransportReceipt
from journeysignal.adapters.persistence.unit_of_work import PostgresTransaction
from journeysignal.adapters.telemetry.metrics import Metrics
from journeysignal.application.ingest_event import IngestEvent
from journeysignal.application.simulator import known_scenario
from journeysignal.domain.rules import quality_rules
from journeysignal.interfaces.worker.main import run_worker


class FixedClock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def now(self) -> datetime:
        return self.value


@pytest.mark.infrastructure
def test_restart_between_db_commit_and_offset_and_transient_failure(
    pg_engine: Engine,
    now: datetime,
) -> None:
    servers = os.getenv("JS_TEST_KAFKA_SERVERS")
    if not servers:
        pytest.skip("Set JS_TEST_KAFKA_SERVERS for real retry/restart test")
    topic = "journeysignal.restart." + uuid4().hex
    group = "restart-" + uuid4().hex
    ensure_topic(servers, topic)
    publish(servers, topic, known_scenario(now))
    registry = load_registry()
    ingest = IngestEvent(
        lambda: PostgresTransaction(pg_engine),
        EventParser(),
        FixedClock(now),
        quality_rules(),
        registry,
    )
    first_source = KafkaSource(servers, topic, group)
    deadline = time.monotonic() + 20
    record = None
    try:
        while record is None and time.monotonic() < deadline:
            record = first_source.poll(0.5)
        assert record is not None
        assert not ingest.execute(record).replay
        # Simulate process loss after DB commit and before offset acknowledgement.
    finally:
        first_source.close()
    failures = [True]

    class FailOneCommit(PostgresTransaction):
        def commit(self) -> None:
            if self.receipt_id is not None and failures:
                failures.pop()
                raise OperationalError("injected transient failure", {}, Exception("unavailable"))
            super().commit()

    retrying = IngestEvent(
        lambda: FailOneCommit(pg_engine), EventParser(), FixedClock(now), quality_rules(), registry
    )
    telemetry = Metrics()
    restarted = KafkaSource(servers, topic, group)
    assert (
        run_worker(
            restarted, retrying, Event(), telemetry, 15, idle_timeout=20, sleep=lambda _: None
        )
        == 15
    )
    assert telemetry.registry.get_sample_value("journeysignal_db_retries_total") == 1
    assert (
        telemetry.registry.get_sample_value(
            "journeysignal_receipts_total", {"disposition": "replay"}
        )
        == 1
    )
    with Session(pg_engine) as session:
        assert session.scalar(select(func.count()).select_from(TransportReceipt)) == 15
        assert session.scalar(select(func.count()).select_from(FindingRow)) == 5
