import os
from datetime import datetime
from threading import Event
from uuid import uuid4

import pytest
from sqlalchemy import Engine, func, select
from sqlalchemy.orm import Session

from journeysignal.adapters.config.journey_rules import load_registry
from journeysignal.adapters.kafka.consumer import KafkaSource
from journeysignal.adapters.kafka.publisher import ensure_topic, publish
from journeysignal.adapters.kafka.serialization import EventParser
from journeysignal.adapters.persistence.models import FindingRow, TransportReceipt
from journeysignal.adapters.persistence.unit_of_work import PostgresTransaction
from journeysignal.adapters.telemetry.metrics import Metrics
from journeysignal.application.ingest_event import IngestEvent
from journeysignal.application.simulator import Attempt, known_scenario
from journeysignal.domain.rules import quality_rules
from journeysignal.interfaces.worker.main import run_worker


class FixedClock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def now(self) -> datetime:
        return self.value


@pytest.fixture
def kafka_topic() -> tuple[str, str]:
    servers = os.getenv("JS_TEST_KAFKA_SERVERS")
    if not servers:
        pytest.skip("Set JS_TEST_KAFKA_SERVERS for real Kafka tests")
    topic = "journeysignal.test." + uuid4().hex
    ensure_topic(servers, topic)
    return servers, topic


@pytest.mark.infrastructure
def test_real_kafka_postgres_poison_and_replay(
    pg_engine: Engine,
    kafka_topic: tuple[str, str],
    now: datetime,
) -> None:
    servers, topic = kafka_topic
    # Poison first, then oversized, then valid events: proves safe continuation.
    attempts = (Attempt(b"{"), Attempt(b"x" * 65537)) + known_scenario(now)
    confirmed = publish(servers, topic, attempts)
    assert len(confirmed) == 17
    ingest = IngestEvent(
        lambda: PostgresTransaction(pg_engine),
        EventParser(),
        FixedClock(now),
        quality_rules(),
        load_registry(),
    )
    for _ in range(2):  # fresh group replays the same transport offsets
        source = KafkaSource(servers, topic, "test-" + uuid4().hex)
        assert run_worker(source, ingest, Event(), Metrics(), 17, idle_timeout=20) == 17
    with Session(pg_engine) as session:
        assert session.scalar(select(func.count()).select_from(TransportReceipt)) == 17
        assert session.scalar(select(func.count()).select_from(FindingRow)) == 7
