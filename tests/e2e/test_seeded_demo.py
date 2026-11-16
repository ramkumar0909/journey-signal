import json
import os
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from threading import Event
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine

from journeysignal.adapters.config.journey_rules import load_registry
from journeysignal.adapters.kafka.consumer import KafkaSource
from journeysignal.adapters.kafka.publisher import ensure_topic, publish
from journeysignal.adapters.kafka.serialization import EventParser
from journeysignal.adapters.persistence.queries import PostgresQueries
from journeysignal.adapters.persistence.unit_of_work import PostgresTransaction
from journeysignal.adapters.telemetry.metrics import Metrics
from journeysignal.application.ingest_event import IngestEvent
from journeysignal.application.simulator import known_scenario
from journeysignal.domain.rules import quality_rules
from journeysignal.interfaces.api.app import create_app
from journeysignal.interfaces.worker.main import run_worker


class FixedClock:
    def __init__(self, now: datetime) -> None:
        self.value = now

    def now(self) -> datetime:
        return self.value


@pytest.mark.infrastructure
def test_exact_seeded_scenario_through_public_api(pg_engine: Engine, now: datetime) -> None:
    servers = os.getenv("JS_TEST_KAFKA_SERVERS")
    if not servers:
        pytest.skip("Set JS_TEST_KAFKA_SERVERS for real E2E")
    topic = "journeysignal.e2e." + uuid4().hex
    ensure_topic(servers, topic)
    publish(servers, topic, known_scenario(now))
    ingest = IngestEvent(
        lambda: PostgresTransaction(pg_engine),
        EventParser(),
        FixedClock(now),
        quality_rules(),
        load_registry(),
    )
    source = KafkaSource(servers, topic, "e2e-" + uuid4().hex)
    assert run_worker(source, ingest, Event(), Metrics(), 15, idle_timeout=20) == 15
    params = {
        "from": (now - timedelta(seconds=1)).isoformat(),
        "to": (now + timedelta(seconds=1)).isoformat(),
    }
    with TestClient(create_app(query=PostgresQueries(pg_engine))) as client:
        assert client.get("/health/ready").status_code == 200
        response = client.get("/api/v1/quality/overview", params=params)
        assert response.status_code == 200, response.text
        overview = response.json()
        assert overview["transport_records"] == 15
        assert overview["parsed_records"] == 14
        assert overview["zero_finding_parsed_records"] == 10
        assert overview["quarantined_records"] == 1 and overview["bad_json_count"] == 0
        assert overview["quality_pass_rate"] == pytest.approx(10 / 14)
        expected = {"JSQ-001": 1, "JSQ-002": 2, "JSQ-003": 1, "JSQ-004": 1}
        assert overview["finding_counts_by_rule"] == expected
        impact_response = client.get(
            "/api/v1/journeys/abandoned_cart_reminder/impact", params=params
        )
        assert impact_response.status_code == 200, impact_response.text
        impact = impact_response.json()
        assert impact["affected_events"] == 5 and impact["finding_count"] == 5
        assert all(item["associated_journeys"][0]["reason"] for item in impact["findings"]["items"])
        items = []
        cursor = None
        while True:
            paging = {"limit": 2}
            if cursor:
                paging["cursor"] = cursor
            response = client.get("/api/v1/quality/findings", params=paging)
            assert response.status_code == 200
            page = response.json()
            items.extend(page["items"])
            cursor = page["next_cursor"]
            if not cursor:
                break
        assert len({item["finding_id"] for item in items}) == 5
        assert Counter(item["rule_id"] for item in items) == expected
        identity = client.get("/api/v1/quality/findings?rule_id=JSQ-002&severity=high").json()
        assert len(identity["items"]) == 2
        event_findings = client.get("/api/v1/events/ev_known42_000000/findings").json()
        assert [item["rule_id"] for item in event_findings["items"]] == ["JSQ-003"]
        params["to"] = now.isoformat()  # half-open end excludes all detection timestamps
        assert (
            client.get("/api/v1/quality/overview", params=params).json()["transport_records"] == 0
        )
    Path("artifacts").mkdir(exist_ok=True)
    Path("artifacts/e2e-report.json").write_text(
        json.dumps(
            {
                "overview": overview,
                "impact": impact,
                "real_kafka_postgresql": True,
            },
            indent=2,
        )
        + "\n"
    )
