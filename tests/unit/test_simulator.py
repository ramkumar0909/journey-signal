import ast
import json
from collections import Counter
from datetime import datetime
from pathlib import Path

import pytest

from journeysignal.adapters.kafka.serialization import EventParser
from journeysignal.application.simulator import generated_scenario, known_scenario
from journeysignal.domain.clocks import SystemClock
from journeysignal.interfaces.cli.analytics import analyse_samples


def test_seeded_generator_matches_offline_oracle(now: datetime) -> None:
    attempts = generated_scenario(now, 42, 1000, "faulty", "test", fault_rate=0.5)
    assert attempts == generated_scenario(now, 42, 1000, "faulty", "test", fault_rate=0.5)
    assert attempts != generated_scenario(now, 43, 1000, "faulty", "test", fault_rate=0.5)
    assert {r for a in attempts for r in a.expected_rule_ids} == {
        "JSQ-001",
        "JSQ-002",
        "JSQ-003",
        "JSQ-004",
    }
    assert {json.loads(a.value)["event_type"] for a in attempts} == {
        "page_view",
        "product_viewed",
        "add_to_cart",
        "checkout_started",
        "order_completed",
    }
    assert all("expected_rule_ids" not in json.loads(a.value) for a in attempts)
    assert all(
        EventParser().parse(a.value).failure is None
        for a in generated_scenario(now, 42, 100, "normal", "test")
    )


@pytest.mark.parametrize(
    "count,rate,profile", [(0, 0.5, "normal"), (2, 2.0, "faulty"), (2, 0.5, "unknown")]
)
def test_invalid_simulator_configuration(
    now: datetime, count: int, rate: float, profile: str
) -> None:
    with pytest.raises(ValueError):
        generated_scenario(now, 42, count, profile, "test", rate)


def test_known_fixture_counts(now: datetime) -> None:
    attempts = known_scenario(now)
    assert len(attempts) == 15
    assert Counter(r for a in attempts for r in a.expected_rule_ids) == {
        "JSQ-001": 1,
        "JSQ-002": 2,
        "JSQ-003": 1,
        "JSQ-004": 1,
    }


def test_domain_has_no_framework_dependencies() -> None:
    allowed_roots = {
        "__future__",
        "dataclasses",
        "datetime",
        "enum",
        "typing",
        "uuid",
        "journeysignal",
    }
    for path in Path("journeysignal/domain").glob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Import):
                assert all(alias.name.split(".")[0] in allowed_roots for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                assert node.module is not None
                assert node.module.split(".")[0] in allowed_roots
                if node.module.startswith("journeysignal"):
                    assert node.module.startswith("journeysignal.domain")


def test_clock_is_aware_and_offline_analytics(tmp_path: Path) -> None:
    assert SystemClock().now().utcoffset() is not None
    path = tmp_path / "samples.jsonl"
    path.write_text("\n".join(json.dumps({"processing_ms": i}) for i in range(1, 101)))
    result = analyse_samples(path)
    assert result["records"] == 100 and 94 <= result["processing_p95_ms"] <= 96


def test_optional_duckdb_example(tmp_path: Path) -> None:
    pytest.importorskip("duckdb", reason="Optional analytics extra is not installed")
    path = tmp_path / "samples.jsonl"
    path.write_text('{"processing_ms": 1}\n')
    assert analyse_samples(path, True)["duckdb_records"] == 1
