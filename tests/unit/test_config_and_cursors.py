import base64
import json
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import ValidationError

from journeysignal.adapters.config.journey_rules import load_registry
from journeysignal.adapters.config.settings import Settings
from journeysignal.application.queries import FindingFilter, decode_cursor, encode_cursor


def test_cursor_roundtrip_and_scope(now: datetime) -> None:
    identity = uuid4()
    filters = FindingFilter(rule_id="JSQ-002")
    cursor = encode_cursor(now, identity, filters)
    assert decode_cursor(cursor, filters) == (now, identity)
    with pytest.raises(ValueError):
        decode_cursor(cursor, FindingFilter(rule_id="JSQ-003"))


@pytest.mark.parametrize(
    "raw",
    [
        "!",
        "x" * 513,
        base64.b64encode(b"{}").decode(),
        base64.b64encode(b"[null,null,null]").decode(),
    ],
)
def test_invalid_cursors(raw: str) -> None:
    with pytest.raises(ValueError):
        decode_cursor(raw, FindingFilter())


def test_naive_cursor() -> None:
    raw = json.dumps(["2026-11-15T10:00:00", str(uuid4()), FindingFilter().scope()])
    with pytest.raises(ValueError):
        decode_cursor(base64.b64encode(raw.encode()).decode(), FindingFilter())


@pytest.mark.parametrize("identity", [1, True, {}, []])
def test_cursor_identity_must_be_a_string(identity: object) -> None:
    raw = json.dumps(["2026-11-15T10:00:00Z", identity, FindingFilter().scope()])
    with pytest.raises(ValueError):
        decode_cursor(base64.b64encode(raw.encode()).decode(), FindingFilter())


def test_invalid_journey_config(tmp_path: Path) -> None:
    source = Path("journeysignal/adapters/config/journeys.yaml").read_text()
    for replacement in (
        source.replace("JSQ-001", "JSQ-999"),
        source.replace("customer_id", "secret_field"),
        "journeys: []",
    ):
        path = tmp_path / "invalid.yaml"
        path.write_text(replacement)
        with pytest.raises(ValidationError):
            load_registry(path)


def test_settings_reject_wrong_database_and_rule() -> None:
    with pytest.raises(ValidationError):
        Settings(database_url="sqlite://")
    with pytest.raises(ValidationError):
        Settings(database_url="postgresql+psycopg://demo", disabled_rules={"JSQ-999"})
