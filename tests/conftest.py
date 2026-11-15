import json
import os
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine, text


@pytest.fixture
def now() -> datetime:
    return datetime(2026, 11, 15, 10, 0, tzinfo=UTC)


@pytest.fixture
def payload() -> dict[str, object]:
    return {
        "event_id": "ev_000001",
        "event_type": "add_to_cart",
        "schema_version": "1.0",
        "event_time": "2026-11-15T10:00:00Z",
        "produced_at": "2026-11-15T10:00:00Z",
        "source": "synthetic_storefront",
        "customer_id": "cust_demo_0042",
        "session_id": "sess_demo_1001",
        "properties": {
            "cart_id": "cart_9001",
            "sku": "SKU-DEMO-19",
            "quantity": 1,
            "currency": "GBP",
            "unit_price_minor": 90000,
        },
    }


def encoded(payload: dict[str, object]) -> bytes:
    return json.dumps(payload).encode()


@pytest.fixture
def pg_engine() -> Iterator[Engine]:
    url = os.getenv("JS_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set JS_TEST_DATABASE_URL to run real PostgreSQL tests")
    schema = "test_" + uuid4().hex
    admin = create_engine(url)
    with admin.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    config = Config("alembic.ini")
    config.attributes["database_url"] = url
    try:
        with engine.begin() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "head")
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()
