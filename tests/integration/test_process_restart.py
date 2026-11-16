"""Real process kill/restart, real broker, and an isolated migrated PostgreSQL database."""

import os
import socket
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine, func, select, text
from sqlalchemy.orm import Session

from journeysignal.adapters.kafka.publisher import ensure_topic, publish
from journeysignal.adapters.persistence.models import EventIdentity, FindingRow, TransportReceipt
from journeysignal.application.simulator import generated_scenario


def receipt_count(engine: Engine) -> int:
    with Session(engine) as session:
        return session.scalar(select(func.count()).select_from(TransportReceipt)) or 0


def wait_for_receipts(engine: Engine, count: int, process: subprocess.Popen[bytes]) -> None:
    deadline = time.monotonic() + 25
    while time.monotonic() < deadline:
        assert process.poll() is None, "Worker exited before completing the experiment"
        if receipt_count(engine) >= count:
            return
        time.sleep(0.05)
    raise AssertionError("Worker did not persist expected receipts before deadline")


@pytest.mark.infrastructure
def test_sigkill_restart_preserves_committed_data(pg_engine: Engine, tmp_path: Path) -> None:
    servers = os.getenv("JS_TEST_KAFKA_SERVERS")
    if not servers:
        pytest.skip("Set JS_TEST_KAFKA_SERVERS for real process restart test")
    identifier = uuid4().hex
    database = "restart_" + identifier
    topic = "journeysignal.process." + identifier
    admin = create_engine(pg_engine.url, isolation_level="AUTOCOMMIT")
    with admin.connect() as connection:
        connection.execute(text(f'CREATE DATABASE "{database}"'))
    url = pg_engine.url.set(database=database)
    engine = create_engine(url)
    children: list[subprocess.Popen[bytes]] = []
    try:
        config = Config("alembic.ini")
        config.attributes["database_url"] = url
        command.upgrade(config, "head")
        ensure_topic(servers, topic)
        attempts = generated_scenario(datetime.now(UTC), 42, 50, "normal", identifier)
        publish(servers, topic, attempts[:25])
        env = {
            **os.environ,
            "JS_DATABASE_URL": url.render_as_string(hide_password=False),
            "JS_KAFKA_BOOTSTRAP_SERVERS": servers,
            "JS_KAFKA_TOPIC": topic,
            "JS_KAFKA_GROUP_ID": "process-" + identifier,
            "JS_JOURNEY_CONFIG_PATH": "journeysignal/adapters/config/journeys.yaml",
        }

        def start_worker() -> subprocess.Popen[bytes]:
            with socket.socket() as port:
                port.bind(("127.0.0.1", 0))
                env["JS_WORKER_METRICS_PORT"] = str(port.getsockname()[1])
            log = (tmp_path / f"worker-{len(children)}.log").open("wb")
            try:
                child = subprocess.Popen(
                    [sys.executable, "-m", "journeysignal.interfaces.worker.main"],
                    env=env,
                    stdout=log,
                    stderr=log,
                )
            finally:
                log.close()
            children.append(child)
            return child

        first = start_worker()
        wait_for_receipts(engine, 1, first)
        first.kill()  # actual SIGKILL: no graceful shutdown or offset flush
        first.wait(timeout=10)
        committed_before_restart = receipt_count(engine)
        assert committed_before_restart >= 1
        publish(servers, topic, attempts[25:])
        restarted = start_worker()
        wait_for_receipts(engine, 50, restarted)
        restarted.terminate()
        assert restarted.wait(timeout=15) == 0
        with Session(engine) as session:
            assert receipt_count(engine) == 50
            assert session.scalar(select(func.count()).select_from(EventIdentity)) == 50
            assert session.scalar(select(func.count()).select_from(FindingRow)) == 0
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=10)
        engine.dispose()
        with admin.connect() as connection:
            connection.execute(text(f'DROP DATABASE "{database}" WITH (FORCE)'))
        admin.dispose()
