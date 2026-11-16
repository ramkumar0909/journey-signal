"""Synchronous worker shell: bounded retries, explicit acknowledgements, safe shutdown."""

import argparse
import logging
import random
import signal
import time
from collections.abc import Callable
from threading import Event as StopEvent
from typing import Protocol

from prometheus_client import start_http_server
from sqlalchemy.exc import DBAPIError

from journeysignal.adapters.kafka.consumer import KafkaSource
from journeysignal.adapters.persistence.queries import PostgresQueries
from journeysignal.adapters.telemetry.logging import configure_logging
from journeysignal.adapters.telemetry.metrics import Metrics, metrics
from journeysignal.application.ingest_event import IngestEvent
from journeysignal.domain.events import TransportRecord

logger = logging.getLogger(__name__)


class EventSource(Protocol):
    def poll(self, timeout: float) -> TransportRecord | None: ...

    def acknowledge(self, record: TransportRecord) -> None: ...

    def close(self) -> None: ...


def transient_database_error(error: DBAPIError) -> bool:
    code = getattr(error.orig, "sqlstate", None)
    return (
        error.connection_invalidated
        or code is None
        or str(code).startswith(("08", "40", "53", "57P"))
        or code == "55P03"
    )


def run_worker(
    source: EventSource,
    ingest: IngestEvent,
    stop: StopEvent,
    telemetry: Metrics,
    max_records: int | None = None,
    idle_timeout: float = 0,
    sleep: Callable[[float], None] = time.sleep,
    readiness: Callable[[], bool] | None = None,
) -> int:
    handled = 0
    last_record = time.monotonic()
    last_dependency_check = 0.0
    try:
        while not stop.is_set() and (max_records is None or handled < max_records):
            if readiness and time.monotonic() - last_dependency_check >= 10:
                if not readiness():
                    raise RuntimeError("Worker dependencies unavailable")
                telemetry.ready.set(1)
                last_dependency_check = time.monotonic()
            record = source.poll(0.5)
            telemetry.heartbeat.set(time.time())
            if record is None:
                if idle_timeout and time.monotonic() - last_record > idle_timeout:
                    break
                continue
            last_record = time.monotonic()
            received_at = ingest.clock.now()
            started = time.monotonic()
            for attempt in range(6):
                try:
                    result = ingest.execute(record, received_at)
                    break
                except DBAPIError as error:
                    telemetry.ready.set(0)
                    if not transient_database_error(error) or attempt == 5:
                        raise RuntimeError("Persistence failed; offset not acknowledged") from None
                    telemetry.retries.inc()
                    logger.warning("db_retry", extra={"code": "db_retry", "attempt": attempt + 1})
                    sleep(min(0.25 * 2**attempt, 4) + random.uniform(0, 0.1))  # nosec B311
            telemetry.processing.observe(time.monotonic() - started)
            telemetry.ready.set(1)
            disposition = (
                "replay"
                if result.replay
                else ("quarantined" if result.outcome and result.outcome.failure else "validated")
            )
            telemetry.receipts.labels(disposition).inc()
            if result.outcome:
                for finding in result.outcome.findings:
                    telemetry.findings.labels(finding.rule_id).inc()
                if result.outcome.event:
                    age = (received_at - result.outcome.event.event_time).total_seconds()
                    if age >= 0:
                        telemetry.event_age.observe(age)
            source.acknowledge(record)  # never acknowledge past a failed transaction
            if isinstance(source, KafkaSource):
                telemetry.lag.labels(str(record.source_ref.partition)).set(source.lag(record))
            handled += 1
            logger.info(
                "record_handled",
                extra={
                    "code": "record_handled",
                    "partition": record.source_ref.partition,
                    "offset": record.source_ref.offset,
                },
            )
    finally:
        telemetry.ready.set(0)
        source.close()
    return handled


def main() -> None:
    from journeysignal.bootstrap import build_ingester, load_settings, make_engine

    parser = argparse.ArgumentParser(description="JourneySignal Kafka consumer")
    parser.add_argument("--max-records", type=int)
    parser.add_argument("--idle-timeout-seconds", type=float, default=0)
    args = parser.parse_args()
    configure_logging()
    settings = load_settings()
    engine = make_engine(settings)
    stop = StopEvent()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    server, thread = start_http_server(
        settings.worker_metrics_port, addr=settings.metrics_bind_host, registry=metrics.registry
    )
    try:
        ingest = build_ingester(settings, engine)
        source = KafkaSource(
            settings.kafka_bootstrap_servers, settings.kafka_topic, settings.kafka_group_id
        )
        run_worker(
            source,
            ingest,
            stop,
            metrics,
            args.max_records,
            args.idle_timeout_seconds,
            readiness=lambda: PostgresQueries(engine).ready() and source.ready(),
        )
    finally:
        engine.dispose()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


if __name__ == "__main__":
    main()
