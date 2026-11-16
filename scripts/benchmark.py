"""Bounded local real-broker/DB experiment; reports observations, never SLA claims."""

import argparse
import json
import platform
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from journeysignal.adapters.kafka.consumer import KafkaSource
from journeysignal.adapters.kafka.publisher import ensure_topic, publish
from journeysignal.application.simulator import generated_scenario
from journeysignal.bootstrap import build_ingester, load_settings, make_engine
from journeysignal.interfaces.cli.analytics import analyse_samples


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=1000)
    args = parser.parse_args()
    if not 1 <= args.count <= 10000:
        parser.error("count must be between 1 and 10000")
    settings = load_settings()
    engine = make_engine(settings)
    run_id = "bench" + uuid4().hex
    topic = "journeysignal.benchmark." + uuid4().hex
    ensure_topic(settings.kafka_bootstrap_servers, topic)
    attempts = generated_scenario(datetime.now(UTC), 42, args.count, "normal", run_id)
    source = KafkaSource(settings.kafka_bootstrap_servers, topic, run_id)
    ingest = build_ingester(settings, engine)
    samples: list[dict[str, float]] = []
    started = time.monotonic()
    try:
        publish(settings.kafka_bootstrap_servers, topic, attempts)
        publish_seconds = time.monotonic() - started
        deadline = time.monotonic() + 120
        while len(samples) < args.count and time.monotonic() < deadline:
            record = source.poll(0.5)
            if record is None:
                continue
            began = time.monotonic()
            result = ingest.execute(record)
            duration_ms = (time.monotonic() - began) * 1000
            source.acknowledge(record)
            if result.replay or result.outcome is None or result.outcome.findings:
                raise RuntimeError("Normal benchmark did not produce a fresh zero-finding receipt")
            samples.append({"processing_ms": duration_ms})
        if len(samples) != args.count:
            raise RuntimeError("Benchmark timed out before all records persisted")
    finally:
        elapsed = time.monotonic() - started
        source.close()
        engine.dispose()
    directory = Path("artifacts")
    directory.mkdir(exist_ok=True)
    sample_path = directory / "benchmark-samples.jsonl"
    sample_path.write_text("\n".join(json.dumps(s) for s in samples) + "\n")
    report = {
        "observed_at": datetime.now(UTC).isoformat(),
        "records": len(samples),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "elapsed_seconds": elapsed,
        "publish_seconds": publish_seconds,
        "publish_records_per_second": args.count / publish_seconds,
        "end_to_end_records_per_second": args.count / elapsed,
        "processing": analyse_samples(sample_path),
        "conditions": "One local broker, one synchronous processor, one DB transaction per record; "
        "elapsed includes publish, consumer assignment and offset commits. "
        "Processing samples cover ingestion through DB commit, excluding offset commit.",
        "interpretation": "Single synthetic local observation, not an SLA or production claim",
    }
    (directory / "benchmark.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
