"""Synthetic simulator, safe API report and bounded offline analytics."""

import argparse
import json
import os
import re
from collections import Counter
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx

from journeysignal import __version__
from journeysignal.adapters.kafka.publisher import ensure_topic, publish
from journeysignal.application.simulator import generated_scenario, known_scenario
from journeysignal.interfaces.cli.scenarios import read_scenario, start_timestamp


def main() -> None:
    parser = argparse.ArgumentParser(description="JourneySignal synthetic event quality")
    parser.add_argument("--version", action="version", version=__version__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    topics = subcommands.add_parser("init-topics", help="Create the documented local event topic")
    topics.add_argument(
        "--bootstrap-servers", default=os.getenv("JS_KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    )
    topics.add_argument("--topic", default=os.getenv("JS_KAFKA_TOPIC", "journeysignal.events.v1"))
    topics.add_argument("--partitions", type=int, default=1)
    simulate = subcommands.add_parser(
        "simulate", help="Publish synthetic events and an offline oracle"
    )
    simulate.add_argument("--scenario", type=Path, default=Path("scenarios/black_friday.yaml"))
    simulate.add_argument("--seed", type=int, default=42)
    simulate.add_argument("--count", type=int)
    simulate.add_argument("--fault-profile", choices=["normal", "faulty", "known"])
    simulate.add_argument("--start-time")
    simulate.add_argument("--run-id")
    simulate.add_argument("--rate-per-second", type=float, default=0)
    simulate.add_argument("--dry-run", action="store_true")
    simulate.add_argument("--output", type=Path, default=Path("artifacts/simulation"))
    simulate.add_argument(
        "--bootstrap-servers", default=os.getenv("JS_KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    )
    simulate.add_argument("--topic", default=os.getenv("JS_KAFKA_TOPIC", "journeysignal.events.v1"))
    report = subcommands.add_parser(
        "report", help="Read quality and potential exposure through HTTP"
    )
    report.add_argument("--api-url", default="http://127.0.0.1:8000")
    report.add_argument("--window-minutes", type=int, default=30)
    analyse = subcommands.add_parser(
        "analyse", help="Summarise bounded benchmark samples with Polars"
    )
    analyse.add_argument("--input", type=Path, required=True)
    analyse.add_argument("--duckdb", action="store_true")
    args = parser.parse_args()
    try:
        if args.command == "init-topics":
            if not 1 <= args.partitions <= 32:
                raise ValueError("Partitions must be between 1 and 32")
            ensure_topic(args.bootstrap_servers, args.topic, args.partitions)
            print("Event topic ready")
        elif args.command == "simulate":
            config = read_scenario(args.scenario)
            profile = args.fault_profile or config.fault_profile
            count = (
                args.count
                if args.count is not None
                else (15 if profile == "known" else config.count)
            )
            start = start_timestamp(args.start_time)
            run_id = args.run_id or f"seed{args.seed}"
            if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", run_id) or args.rate_per_second < 0:
                raise ValueError("Invalid run ID or rate")
            if profile == "known" and count != 15:
                raise ValueError("Known scenario requires exactly 15 records")
            attempts = (
                known_scenario(start, run_id)
                if profile == "known"
                else generated_scenario(
                    start,
                    args.seed,
                    count,
                    profile,
                    run_id,
                    config.fault_rate,
                )
            )
            args.output.mkdir(parents=True, exist_ok=True)
            (args.output / "events.jsonl").write_bytes(
                b"\n".join(a.value for a in attempts) + b"\n"
            )
            expected = Counter(rule for attempt in attempts for rule in attempt.expected_rule_ids)
            manifest: dict[str, object] = {
                "seed": args.seed,
                "run_id": run_id,
                "start_time": start.isoformat(),
                "count": len(attempts),
                "fault_profile": profile,
                "expected_findings_by_rule": dict(sorted(expected.items())),
                "attempts": [
                    {"attempt_index": i, "expected_rule_ids": list(a.expected_rule_ids)}
                    for i, a in enumerate(attempts)
                ],
                "interpretation": "Synthetic injection oracle; never consumed by detection rules",
                "deliveries": [],
            }
            manifest_path = args.output / "manifest.json"
            manifest["publication_status"] = "dry_run" if args.dry_run else "prepared"
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
            if not args.dry_run:
                try:
                    ensure_topic(args.bootstrap_servers, args.topic)
                    manifest["deliveries"] = [
                        asdict(item)
                        for item in publish(
                            args.bootstrap_servers, args.topic, attempts, args.rate_per_second
                        )
                    ]
                    manifest["publication_status"] = "confirmed"
                except Exception:
                    manifest["publication_status"] = "failed_or_partial"
                    raise
                finally:
                    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
            print(
                json.dumps(
                    {
                        "records": len(attempts),
                        "expected_findings_by_rule": expected,
                        "dry_run": args.dry_run,
                        "output": str(args.output),
                    },
                    indent=2,
                )
            )
        elif args.command == "report":
            if not 1 <= args.window_minutes <= 44640:
                raise ValueError("Window minutes must be between 1 and 44640")
            end = datetime.now(UTC)
            params = {
                "from": (end - timedelta(minutes=args.window_minutes)).isoformat(),
                "to": end.isoformat(),
            }
            with httpx.Client(base_url=args.api_url, timeout=10) as client:
                overview = client.get("/api/v1/quality/overview", params=params)
                overview.raise_for_status()
                impact = client.get(
                    "/api/v1/journeys/abandoned_cart_reminder/impact", params=params
                )
                impact.raise_for_status()
            print(
                json.dumps({"quality": overview.json(), "journey_impact": impact.json()}, indent=2)
            )
        else:
            from journeysignal.interfaces.cli.analytics import analyse_samples

            print(json.dumps(analyse_samples(args.input, args.duckdb), indent=2))
    except (ValueError, OSError, RuntimeError, httpx.HTTPError):
        parser.exit(1, "Command failed; check configuration, inputs and dependency health.\n")


if __name__ == "__main__":
    main()
