# Measurement and pilot design

## Definitions

The north-star hypothesis is shorter time from an introduced fault to actionable,
evidence-backed visibility of potentially affected journeys. M1 persists findings and
serves a read API; it does not send notifications. Measure receive-to-DB-return with a
monotonic clock, broker/publishing delay separately, and event-to-receive age with aware
wall-clock timestamps. Do not subtract wall clocks to estimate rule execution duration.

- Quality pass rate: zero-finding parsed receipts / parsed receipts; null for zero denominator.
- Affected events: unique consumed transport receipts with relevant findings, not customers.
- Associated journeys: configured dependency matches, not confirmed failures.
- Precision/recall: compare observed rule/receipt pairs against an external synthetic manifest.
- Traceability: proportion of findings having rule ID, source reference and (where mapped)
  dependency reason. Unmapped unknown-type poison records must not invent a journey reason.
- Robustness: durable totals unchanged after replay, no acknowledgement on failed persistence.

The 15-record fixed-clock E2E has five labelled positive rule/receipt pairs, all five
detected, with no extra findings. For these examples JSQ-001/002/003/004 precision and
recall are 1.0. JSQ-005 has no positive example in this fixture; do not infer its precision
or recall from that absence. Positive/negative boundary tests cover it separately.
These synthetic outcomes do not establish real-world false-positive rates.

## Re-run the local benchmark

With local infrastructure running, migrations applied and `.env` configured:

```bash
uv run python scripts/benchmark.py --count 1000
uv run journeysignal analyse --input artifacts/benchmark-samples.jsonl
# Optional, exercised analytics extra:
uv sync --locked --all-extras
uv run --all-extras journeysignal analyse --input artifacts/benchmark-samples.jsonl --duckdb
```

The harness publishes normal events to a unique Kafka topic and uses the actual parser,
rules and PostgreSQL transaction adapter, with synchronous offset commits. It requires
1000 fresh, zero-finding outcomes or fails. The harness itself records bounded samples;
the production worker does not retain an unbounded latency list. Record conditions,
versions and resources. Repeated runs use unique logical IDs and add durable records;
run them against a disposable development database. Artifacts are ignored by Git.

## Observed local result — 15 November 2026

One observed run of 1000 normal records on Apple M4 (Mac16,10), 24 GiB host memory,
macOS 26.6.2 arm64, CPython 3.13.18. Docker 29.2.1 reported 10 CPUs and approximately
7.65 GiB available memory. Kafka 4.4.0 and PostgreSQL 18.7 ran locally in Compose.
Another small demo and the containerised API/worker were active concurrently.

| Observation | Value |
|---|---:|
| Confirmed publish duration | 0.515 s |
| Publish throughput | 1,942.96 records/s |
| Whole experiment duration | 7.138 s |
| Whole experiment throughput | 140.10 records/s |
| Receive/application processing through DB commit-return p50 | 2.676 ms |
| Same duration p95 | 5.985 ms |

Whole duration includes publishing, consumer group assignment and offset acknowledgements.
Processing samples exclude broker wait and Kafka offset commit. Quantiles use Polars;
raw samples and metadata are in `artifacts/benchmark-samples.jsonl` and `benchmark.json`
from this workspace run. These are observations, not production throughput promises or SLAs.
The benchmark does not measure notification delivery, human review or campaign outcomes.

## Future controlled pilot

Pre-register labelled faults and evaluation windows. Measure detection precision/recall
by rule, receive-to-durable-finding p50/p95, source/dependency traceability and replay
invariance. Compare human time to identify relevant journeys with and without the tool;
record review disagreements and real false positives. Test fault-free controls as well
as injected failures. Define acceptance criteria with the customer before using results
commercially.

Potential commercial value may combine investigation time saved, remediation/rework
avoided and faster issue resolution, less operating/adoption cost. Event delay or count
alone cannot be converted into lost sales without causal evidence and customer validation.
