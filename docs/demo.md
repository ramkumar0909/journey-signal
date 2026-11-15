# Reproducible abandoned-cart experiment

Start the complete stack using the README. For a live demo:

```bash
uv run journeysignal simulate --scenario scenarios/black_friday.yaml --seed 42 \
  --fault-profile known --run-id demo1
uv run journeysignal report --window-minutes 30
```

For an offline preview with reproducible timestamps:

```bash
uv run journeysignal simulate --fault-profile known --seed 42 \
  --start-time 2026-11-15T10:00:00Z --dry-run
```

Do not publish that historical timestamp into a current-time worker and expect the fixed
oracle: all normally timed records can become late. E2E injects a fixed receive clock
inside the test harness. Production worker time is never overridden by message content.

## Exact 15-record oracle

The exact oracle uses a single-partition topic. All event types are add_to_cart.
Ten normal records are followed by two new records
missing customer_id, one duplicate of the first normal record, one unsupported schema,
and one event 301 seconds old. The five faulty **occurrences are mutually exclusive**;
the first occurrence of the duplicated ID remains normal. Producer time is the fixture
start. Receive time is the same fixed start for every occurrence.

| Measure | Expected |
|---|---:|
| Receipts | 15 |
| Parsed / quarantined | 14 / 1 |
| JSQ-001 / JSQ-002 / JSQ-003 / JSQ-004 / JSQ-005 | 1 / 2 / 1 / 1 / 0 |
| Findings / exposures / affected receipts | 5 / 5 / 5 |
| Zero-finding parsed receipts / quality pass rate | 10 / 10÷14 |

Every exposure refers to `abandoned_cart_reminder` and its configured dependency ID.
An invalid product_viewed record has no association with that journey. Malformed JSON
cannot establish an event type and has no speculative journey exposure.

```bash
JS_TEST_DATABASE_URL=postgresql+psycopg://journeysignal_owner:disposable_owner_only@127.0.0.1:5432/journeysignal \
JS_TEST_KAFKA_SERVERS=127.0.0.1:9092 \
uv run pytest -q tests/e2e/test_seeded_demo.py
```

The test publishes to a unique real Kafka topic, persists to a migrated isolated real
PostgreSQL schema, and asserts exact counts through the FastAPI HTTP interface using
TestClient. It writes `artifacts/e2e-report.json`. Container smoke checks separately
exercise a running HTTP listener. The integration suite also tests malformed-record
continuation, fresh-group replay, restart between DB commit and acknowledgement, and an
injected transient failure before database commit.

## Interpret the report

The report shows missing identity on two event receipts, JSQ-002, and the reason that
the dependency requires customer_id. The recommended investigation is to validate
storefront identity mapping before downstream audience activation. This is evidence
for triage, not proof that any real customer was targeted incorrectly.

`--fault-profile normal` emits all five event categories without injected faults.
`--fault-profile faulty` uses seeded, exclusive fault choices and the YAML fault rate.
`--seed`, `--count`, `--run-id`, `--start-time`, `--rate-per-second`, `--output`, `--topic`
and `--bootstrap-servers` are supported. `--dry-run` writes files without broker access.
At a fixed start/run ID/seed, outputs are reproducible. Live observed freshness may
add findings if publishing or consumption takes longer than the configured threshold.

The manifest contains expected injections and confirmed transport metadata outside the
event bodies. Detection never reads it. Reusing the same run ID in a later publication
intentionally demonstrates additional business duplicates; use a fresh ID for clean
comparisons. Use fresh data volumes only when you deliberately want to discard previous
local results. Small test topics have 24-hour record retention; delete obsolete topics
manually in long-lived development environments.
