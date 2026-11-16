# JourneySignal

**When customer events fail, know which journeys are at risk.**

JourneySignal makes customer-event data failures visible in terms of affected business
journeys, helping digital teams investigate problems before downstream activation.
The Milestone 1 implementation uses deterministic rules and synthetic ecommerce data.

It detects contract failures, missing customer identity, business duplicates, late events
and future timestamps, then associates findings with the configured
`abandoned_cart_reminder` dependency. Every finding has a source reference, safe evidence
and an investigation step. Associations describe **potential exposure**, not confirmed
campaign failure, lost customers or measured revenue. There are no live CDP integrations,
marketing sends, identity graphs or LLM features.

## Run the complete local demo

Prerequisites: Docker with Compose v2, uv, internet access for the first install, and
available local ports 5432, 9092, 8000 and 9101. Python 3.13 is selected by the project;
uv can download it. Infrastructure uses disposable local credentials and loopback ports.

```bash
uv sync --locked
# Build the shared application image once, then start the complete stack.
docker compose build api
docker compose --profile app up -d --wait
uv run journeysignal simulate --scenario scenarios/black_friday.yaml --seed 42 --fault-profile known
uv run journeysignal report --window-minutes 30
curl --fail http://127.0.0.1:8000/health/ready
```

The known scenario publishes 15 records with five mutually exclusive faulty receipts:
JSQ-001: 1, JSQ-002: 2, JSQ-003: 1, JSQ-004: 1, JSQ-005: 0. With a fresh database,
14 records parse, 10 have zero findings, and five receipts have potential journey impact.
Use the [deterministic E2E test](docs/demo.md) for a fixed-clock exact oracle. A live demo's
age depends on how long records wait before consumption.

For the larger mixed-event demonstration:

```bash
uv run journeysignal simulate --seed 42 --count 1000 --run-id blackfriday1000
uv run journeysignal report
```

The CLI writes synthetic event bodies and a separate ground-truth manifest under ignored
`artifacts/simulation/`. A repeated run ID deliberately reuses logical IDs; use a new
`--run-id` for a new experiment. See [demo details](docs/demo.md).

## Develop with host processes

Choose this instead of the containerised API/worker above:

```bash
cp .env.example .env
uv sync --locked --all-extras
docker compose up -d --wait
uv run alembic upgrade head
uv run journeysignal init-topics
# Separate terminals:
uv run journeysignal-worker
uv run uvicorn journeysignal.interfaces.api.app:app --host 127.0.0.1 --port 8000 --no-access-log
```

The owner URL is used only by Alembic, the worker uses a non-owner writer, and the API
uses a reader. Configuration has no embedded runtime credential fallback. Local Compose
is a development environment, not a production-secured Kafka deployment.

## Verify

```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy journeysignal
uv run pytest -q tests/unit tests/property tests/contract
uv run bandit -q -r journeysignal
uv run pip-audit
```

For the complete real-infrastructure suite (temporary isolated schemas and a restart-test
database; the test role needs CREATE SCHEMA and CREATEDB):

```bash
docker compose up -d --wait
JS_TEST_DATABASE_URL=postgresql+psycopg://journeysignal_owner:disposable_owner_only@127.0.0.1:5432/journeysignal \
JS_TEST_KAFKA_SERVERS=127.0.0.1:9092 \
uv run pytest -q tests --cov --cov-report=xml
```

Without test connection variables, infrastructure-dependent tests explicitly skip.
Skipping them is not evidence that Kafka/PostgreSQL integration passed. CI supplies the
variables and starts real services. See [actual local verification](docs/verification.md);
GitHub-hosted CI has not been run from this workspace.

## Design and operations

One package, separate API/worker/simulator processes, framework-free domain, explicit
ports and adapters. Delivery is **at least once with idempotent writes**. Transport replay
and new business duplicate occurrences have different durable semantics.

- [Architecture and diagrams](docs/architecture.md), [ADRs](docs/adr/001-boundaries.md)
- [Event contract](docs/event-contract.md), [rules](docs/quality-rules.md), [read API](docs/api.md)
- [Operations](docs/operations.md), [measurement](docs/measurement.md), [implementation checklist](docs/implementation-plan.md)
- [Contributing](CONTRIBUTING.md), [security](SECURITY.md), [GPLv3 licence](LICENSE)

M1 intentionally has no frontend, authentication service, automatic remediation or
missing-event baseline. M2 may add incident lifecycle and a lightweight dashboard; M3
connectors and schema registry support; M4 optional grounded summaries after evaluation.
No later-milestone infrastructure is included in the runtime.
