# Verification record

Local observations, 15 November 2026. This file distinguishes executed checks from
configured CI. It is not a claim that GitHub Actions has run.

## Executed environment

- CPython 3.13.18, uv 0.13.8; Apple M4 / arm64, 24 GiB host memory.
- Docker 29.2.1; PostgreSQL 18.7 and Apache Kafka 4.4.0, pinned official image digests.
- The initial sandbox could neither resolve PyPI nor access Docker. Approved elevated
  commands successfully resolved dependencies, installed Python and ran real infrastructure.
- uv and managed Python were installed in temporary project-specific locations under
  `/private/tmp/journeysignal-tools` and `/private/tmp/journeysignal-python`. For this
  workspace session, uv is `/private/tmp/journeysignal-tools/bin/uv`; install uv normally
  for ongoing development. No system Python was replaced.

## Results so far

- `uv sync --locked --all-extras`: succeeded, including exercised DuckDB analytics extra.
- Ruff formatter/linter and strict mypy: passed; no application type suppressions.
- Bandit: passed after replacing runtime assertions with explicit guards.
- pip-audit: no known vulnerabilities; local journeysignal source package has no PyPI
  advisory identity and is reported as skipped by that tool, not by the source scanners.
- Gitleaks 8.30.1, verified against pinned published SHA-256: no leaks.
- Expanded layered suite: 62 passed with 99.05% domain/application branch-aware coverage
  before the last process-restart/property additions. Final result is recorded below.
- Real Kafka/PostgreSQL poison handling, replay, transaction rollback and concurrent
  cross-partition duplicate detection ran successfully.
- Fixed-clock 15-record public-API E2E: 15 receipts, 14 parsed, 5 findings, 5 exposures,
  5 potentially affected receipts, pass rate 10/14. Report: artifacts/e2e-report.json.
- Shared image built successfully; API and worker healthy as non-root, read-only
  containers. Migration and topic setup jobs exited 0. Live simulator/report commands
  returned the expected rule counts through a running HTTP listener.
- 1000 fresh normal events benchmark: see measurement.md for observations and conditions.

## Known gaps

- GitHub-hosted CI is configured but unexecuted in this workspace; its status is unknown.
- Installed FastAPI/Starlette emits a deprecation warning for its HTTPX TestClient path.
  The tests pass; no warning is suppressed. A future client migration should follow the
  framework's supported testing interface, rather than silently swapping the API client.
- Deployed authentication/TLS, automatic DB retention/backups, live integrations,
  multi-tenant controls and missing-event baselines are outside M1. No production
  readiness certification, revenue outcome or notification SLA is claimed.
- Linux CI and the local macOS run differ in platform wheels; CI must establish its
  own result. The Linux application image did install and boot locally under Docker.

## Final regression

Final full suite with all extras and real Kafka/PostgreSQL: **76 passed, 1 framework
deprecation warning, no skips**, in 35.45 seconds. Core branch-aware coverage: **99.05%**.
The suite includes actual worker SIGKILL/restart, deterministic retry before database
commit, poison/oversized-record continuation, exact and changed-content duplicates,
cross-partition concurrency, replay, corrected invalid events and public-API assertions.
Ruff formatting/lint, strict mypy (40 application modules), Bandit and pip-audit passed.
The final OpenAPI error-schema addition was separately checked with API contract tests.
Gitleaks was rerun after source and documentation changes: no leaks. Alembic check
reported no new upgrade operations; documentation links and workflow YAML parsed successfully.

## Acceptance evidence

- [x] Compose Kafka/PostgreSQL health checks pass; complete application stack boots.
- [x] Python 3.13 locked install succeeds; API/worker entrypoints run with env configuration.
- [x] Typed contracts, all five rules, bounded quarantine and safe evidence are implemented.
- [x] Replay, exact duplicates, changed-content collisions and cross-partition races are tested.
- [x] Validated journey configuration supplies dependency reasons and potential-impact wording.
- [x] Exact seeded counts are asserted through the API; live listener smoke test also passed.
- [x] Read API schemas, filters, paging, windows and redacted error boundaries are tested.
- [x] Format/lint/typing/security gates and real integration/E2E tests pass locally.
- [x] CI workflow is configured with pinned actions and least privileges; hosted status is unknown.
- [x] Metrics, safe logs, dependency readiness, graceful shutdown and SIGKILL recovery are checked.
- [x] Architecture/ADRs/contracts/rules/API/operations/demo/measurement/contributor docs are present.
- [x] Existing GPLv3 licence is preserved; fixtures contain synthetic data only.

Next recommended work: run the configured workflow on GitHub and review its Linux result.
Do not start M2 features without a separate request. The current Compose services and
local demo/benchmark data are left running for inspection.
