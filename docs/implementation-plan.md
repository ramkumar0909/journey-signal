# Implementation plan and verification checklist

Implement M1 only, in sequence. Every slice includes code, behaviour tests, docs and
an executable example. Check format, lint, strict typing and relevant tests after
each slice. A blocked check is not a passed check.

- [x] A: Architecture/ADRs, packaging/lock, settings, entrypoints, health smoke tests.
- [x] B: Immutable domain, bounded parsing, five rules, YAML mapping, unit/property tests.
- [x] C: Migrations, PostgreSQL transaction adapter, in-memory shared contracts.
- [x] D: Compose/Kafka, seeded simulator/manifest, worker/retry/replay integration tests.
- [x] E: Typed read API, bounded pagination, exact 15-record API E2E, report CLI.
- [x] F: Metrics/logs, hardening, CI/security, documentation, benchmark and final audit.

## Initial environment evidence

Repository: README and GPLv3 LICENSE, untracked IDE settings. No application yet.
Python on PATH: 3.9.6; uv and Python 3.13 not found in checked locations.
Docker client exists but sandbox socket access is denied. Shell PyPI DNS fails.
No tests have run. Resolve environment access before claiming locked installation or
real integration success. Preserve user changes and IDE configuration.

## Completed slice evidence

A: format/lint/strict typing, CLI version and 2 health contracts passed.
B: 25 unit/property/contract tests passed, including the deterministic offline fixture.
C: 32 tests passed with real PostgreSQL rollback/concurrency contracts.
D: 36 tests passed including real Kafka poison/replay integration.
E: 45 tests passed including exact public-API E2E; core coverage 85.10% at that point.
F: expanded suite reached 62 passed / 99.05% coverage; scanners clear and hardened
Compose stack healthy. Final additional regression and actual SIGKILL evidence are
recorded in docs/verification.md. GitHub-hosted CI has not run here.
