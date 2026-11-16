# Contributing

Use Python 3.13 and `uv sync --locked --all-extras`. Follow the README verification
commands before submitting a change. The full suite requires real Compose Kafka and
PostgreSQL with JS_TEST_DATABASE_URL and JS_TEST_KAFKA_SERVERS; skips are not passes.
The test DB owner needs CREATE SCHEMA and CREATEDB for isolated migration/restart tests.
Core domain/application coverage is gated at 85%, including branches.

Keep domain imports framework-free. New quality rules implement QualityRule, use a stable
ID, are registered in fixed order and have positive, negative and time-boundary tests.
Update the rule catalogue, config validator and dependency examples together. Repository
changes must preserve transport replay and cross-partition logical duplicate semantics.
Use Alembic for schema changes; never edit a released migration to change deployed data.

Use original code and synthetic fixtures only. Test behaviour rather than private details.
Update docs and exact demonstrations with implementation changes. Do not include raw event
payloads, .env files, local artifacts or IDE settings. Use concise Conventional Commit
messages and describe validation actually performed. No GitHub CI claim before it runs.

M1 excludes real customer integrations, marketing sends, LLM features and identity graphs.
Discuss later-milestone changes separately. Contributions retain the repository GPLv3
licence; dependency changes need compatibility, wheel and licence review, lock updates
and security auditing. See SECURITY.md for vulnerability reporting and exceptions.
