# ADR 004: PostgreSQL operational store; optional DuckDB analytics

Accepted. PostgreSQL owns receipts, logical identity, events, findings and exposures.
Use parameterised SQLAlchemy queries and Alembic migrations. Operational counts are
query-time aggregates; do not add a premature hourly aggregate service. Polars analyses
bounded offline measurement files. DuckDB is an optional, exercised export-query extra.
