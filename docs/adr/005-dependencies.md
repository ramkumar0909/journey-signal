# ADR 005: Dependency and licence decisions

Accepted. Require CPython 3.13, uv and a generated hash-bearing uv.lock. Stable versions
must resolve and install together before being recorded as verified. Use FastAPI,
Pydantic 2, SQLAlchemy 2, Alembic, Psycopg 3 and synchronous confluent-kafka 2.x.
Use pytest/Hypothesis, Ruff, strict mypy, Bandit and pip-audit. Compose supplies official
Apache Kafka KRaft and PostgreSQL images. Exact resolved versions are in uv.lock.

References: https://docs.confluent.io/kafka-clients/python/current/overview.html,
https://pypi.org/project/confluent-kafka/,
https://devguide.python.org/versions/,
https://duckdb.org/docs/current/guides/python/polars,
https://pandera.readthedocs.io/en/stable/polars.html.

Confluent 2.16.1 was the published candidate during planning, not an installation
claim. Resolve an available stable 2.x release and record any change with actual
index evidence. Do not invent versions or a lockfile. Pandera and OpenTelemetry SDK
are omitted because M1 needs neither dataframe contracts nor tracing infrastructure;
Prometheus and structured logs cover the implemented diagnostics.

Preserve the repository's GPLv3 licence. Do not replace existing licence terms.

## Observed resolution and metadata review

The generated lock installed successfully on CPython 3.13.18. Primary resolved versions:
FastAPI 0.163.1, Pydantic 2.14.3, confluent-kafka 2.16.1, SQLAlchemy 2.0.57, Alembic
1.20.1, Psycopg 3.3.9, Polars 1.44.2 and optional DuckDB 2.0.0. Official runtime images
are PostgreSQL 18.7, Kafka 4.4.0 and Python 3.13.18-slim; digests are in Compose/Dockerfile.
Python 3.13 wheels were actually downloaded for macOS and the Linux application image.

Installed package metadata reports MIT for FastAPI/Pydantic/SQLAlchemy/Alembic/Polars/
DuckDB, Apache for Confluent, LGPL-3.0-only for Psycopg, and Apache-2.0 AND BSD-2-Clause
for prometheus-client. Retain upstream distribution notices; do not relicense dependencies
as project code. Existing GPLv3 project terms are preserved. pip-audit reported no known
vulnerabilities during local verification. Review metadata and advisories again on updates.
