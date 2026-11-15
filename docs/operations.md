# Operations

## Runtime choices

`docker compose up -d --wait` starts only Kafka and PostgreSQL. To run every process,
first `docker compose build api`, then `docker compose --profile app up -d --wait`.
The application image is shared by migration, topic setup, API and worker services;
only the API service defines a build, avoiding simultaneous image-tag exports.
`docker compose --profile app ps -a` shows completed setup jobs and running services.
Stop with `docker compose --profile app down`; this preserves named data volumes.
`down -v` deletes **this project's** local demo data; use only when intentionally resetting.

Alternatively use the README's host commands with `.env`. Do not run host and container
workers/APIs on the same ports simultaneously. The API does not start a consumer.

## Configuration

Settings load `.env` and `JS_` environment variables; environment wins. Invalid settings
fail startup. Never commit `.env`, deploy these disposable passwords, or enable SQL echo.

| Setting | Default / purpose |
|---|---|
| DATABASE_URL | Required PostgreSQL+psycopg URL; non-owner writer for worker |
| API_DATABASE_URL | Optional reader URL; API falls back to DATABASE_URL |
| MIGRATION_DATABASE_URL | Optional owner URL, used only by Alembic |
| KAFKA_BOOTSTRAP_SERVERS | 127.0.0.1:9092 (container internal listener kafka:29092) |
| KAFKA_TOPIC | journeysignal.events.v1 |
| KAFKA_GROUP_ID | journeysignal-worker-v1 |
| FRESHNESS_SECONDS | 300, range 1–86400 |
| FUTURE_SKEW_SECONDS | 30, range 0–3600 |
| MAX_RECORD_BYTES / MAX_JSON_DEPTH | 65536 / 8 |
| DISABLED_RULES | JSON list, e.g. `["JSQ-005"]`; unknown IDs fail startup |
| JOURNEY_CONFIG_PATH | Optional external validated YAML; otherwise packaged configuration |
| WORKER_METRICS_PORT / METRICS_BIND_HOST | 9101 / 127.0.0.1; container binds 0.0.0.0 behind loopback publishing |

Journey freshness must match the active M1 freshness rule threshold. To change it, supply
both an updated journey YAML and FRESHNESS_SECONDS. Rules can be independently disabled;
contract parsing and safe quarantine still run even when JSQ-001 reporting is disabled.
Changes to rule policy apply to new receipts, not already committed outcomes. Resetting
consumer offsets alone does not re-evaluate durable receipts.

## Health, metrics and logs

API `/health/live` checks process liveness; `/health/ready` verifies the database and
expected Alembic revision. It returns Problem Details with 503 when unavailable.
Kafka health is deliberately independent of API readiness. `/docs` and `/openapi.json`
expose the read API schema; CORS is disabled by default.

Worker metrics are at `http://127.0.0.1:9101/`. `journeysignal_worker_ready` is set after
successful dependency checks and cleared on failure/shutdown. Idle dependency checks
run every ten seconds. `journeysignal_worker_heartbeat_timestamp_seconds` advances on
successful polls. Alert if readiness is zero or heartbeat age exceeds 30 seconds.
A zero readiness gauge alone is also used by the Compose worker health check.

Metrics include handled receipts by disposition, new findings by rule, DB retry count,
receive-to-DB-return duration, event-to-receive age and cached lag by partition. Counters
are process-local diagnostics, reset on restart, and are not durable business totals.
Use the read API for idempotent counts. Topic/partition/offset correlate structured logs;
raw event bodies, arbitrary messages, headers and exception strings are excluded.

Inspect lag directly when necessary:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 --describe --group journeysignal-worker-v1
```

## Failure and replay

One record is processed per database transaction. Auto commit and auto offset storage
are disabled. Database commit precedes synchronous offset commit. A crash in between
causes safe transport replay. A new offset reusing an event ID creates JSQ-003; replay of
an old offset creates no additional finding. First-seen identity is protected across
partitions by a PostgreSQL primary key and atomic conflict handling.

Malformed and oversized records go to durable PostgreSQL quarantine metadata, not an
external DLQ. Find JSQ-001 via the API, inspect error codes/field paths, fix the producer,
and publish a corrected new record. No raw poison payload is retained. Never acknowledge
if that quarantine write fails. Alert on any quarantine spike; >0 for five minutes is an
initial investigation threshold, not an empirically calibrated operational SLA.

Transient connectivity, serialization/deadlock, capacity and selected availability errors
retry five times after the first attempt, with 0.25s exponential backoff capped at 4s plus
0–0.1s jitter. SQL statements are capped at 10s and connections at 3s. Non-transient errors
or exhaustion stop without acknowledging the failed offset. Alert on >5 DB retries in
five minutes and any worker exit; investigate database health before restarting. Never
skip a poison record manually to make the dashboard green.

Revoke callbacks do not commit speculative progress. Failed offset commits stop the loop;
completed DB writes remain replay-safe. SIGTERM/SIGINT stop after the current record's
bounded work; connections and consumer close in finally blocks. No later record is
acknowledged past a failed transaction. There is no infinite retry or in-memory history.

For a controlled replay demonstration, use a fresh consumer group on the same topic.
Receipts remain unchanged. Do not casually reset production consumer groups. Local Kafka
retains records for 24 hours; the operational database has no automatic retention job in
M1. Plan database retention and backups separately before any deployed usage.

## Security boundaries

Compose binds host services to loopback. Kafka has no TLS/SASL in this development setup.
The application runs as UID 10001, read-only, without Linux capabilities. The migration
owner has DDL rights; runtime writer has SELECT/INSERT/UPDATE, reader only SELECT. Both
are non-superusers. Broker and database images retain their vendor startup/user model.
There is no M1 multi-user authentication: do not expose the API to an untrusted network.

To increase local parallelism, create the topic with `journeysignal init-topics
--partitions N` before publishing and run multiple workers with the same group ID.
Assign each host worker a distinct WORKER_METRICS_PORT. Each processes one record at
a time; first-seen business identity remains database-serialised across partitions.
The exact fixture oracle uses one partition to make first-occurrence ordering explicit.
