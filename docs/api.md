# Read API v1

The running API exposes `/docs` and `/openapi.json` with typed response schemas.
This document describes implemented endpoints. All potentially unbounded detail lists
use keyset pagination; no endpoint accepts arbitrary SQL or sort expressions.

| GET path | Behaviour |
|---|---|
| /health/live | 200, process liveness; no dependency check |
| /health/ready | 200 when expected DB migration is present; otherwise 503 Problem Details |
| /api/v1/quality/overview | Receipt/parsed/quarantine counts, pass-rate inputs, grouped findings and event-age percentiles |
| /api/v1/quality/findings | Filters: rule_id, severity; limit 1–100 (default 50), cursor |
| /api/v1/journeys | Configured catalog including dependencies, required fields, rules and owner role |
| /api/v1/journeys/{journey_id}/impact | Windowed affected receipt count, finding count and paginated findings with dependency reasons |
| /api/v1/events/{event_id}/findings | Findings for all occurrences of a logical event ID, paginated; unknown IDs return an empty page |
| /metrics | Prometheus diagnostics; not durable quality totals |

## Time and counting

Overview and impact accept `from`/`to` aware ISO timestamps. Bounds are `[from,to)`;
default `to` is current UTC and default `from` is 30 minutes before `to`. Empty or
reversed ranges and naive timestamps return 422. Overview scopes receipts by receive
time and findings by detection time (currently captured at receive time).

`quality_pass_rate = zero_finding_parsed_records / parsed_records`, or null when there
are no parsed records. Parsed includes valid-contract business duplicates and missing
identity events. Contract failures are quarantined and excluded from this denominator.
`quarantined_records` includes all contract failures; `bad_json_count` is the subset
with invalid_json, object_required or null_record. Oversize/schema failures remain
visible in quarantine and JSQ-001 totals. A receipt can have multiple findings.

`affected_events` is distinct transport receipts having relevant findings, including
contract-invalid receipts whose allowlisted type establishes the configured dependency.
It is not distinct logical event IDs or customers. Every associated journey is a
configured potential exposure. Event-age percentiles are SQL continuous percentiles of
receive minus event time for parsed records, including negative ages for future events.

## Examples

Run the seeded E2E test to produce a real `artifacts/e2e-report.json`. Its fixed test
clock is 2026-11-15T10:00:00Z; the following query scopes that test window:

```bash
curl --get http://127.0.0.1:8000/api/v1/quality/overview \
  --data-urlencode 'from=2026-11-15T09:59:59Z' \
  --data-urlencode 'to=2026-11-15T10:00:01Z'
curl 'http://127.0.0.1:8000/api/v1/quality/findings?rule_id=JSQ-002&severity=high&limit=2'
```

E2E uses an isolated schema which is removed after assertions, so those records do not
appear in the separate running server. Publish the live demo and omit explicit times
to inspect the running server. Expected fields from the verified fixed-clock response:

```json
{
  "transport_records": 15,
  "parsed_records": 14,
  "zero_finding_parsed_records": 10,
  "quarantined_records": 1,
  "bad_json_count": 0,
  "quality_pass_rate": 0.7142857142857143,
  "finding_counts_by_rule": {"JSQ-001": 1, "JSQ-002": 2, "JSQ-003": 1, "JSQ-004": 1},
  "pass_rate_definition": "zero_finding_parsed_records / parsed_records"
}
```

This is a field subset, not the entire response. Full overview also includes window
metadata, groups by severity/event type and freshness percentiles. The corresponding
journey response has affected_events=5, finding_count=5 and
impact_type=POTENTIAL_DATA_QUALITY_IMPACT. Finding items contain finding_id, rule_id,
category, severity, nullable logical identity/type, source_ref, detected_at, allowlisted
evidence, message, suggested_action and associated_journeys.

## Paging and errors

Lists return `{"items": [...], "next_cursor": "..."}`; next_cursor is null at the end.
Sort is ascending detection timestamp then finding UUID. Repeat the same filters when
using a cursor. For impact paging, repeat the **explicit returned window bounds** as
`from`/`to`; a moving default window is a different scope. Cursors are bounded, validated
and bound to their filters. They are opaque transport tokens, not authentication.
Concurrent older inserts are not a snapshot guarantee; start a new scan for a refreshed view.

Unknown journeys return 404; invalid query parameters 422; malformed or mismatched
cursors 400; storage errors 503; unexpected service errors 500. Errors do not echo
submitted values, SQL or stack traces:

```json
{"type":"about:blank","title":"Service unavailable","status":503,"detail":"Storage is temporarily unavailable"}
```

Content type is `application/problem+json`. No raw event-payload endpoint exists.
