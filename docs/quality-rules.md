# Quality rules

| ID | Meaning | Severity |
|---|---|---|
| JSQ-001 | Malformed/oversized JSON or contract mismatch | high |
| JSQ-002 | add_to_cart missing customer_id | high |
| JSQ-003 | New transport receipt reuses a valid logical event ID | medium |
| JSQ-004 | receive time minus event time > freshness seconds (300 default) | medium |
| JSQ-005 | event time minus receive time > future skew seconds (30 default) | high |

Rules run in this fixed order; disabled rule IDs are validated at startup. Contract
failures skip rules requiring a valid event. They do not reserve logical identity.
Exact threshold equality passes; 1 ms over fails. Freshness evidence separates
producer and transport delay, but cannot prove which component caused a delay.

Replay is an already committed transport identity and creates no additional outcome.
A business duplicate is a new receipt; its fingerprint may match or differ from
the first approved event. Fingerprints canonicalise validated fields and UTC times.
Out-of-order arrivals are not inherently late. Absent events cannot be detected
from the records which arrive. Future timestamps can indicate clock error rather
than fraud. Severity is configured engineering triage, not calibrated financial risk.

Journey association requires matching configured event type and relevant rule.
Unsupported schemas may retain a safely allowlisted event type. Unknown types and
malformed JSON have no speculative association. Every exposure has a dependency ID
and reason. Missing identity does not prove a customer was mistargeted.
