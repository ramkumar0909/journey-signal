# ADR 002: At-least-once delivery and transactional idempotency

Accepted. Disable Kafka automatic commit and offset storage. Commit offsets only after
the whole receipt transaction commits. Use unique transport identity and an atomic
first-seen business-ID registry. Store malformed-record quarantine metadata in that
same transaction. No Kafka exactly-once or DB/Kafka distributed transaction is claimed.
