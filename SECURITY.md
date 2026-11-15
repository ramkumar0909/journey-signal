# Security policy

JourneySignal M1 is a synthetic-data local development product, not a production CDP,
authorisation system, privacy certification or marketing service. Do not feed real names,
emails, phone numbers, payment records, cookies or customer exports into it. Synthetic
prefix checks cannot prove an identifier is fictitious; the operator must use synthetic data.

Do not open a public issue containing secrets, private payloads or exploit credentials.
Use the repository's GitHub **Report a vulnerability** / private security advisory feature
when enabled. If unavailable, ask maintainers for a private disclosure channel without
including sensitive details. No private mailbox or response-time SLA is invented here.
Until a release/support policy exists, security fixes target the current main branch.

Runtime stores only allowlisted fields. Logs omit arbitrary input and exception text.
Configuration secrets are env-only; .env is ignored. Compose's explicitly disposable
credentials and plaintext loopback Kafka are for local use only. API and worker use
non-owner roles; the application container runs as UID 10001 on a read-only filesystem.
No CORS or administrative write API is enabled. There is no multi-user authentication;
keep all ports loopback-bound and design deployed authentication separately.

CI runs Bandit, pip-audit and pinned/checksum-verified Gitleaks. Dependency audit fails
on any reported vulnerability rather than silently ignoring severity. The local package
is scanned as source and is not expected to have a PyPI advisory identity. Before any
exception, record the advisory/rule, affected version/path, reachability, mitigation,
review owner and expiry in a reviewed security ADR. Never blanket-disable a scanner or
publish tokens in an exception. No advisory exceptions are currently configured.

Image digests, lockfile hashes and CI action revisions are pinned; periodically review
updates. A successful scanner run is bounded evidence, not proof of absence of defects.
