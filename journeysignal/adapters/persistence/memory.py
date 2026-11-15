"""Transactional test adapter. Never used as the runtime durable store."""

from datetime import datetime
from threading import RLock
from types import TracebackType

from journeysignal.domain.events import Event, SourceRef
from journeysignal.domain.findings import QualityOutcome


class MemoryStore:
    def __init__(self) -> None:
        self.outcomes: dict[SourceRef, QualityOutcome] = {}
        self.identities: dict[str, str] = {}
        self.lock = RLock()

    def transaction(self) -> "MemoryTransaction":
        return MemoryTransaction(self)


class MemoryTransaction:
    def __init__(self, store: MemoryStore) -> None:
        self.store = store
        self.committed = False
        self.pending_ref: SourceRef | None = None

    def __enter__(self) -> "MemoryTransaction":
        self.store.lock.acquire()
        self.outcomes = self.store.outcomes.copy()
        self.identities = self.store.identities.copy()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.store.lock.release()

    def claim_receipt(self, source_ref: SourceRef, received_at: datetime) -> bool:
        if self.pending_ref is not None:
            raise RuntimeError("Only one new receipt per transaction")
        if source_ref in self.outcomes:
            return False
        self.pending_ref = source_ref
        return True

    def claim_business_id(self, event: Event) -> str | None:
        if self.pending_ref is None:
            raise RuntimeError("Claim a receipt before business identity")
        previous = self.identities.get(event.event_id)
        self.identities.setdefault(event.event_id, event.fingerprint)
        return previous

    def save(self, outcome: QualityOutcome) -> None:
        if outcome.source_ref != self.pending_ref:
            raise RuntimeError("Outcome does not match claimed receipt")
        self.outcomes.setdefault(outcome.source_ref, outcome)

    def commit(self) -> None:
        if self.pending_ref is not None and self.pending_ref not in self.outcomes:
            raise RuntimeError("Cannot commit an incomplete receipt outcome")
        self.store.outcomes = self.outcomes
        self.store.identities = self.identities
        self.committed = True
