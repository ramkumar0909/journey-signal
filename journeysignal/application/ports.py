"""Narrow application ports; SQL and broker objects never cross these boundaries."""

from datetime import datetime
from types import TracebackType
from typing import Protocol

from journeysignal.domain.events import Event, ParsedRecord, SourceRef
from journeysignal.domain.findings import QualityOutcome


class EventParserPort(Protocol):
    def parse(self, raw: bytes | None) -> ParsedRecord: ...


class IngestionTransaction(Protocol):
    def __enter__(self) -> "IngestionTransaction": ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

    def claim_receipt(self, source_ref: SourceRef, received_at: datetime) -> bool: ...

    def claim_business_id(self, event: Event) -> str | None: ...

    def save(self, outcome: QualityOutcome) -> None: ...

    def commit(self) -> None: ...


class TransactionFactory(Protocol):
    def __call__(self) -> IngestionTransaction: ...
