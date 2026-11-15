"""Immutable domain values; transport receipt identity differs from business identity."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class EventType(StrEnum):
    PAGE_VIEW = "page_view"
    PRODUCT_VIEWED = "product_viewed"
    ADD_TO_CART = "add_to_cart"
    CHECKOUT_STARTED = "checkout_started"
    ORDER_COMPLETED = "order_completed"


@dataclass(frozen=True)
class SourceRef:
    topic: str
    partition: int
    offset: int


@dataclass(frozen=True)
class TransportRecord:
    source_ref: SourceRef
    value: bytes | None


@dataclass(frozen=True)
class Event:
    event_id: str
    event_type: EventType
    schema_version: str
    event_time: datetime
    produced_at: datetime
    source: str
    customer_id: str | None
    session_id: str | None
    properties: tuple[tuple[str, str | int], ...]
    fingerprint: str


@dataclass(frozen=True)
class ParseFailure:
    code: str
    field_paths: tuple[str, ...] = ()
    event_id: str | None = None
    event_type: EventType | None = None


@dataclass(frozen=True)
class ParsedRecord:
    event: Event | None
    failure: ParseFailure | None
