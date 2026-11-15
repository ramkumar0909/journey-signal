"""Bounded, strict event contract parsing with safe error metadata only."""

import hashlib
import json
import re
from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, ValidationError, field_validator

from journeysignal.domain.events import Event, EventType, ParsedRecord, ParseFailure

EventId = Annotated[str, Field(pattern=r"^ev_[A-Za-z0-9_-]{1,96}$")]
OpaqueCustomer = Annotated[str, Field(pattern=r"^cust_demo_[A-Za-z0-9_-]{1,64}$")]
OpaqueSession = Annotated[str, Field(pattern=r"^sess_demo_[A-Za-z0-9_-]{1,64}$")]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_max_length=128, strict=True)


class PageProperties(StrictModel):
    page_id: Annotated[str, Field(pattern=r"^page_demo_[A-Za-z0-9_-]+$")]


class ProductProperties(StrictModel):
    sku: Annotated[str, Field(pattern=r"^SKU-DEMO-[A-Za-z0-9_-]+$")]
    currency: Literal["GBP", "USD", "EUR"]
    unit_price_minor: Annotated[StrictInt, Field(ge=0, le=100000000)]


class CartProperties(ProductProperties):
    cart_id: Annotated[str, Field(pattern=r"^cart_[A-Za-z0-9_-]+$")]
    quantity: Annotated[StrictInt, Field(ge=1, le=1000)]


class CheckoutProperties(StrictModel):
    cart_id: Annotated[str, Field(pattern=r"^cart_[A-Za-z0-9_-]+$")]
    currency: Literal["GBP", "USD", "EUR"]
    total_minor: Annotated[StrictInt, Field(ge=0, le=100000000)]


class OrderProperties(CheckoutProperties):
    order_id: Annotated[str, Field(pattern=r"^order_demo_[A-Za-z0-9_-]+$")]


class Envelope(StrictModel):
    event_id: EventId
    event_type: str
    schema_version: Literal["1.0"]
    event_time: datetime
    produced_at: datetime
    source: Literal["synthetic_storefront"]
    customer_id: OpaqueCustomer | None = None
    session_id: OpaqueSession | None = None
    properties: dict[str, object]

    @field_validator("event_time", "produced_at", mode="before")
    @classmethod
    def aware_timestamp(cls, value: object) -> datetime:
        if not isinstance(value, str) or len(value) > 40:
            raise ValueError("ISO timestamp required")
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("Timezone required")
        parsed = parsed.astimezone(UTC)
        if not 2000 <= parsed.year <= 2100:
            raise ValueError("Timestamp outside synthetic contract range")
        return parsed


PROPERTY_MODELS: dict[EventType, type[StrictModel]] = {
    EventType.PAGE_VIEW: PageProperties,
    EventType.PRODUCT_VIEWED: ProductProperties,
    EventType.ADD_TO_CART: CartProperties,
    EventType.CHECKOUT_STARTED: CheckoutProperties,
    EventType.ORDER_COMPLETED: OrderProperties,
}
SAFE_FIELDS = frozenset(Envelope.model_fields) | frozenset(
    field for model in PROPERTY_MODELS.values() for field in model.model_fields
)


def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON key")
        result[key] = value
    return result


def reject_constant(value: str) -> None:
    raise ValueError("Non-finite number")


class EventParser:
    def __init__(self, max_bytes: int = 65536, max_depth: int = 8) -> None:
        self.max_bytes = max_bytes
        self.max_depth = max_depth

    def parse(self, raw: bytes | None) -> ParsedRecord:
        if raw is None:
            return ParsedRecord(None, ParseFailure("null_record"))
        if len(raw) > self.max_bytes:
            return ParsedRecord(None, ParseFailure("record_too_large"))
        try:
            payload: object = json.loads(
                raw, object_pairs_hook=unique_object, parse_constant=reject_constant
            )
        except (ValueError, UnicodeDecodeError, RecursionError):
            return ParsedRecord(None, ParseFailure("invalid_json"))
        pending = [(payload, 0)]
        while pending:
            node, depth = pending.pop()
            if isinstance(node, (list, dict)):
                if depth >= self.max_depth:
                    return ParsedRecord(None, ParseFailure("nesting_limit"))
                children = node.values() if isinstance(node, dict) else node
                pending.extend((child, depth + 1) for child in children)
        if not isinstance(payload, dict):
            return ParsedRecord(None, ParseFailure("object_required"))
        raw_id = payload.get("event_id")
        event_id = (
            raw_id
            if isinstance(raw_id, str) and re.fullmatch(r"ev_[A-Za-z0-9_-]{1,96}", raw_id)
            else None
        )
        raw_type = payload.get("event_type")
        event_type = (
            EventType(raw_type)
            if isinstance(raw_type, str) and raw_type in set(EventType)
            else None
        )
        if event_type is None:
            return ParsedRecord(None, ParseFailure("unsupported_event_type", (), event_id))
        try:
            envelope = Envelope.model_validate(payload)
            properties = PROPERTY_MODELS[event_type].model_validate(envelope.properties)
        except ValidationError as error:
            paths = tuple(
                sorted(
                    {
                        ".".join(
                            str(part) if part in SAFE_FIELDS else "unknown" for part in item["loc"]
                        )
                        for item in error.errors(include_input=False, include_url=False)
                    }
                )
            )[:8]
            return ParsedRecord(
                None, ParseFailure("contract_mismatch", paths, event_id, event_type)
            )
        approved = envelope.model_dump(mode="json")
        approved["properties"] = properties.model_dump()
        fingerprint = hashlib.sha256(
            json.dumps(approved, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return ParsedRecord(
            Event(
                envelope.event_id,
                event_type,
                envelope.schema_version,
                envelope.event_time,
                envelope.produced_at,
                envelope.source,
                envelope.customer_id,
                envelope.session_id,
                tuple(sorted(properties.model_dump().items())),
                fingerprint,
            ),
            None,
        )
