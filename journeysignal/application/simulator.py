"""Synthetic, seeded fixtures. Ground truth stays outside published event bodies."""

import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta

from journeysignal.domain.events import EventType


@dataclass(frozen=True)
class Attempt:
    value: bytes
    expected_rule_ids: tuple[str, ...] = ()


def event_payload(
    index: int,
    start: datetime,
    run_id: str,
    event_type: EventType = EventType.ADD_TO_CART,
) -> dict[str, object]:
    product: dict[str, object] = {
        "sku": f"SKU-DEMO-{index % 20}",
        "currency": "GBP",
        "unit_price_minor": 90000,
    }
    checkout: dict[str, object] = {
        "cart_id": f"cart_{index}",
        "currency": "GBP",
        "total_minor": 90000,
    }
    properties: dict[str, object]
    if event_type == EventType.PAGE_VIEW:
        properties = {"page_id": f"page_demo_{index % 5}"}
    elif event_type == EventType.PRODUCT_VIEWED:
        properties = product
    elif event_type == EventType.ADD_TO_CART:
        properties = {**product, "cart_id": f"cart_{index}", "quantity": 1}
    elif event_type == EventType.CHECKOUT_STARTED:
        properties = checkout
    else:
        properties = {**checkout, "order_id": f"order_demo_{index}"}
    return {
        "event_id": f"ev_{run_id}_{index:06d}",
        "event_type": event_type.value,
        "schema_version": "1.0",
        "event_time": start.isoformat(),
        "produced_at": start.isoformat(),
        "source": "synthetic_storefront",
        "customer_id": f"cust_demo_{index:06d}",
        "session_id": f"sess_demo_{index:06d}",
        "properties": properties,
    }


def encode(payload: dict[str, object], rules: tuple[str, ...] = ()) -> Attempt:
    return Attempt(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(), rules)


def known_scenario(start: datetime, run_id: str = "known42") -> tuple[Attempt, ...]:
    normal = [event_payload(i, start, run_id) for i in range(10)]
    attempts = [encode(p) for p in normal]
    for i in (10, 11):
        missing = event_payload(i, start, run_id)
        missing.pop("customer_id")
        attempts.append(encode(missing, ("JSQ-002",)))
    attempts.append(encode(normal[0], ("JSQ-003",)))
    unsupported = event_payload(13, start, run_id)
    unsupported["schema_version"] = "99.0"
    attempts.append(encode(unsupported, ("JSQ-001",)))
    late = event_payload(14, start, run_id)
    late["event_time"] = (start - timedelta(seconds=301)).isoformat()
    attempts.append(encode(late, ("JSQ-004",)))
    return tuple(attempts)


def generated_scenario(
    start: datetime,
    seed: int,
    count: int,
    fault_profile: str,
    run_id: str,
    fault_rate: float = 0.15,
) -> tuple[Attempt, ...]:
    if count < 1 or count > 100000 or not 0 <= fault_rate <= 1:
        raise ValueError("Invalid count or fault rate")
    if fault_profile not in {"normal", "faulty"}:
        raise ValueError("Unknown generated fault profile")
    # Seeded synthetic fixture generation is deliberately reproducible, not cryptographic.
    rng = random.Random(seed)  # nosec B311
    attempts: list[Attempt] = []
    first_payload: dict[str, object] | None = None
    for index in range(count):
        payload = event_payload(index, start, run_id, rng.choice(tuple(EventType)))
        rules: tuple[str, ...] = ()
        if fault_profile == "faulty" and index > 0 and rng.random() < fault_rate:
            fault = rng.choice(("identity", "schema", "duplicate", "late"))
            if fault == "identity":
                payload = event_payload(index, start, run_id)
                payload.pop("customer_id")
                rules = ("JSQ-002",)
            elif fault == "schema":
                payload["schema_version"] = "99.0"
                rules = ("JSQ-001",)
            elif fault == "duplicate":
                if first_payload is None:
                    raise RuntimeError("Missing first synthetic event")
                payload = first_payload.copy()
                rules = ("JSQ-003",)
            else:
                payload["event_time"] = (start - timedelta(seconds=301)).isoformat()
                rules = ("JSQ-004",)
        if index == 0:
            first_payload = payload.copy()
        attempts.append(encode(payload, rules))
    return tuple(attempts)
