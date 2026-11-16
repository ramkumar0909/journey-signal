# Event contract v1

Only synthetic_storefront events are accepted. The allowlisted event types are
page_view, product_viewed, add_to_cart, checkout_started and order_completed. The
only schema version is `1.0`. Unknown fields are rejected, never stored verbatim.

Envelopes require event_id (`ev_…`), type, version, event_time, produced_at, source
and properties. Optional customer/session IDs must use `cust_demo_…`/`sess_demo_…`
synthetic prefixes. Missing add-to-cart identity is a quality finding, not a parse
failure. IDs are opaque and are never evidence that a real person exists.

Properties:

| Type | Required keys |
|---|---|
| page_view | page_id (`page_demo_…`) |
| product_viewed | sku (`SKU-DEMO-…`), currency, unit_price_minor |
| add_to_cart | product keys plus cart_id (`cart_…`), quantity |
| checkout_started | cart_id, currency, total_minor |
| order_completed | checkout keys plus order_id (`order_demo_…`) |

Currency is GBP/USD/EUR. Money is an integer in [0,100000000], quantity an integer
in [1,1000]. Boolean and floating money values are rejected. Strings are bounded.
Timestamps must be aware ISO-8601 strings in years 2000–2100; offsets normalise to
UTC. No implicit naive-time conversion. Transport accepts at most 64 KiB and eight
container levels by default. Duplicate JSON keys and non-finite numbers fail.

event_time is action time; produced_at is producer diagnostic time; received_at is
the worker's injected receive clock. Persistence time is measured separately.
Transport identity is topic/partition/offset; event_id is untrusted logical identity.
All valid occurrences remain inspectable. A new supported version requires an
explicit parser and compatibility tests; incompatible versions are quarantined.
