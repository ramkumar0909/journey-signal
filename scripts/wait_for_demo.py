"""Bounded public-API smoke check for the isolated CI Compose database."""

import time

import httpx


def main() -> None:
    deadline = time.monotonic() + 30
    with httpx.Client(base_url="http://127.0.0.1:8000", timeout=3) as client:
        while time.monotonic() < deadline:
            response = client.get("/api/v1/quality/overview")
            response.raise_for_status()
            data = response.json()
            if data["transport_records"] >= 15:
                if data["finding_counts_by_rule"] != {
                    "JSQ-001": 1,
                    "JSQ-002": 2,
                    "JSQ-003": 1,
                    "JSQ-004": 1,
                }:
                    raise RuntimeError("Unexpected CI Compose demo findings")
                print("Containerised seeded demo: 15 receipts and 5 expected findings")
                return
            time.sleep(0.25)
    raise RuntimeError("Containerised demo did not complete before deadline")


if __name__ == "__main__":
    main()
