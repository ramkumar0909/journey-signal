import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError

from journeysignal.application.queries import (
    FindingFilter,
    FindingPage,
    ImpactCounts,
    Overview,
    TimeWindow,
)
from journeysignal.interfaces.api.app import create_app


class EmptyQuery:
    def ready(self) -> bool:
        return True

    def overview(self, window: TimeWindow) -> Overview:
        return Overview(window, 0, 0, 0, 0, 0, None, {}, {}, {}, None, None)

    def findings(self, filters: FindingFilter, limit: int, cursor: str | None) -> FindingPage:
        if cursor:
            raise ValueError("Invalid cursor")
        return FindingPage((), None)

    def impact_counts(self, journey_id: str, window: TimeWindow) -> ImpactCounts:
        return ImpactCounts(0, 0)


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/quality/findings?limit=0",
        "/api/v1/quality/findings?limit=101",
        "/api/v1/quality/findings?rule_id=unknown",
        "/api/v1/quality/findings?severity=invalid",
        "/api/v1/quality/overview?from=2026-11-15T10:00:00",
        "/api/v1/quality/overview?from=2026-11-16T00:00:00Z&to=2026-11-15T00:00:00Z",
    ],
)
def test_invalid_parameters_return_redacted_problems(path: str) -> None:
    with TestClient(create_app(query=EmptyQuery())) as client:
        response = client.get(path)
    assert response.status_code == 422
    assert response.headers["content-type"] == "application/problem+json"
    assert response.json()["status"] == 422


def test_empty_catalog_paging_and_openapi() -> None:
    with TestClient(create_app(query=EmptyQuery())) as client:
        assert client.get("/health/ready").status_code == 200
        assert client.get("/api/v1/quality/overview").json()["quality_pass_rate"] is None
        assert client.get("/api/v1/quality/findings").json() == {"items": [], "next_cursor": None}
        assert client.get("/api/v1/quality/findings?cursor=bad").status_code == 400
        assert client.get("/api/v1/journeys/unknown/impact").status_code == 404
        assert client.get("/api/v1/journeys").json()[0]["journey_id"] == "abandoned_cart_reminder"
        assert client.get("/api/v1/events/ev_unknown/findings").json()["items"] == []
        schema = client.get("/openapi.json").json()
        assert "OverviewResponse" in schema["components"]["schemas"]
        assert "Problem" in schema["components"]["schemas"]
        assert "500" in schema["paths"]["/api/v1/quality/overview"]["get"]["responses"]
        assert client.get("/metrics").status_code == 200


def test_storage_errors_do_not_expose_credentials() -> None:
    class Unavailable(EmptyQuery):
        def overview(self, window: TimeWindow) -> Overview:
            raise OperationalError("select secret", {}, Exception("password=SECRET"))

    with TestClient(create_app(query=Unavailable())) as client:
        response = client.get("/api/v1/quality/overview")
    assert response.status_code == 503 and "SECRET" not in response.text


def test_unexpected_errors_are_redacted() -> None:
    class Broken(EmptyQuery):
        def overview(self, window: TimeWindow) -> Overview:
            raise RuntimeError("SECRET arbitrary input")

    with TestClient(create_app(query=Broken())) as client:
        response = client.get("/api/v1/quality/overview")
    assert response.status_code == 500 and "SECRET" not in response.text
    assert response.json()["detail"] == "Unexpected service error"
