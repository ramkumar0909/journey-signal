from fastapi.testclient import TestClient

from journeysignal.interfaces.api.app import create_app


def test_liveness_is_independent_of_storage() -> None:
    with TestClient(create_app(lambda: False)) as client:
        assert client.get("/health/live").json() == {"status": "alive"}
        assert client.get("/health/ready").status_code == 503


def test_readiness_checks_dependency() -> None:
    with TestClient(create_app(lambda: True)) as client:
        assert client.get("/health/ready").status_code == 200
