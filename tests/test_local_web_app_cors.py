import fastapi
import pytest
from fastapi import testclient

from cloud_pipelines_backend import local_web_app_cors


@pytest.fixture
def client() -> testclient.TestClient:
    app = fastapi.FastAPI()

    @app.get("/services/ping")
    def ping():
        return {}

    local_web_app_cors.allow_local_web_app_origins(app)
    return testclient.TestClient(app)


LOCAL_ORIGINS = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:5173",
    "http://localhost",
]

FOREIGN_ORIGINS = [
    "https://example.com",
    "http://localhost.example.com:3000",
    "http://example.com:3000",
    "https://localhost:3000",
]


@pytest.mark.parametrize("origin", LOCAL_ORIGINS)
def test_preflight_from_local_origin_is_allowed(client, origin):
    response = client.options(
        "/services/ping",
        headers={
            "Origin": origin,
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "content-type",
        },
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == origin
    assert response.headers["access-control-allow-credentials"] == "true"


@pytest.mark.parametrize("origin", LOCAL_ORIGINS)
def test_response_to_local_origin_carries_cors_headers(client, origin):
    response = client.get("/services/ping", headers={"Origin": origin})

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == origin


@pytest.mark.parametrize("origin", FOREIGN_ORIGINS)
def test_preflight_from_foreign_origin_is_rejected(client, origin):
    response = client.options(
        "/services/ping",
        headers={"Origin": origin, "Access-Control-Request-Method": "GET"},
    )

    assert response.status_code == 400
    assert "access-control-allow-origin" not in response.headers


@pytest.mark.parametrize("origin", FOREIGN_ORIGINS)
def test_response_to_foreign_origin_has_no_cors_headers(client, origin):
    response = client.get("/services/ping", headers={"Origin": origin})

    assert "access-control-allow-origin" not in response.headers
