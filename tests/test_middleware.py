"""Tests for the middleware module, including CORS configuration."""

import os
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from cloud_pipelines_backend import middleware


@pytest.fixture
def app():
    """Create a minimal FastAPI app for testing."""
    app = FastAPI()

    @app.get("/test")
    def test_endpoint():
        return {"message": "success"}

    return app


@pytest.fixture
def clean_env(monkeypatch):
    """Ensure clean environment for each test."""
    monkeypatch.delenv("TANGLE_CORS_ALLOWED_ORIGINS", raising=False)


def test_cors_middleware_with_default_origins(app, clean_env, monkeypatch):
    """Test that CORS middleware uses default origins when env var is not set."""
    # Don't set TANGLE_CORS_ALLOWED_ORIGINS - should use defaults
    middleware.setup_cors_middleware(app)
    client = TestClient(app)

    # Test request from default origin (localhost:3000)
    response = client.get("/test", headers={"Origin": "http://localhost:3000"})
    assert response.status_code == 200
    assert (
        response.headers.get("access-control-allow-origin") == "http://localhost:3000"
    )

    # Test request from default origin (127.0.0.1:3000)
    response = client.get("/test", headers={"Origin": "http://127.0.0.1:3000"})
    assert response.status_code == 200
    assert (
        response.headers.get("access-control-allow-origin") == "http://127.0.0.1:3000"
    )


def test_cors_middleware_with_custom_single_origin(app, clean_env, monkeypatch):
    """Test CORS middleware with a single custom origin."""
    monkeypatch.setenv("TANGLE_CORS_ALLOWED_ORIGINS", "https://app.example.com")
    middleware.setup_cors_middleware(app)
    client = TestClient(app)

    # Test request from allowed origin
    response = client.get("/test", headers={"Origin": "https://app.example.com"})
    assert response.status_code == 200
    assert (
        response.headers.get("access-control-allow-origin") == "https://app.example.com"
    )

    # Test request from disallowed origin
    response = client.get("/test", headers={"Origin": "http://localhost:3000"})
    assert response.status_code == 200
    # Should not match the disallowed origin
    assert (
        response.headers.get("access-control-allow-origin") != "http://localhost:3000"
    )


def test_cors_middleware_with_multiple_origins(app, clean_env, monkeypatch):
    """Test CORS middleware with multiple comma-separated origins."""
    origins = (
        "http://localhost:3000,https://staging.example.com,https://app.example.com"
    )
    monkeypatch.setenv("TANGLE_CORS_ALLOWED_ORIGINS", origins)
    middleware.setup_cors_middleware(app)
    client = TestClient(app)

    # Test each allowed origin
    for origin in [
        "http://localhost:3000",
        "https://staging.example.com",
        "https://app.example.com",
    ]:
        response = client.get("/test", headers={"Origin": origin})
        assert response.status_code == 200
        assert response.headers.get("access-control-allow-origin") == origin

    # Test disallowed origin
    response = client.get("/test", headers={"Origin": "http://evil.com"})
    assert response.status_code == 200
    assert response.headers.get("access-control-allow-origin") != "http://evil.com"


def test_cors_middleware_with_whitespace_in_origins(app, clean_env, monkeypatch):
    """Test that whitespace around origins is properly handled."""
    origins = (
        " http://localhost:3000 , https://app.example.com , http://127.0.0.1:3000 "
    )
    monkeypatch.setenv("TANGLE_CORS_ALLOWED_ORIGINS", origins)
    middleware.setup_cors_middleware(app)
    client = TestClient(app)

    # Test that whitespace is properly stripped
    response = client.get("/test", headers={"Origin": "http://localhost:3000"})
    assert response.status_code == 200
    assert (
        response.headers.get("access-control-allow-origin") == "http://localhost:3000"
    )


def test_cors_middleware_without_origin_header(app, clean_env, monkeypatch):
    """Test that requests without Origin header work normally."""
    monkeypatch.setenv("TANGLE_CORS_ALLOWED_ORIGINS", "http://localhost:3000")
    middleware.setup_cors_middleware(app)
    client = TestClient(app)

    # Request without Origin header should still work
    response = client.get("/test")
    assert response.status_code == 200
    assert response.json() == {"message": "success"}


def test_cors_middleware_rejects_invalid_url_formats(
    app, clean_env, monkeypatch, caplog
):
    """Test that invalid URL formats are rejected and logged."""
    # Mix of valid and invalid origins
    origins = "http://localhost:3000,invalid-url,ftp://wrong-scheme.com,http://example.com/path"
    monkeypatch.setenv("TANGLE_CORS_ALLOWED_ORIGINS", origins)

    middleware.setup_cors_middleware(app)
    client = TestClient(app)

    # Valid origin should work
    response = client.get("/test", headers={"Origin": "http://localhost:3000"})
    assert response.status_code == 200
    assert (
        response.headers.get("access-control-allow-origin") == "http://localhost:3000"
    )

    # Check that invalid origins were logged
    assert "Invalid CORS origins found and ignored" in caplog.text
    assert "invalid-url" in caplog.text


def test_cors_middleware_validates_scheme_http_https_only(app, clean_env, monkeypatch):
    """Test that only http and https schemes are accepted."""
    origins = "http://localhost:3000,ftp://example.com,ws://example.com"
    monkeypatch.setenv("TANGLE_CORS_ALLOWED_ORIGINS", origins)
    middleware.setup_cors_middleware(app)
    client = TestClient(app)

    # http should work
    response = client.get("/test", headers={"Origin": "http://localhost:3000"})
    assert response.status_code == 200
    assert (
        response.headers.get("access-control-allow-origin") == "http://localhost:3000"
    )

    # ftp and ws should not be in allowed origins
    response = client.get("/test", headers={"Origin": "ftp://example.com"})
    assert response.headers.get("access-control-allow-origin") != "ftp://example.com"


def test_cors_middleware_rejects_origins_with_paths(app, clean_env, monkeypatch):
    """Test that origins with paths are rejected."""
    origins = "http://localhost:3000,http://example.com/api,http://example.com/path/to/resource"
    monkeypatch.setenv("TANGLE_CORS_ALLOWED_ORIGINS", origins)
    middleware.setup_cors_middleware(app)
    client = TestClient(app)

    # Origin without path should work
    response = client.get("/test", headers={"Origin": "http://localhost:3000"})
    assert response.status_code == 200
    assert (
        response.headers.get("access-control-allow-origin") == "http://localhost:3000"
    )

    # Origins with paths should not work
    response = client.get("/test", headers={"Origin": "http://example.com/api"})
    assert (
        response.headers.get("access-control-allow-origin") != "http://example.com/api"
    )


def test_cors_middleware_empty_origins_string(app, clean_env, monkeypatch, caplog):
    """Test behavior when TANGLE_CORS_ALLOWED_ORIGINS is set to empty string."""
    monkeypatch.setenv("TANGLE_CORS_ALLOWED_ORIGINS", "")
    middleware.setup_cors_middleware(app)

    # Should log warning about no valid origins
    assert "No valid CORS origins found" in caplog.text


def test_cors_middleware_allows_credentials(app, clean_env, monkeypatch):
    """Test that CORS middleware is configured to allow credentials."""
    monkeypatch.setenv("TANGLE_CORS_ALLOWED_ORIGINS", "http://localhost:3000")
    middleware.setup_cors_middleware(app)
    client = TestClient(app)

    response = client.get("/test", headers={"Origin": "http://localhost:3000"})
    assert response.status_code == 200
    # FastAPI's CORSMiddleware should set this header
    assert "access-control-allow-credentials" in response.headers


def test_cors_middleware_allows_all_methods_and_headers(app, clean_env, monkeypatch):
    """Test that CORS middleware allows all methods and headers."""
    monkeypatch.setenv("TANGLE_CORS_ALLOWED_ORIGINS", "http://localhost:3000")
    middleware.setup_cors_middleware(app)
    client = TestClient(app)

    # Preflight request
    response = client.options(
        "/test",
        headers={
            "Origin": "http://localhost:3000",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "X-Custom-Header",
        },
    )

    assert response.status_code == 200
    assert "access-control-allow-methods" in response.headers
    assert "access-control-allow-headers" in response.headers


def test_is_valid_origin_helper():
    """Test the _is_valid_origin helper function directly."""
    # Valid origins
    assert middleware._is_valid_origin("http://localhost:3000") == True
    assert middleware._is_valid_origin("https://example.com") == True
    assert middleware._is_valid_origin("http://127.0.0.1:8080") == True
    assert middleware._is_valid_origin("https://subdomain.example.com:443") == True

    # Invalid origins - no scheme
    assert middleware._is_valid_origin("localhost:3000") == False
    assert middleware._is_valid_origin("example.com") == False

    # Invalid origins - wrong scheme
    assert middleware._is_valid_origin("ftp://example.com") == False
    assert middleware._is_valid_origin("ws://example.com") == False
    assert middleware._is_valid_origin("file:///path/to/file") == False

    # Invalid origins - has path
    assert middleware._is_valid_origin("http://example.com/api") == False
    assert middleware._is_valid_origin("https://example.com/path/to/resource") == False

    # Invalid origins - malformed
    assert middleware._is_valid_origin("not-a-url") == False
    assert middleware._is_valid_origin("http://") == False
    assert middleware._is_valid_origin("") == False


def test_setup_cors_middleware_integration(app, clean_env, monkeypatch):
    """Test that setup_cors_middleware works end-to-end."""
    monkeypatch.setenv("TANGLE_CORS_ALLOWED_ORIGINS", "http://localhost:3000")

    # Call setup_cors_middleware
    middleware.setup_cors_middleware(app)
    client = TestClient(app)

    # Verify CORS is working
    response = client.get("/test", headers={"Origin": "http://localhost:3000"})
    assert response.status_code == 200
    assert (
        response.headers.get("access-control-allow-origin") == "http://localhost:3000"
    )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
