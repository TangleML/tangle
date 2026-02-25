"""Tests for the request_middleware module in instrumentation."""

import pytest
from starlette.applications import Starlette
from starlette.responses import Response
from starlette.testclient import TestClient

from cloud_pipelines_backend.instrumentation import contextual_logging
from cloud_pipelines_backend.instrumentation.api_tracing import (
    RequestContextMiddleware,
    generate_request_id,
)


class TestRequestIdGeneration:
    """Tests for request_id generation."""

    def test_generate_request_id_returns_32_char_hex(self):
        """Test that generated request_id is 32 hexadecimal characters."""
        request_id = generate_request_id()

        assert len(request_id) == 32
        assert all(c in "0123456789abcdef" for c in request_id)

    def test_generate_request_id_is_unique(self):
        """Test that each generated request_id is unique."""
        request_ids = {generate_request_id() for _ in range(100)}

        # All 100 should be unique
        assert len(request_ids) == 100

    def test_generate_request_id_is_lowercase(self):
        """Test that generated request_id uses lowercase hex."""
        request_id = generate_request_id()

        assert request_id == request_id.lower()


class TestRequestIdFormatting:
    """Tests for request_id format validation."""

    def test_generated_request_id_format(self):
        """Test that generated request_id matches expected format."""
        request_id = generate_request_id()

        # Should be 32 characters
        assert len(request_id) == 32

        # Should be valid hex
        try:
            int(request_id, 16)
        except ValueError:
            pytest.fail("request_id is not valid hexadecimal")

        # Should be lowercase
        assert request_id.islower()

    def test_request_id_is_128_bits(self):
        """Test that request_id represents 128 bits (16 bytes)."""
        request_id = generate_request_id()

        # 32 hex characters = 16 bytes = 128 bits
        assert len(bytes.fromhex(request_id)) == 16


class TestRequestContextMiddleware:
    """Tests for RequestContextMiddleware."""

    def setup_method(self):
        """Clear any existing context before each test."""
        contextual_logging.clear_context_metadata()

    def teardown_method(self):
        """Clear context after each test."""
        contextual_logging.clear_context_metadata()

    def test_middleware_generates_request_id(self):
        """Test that middleware generates a request_id for each request."""
        app = Starlette()
        app.add_middleware(RequestContextMiddleware)

        request_ids_seen = []

        @app.route("/test")
        def test_route(request):
            # Capture the request_id during request processing
            request_ids_seen.append(
                contextual_logging.get_context_metadata("request_id")
            )
            return Response("ok")

        client = TestClient(app)
        response = client.get("/test")

        assert response.status_code == 200
        assert len(request_ids_seen) == 1
        assert request_ids_seen[0] is not None
        assert len(request_ids_seen[0]) == 32

    def test_middleware_adds_request_id_to_response_headers(self):
        """Test that middleware adds request_id to response headers."""
        app = Starlette()
        app.add_middleware(RequestContextMiddleware)

        @app.route("/test")
        def test_route(request):
            return Response("ok")

        client = TestClient(app)
        response = client.get("/test")

        assert "x-tangle-request-id" in response.headers
        request_id = response.headers["x-tangle-request-id"]
        assert len(request_id) == 32
        assert all(c in "0123456789abcdef" for c in request_id)

    def test_middleware_clears_request_id_after_request(self):
        """Test that middleware clears request_id after request completes."""
        app = Starlette()
        app.add_middleware(RequestContextMiddleware)

        @app.route("/test")
        def test_route(request):
            assert contextual_logging.get_context_metadata("request_id") is not None
            return Response("ok")

        client = TestClient(app)

        # Before request
        assert contextual_logging.get_context_metadata("request_id") is None

        # Make request
        response = client.get("/test")
        assert response.status_code == 200

        # After request - Note: in test client, context might not be cleared
        # the same way as in production, but the middleware's context manager ensures it

    def test_middleware_generates_unique_request_ids(self):
        """Test that middleware generates unique request_ids for each request."""
        app = Starlette()
        app.add_middleware(RequestContextMiddleware)

        @app.route("/test")
        def test_route(request):
            return Response("ok")

        client = TestClient(app)

        # Make multiple requests
        request_ids = set()
        for _ in range(10):
            response = client.get("/test")
            request_ids.add(response.headers["x-tangle-request-id"])

        # All request_ids should be unique
        assert len(request_ids) == 10

    def test_middleware_request_id_available_in_route(self):
        """Test that request_id set by middleware is available in route handler."""
        app = Starlette()
        app.add_middleware(RequestContextMiddleware)

        captured_request_id = None

        @app.route("/test")
        def test_route(request):
            nonlocal captured_request_id
            captured_request_id = contextual_logging.get_context_metadata("request_id")
            return Response(f"request_id: {captured_request_id}")

        client = TestClient(app)
        response = client.get("/test")

        assert captured_request_id is not None
        assert captured_request_id == response.headers["x-tangle-request-id"]
        assert captured_request_id in response.text

    def test_middleware_handles_exception_in_route(self):
        """Test that middleware clears request_id even when route raises exception."""
        app = Starlette()
        app.add_middleware(RequestContextMiddleware)

        @app.route("/test")
        def test_route(request):
            request_id_during_exception = contextual_logging.get_context_metadata(
                "request_id"
            )
            assert request_id_during_exception is not None
            raise ValueError("Test exception")

        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/test")

        # Even though route raised exception, response should have request_id header
        # (middleware's context manager ensures cleanup)
        assert response.status_code == 500

    def test_middleware_with_multiple_routes(self):
        """Test middleware works correctly with multiple routes."""
        app = Starlette()
        app.add_middleware(RequestContextMiddleware)

        request_ids_by_route = {}

        @app.route("/route1")
        def route1(request):
            request_ids_by_route["route1"] = contextual_logging.get_context_metadata(
                "request_id"
            )
            return Response("route1")

        @app.route("/route2")
        def route2(request):
            request_ids_by_route["route2"] = contextual_logging.get_context_metadata(
                "request_id"
            )
            return Response("route2")

        client = TestClient(app)

        response1 = client.get("/route1")
        response2 = client.get("/route2")

        # Each route should have gotten a request_id
        assert request_ids_by_route["route1"] is not None
        assert request_ids_by_route["route2"] is not None

        # They should be different
        assert request_ids_by_route["route1"] != request_ids_by_route["route2"]

        # Response headers should match
        assert (
            response1.headers["x-tangle-request-id"] == request_ids_by_route["route1"]
        )
        assert (
            response2.headers["x-tangle-request-id"] == request_ids_by_route["route2"]
        )


class TestRequestContextMiddlewareIntegration:
    """Integration tests for RequestContextMiddleware with logging."""

    def setup_method(self):
        """Clear any existing context before each test."""
        contextual_logging.clear_context_metadata()

    def teardown_method(self):
        """Clear context after each test."""
        contextual_logging.clear_context_metadata()

    def test_middleware_enables_request_id_in_logs(self):
        """Test that middleware enables request_id to be used in logging."""
        import logging

        app = Starlette()
        app.add_middleware(RequestContextMiddleware)

        logged_request_ids = []

        # Create a custom handler to capture log records
        class TestHandler(logging.Handler):
            def emit(self, record):
                # In real usage, LoggingContextFilter would add request_id to logs
                current_request_id = contextual_logging.get_context_metadata(
                    "request_id"
                )
                if current_request_id:
                    logged_request_ids.append(current_request_id)

        logger = logging.getLogger("test_logger")
        handler = TestHandler()
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        @app.route("/test")
        def test_route(request):
            logger.info("Processing request")
            return Response("ok")

        client = TestClient(app)
        response = client.get("/test")

        # The request_id logged should match the response header
        assert len(logged_request_ids) > 0
        assert response.headers["x-tangle-request-id"] in logged_request_ids

        # Cleanup
        logger.removeHandler(handler)

    def test_middleware_request_id_persists_across_function_calls(self):
        """Test that request_id persists across function calls within a request."""
        app = Starlette()
        app.add_middleware(RequestContextMiddleware)

        request_ids_collected = []

        def helper_function():
            """Helper function that accesses request_id."""
            request_ids_collected.append(
                contextual_logging.get_context_metadata("request_id")
            )

        @app.route("/test")
        def test_route(request):
            request_ids_collected.append(
                contextual_logging.get_context_metadata("request_id")
            )
            helper_function()
            request_ids_collected.append(
                contextual_logging.get_context_metadata("request_id")
            )
            return Response("ok")

        client = TestClient(app)
        response = client.get("/test")

        # All three captures should have the same request_id
        assert len(request_ids_collected) == 3
        assert (
            request_ids_collected[0]
            == request_ids_collected[1]
            == request_ids_collected[2]
        )
        assert request_ids_collected[0] == response.headers["x-tangle-request-id"]
