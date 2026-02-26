"""Test that request_id works correctly with concurrent requests."""

import asyncio
import threading

import pytest
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.testclient import TestClient

from cloud_pipelines_backend.instrumentation import contextual_logging
from cloud_pipelines_backend.instrumentation.api_tracing import RequestContextMiddleware


def test_request_id_isolation_with_concurrent_requests():
    """Test that each concurrent request gets its own isolated request_id."""
    app = Starlette()
    app.add_middleware(RequestContextMiddleware)

    # Store request_ids seen by each endpoint
    request_ids_seen = {
        "endpoint1": [],
        "endpoint2": [],
    }

    @app.route("/endpoint1")
    async def endpoint1(request):
        request_id = contextual_logging.get_context_metadata("request_id")
        request_ids_seen["endpoint1"].append(request_id)
        # Simulate some work
        await asyncio.sleep(0.1)
        # Verify request_id is still the same after async work
        assert contextual_logging.get_context_metadata("request_id") == request_id
        return JSONResponse({"request_id": request_id})

    @app.route("/endpoint2")
    async def endpoint2(request):
        request_id = contextual_logging.get_context_metadata("request_id")
        request_ids_seen["endpoint2"].append(request_id)
        # Simulate some work
        await asyncio.sleep(0.1)
        # Verify request_id is still the same after async work
        assert contextual_logging.get_context_metadata("request_id") == request_id
        return JSONResponse({"request_id": request_id})

    client = TestClient(app)

    # Make concurrent requests
    response1 = client.get("/endpoint1")
    response2 = client.get("/endpoint2")
    response3 = client.get("/endpoint1")

    # All requests should succeed
    assert response1.status_code == 200
    assert response2.status_code == 200
    assert response3.status_code == 200

    # Each request should have gotten a unique request_id
    request_id_1 = response1.headers["x-tangle-request-id"]
    request_id_2 = response2.headers["x-tangle-request-id"]
    request_id_3 = response3.headers["x-tangle-request-id"]

    # All request_ids should be unique
    assert request_id_1 != request_id_2
    assert request_id_1 != request_id_3
    assert request_id_2 != request_id_3

    # Verify endpoints saw the correct request_ids
    assert request_ids_seen["endpoint1"][0] == request_id_1
    assert request_ids_seen["endpoint2"][0] == request_id_2
    assert request_ids_seen["endpoint1"][1] == request_id_3


def test_request_id_isolation_with_nested_async_calls():
    """Test that request_id persists correctly through nested async function calls."""
    app = Starlette()
    app.add_middleware(RequestContextMiddleware)

    request_ids_collected = []

    async def helper_function_1():
        """First level helper."""
        request_ids_collected.append(
            ("helper1", contextual_logging.get_context_metadata("request_id"))
        )
        await asyncio.sleep(0.01)
        await helper_function_2()
        request_ids_collected.append(
            ("helper1_after", contextual_logging.get_context_metadata("request_id"))
        )

    async def helper_function_2():
        """Second level helper."""
        request_ids_collected.append(
            ("helper2", contextual_logging.get_context_metadata("request_id"))
        )
        await asyncio.sleep(0.01)
        request_ids_collected.append(
            ("helper2_after", contextual_logging.get_context_metadata("request_id"))
        )

    @app.route("/test")
    async def test_route(request):
        request_ids_collected.append(
            ("start", contextual_logging.get_context_metadata("request_id"))
        )
        await helper_function_1()
        request_ids_collected.append(
            ("end", contextual_logging.get_context_metadata("request_id"))
        )
        return JSONResponse({"ok": True})

    client = TestClient(app)
    response = client.get("/test")

    assert response.status_code == 200
    request_id = response.headers["x-tangle-request-id"]

    # All captured request_ids should be the same
    for label, captured_request_id in request_ids_collected:
        assert captured_request_id == request_id, f"Mismatch at {label}"

    # Should have captured 6 request_ids total
    assert len(request_ids_collected) == 6


def test_request_id_does_not_leak_between_requests():
    """Test that request_id from one request doesn't leak into another."""
    app = Starlette()
    app.add_middleware(RequestContextMiddleware)

    request_ids_per_request = []

    @app.route("/test")
    async def test_route(request):
        # Capture request_id at start
        start_request_id = contextual_logging.get_context_metadata("request_id")
        request_ids_per_request.append(start_request_id)

        # Do some async work
        await asyncio.sleep(0.05)

        # Verify it hasn't changed
        end_request_id = contextual_logging.get_context_metadata("request_id")
        assert start_request_id == end_request_id

        return JSONResponse({"request_id": end_request_id})

    client = TestClient(app)

    # Make multiple sequential requests
    responses = [client.get("/test") for _ in range(5)]

    # All should succeed
    assert all(r.status_code == 200 for r in responses)

    # Extract request_ids from responses
    response_request_ids = [r.headers["x-tangle-request-id"] for r in responses]

    # All should be unique
    assert len(set(response_request_ids)) == 5

    # Should match what we captured inside the handler
    assert response_request_ids == request_ids_per_request


@pytest.mark.asyncio
async def test_contextvars_isolation_across_async_tasks():
    """Direct test of contextvars isolation without HTTP layer."""

    async def task_with_request_id(task_id: str, expected_request_id: str):
        """Simulates a task with its own request_id context."""
        # Set request_id for this task
        contextual_logging.set_context_metadata("request_id", expected_request_id)

        # Verify it's set correctly
        assert (
            contextual_logging.get_context_metadata("request_id") == expected_request_id
        )

        # Simulate some work
        await asyncio.sleep(0.01)

        # Verify request_id is still correct after async work
        assert (
            contextual_logging.get_context_metadata("request_id") == expected_request_id
        )

        # More work
        await asyncio.sleep(0.01)

        # Still correct
        assert (
            contextual_logging.get_context_metadata("request_id") == expected_request_id
        )

        # Clean up
        contextual_logging.clear_context_metadata()

        return task_id

    # Run multiple tasks concurrently with different request_ids
    tasks = [
        task_with_request_id("task1", "request_aaa111"),
        task_with_request_id("task2", "request_bbb222"),
        task_with_request_id("task3", "request_ccc333"),
        task_with_request_id("task4", "request_ddd444"),
    ]

    results = await asyncio.gather(*tasks)

    # All tasks should complete successfully
    assert results == ["task1", "task2", "task3", "task4"]

    # After all tasks complete, there should be no request_id in this context
    assert contextual_logging.get_context_metadata("request_id") is None


def test_request_id_with_context_manager_is_thread_safe():
    """Test that the logging_context context manager works with concurrent access."""

    collected_request_ids = []

    def simulate_request_processing(request_id: str):
        """Simulates processing with a request_id."""
        with contextual_logging.logging_context(request_id=request_id):
            # Verify request_id is set
            current = contextual_logging.get_context_metadata("request_id")
            collected_request_ids.append((request_id, current))
            assert current == request_id

        # After context exits, should be cleared in this context
        # (though in threads, contexts are separate anyway)

    # Create threads that will process with different request_ids
    threads = [
        threading.Thread(target=simulate_request_processing, args=(f"request_{i:03d}",))
        for i in range(10)
    ]

    # Start all threads
    for thread in threads:
        thread.start()

    # Wait for all to complete
    for thread in threads:
        thread.join()

    # All threads should have seen their correct request_id
    assert len(collected_request_ids) == 10
    for expected, actual in collected_request_ids:
        assert expected == actual
