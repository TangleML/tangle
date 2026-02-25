"""Tests for the logging_context module in instrumentation."""

import pytest

from cloud_pipelines_backend.instrumentation import contextual_logging
from cloud_pipelines_backend.instrumentation.api_tracing import generate_request_id


class TestLoggingContext:
    """Tests for logging context management."""

    def setup_method(self):
        """Clear any existing context before each test."""
        contextual_logging.clear_context_metadata()

    def teardown_method(self):
        """Clear context after each test."""
        contextual_logging.clear_context_metadata()

    def test_set_and_get_context_metadata(self):
        """Test setting and getting context metadata."""
        test_id = "abc123def456abc123def456abc12345"

        contextual_logging.set_context_metadata("request_id", test_id)

        assert contextual_logging.get_context_metadata("request_id") == test_id

    def test_get_context_metadata_returns_none_when_not_set(self):
        """Test that get_context_metadata returns None when key is not set."""
        assert contextual_logging.get_context_metadata("request_id") is None

    def test_clear_context_metadata(self):
        """Test clearing all context metadata."""
        contextual_logging.set_context_metadata("request_id", "test123")
        contextual_logging.set_context_metadata("execution_id", "exec456")
        contextual_logging.clear_context_metadata()

        assert contextual_logging.get_context_metadata("request_id") is None
        assert contextual_logging.get_context_metadata("execution_id") is None

    def test_overwrite_context_metadata(self):
        """Test that setting a new value overwrites the old one."""
        contextual_logging.set_context_metadata("request_id", "first_id")
        contextual_logging.set_context_metadata("request_id", "second_id")

        assert contextual_logging.get_context_metadata("request_id") == "second_id"

    def test_get_all_context_metadata(self):
        """Test getting all context metadata at once."""
        contextual_logging.set_context_metadata("request_id", "req123")
        contextual_logging.set_context_metadata("execution_id", "exec456")
        contextual_logging.set_context_metadata("custom_field", "value789")

        all_metadata = contextual_logging.get_all_context_metadata()

        assert all_metadata["request_id"] == "req123"
        assert all_metadata["execution_id"] == "exec456"
        assert all_metadata["custom_field"] == "value789"


class TestLoggingContextManager:
    """Tests for the logging_context context manager."""

    def setup_method(self):
        """Clear any existing context before each test."""
        contextual_logging.clear_context_metadata()

    def teardown_method(self):
        """Clear context after each test."""
        contextual_logging.clear_context_metadata()

    def test_context_manager_sets_and_restores_metadata(self):
        """Test that context manager sets metadata on enter and restores on exit."""
        test_id = "context_test_123"

        assert contextual_logging.get_context_metadata("request_id") is None

        with contextual_logging.logging_context(request_id=test_id):
            assert contextual_logging.get_context_metadata("request_id") == test_id

        assert contextual_logging.get_context_metadata("request_id") is None

    def test_context_manager_with_multiple_keys(self):
        """Test that context manager handles multiple metadata keys."""
        with contextual_logging.logging_context(
            request_id="req123", execution_id="exec456", pipeline_run_id="run789"
        ):
            assert contextual_logging.get_context_metadata("request_id") == "req123"
            assert contextual_logging.get_context_metadata("execution_id") == "exec456"
            assert (
                contextual_logging.get_context_metadata("pipeline_run_id") == "run789"
            )

        assert contextual_logging.get_context_metadata("request_id") is None
        assert contextual_logging.get_context_metadata("execution_id") is None
        assert contextual_logging.get_context_metadata("pipeline_run_id") is None

    def test_context_manager_with_none_values(self):
        """Test that context manager skips None values."""
        with contextual_logging.logging_context(request_id="req123", execution_id=None):
            assert contextual_logging.get_context_metadata("request_id") == "req123"
            assert contextual_logging.get_context_metadata("execution_id") is None

        assert contextual_logging.get_context_metadata("request_id") is None

    def test_context_manager_clears_on_exception(self):
        """Test that context manager restores metadata even when exception occurs."""
        test_id = "exception_test"

        with pytest.raises(ValueError), contextual_logging.logging_context(request_id=test_id):
            assert contextual_logging.get_context_metadata("request_id") == test_id
            raise ValueError("Test exception")

        # Metadata should be cleared even after exception
        assert contextual_logging.get_context_metadata("request_id") is None

    def test_context_manager_nested(self):
        """Test nested context managers."""
        outer_id = "outer_id"
        inner_id = "inner_id"

        with contextual_logging.logging_context(request_id=outer_id):
            assert contextual_logging.get_context_metadata("request_id") == outer_id

            with contextual_logging.logging_context(request_id=inner_id):
                assert contextual_logging.get_context_metadata("request_id") == inner_id

            # After inner context exits, outer context is restored
            assert contextual_logging.get_context_metadata("request_id") == outer_id

        assert contextual_logging.get_context_metadata("request_id") is None

    def test_context_manager_with_generated_request_id(self):
        """Test using context manager with a generated request_id."""
        generated_id = generate_request_id()

        with contextual_logging.logging_context(request_id=generated_id):
            assert contextual_logging.get_context_metadata("request_id") == generated_id
            assert len(contextual_logging.get_context_metadata("request_id")) == 32

        assert contextual_logging.get_context_metadata("request_id") is None

    def test_context_manager_multiple_sequential_uses(self):
        """Test using context manager multiple times sequentially."""
        ids = ["id1", "id2", "id3"]

        for test_id in ids:
            with contextual_logging.logging_context(request_id=test_id):
                assert contextual_logging.get_context_metadata("request_id") == test_id
            assert contextual_logging.get_context_metadata("request_id") is None

    def test_context_manager_preserves_existing_metadata(self):
        """Test that nested context preserves existing metadata not being overwritten."""
        with contextual_logging.logging_context(
            request_id="req123", execution_id="exec456"
        ):
            assert contextual_logging.get_context_metadata("request_id") == "req123"
            assert contextual_logging.get_context_metadata("execution_id") == "exec456"

            # Inner context only sets pipeline_run_id
            with contextual_logging.logging_context(pipeline_run_id="run789"):
                # Previous values should still be accessible
                assert contextual_logging.get_context_metadata("request_id") == "req123"
                assert (
                    contextual_logging.get_context_metadata("execution_id") == "exec456"
                )
                assert (
                    contextual_logging.get_context_metadata("pipeline_run_id")
                    == "run789"
                )

            # After inner exits, pipeline_run_id is gone but others remain
            assert contextual_logging.get_context_metadata("request_id") == "req123"
            assert contextual_logging.get_context_metadata("execution_id") == "exec456"
            assert contextual_logging.get_context_metadata("pipeline_run_id") is None
