"""
Test to demonstrate how logger.exception() behaves with ContextAwareFormatter.

This test shows that the ContextAwareFormatter properly handles exception logging
including full tracebacks.
"""

import io
import logging
import logging.config
import os
import sys

# Add the parent directory to the path so we can import the module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cloud_pipelines_backend.instrumentation import contextual_logging
from cloud_pipelines_backend.instrumentation.structured_logging import (
    ContextAwareFormatter,
    LoggingContextFilter,
)


def test_logger_exception_with_context_aware_formatter():
    """Test that logger.exception() properly includes tracebacks with ContextAwareFormatter."""

    # Create a string buffer to capture log output
    log_buffer = io.StringIO()

    # Configure logging with ContextAwareFormatter
    LOGGING_CONFIG = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "with_context": {
                "()": ContextAwareFormatter,
            },
        },
        "filters": {
            "context_filter": {
                "()": LoggingContextFilter,
            },
        },
        "handlers": {
            "test_handler": {
                "level": "DEBUG",
                "formatter": "with_context",
                "class": "logging.StreamHandler",
                "stream": log_buffer,
                "filters": ["context_filter"],
            },
        },
        "loggers": {
            "test_logger": {
                "level": "DEBUG",
                "handlers": ["test_handler"],
                "propagate": False,
            },
        },
    }

    logging.config.dictConfig(LOGGING_CONFIG)
    test_logger = logging.getLogger("test_logger")

    print("\n" + "=" * 80)
    print("TEST 1: logger.exception() WITHOUT context")
    print("=" * 80)

    try:
        # Simulate an error
        _ = 1 / 0
    except ZeroDivisionError:
        test_logger.exception("An error occurred while dividing")

    output1 = log_buffer.getvalue()
    print(output1)

    # Verify the traceback is present
    assert "Traceback" in output1, "Traceback should be present in exception logs"
    assert "ZeroDivisionError" in output1, "Exception type should be in logs"
    assert "division by zero" in output1, "Exception message should be in logs"

    # Clear buffer for next test
    log_buffer.truncate(0)
    log_buffer.seek(0)

    print("\n" + "=" * 80)
    print("TEST 2: logger.exception() WITH context metadata")
    print("=" * 80)

    # Add some context metadata
    with contextual_logging.logging_context(
        execution_id="exec-12345",
        container_execution_id="container-67890",
        pipeline_run_id="run-abc123",
    ):
        try:
            # Simulate another error with nested function calls
            def inner_function():
                return {"key": "value"}[NonExistentKey]  # noqa: F821

            def outer_function():
                inner_function()

            outer_function()
        except Exception:
            test_logger.exception("Failed to process pipeline")

    output2 = log_buffer.getvalue()
    print(output2)

    # Verify context is included
    assert "execution_id=exec-12345" in output2, "execution_id should be in logs"
    assert "container_execution_id=container-67890" in output2, "container_execution_id should be in logs"
    assert "pipeline_run_id=run-abc123" in output2, "pipeline_run_id should be in logs"

    # Verify the traceback is still present with context
    assert "Traceback" in output2, "Traceback should be present even with context"
    assert "NameError" in output2, "Exception type should be in logs"
    assert "inner_function" in output2, "Function names from traceback should be present"
    assert "outer_function" in output2, "Function names from traceback should be present"

    # Clear buffer for next test
    log_buffer.truncate(0)
    log_buffer.seek(0)

    print("\n" + "=" * 80)
    print("TEST 3: logger.error() vs logger.exception() comparison")
    print("=" * 80)

    with contextual_logging.logging_context(execution_id="exec-99999"):
        try:
            raise ValueError("This is a test error")
        except ValueError:
            test_logger.error("Error logged with .error()")

    output3 = log_buffer.getvalue()
    print("logger.error() output:")
    print(output3)
    print()

    # Verify .error() does NOT include traceback
    assert "Traceback" not in output3, "logger.error() should NOT include traceback"
    assert "ValueError" not in output3, "logger.error() should NOT include exception details"

    # Clear buffer
    log_buffer.truncate(0)
    log_buffer.seek(0)

    with contextual_logging.logging_context(execution_id="exec-99999"):
        try:
            raise ValueError("This is a test error")
        except ValueError:
            test_logger.exception("Error logged with .exception()")

    output4 = log_buffer.getvalue()
    print("logger.exception() output:")
    print(output4)

    # Verify .exception() DOES include traceback
    assert "Traceback" in output4, "logger.exception() SHOULD include traceback"
    assert "ValueError" in output4, "logger.exception() SHOULD include exception details"

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print("✅ ContextAwareFormatter properly handles logger.exception()")
    print("✅ Full tracebacks are included in exception logs")
    print("✅ Context metadata is preserved alongside tracebacks")
    print("✅ The formatter delegates to logging.Formatter.format() which handles exc_info")
    print("=" * 80)


if __name__ == "__main__":
    test_logger_exception_with_context_aware_formatter()
