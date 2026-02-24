"""Logging context management for distributed tracing and execution tracking.

This module provides utilities for managing arbitrary metadata in the logging context.
This metadata is automatically added to all log records for better filtering and correlation.

Common metadata keys:
- request_id: From API requests - groups all logs from a single API call
- pipeline_run_id: From PipelineRun.id - tracks the entire pipeline run
- execution_id: From ExecutionNode.id - tracks individual execution nodes
- container_execution_id: From ContainerExecution.id - tracks running containers
- user_id: User who initiated the operation
- Any other metadata you want to track in logs

Usage:
    # Set metadata in context
    with logging_context(request_id="abc123", user_id="user@example.com"):
        logger.info("Processing")  # Both fields in logs

    # Or use individual functions
    set_context_metadata("request_id", "abc123")
    delete_context_metadata("request_id")  # Remove a specific key
"""

import contextvars
from contextlib import contextmanager
from typing import Any

# Single context variable to store all metadata as a dictionary
_context_metadata: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar(
    "context_metadata",
)


def _get_metadata() -> dict[str, Any]:
    try:
        return _context_metadata.get()
    except LookupError:
        metadata: dict[str, Any] = {}
        _context_metadata.set(metadata)
        return metadata


def set_context_metadata(key: str, value: Any) -> None:
    """Set a metadata value in the current context.

    Args:
        key: The metadata key (e.g., 'execution_id', 'request_id', 'user_id')
        value: The value to set
    """
    metadata = _get_metadata().copy()
    metadata[key] = value
    _context_metadata.set(metadata)


def delete_context_metadata(key: str) -> None:
    """Delete a metadata key from the current context.

    Similar to dict.pop() but doesn't return a value. If the key doesn't exist,
    this is a no-op (no error is raised).

    Args:
        key: The metadata key to delete (e.g., 'execution_id', 'request_id')
    """
    metadata = _get_metadata().copy()
    metadata.pop(key, None)
    _context_metadata.set(metadata)


def get_context_metadata(key: str) -> Any | None:
    """Get a metadata value from the current context.

    Args:
        key: The metadata key to retrieve

    Returns:
        The metadata value or None if not set
    """
    return _get_metadata().get(key)


def get_all_context_metadata() -> dict[str, Any]:
    """Get all metadata from the current context.

    Returns:
        Dictionary of all context metadata
    """
    return _get_metadata().copy()


def clear_context_metadata() -> None:
    """Clear all metadata from the current context."""
    _context_metadata.set({})


@contextmanager
def logging_context(**metadata: Any):
    """Context manager for setting arbitrary metadata that is automatically cleared.

    This is the recommended way to set logging context. It ensures metadata is
    always cleaned up, even if an exception occurs.

    You can pass any keyword arguments, and they will be available in log records.
    Common keys include: request_id, pipeline_run_id, execution_id, container_execution_id, user_id

    Args:
        **metadata: Arbitrary keyword arguments to add to the context

    Example with IDs:
        >>> with logging_context(pipeline_run_id="run123", execution_id="exec456"):
        ...     logger.info("Processing execution")  # Will include both IDs

    Example with custom metadata:
        >>> with logging_context(execution_id="exec456", user_id="user@example.com", operation="reprocessing"):
        ...     logger.info("Custom operation")  # All metadata in logs

    Example for API requests:
        >>> request_id = generate_request_id()
        >>> with logging_context(request_id=request_id):
        ...     logger.info("Handling API request")
    """
    # Store previous metadata to restore nested contexts
    prev_metadata = get_all_context_metadata()

    try:
        # Set all provided metadata
        for key, value in metadata.items():
            if value is not None:  # Only set non-None values
                set_context_metadata(key, value)
        yield
    finally:
        # Restore previous metadata
        _context_metadata.set(prev_metadata)
