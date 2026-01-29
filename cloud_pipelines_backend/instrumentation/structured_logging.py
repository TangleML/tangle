"""Structured logging components for context-aware log formatting.

This module provides logging filters and formatters that integrate with the
contextual_logging module to automatically include context metadata in log records.
"""

import logging

from . import contextual_logging


class LoggingContextFilter(logging.Filter):
    """Logging filter that adds contextual metadata to log records.

    This filter automatically adds metadata like execution_id and container_execution_id
    to log records, making it easier to trace logs for specific executions.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """Add contextual metadata to the log record."""
        for key, value in contextual_logging.get_all_context_metadata().items():
            if value is not None:
                setattr(record, key, value)
        return True


class ContextAwareFormatter(logging.Formatter):
    """Formatter that dynamically includes context fields only when they're set."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record with dynamic context fields."""
        # Base format
        base_format = "%(asctime)s [%(levelname)s] %(name)s"

        # Collect context fields that are present
        context_parts = []
        context_metadata = contextual_logging.get_all_context_metadata()
        for key, value in context_metadata.items():
            if value is not None and hasattr(record, key):
                context_parts.append(f"{key}={value}")

        # Add context to format if any exists
        if context_parts:
            base_format += " [" + " ".join(context_parts) + "]"

        base_format += ": %(message)s"

        # Create formatter with the dynamic format
        formatter = logging.Formatter(base_format)
        return formatter.format(record)
