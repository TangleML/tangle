"""
Normalizes exception messages into stable strings for error grouping.

Strips instance-specific values (pod names, IDs, memory addresses, byte
offsets) so that structurally identical errors produce the same key
regardless of which specific resource was involved.
"""

import json
import re

_POD_NAME_PATTERN = re.compile(r"(?:task|tangle(?:-ce)?)-[a-zA-Z0-9]+-[a-zA-Z0-9]+")
_OBJECT_REPR_PATTERN = re.compile(r"<[^>]+ object at 0x[0-9a-fA-F]+>")
_HEX_ADDRESS_PATTERN = re.compile(r"\b0x[0-9a-fA-F]+\b")
_UUID_PATTERN = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE
)
_LONG_ALNUM_ID_PATTERN = re.compile(r"\b[a-zA-Z0-9]{16,}\b")


def _strip_generic(*, message: str) -> str:
    message = _OBJECT_REPR_PATTERN.sub("{object}", message)
    message = _HEX_ADDRESS_PATTERN.sub("{addr}", message)
    message = _UUID_PATTERN.sub("{uuid}", message)
    message = _LONG_ALNUM_ID_PATTERN.sub("{id}", message)
    return message.strip()


def _normalize_k8s_api_exception(*, exception: BaseException) -> str | None:
    try:
        from kubernetes.client import exceptions as k8s_exceptions

        if not isinstance(exception, k8s_exceptions.ApiException):
            return None
    except ImportError:
        return None

    status_code = exception.status
    try:
        body = json.loads(exception.body)
        reason = body.get("reason", "")
        message = body.get("message", "")
    except (json.JSONDecodeError, TypeError):
        reason = ""
        message = str(exception)

    message = _POD_NAME_PATTERN.sub("{pod}", message)
    parts = [f"kubernetes ApiException ({status_code})"]
    if reason:
        parts.append(reason)
    if message:
        parts.append(message)
    return ": ".join(parts)


def _normalize_max_retry_error(*, exception: BaseException) -> str | None:
    try:
        from urllib3.exceptions import MaxRetryError

        if not isinstance(exception, MaxRetryError):
            return None
    except ImportError:
        return None

    cause = type(exception.reason).__name__ if exception.reason else "unknown"
    return f"MaxRetryError: k8s connection pool max retries exceeded ({cause})"


def _normalize_unicode_decode_error(*, exception: BaseException) -> str | None:
    if not isinstance(exception, UnicodeDecodeError):
        return None
    return f"UnicodeDecodeError: '{exception.encoding}' codec can't decode byte at position {{n}}"


def _normalize_orchestrator_error(*, exception: BaseException) -> str | None:
    try:
        from ..orchestrator_sql import OrchestratorError

        if not isinstance(exception, OrchestratorError):
            return None
    except ImportError:
        return None

    message = _OBJECT_REPR_PATTERN.sub("{object}", str(exception))
    return f"OrchestratorError: {message}"


def normalize_error_message(*, exception: BaseException) -> str:
    """Return a stable normalized string for error grouping."""
    for normalizer in (
        _normalize_k8s_api_exception,
        _normalize_max_retry_error,
        _normalize_unicode_decode_error,
        _normalize_orchestrator_error,
    ):
        result = normalizer(exception=exception)
        if result is not None:
            return result

    return f"{type(exception).__name__}: {_strip_generic(message=str(exception))}"
