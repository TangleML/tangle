"""
Normalizes exception messages into stable strings for error grouping.

Strips instance-specific values (pod names, IDs, memory addresses, byte
offsets) so that structurally identical errors produce the same key
regardless of which specific resource was involved.
"""

import json
import re

try:
    from ..launchers.interfaces import LauncherError as _LauncherError

    _LAUNCHER_ERROR_AVAILABLE = True
except ImportError:
    _LauncherError = None  # type: ignore[assignment,misc]
    _LAUNCHER_ERROR_AVAILABLE = False

_POD_NAME_PATTERN = re.compile(r"(?:task|tangle(?:-ce)?)-[a-zA-Z0-9]+-[a-zA-Z0-9]+")
_OBJECT_REPR_PATTERN = re.compile(r"<[^>]+ object at 0x[0-9a-fA-F]+>")
_HEX_ADDRESS_PATTERN = re.compile(r"\b0x[0-9a-fA-F]+\b")
_UUID_PATTERN = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE
)
_LONG_ALNUM_ID_PATTERN = re.compile(r"\b[a-zA-Z0-9]{16,}\b")
# Matches from the first `{"`, `{'`, or `{ "` / `{ '` to end of string.
# Both the embedded dict/JSON literal and any trailing message text are replaced
# with `{...}` — the greedy match is intentional: anything after a runtime-data
# dict in an error message is typically also variable and should not affect grouping.
_JSON_OBJECT_PATTERN = re.compile(r"\{\s*['\"].*", re.DOTALL)


def _strip_generic(*, message: str) -> str:
    message = _JSON_OBJECT_PATTERN.sub("{...}", message)
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


def _normalize_launcher_error(*, exception: BaseException) -> str | None:
    if not _LAUNCHER_ERROR_AVAILABLE or not isinstance(exception, _LauncherError):
        return None
    message = _JSON_OBJECT_PATTERN.sub("{...}", str(exception))
    return f"LauncherError: {message.strip()}"


def normalize_error_message(*, exception: BaseException) -> str:
    """Return a stable normalized string for a single exception (no chain traversal)."""
    for normalizer in (
        _normalize_k8s_api_exception,
        _normalize_max_retry_error,
        _normalize_unicode_decode_error,
        _normalize_orchestrator_error,
        _normalize_launcher_error,
    ):
        result = normalizer(exception=exception)
        if result is not None:
            return result

    return f"{type(exception).__name__}: {_strip_generic(message=str(exception))}"


_CHAIN_PART_MAX_LEN = 80
_CHAIN_GROUPING_KEY_MAX_PART_LEN = 200
_CHAIN_MAX_DEPTH = 4


def _walk_chain(exception: BaseException) -> list[BaseException]:
    """Return the exception chain up to ``_CHAIN_MAX_DEPTH`` levels, cycle-safe."""
    excs: list[BaseException] = []
    seen: set[int] = set()
    exc: BaseException | None = exception
    while exc is not None and id(exc) not in seen and len(excs) < _CHAIN_MAX_DEPTH:
        seen.add(id(exc))
        excs.append(exc)
        exc = exc.__cause__ or (None if exc.__suppress_context__ else exc.__context__)
    return excs


def normalize_error_chain(*, exception: BaseException) -> str:
    """Return a stable normalized string covering the full exception chain.

    Walks ``__cause__`` (and ``__context__`` when not suppressed) and joins
    each level with `` <- ``.  Use this for grouping keys so that chained
    exceptions like ``LauncherError <- TimeoutError`` produce one stable group
    rather than one per root cause.
    """
    parts = [
        normalize_error_message(exception=exc)[:_CHAIN_GROUPING_KEY_MAX_PART_LEN]
        for exc in _walk_chain(exception)
    ]
    return " <- ".join(parts)


def build_chain_title(*, exception: BaseException) -> str | None:
    """Return a human-readable chain title for display, or ``None`` for single exceptions.

    Like ``normalize_error_chain`` but truncates each level so the result fits
    in a Bugsnag error list title.  Returns ``None`` when there is no
    chain so callers can skip overriding the default title.
    """
    parts: list[str] = []
    for exc in _walk_chain(exception):
        part = normalize_error_message(exception=exc)
        if len(part) > _CHAIN_PART_MAX_LEN:
            part = part[:_CHAIN_PART_MAX_LEN].rstrip() + "..."
        parts.append(part)
    if len(parts) <= 1:
        return None
    return " <- ".join(parts)
