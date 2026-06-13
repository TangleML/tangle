"""
Bugsnag error reporting integration.

Provides entry points to configure Bugsnag and report exceptions.

No-op if TANGLE_BUGSNAG_API_KEY is not set.

Environment variables:
    TANGLE_BUGSNAG_API_KEY             Required to enable Bugsnag reporting.
    TANGLE_ENV                         Release stage (e.g. "staging", "production").
    TANGLE_REVISION                    App version tag (e.g. git SHA). Optional.
    TANGLE_BUGSNAG_NOTIFY_ENDPOINT     Custom notify URL. Optional.
    TANGLE_BUGSNAG_SESSIONS_ENDPOINT   Custom sessions URL. Optional.
    TANGLE_BUGSNAG_CUSTOM_GROUPING_KEY Metadata key for normalized error grouping. Optional.
"""

import logging
import os
from typing import Any

import bugsnag as bugsnag_sdk
import bugsnag.event as bugsnag_event

from . import contextual_logging
from . import error_normalization

_logger = logging.getLogger(__name__)

_BUGSNAG_API_KEY = os.environ.get("TANGLE_BUGSNAG_API_KEY")
_TANGLE_ENV = os.environ.get("TANGLE_ENV")
_SERVICE_VERSION = os.environ.get("TANGLE_REVISION")
_NOTIFY_ENDPOINT = os.environ.get("TANGLE_BUGSNAG_NOTIFY_ENDPOINT")
_SESSIONS_ENDPOINT = os.environ.get("TANGLE_BUGSNAG_SESSIONS_ENDPOINT")
_CUSTOM_GROUPING_KEY = os.environ.get("TANGLE_BUGSNAG_CUSTOM_GROUPING_KEY")

IS_BUGSNAG_ENABLED: bool = bool(_BUGSNAG_API_KEY)
_setup_called: bool = False


def _before_notify(event: bugsnag_event.Event) -> None:
    """Attach contextual logging metadata to every Bugsnag event."""
    context = contextual_logging.get_all_context_metadata()
    if context:
        event.add_tab("tangle_context", context)
    if _CUSTOM_GROUPING_KEY and event.original_error:
        # Use the full chain for grouping so that "LauncherError <- TimeoutError"
        # and "LauncherError <- ApiException" land in separate, stable groups.
        chain = error_normalization.normalize_error_chain(
            exception=event.original_error
        )
        prefix = (event.metadata.get("extra") or {}).get("grouping_prefix")
        key_value = f"{prefix}: {chain}" if prefix else chain
        event.add_tab("custom", {_CUSTOM_GROUPING_KEY: key_value})
        if prefix and event.errors:
            try:
                for error in event.errors:
                    error.error_class = f"{prefix}: {error.error_class}"
            except Exception:
                _logger.warning(
                    "Could not prepend grouping prefix to errorClass", exc_info=True
                )
        # For chained exceptions, override the root-cause title (what Bugsnag
        # displays in the error list) with the full chain so reviewers can see
        # the complete raise path without opening the stacktrace.
        chain_title = error_normalization.build_chain_title(
            exception=event.original_error
        )
        if chain_title and event.errors:
            try:
                title = f"{prefix}: {chain_title}" if prefix else chain_title
                event.errors[-1].error_class = title
            except Exception:
                _logger.warning(
                    "Could not set chain title on errorClass", exc_info=True
                )


def setup(*, service_name: str | None = None) -> None:
    """Configure the Bugsnag client.

    No-op if TANGLE_BUGSNAG_API_KEY is not set.

    Args:
        service_name: Identifies the process in Bugsnag (e.g. "tangle-api").
    """
    if not IS_BUGSNAG_ENABLED:
        return

    try:
        bugsnag_sdk.configure(
            api_key=_BUGSNAG_API_KEY,
            release_stage=_TANGLE_ENV,
            auto_capture_sessions=True,
            params_filters=[
                "authorization",
                "cookie",
                "x-api-key",
                "x-forwarded-for",
                "proxy-authorization",
            ],
            app_version=_SERVICE_VERSION,
            endpoint=_NOTIFY_ENDPOINT,
            session_endpoint=_SESSIONS_ENDPOINT,
            project_root=service_name,
        )
        bugsnag_sdk.before_notify(_before_notify)
        global _setup_called
        _setup_called = True
    except Exception:
        _logger.exception("Failed to initialize Bugsnag")


def notify(*, exception: BaseException, **metadata: Any) -> None:
    """Report an exception to Bugsnag.

    No-op if Bugsnag is disabled.

    Args:
        exception: The exception to report.
        **metadata: Additional key/value pairs attached as a "extra" metadata tab.
    """
    if not IS_BUGSNAG_ENABLED or not _setup_called:
        return

    extra_data = metadata or None

    try:
        bugsnag_sdk.notify(
            exception,
            meta_data={"extra": extra_data} if extra_data else {},
        )
    except Exception:
        _logger.exception("Failed to notify Bugsnag")
