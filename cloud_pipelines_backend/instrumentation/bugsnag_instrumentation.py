"""
Bugsnag error reporting integration.

Provides entry points to configure Bugsnag and report exceptions.

No-op if TANGLE_BUGSNAG_API_KEY is not set.

Environment variables:
    TANGLE_BUGSNAG_API_KEY        Required to enable Bugsnag reporting.
    TANGLE_ENV                    Release stage (e.g. "staging", "production").
    TANGLE_SERVICE_VERSION        App version tag (e.g. git SHA). Optional.
    TANGLE_BUGSNAG_NOTIFY_ENDPOINT   Custom notify URL. Optional.
    TANGLE_BUGSNAG_SESSIONS_ENDPOINT Custom sessions URL. Optional.
"""

import logging
import os
from typing import Any

import bugsnag as bugsnag_sdk
import bugsnag.event as bugsnag_event

from . import contextual_logging

_logger = logging.getLogger(__name__)

_BUGSNAG_API_KEY = os.environ.get("TANGLE_BUGSNAG_API_KEY")
_TANGLE_ENV = os.environ.get("TANGLE_ENV")
_SERVICE_VERSION = os.environ.get("TANGLE_SERVICE_VERSION")
_NOTIFY_ENDPOINT = os.environ.get("TANGLE_BUGSNAG_NOTIFY_ENDPOINT")
_SESSIONS_ENDPOINT = os.environ.get("TANGLE_BUGSNAG_SESSIONS_ENDPOINT")

IS_BUGSNAG_ENABLED: bool = bool(_BUGSNAG_API_KEY)
_setup_called: bool = False


def _before_notify(event: bugsnag_event.Event) -> None:
    """Attach contextual logging metadata to every Bugsnag event."""
    context = contextual_logging.get_all_context_metadata()
    if context:
        event.add_tab("tangle_context", context)


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
