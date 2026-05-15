"""Tests for the Bugsnag instrumentation module."""

import unittest.mock as mock

import pytest


def test_is_bugsnag_enabled_false_when_no_env_vars(monkeypatch):
    monkeypatch.delenv("TANGLE_BUGSNAG_API_KEY", raising=False)
    monkeypatch.delenv("TANGLE_ENV", raising=False)

    import importlib
    import cloud_pipelines_backend.instrumentation.bugsnag_instrumentation as bugsnag_module

    importlib.reload(bugsnag_module)

    assert bugsnag_module.IS_BUGSNAG_ENABLED is False


def test_is_bugsnag_enabled_true_when_only_api_key(monkeypatch):
    monkeypatch.setenv("TANGLE_BUGSNAG_API_KEY", "test-key")
    monkeypatch.delenv("TANGLE_ENV", raising=False)

    import importlib
    import cloud_pipelines_backend.instrumentation.bugsnag_instrumentation as bugsnag_module

    importlib.reload(bugsnag_module)

    assert bugsnag_module.IS_BUGSNAG_ENABLED is True


def test_is_bugsnag_enabled_false_when_only_env(monkeypatch):
    monkeypatch.delenv("TANGLE_BUGSNAG_API_KEY", raising=False)
    monkeypatch.setenv("TANGLE_ENV", "staging")

    import importlib
    import cloud_pipelines_backend.instrumentation.bugsnag_instrumentation as bugsnag_module

    importlib.reload(bugsnag_module)

    assert bugsnag_module.IS_BUGSNAG_ENABLED is False


def test_is_bugsnag_enabled_true_when_both_set(monkeypatch):
    monkeypatch.setenv("TANGLE_BUGSNAG_API_KEY", "test-key")
    monkeypatch.setenv("TANGLE_ENV", "staging")

    import importlib
    import cloud_pipelines_backend.instrumentation.bugsnag_instrumentation as bugsnag_module

    importlib.reload(bugsnag_module)

    assert bugsnag_module.IS_BUGSNAG_ENABLED is True


def test_setup_noop_when_disabled(monkeypatch):
    monkeypatch.delenv("TANGLE_BUGSNAG_API_KEY", raising=False)
    monkeypatch.delenv("TANGLE_ENV", raising=False)

    import importlib
    import cloud_pipelines_backend.instrumentation.bugsnag_instrumentation as bugsnag_module

    importlib.reload(bugsnag_module)

    with mock.patch("bugsnag.configure") as mock_configure:
        bugsnag_module.setup(service_name="test-service")
        mock_configure.assert_not_called()


def test_setup_calls_bugsnag_configure_when_enabled(monkeypatch):
    monkeypatch.setenv("TANGLE_BUGSNAG_API_KEY", "test-api-key")
    monkeypatch.setenv("TANGLE_ENV", "staging")
    monkeypatch.delenv("TANGLE_SERVICE_VERSION", raising=False)

    import importlib
    import cloud_pipelines_backend.instrumentation.bugsnag_instrumentation as bugsnag_module

    importlib.reload(bugsnag_module)

    with mock.patch("bugsnag.configure") as mock_configure:
        bugsnag_module.setup(service_name="tangle-api")
        mock_configure.assert_called_once()
        call_kwargs = mock_configure.call_args.kwargs
        assert call_kwargs["api_key"] == "test-api-key"
        assert call_kwargs["release_stage"] == "staging"


def test_setup_includes_app_version_when_set(monkeypatch):
    monkeypatch.setenv("TANGLE_BUGSNAG_API_KEY", "test-api-key")
    monkeypatch.setenv("TANGLE_ENV", "production")
    monkeypatch.setenv("TANGLE_SERVICE_VERSION", "abc123")

    import importlib
    import cloud_pipelines_backend.instrumentation.bugsnag_instrumentation as bugsnag_module

    importlib.reload(bugsnag_module)

    with mock.patch("bugsnag.configure") as mock_configure:
        bugsnag_module.setup()
        call_kwargs = mock_configure.call_args.kwargs
        assert call_kwargs["app_version"] == "abc123"


def test_notify_noop_when_disabled(monkeypatch):
    monkeypatch.delenv("TANGLE_BUGSNAG_API_KEY", raising=False)
    monkeypatch.delenv("TANGLE_ENV", raising=False)

    import importlib
    import cloud_pipelines_backend.instrumentation.bugsnag_instrumentation as bugsnag_module

    importlib.reload(bugsnag_module)

    with mock.patch("bugsnag.notify") as mock_notify:
        bugsnag_module.notify(exception=ValueError("test error"))
        mock_notify.assert_not_called()


def test_notify_noop_when_setup_not_called(monkeypatch):
    monkeypatch.setenv("TANGLE_BUGSNAG_API_KEY", "test-api-key")
    monkeypatch.setenv("TANGLE_ENV", "staging")

    import importlib
    import cloud_pipelines_backend.instrumentation.bugsnag_instrumentation as bugsnag_module

    importlib.reload(bugsnag_module)

    # setup() not called — _setup_called remains False
    with mock.patch("bugsnag.notify") as mock_notify:
        bugsnag_module.notify(exception=ValueError("test error"))
        mock_notify.assert_not_called()


def test_notify_calls_bugsnag_when_enabled(monkeypatch):
    monkeypatch.setenv("TANGLE_BUGSNAG_API_KEY", "test-api-key")
    monkeypatch.setenv("TANGLE_ENV", "staging")

    import importlib
    import cloud_pipelines_backend.instrumentation.bugsnag_instrumentation as bugsnag_module

    importlib.reload(bugsnag_module)

    with mock.patch("bugsnag.configure"), mock.patch("bugsnag.before_notify"):
        bugsnag_module.setup()

    exc = ValueError("something went wrong")
    with mock.patch("bugsnag.notify") as mock_notify:
        bugsnag_module.notify(exception=exc, execution_id="exec-123")
        mock_notify.assert_called_once()
        call_args = mock_notify.call_args
        assert call_args.args[0] is exc
        assert call_args.kwargs["meta_data"] == {"extra": {"execution_id": "exec-123"}}


def test_notify_handles_bugsnag_failure_gracefully(monkeypatch):
    monkeypatch.setenv("TANGLE_BUGSNAG_API_KEY", "test-api-key")
    monkeypatch.setenv("TANGLE_ENV", "staging")

    import importlib
    import cloud_pipelines_backend.instrumentation.bugsnag_instrumentation as bugsnag_module

    importlib.reload(bugsnag_module)

    with mock.patch("bugsnag.configure"), mock.patch("bugsnag.before_notify"):
        bugsnag_module.setup()

    with mock.patch("bugsnag.notify", side_effect=RuntimeError("network error")):
        # Should not raise
        bugsnag_module.notify(exception=ValueError("original error"))


def test_before_notify_attaches_context_metadata(monkeypatch):
    monkeypatch.setenv("TANGLE_BUGSNAG_API_KEY", "test-api-key")
    monkeypatch.setenv("TANGLE_ENV", "staging")

    import importlib
    import cloud_pipelines_backend.instrumentation.bugsnag_instrumentation as bugsnag_module

    importlib.reload(bugsnag_module)

    from cloud_pipelines_backend.instrumentation import contextual_logging

    mock_event = mock.MagicMock()

    with contextual_logging.logging_context(
        request_id="req-abc", user_id="user@example.com"
    ):
        bugsnag_module._before_notify(mock_event)

    mock_event.add_tab.assert_called_once_with(
        "tangle_context",
        {"request_id": "req-abc", "user_id": "user@example.com"},
    )


def test_before_notify_skips_empty_context(monkeypatch):
    monkeypatch.setenv("TANGLE_BUGSNAG_API_KEY", "test-api-key")
    monkeypatch.setenv("TANGLE_ENV", "staging")

    import importlib
    import cloud_pipelines_backend.instrumentation.bugsnag_instrumentation as bugsnag_module

    importlib.reload(bugsnag_module)

    from cloud_pipelines_backend.instrumentation import contextual_logging

    contextual_logging.clear_context_metadata()

    mock_event = mock.MagicMock()
    bugsnag_module._before_notify(mock_event)
    mock_event.add_tab.assert_not_called()
