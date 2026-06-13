"""Tests for error_normalization module."""

import json
import unittest.mock as mock

import pytest

from cloud_pipelines_backend.instrumentation import error_normalization


class TestNormalizeK8sApiException:
    def _make_exception(self, status: int, body: dict) -> Exception:
        try:
            from kubernetes.client.exceptions import ApiException
        except ImportError:
            pytest.skip("kubernetes not installed")
        exc = ApiException(status=status, reason=body.get("reason", ""))
        exc.body = json.dumps(body)
        return exc

    def test_pod_not_found(self):
        exc = self._make_exception(
            404,
            {
                "reason": "NotFound",
                "message": 'pods "task-abc123-xyz789" not found',
            },
        )
        result = error_normalization.normalize_error_message(exception=exc)
        assert (
            result == 'kubernetes ApiException (404): NotFound: pods "{pod}" not found'
        )

    def test_container_terminated(self):
        exc = self._make_exception(
            400,
            {
                "reason": "BadRequest",
                "message": 'container "main" in pod task-abc123-xyz789 is terminated',
            },
        )
        result = error_normalization.normalize_error_message(exception=exc)
        assert (
            result
            == 'kubernetes ApiException (400): BadRequest: container "main" in pod {pod} is terminated'
        )

    def test_pod_initializing(self):
        exc = self._make_exception(
            400,
            {
                "reason": "BadRequest",
                "message": 'container "main" in pod task-abc123-xyz789 is waiting to start: PodInitializing',
            },
        )
        result = error_normalization.normalize_error_message(exception=exc)
        assert result == (
            'kubernetes ApiException (400): BadRequest: container "main" in pod {pod} is waiting to start: PodInitializing'
        )

    def test_container_not_available(self):
        exc = self._make_exception(
            400,
            {
                "reason": "BadRequest",
                "message": 'container "main" in pod task-abc123-xyz789 is not available',
            },
        )
        result = error_normalization.normalize_error_message(exception=exc)
        assert (
            result
            == 'kubernetes ApiException (400): BadRequest: container "main" in pod {pod} is not available'
        )

    def test_webhook_timeout(self):
        exc = self._make_exception(
            500,
            {
                "reason": "InternalError",
                "message": 'failed calling webhook "validate.kyverno.svc-fail": context deadline exceeded',
            },
        )
        result = error_normalization.normalize_error_message(exception=exc)
        assert result == (
            'kubernetes ApiException (500): InternalError: failed calling webhook "validate.kyverno.svc-fail": context deadline exceeded'
        )

    def test_pod_not_found_tangle_ce_prefix(self):
        exc = self._make_exception(
            404,
            {
                "reason": "NotFound",
                "message": 'pods "tangle-ce-abc123-xyz789" not found',
            },
        )
        result = error_normalization.normalize_error_message(exception=exc)
        assert (
            result == 'kubernetes ApiException (404): NotFound: pods "{pod}" not found'
        )

    def test_pod_not_found_tangle_prefix(self):
        exc = self._make_exception(
            404,
            {
                "reason": "NotFound",
                "message": 'pods "tangle-abc123-xyz789" not found',
            },
        )
        result = error_normalization.normalize_error_message(exception=exc)
        assert (
            result == 'kubernetes ApiException (404): NotFound: pods "{pod}" not found'
        )

    def test_invalid_body_falls_back_to_str(self):
        try:
            from kubernetes.client.exceptions import ApiException
        except ImportError:
            pytest.skip("kubernetes not installed")
        exc = ApiException(status=503, reason="ServiceUnavailable")
        exc.body = "not json"
        result = error_normalization.normalize_error_message(exception=exc)
        assert result.startswith("kubernetes ApiException (503)")


class TestNormalizeMaxRetryError:
    def test_read_timeout(self):
        try:
            from urllib3.exceptions import MaxRetryError, ReadTimeoutError
        except ImportError:
            pytest.skip("urllib3 not installed")
        cause = ReadTimeoutError(pool=None, url="/api/v1/pods", message="timed out")
        exc = MaxRetryError(pool=None, url="http://10.0.0.1/api/v1/pods", reason=cause)
        result = error_normalization.normalize_error_message(exception=exc)
        assert (
            result
            == "MaxRetryError: k8s connection pool max retries exceeded (ReadTimeoutError)"
        )

    def test_no_cause(self):
        try:
            from urllib3.exceptions import MaxRetryError
        except ImportError:
            pytest.skip("urllib3 not installed")
        exc = MaxRetryError(pool=None, url="http://10.0.0.1/api/v1/pods", reason=None)
        result = error_normalization.normalize_error_message(exception=exc)
        assert (
            result
            == "MaxRetryError: k8s connection pool max retries exceeded (unknown)"
        )


class TestNormalizeUnicodeDecodeError:
    def test_strips_offset(self):
        exc = UnicodeDecodeError("utf-8", b"\xff\xfe", 42, 43, "invalid start byte")
        result = error_normalization.normalize_error_message(exception=exc)
        assert (
            result
            == "UnicodeDecodeError: 'utf-8' codec can't decode byte at position {n}"
        )

    def test_different_encoding(self):
        exc = UnicodeDecodeError("ascii", b"\x80", 0, 1, "ordinal not in range")
        result = error_normalization.normalize_error_message(exception=exc)
        assert (
            result
            == "UnicodeDecodeError: 'ascii' codec can't decode byte at position {n}"
        )


class TestNormalizeOrchestratorError:
    def test_strips_object_repr(self):
        try:
            from cloud_pipelines_backend.orchestrator_sql import (
                OrchestratorError,
            )  # absolute OK in tests
        except ImportError:
            pytest.skip("OrchestratorError not importable")
        exc = OrchestratorError(
            "Unexpected running container status: <ContainerStatus object at 0xdeadbeef>"
        )
        result = error_normalization.normalize_error_message(exception=exc)
        assert (
            result == "OrchestratorError: Unexpected running container status: {object}"
        )


class TestNormalizeLauncherError:
    def _make_launcher_error(
        self, message: str, cause: BaseException | None = None
    ) -> Exception:
        try:
            from cloud_pipelines_backend.launchers.interfaces import LauncherError
        except ImportError:
            pytest.skip("LauncherError not importable")
        if cause:
            try:
                raise LauncherError(message) from cause
            except LauncherError as exc:
                return exc
        return LauncherError(message)

    def test_strips_pod_spec_json(self):
        pod_spec = (
            "{'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'task-abc-xyz'}}"
        )
        exc = self._make_launcher_error(f"Failed to create pod: {pod_spec}")
        result = error_normalization.normalize_error_message(exception=exc)
        assert result == "LauncherError: Failed to create pod: {...}"

    def test_with_timeout_cause(self):
        cause = TimeoutError("The read operation timed out")
        exc = self._make_launcher_error(
            "Failed to create pod: {'apiVersion': 'v1'}", cause=cause
        )
        result = error_normalization.normalize_error_message(exception=exc)
        assert result == "LauncherError: Failed to create pod: {...}"

    def test_no_colon_in_message(self):
        exc = self._make_launcher_error("launch failed")
        result = error_normalization.normalize_error_message(exception=exc)
        assert result == "LauncherError: launch failed"

    def test_multi_colon_diagnostic_preserved(self):
        exc = self._make_launcher_error(
            "creating pod: spec invalid: missing field 'name'"
        )
        result = error_normalization.normalize_error_message(exception=exc)
        assert (
            result == "LauncherError: creating pod: spec invalid: missing field 'name'"
        )


class TestFallback:
    def test_strips_hex_address(self):
        exc = ValueError("object at 0xdeadbeef failed")
        result = error_normalization.normalize_error_message(exception=exc)
        assert result == "ValueError: object at {addr} failed"

    def test_strips_uuid(self):
        exc = RuntimeError("resource 550e8400-e29b-41d4-a716-446655440000 not found")
        result = error_normalization.normalize_error_message(exception=exc)
        assert result == "RuntimeError: resource {uuid} not found"

    def test_strips_long_alnum_id(self):
        exc = KeyError("abcdefghijklmnopq")  # 17 chars
        result = error_normalization.normalize_error_message(exception=exc)
        assert "abcdefghijklmnopq" not in result

    def test_stable_message_unchanged(self):
        exc = AttributeError("'NoneType' object has no attribute 'encode'")
        result = error_normalization.normalize_error_message(exception=exc)
        assert result == "AttributeError: 'NoneType' object has no attribute 'encode'"

    def test_strips_json_object(self):
        exc = RuntimeError("operation failed: {'key': 'value', 'nested': {'a': 1}}")
        result = error_normalization.normalize_error_message(exception=exc)
        assert result == "RuntimeError: operation failed: {...}"

    def test_strips_json_object_double_quotes(self):
        exc = RuntimeError('operation failed: {"key": "value"}')
        result = error_normalization.normalize_error_message(exception=exc)
        assert result == "RuntimeError: operation failed: {...}"


class TestNormalizeErrorChain:
    def _chained(
        self, outer_msg: str, inner: BaseException, outer_cls: type = RuntimeError
    ) -> BaseException:
        try:
            raise outer_cls(outer_msg) from inner
        except outer_cls as exc:
            return exc

    def test_single_exception_no_arrow(self):
        exc = ValueError("something went wrong")
        result = error_normalization.normalize_error_chain(exception=exc)
        assert result == "ValueError: something went wrong"
        assert " <- " not in result

    def test_two_level_chain(self):
        inner = TimeoutError("The read operation timed out")
        outer = self._chained("Failed to create pod: {'apiVersion': 'v1'}", inner)
        result = error_normalization.normalize_error_chain(exception=outer)
        assert result == (
            "RuntimeError: Failed to create pod: {...} <- TimeoutError: The read operation timed out"
        )

    def test_launcher_error_chain(self):
        try:
            from cloud_pipelines_backend.launchers.interfaces import LauncherError
        except ImportError:
            pytest.skip("LauncherError not importable")
        inner = TimeoutError("The read operation timed out")
        try:
            raise LauncherError("Failed to create pod: {spec}") from inner
        except LauncherError as outer:
            result = error_normalization.normalize_error_chain(exception=outer)
        assert result == (
            "LauncherError: Failed to create pod: {spec} <- TimeoutError: The read operation timed out"
        )

    def test_caps_at_four_levels(self):
        exc: BaseException = ValueError("level 4")
        for i in range(3, 0, -1):
            exc = self._chained(f"level {i}", exc)
        # Chain is 4 deep; add a 5th
        exc = self._chained("level 0", exc)
        result = error_normalization.normalize_error_chain(exception=exc)
        assert result.count(" <- ") == 3  # 4 parts max


class TestBuildChainTitle:
    def test_single_exception_returns_none(self):
        exc = ValueError("nothing to chain")
        assert error_normalization.build_chain_title(exception=exc) is None

    def test_two_level_chain_returns_string(self):
        inner = TimeoutError("The read operation timed out")
        try:
            raise RuntimeError("outer problem") from inner
        except RuntimeError as outer:
            result = error_normalization.build_chain_title(exception=outer)
        assert result == (
            "RuntimeError: outer problem <- TimeoutError: The read operation timed out"
        )

    def test_truncates_long_parts(self):
        inner = ValueError("x" * 200)
        try:
            raise RuntimeError("outer") from inner
        except RuntimeError as outer:
            result = error_normalization.build_chain_title(exception=outer)
        assert result is not None
        inner_part = result.split(" <- ")[1]
        assert len(inner_part) <= 83  # 80 + "..."
