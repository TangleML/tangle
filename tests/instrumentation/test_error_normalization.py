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
