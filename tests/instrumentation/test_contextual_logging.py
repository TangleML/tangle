"""Tests for contextual_logging.execution_logging_context."""

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.instrumentation import contextual_logging

_CLOUD_PROVIDER_KEY = "cloud-pipelines.net/orchestration/cloud_provider"


def _make_execution(*, task_spec: dict | None = None) -> bts.ExecutionNode:
    node = bts.ExecutionNode(task_spec=task_spec or {})
    node.id = "test-execution-id"
    node.extra_data = {}
    return node


class TestExecutionLoggingContext:
    def test_always_sets_execution_id(self):
        execution = _make_execution()
        with contextual_logging.execution_logging_context(execution):
            assert (
                contextual_logging.get_context_metadata("execution_id")
                == "test-execution-id"
            )

    def test_no_cloud_provider_when_annotation_absent(self):
        execution = _make_execution(task_spec={})
        with contextual_logging.execution_logging_context(execution):
            assert contextual_logging.get_context_metadata("cloud_provider") is None

    def test_sets_cloud_provider_from_annotation(self):
        execution = _make_execution(
            task_spec={"annotations": {_CLOUD_PROVIDER_KEY: "nebius"}}
        )
        with contextual_logging.execution_logging_context(execution):
            assert contextual_logging.get_context_metadata("cloud_provider") == "nebius"

    def test_no_cloud_provider_when_task_spec_is_none(self):
        execution = _make_execution(task_spec=None)
        with contextual_logging.execution_logging_context(execution):
            assert contextual_logging.get_context_metadata("cloud_provider") is None

    def test_no_cloud_provider_when_annotations_is_none(self):
        execution = _make_execution(task_spec={"annotations": None})
        with contextual_logging.execution_logging_context(execution):
            assert contextual_logging.get_context_metadata("cloud_provider") is None

    def test_context_is_cleared_after_block(self):
        execution = _make_execution(
            task_spec={"annotations": {_CLOUD_PROVIDER_KEY: "gcp"}}
        )
        with contextual_logging.execution_logging_context(execution):
            pass
        assert contextual_logging.get_context_metadata("execution_id") is None
        assert contextual_logging.get_context_metadata("cloud_provider") is None
