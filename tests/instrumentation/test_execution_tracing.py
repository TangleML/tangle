"""Tests for execution lifecycle OTel trace emission."""

import datetime

import pytest
from opentelemetry import trace
from opentelemetry.sdk import trace as otel_sdk_trace
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.instrumentation import execution_tracing


@pytest.fixture()
def span_exporter(monkeypatch: pytest.MonkeyPatch) -> InMemorySpanExporter:
    """Isolated in-memory span exporter for each test.

    Patches ``execution_tracing._tracer`` directly so tests are independent of
    global OTel provider state (the module-level ProxyTracer would otherwise
    remain bound to the provider from the first test run).
    """
    exporter = InMemorySpanExporter()
    provider = otel_sdk_trace.TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(
        execution_tracing, "_tracer", provider.get_tracer("tangle.orchestrator")
    )
    return exporter


def _make_execution(
    *, statuses: list[str], extra: dict | None = None
) -> bts.ExecutionNode:
    """Build an ExecutionNode stub with a pre-populated status history.

    Assigns a deterministic ID because OTel drops None-valued attributes and
    execution.id is only set by the DB insert_default in production.
    """
    history = [
        {
            "status": s,
            "first_observed_at": (
                datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
                + datetime.timedelta(minutes=i * 5)
            ).isoformat(),
        }
        for i, s in enumerate(statuses)
    ]
    node = bts.ExecutionNode(task_spec={})
    node.id = "test-execution-id"
    node.extra_data = {
        bts.EXECUTION_NODE_EXTRA_DATA_STATUS_HISTORY_KEY: history,
        **(extra or {}),
    }
    return node


class TestTryEmitExecutionTrace:
    def test_no_spans_for_non_terminal_execution(
        self, span_exporter: InMemorySpanExporter
    ) -> None:
        execution = _make_execution(statuses=["QUEUED", "RUNNING"])
        execution_tracing.emit_execution_trace(execution=execution)
        assert span_exporter.get_finished_spans() == ()

    def test_no_spans_for_empty_history(
        self, span_exporter: InMemorySpanExporter
    ) -> None:
        execution = _make_execution(statuses=[])
        execution_tracing.emit_execution_trace(execution=execution)
        assert span_exporter.get_finished_spans() == ()

    def test_emits_root_and_child_spans_on_terminal(
        self, span_exporter: InMemorySpanExporter
    ) -> None:
        execution = _make_execution(statuses=["QUEUED", "RUNNING", "SUCCEEDED"])
        execution_tracing.emit_execution_trace(execution=execution)

        names = {s.name for s in span_exporter.get_finished_spans()}
        assert "execution" in names
        assert any(n.startswith("execution.status ") for n in names)

    def test_child_span_count_matches_history(
        self, span_exporter: InMemorySpanExporter
    ) -> None:
        execution = _make_execution(statuses=["QUEUED", "RUNNING", "SUCCEEDED"])
        execution_tracing.emit_execution_trace(execution=execution)

        status_spans = [
            s
            for s in span_exporter.get_finished_spans()
            if s.name.startswith("execution.status ")
        ]
        assert len(status_spans) == 3

    def test_root_span_has_execution_id_attribute(
        self, span_exporter: InMemorySpanExporter
    ) -> None:
        execution = _make_execution(statuses=["QUEUED", "SUCCEEDED"])
        execution_tracing.emit_execution_trace(execution=execution)

        root = next(
            s for s in span_exporter.get_finished_spans() if s.name == "execution"
        )
        assert root.attributes["execution.id"] == execution.id

    def test_child_spans_share_trace_id_with_root(
        self, span_exporter: InMemorySpanExporter
    ) -> None:
        execution = _make_execution(statuses=["QUEUED", "RUNNING", "SUCCEEDED"])
        execution_tracing.emit_execution_trace(execution=execution)

        finished = span_exporter.get_finished_spans()
        trace_ids = {s.context.trace_id for s in finished}
        assert len(trace_ids) == 1

    def test_root_span_duration_matches_history(
        self, span_exporter: InMemorySpanExporter
    ) -> None:
        execution = _make_execution(statuses=["QUEUED", "RUNNING", "SUCCEEDED"])
        execution_tracing.emit_execution_trace(execution=execution)

        root = next(
            s for s in span_exporter.get_finished_spans() if s.name == "execution"
        )
        duration_ns = root.end_time - root.start_time
        assert duration_ns == int(
            datetime.timedelta(minutes=10).total_seconds() * 1_000_000_000
        )

    def test_child_span_status_attribute(
        self, span_exporter: InMemorySpanExporter
    ) -> None:
        execution = _make_execution(statuses=["QUEUED", "SUCCEEDED"])
        execution_tracing.emit_execution_trace(execution=execution)

        status_spans = [
            s
            for s in span_exporter.get_finished_spans()
            if s.name.startswith("execution.status ")
        ]
        assert {s.name for s in status_spans} == {
            "execution.status QUEUED",
            "execution.status SUCCEEDED",
        }


class TestErrorDataAttrs:
    def test_failed_span_carries_orchestration_error_message(
        self, span_exporter: InMemorySpanExporter
    ) -> None:
        execution = _make_execution(
            statuses=["QUEUED", "RUNNING", "FAILED"],
            extra={
                bts.EXECUTION_NODE_EXTRA_DATA_ORCHESTRATION_ERROR_MESSAGE_KEY: "missing outputs"
            },
        )
        execution_tracing.emit_execution_trace(execution=execution)

        failed_span = next(
            s
            for s in span_exporter.get_finished_spans()
            if s.attributes.get("execution.status") == "FAILED"
        )
        assert failed_span.attributes["exception.message"] == "missing outputs"

    def test_system_error_span_carries_exception_message_and_stacktrace(
        self, span_exporter: InMemorySpanExporter
    ) -> None:
        execution = _make_execution(
            statuses=["QUEUED", "SYSTEM_ERROR"],
            extra={
                bts.EXECUTION_NODE_EXTRA_DATA_SYSTEM_ERROR_EXCEPTION_MESSAGE_KEY: "RuntimeError",
                bts.EXECUTION_NODE_EXTRA_DATA_SYSTEM_ERROR_EXCEPTION_FULL_KEY: "Traceback...",
            },
        )
        execution_tracing.emit_execution_trace(execution=execution)

        err_span = next(
            s
            for s in span_exporter.get_finished_spans()
            if s.attributes.get("execution.status") == "SYSTEM_ERROR"
        )
        assert err_span.attributes["exception.message"] == "RuntimeError"
        assert err_span.attributes["exception.stacktrace"] == "Traceback..."

    def test_root_span_marked_error_on_failed(
        self, span_exporter: InMemorySpanExporter
    ) -> None:
        from opentelemetry.trace import StatusCode

        execution = _make_execution(statuses=["QUEUED", "FAILED"])
        execution_tracing.emit_execution_trace(execution=execution)

        root = next(
            s for s in span_exporter.get_finished_spans() if s.name == "execution"
        )
        assert root.status.status_code == StatusCode.ERROR

    def test_root_span_not_marked_error_on_succeeded(
        self, span_exporter: InMemorySpanExporter
    ) -> None:
        from opentelemetry.trace import StatusCode

        execution = _make_execution(statuses=["QUEUED", "SUCCEEDED"])
        execution_tracing.emit_execution_trace(execution=execution)

        root = next(
            s for s in span_exporter.get_finished_spans() if s.name == "execution"
        )
        assert root.status.status_code != StatusCode.ERROR


class TestLauncherPodAttrs:
    def test_pending_span_has_k8s_attributes_when_launcher_data_present(
        self, span_exporter: InMemorySpanExporter
    ) -> None:
        execution = _make_execution(
            statuses=["QUEUED", "PENDING", "RUNNING", "SUCCEEDED"]
        )
        ce = bts.ContainerExecution(
            status=bts.ContainerExecutionStatus.SUCCEEDED,
            launcher_data={
                "kubernetes": {
                    "pod_name": "exec-abc-pod",
                    "namespace": "tangle",
                    "cluster_server": "https://gke.example.com",
                }
            },
        )
        execution.container_execution = ce
        execution.container_execution_id = "ce-test-id"
        execution_tracing.emit_execution_trace(execution=execution)

        pending_span = next(
            s
            for s in span_exporter.get_finished_spans()
            if s.attributes.get("execution.status") == "PENDING"
        )
        assert pending_span.attributes["k8s.pod.name"] == "exec-abc-pod"
        assert pending_span.attributes["k8s.namespace.name"] == "tangle"
        assert pending_span.attributes["k8s.cluster.url"] == "https://gke.example.com"

    def test_pending_span_absent_when_no_container_execution(
        self, span_exporter: InMemorySpanExporter
    ) -> None:
        execution = _make_execution(
            statuses=["QUEUED", "PENDING", "RUNNING", "SUCCEEDED"]
        )
        execution_tracing.emit_execution_trace(execution=execution)

        pending_span = next(
            s
            for s in span_exporter.get_finished_spans()
            if s.attributes.get("execution.status") == "PENDING"
        )
        assert "k8s.pod.name" not in (pending_span.attributes or {})

    def test_non_pending_span_has_no_k8s_attributes(
        self, span_exporter: InMemorySpanExporter
    ) -> None:
        execution = _make_execution(statuses=["QUEUED", "SUCCEEDED"])
        ce = bts.ContainerExecution(
            status=bts.ContainerExecutionStatus.SUCCEEDED,
            launcher_data={
                "kubernetes": {
                    "pod_name": "exec-abc-pod",
                    "namespace": "tangle",
                    "cluster_server": "https://gke.example.com",
                }
            },
        )
        execution.container_execution = ce
        execution.container_execution_id = "ce-test-id"
        execution_tracing.emit_execution_trace(execution=execution)

        for span in span_exporter.get_finished_spans():
            assert "k8s.pod.name" not in (span.attributes or {})


class TestLauncherTypeAttrs:
    def test_root_span_carries_launcher_type_and_cluster_url(
        self, span_exporter: InMemorySpanExporter
    ) -> None:
        execution = _make_execution(statuses=["QUEUED", "SUCCEEDED"])
        ce = bts.ContainerExecution(
            status=bts.ContainerExecutionStatus.SUCCEEDED,
            launcher_data={
                "kubernetes": {
                    "pod_name": "exec-abc-pod",
                    "namespace": "tangle",
                    "cluster_server": "https://gke.example.com",
                }
            },
        )
        execution.container_execution = ce
        execution.container_execution_id = "ce-test-id"
        execution_tracing.emit_execution_trace(execution=execution)

        root = next(
            s for s in span_exporter.get_finished_spans() if s.name == "execution"
        )
        assert root.attributes["execution.launcher"] == "kubernetes"
        assert root.attributes["k8s.cluster.url"] == "https://gke.example.com"

    def test_root_span_omits_launcher_without_container_execution(
        self, span_exporter: InMemorySpanExporter
    ) -> None:
        execution = _make_execution(statuses=["QUEUED", "SUCCEEDED"])
        execution_tracing.emit_execution_trace(execution=execution)

        root = next(
            s for s in span_exporter.get_finished_spans() if s.name == "execution"
        )
        assert "execution.launcher" not in (root.attributes or {})

    def test_root_span_carries_cloud_provider_when_annotation_set(
        self, span_exporter: InMemorySpanExporter
    ) -> None:
        execution = _make_execution(statuses=["QUEUED", "SUCCEEDED"])
        execution.task_spec = {
            "annotations": {
                "cloud-pipelines.net/orchestration/cloud_provider": "nebius"
            }
        }
        execution_tracing.emit_execution_trace(execution=execution)

        root = next(
            s for s in span_exporter.get_finished_spans() if s.name == "execution"
        )
        assert root.attributes["execution.cloud_provider"] == "nebius"

    def test_root_span_omits_cloud_provider_when_annotation_absent(
        self, span_exporter: InMemorySpanExporter
    ) -> None:
        execution = _make_execution(statuses=["QUEUED", "SUCCEEDED"])
        execution_tracing.emit_execution_trace(execution=execution)

        root = next(
            s for s in span_exporter.get_finished_spans() if s.name == "execution"
        )
        assert "execution.cloud_provider" not in (root.attributes or {})


class TestCacheAttrs:
    def test_cache_miss_sets_hit_false(
        self, span_exporter: InMemorySpanExporter
    ) -> None:
        execution = _make_execution(statuses=["QUEUED", "SUCCEEDED"])
        execution_tracing.try_emit_execution_trace(execution=execution)

        root = next(
            s for s in span_exporter.get_finished_spans() if s.name == "execution"
        )
        assert root.attributes["execution.cache.hit"] is False

    def test_cache_hit_sets_hit_true_and_reused_from_id(
        self, span_exporter: InMemorySpanExporter
    ) -> None:
        execution = _make_execution(
            statuses=["QUEUED", "SUCCEEDED"],
            extra={"reused_from_execution_node_id": "source-execution-id"},
        )
        execution_tracing.try_emit_execution_trace(execution=execution)

        root = next(
            s for s in span_exporter.get_finished_spans() if s.name == "execution"
        )
        assert root.attributes["execution.cache.hit"] is True
        assert (
            root.attributes["execution.cache.reused_from_id"] == "source-execution-id"
        )
