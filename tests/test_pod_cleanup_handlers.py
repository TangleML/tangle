"""Tests for the launcher's post-delete pod cleanup handler registry.

The registry is a generic extension point: an operator that stamps a finalizer
on its pods can register a handler to remove it inline the moment the pod is
deleted, rather than reconciling it out of band. These pin that handlers run on
every pod delete, receive the pod's identity, and are best-effort.
"""

from __future__ import annotations

from unittest import mock

import pytest

from cloud_pipelines_backend.launchers import kubernetes_launchers as k8sL


@pytest.fixture(autouse=True)
def _clear_handlers():
    k8sL._pod_cleanup_handlers.clear()
    yield
    k8sL._pod_cleanup_handlers.clear()


def test_a_registered_handler_runs_with_the_pod_identity() -> None:
    calls = []

    def handler(*, pod_name, namespace, api_client, pod=None) -> None:
        calls.append((pod_name, namespace, api_client, pod))

    k8sL._register_pod_cleanup_handler(handler)
    k8sL._run_pod_cleanup_handlers(
        pod_name="task-1",
        namespace="ns",
        api_client=mock.sentinel.api_client,
        pod=mock.sentinel.pod,
    )

    assert calls == [("task-1", "ns", mock.sentinel.api_client, mock.sentinel.pod)]


def test_a_failing_handler_does_not_stop_the_others() -> None:
    ran = []

    def boom(*, pod_name, namespace, api_client, pod=None) -> None:
        raise RuntimeError("handler blew up")

    def ok(*, pod_name, namespace, api_client, pod=None) -> None:
        ran.append(pod_name)

    k8sL._register_pod_cleanup_handler(boom)
    k8sL._register_pod_cleanup_handler(ok)

    # Best-effort: the exception is swallowed and the delete/other handlers proceed.
    k8sL._run_pod_cleanup_handlers(pod_name="task-2", namespace="ns", api_client=None)

    assert ran == ["task-2"]


def test_delete_pod_runs_the_handlers_after_deleting() -> None:
    seen = []
    k8sL._register_pod_cleanup_handler(
        lambda *, pod_name, namespace, api_client, pod=None: seen.append(pod_name)
    )

    launcher = mock.MagicMock()
    launcher._api_client = mock.sentinel.api_client
    container = k8sL.LaunchedKubernetesContainer(
        pod_name="task-9",
        namespace="ns",
        output_uris={},
        log_uri="",
        debug_pod=mock.sentinel.debug_pod,
        launcher=launcher,
    )

    with mock.patch.object(k8sL.k8s_client_lib, "CoreV1Api") as core_api_cls:
        container._delete_pod()
        core_api_cls.return_value.delete_namespaced_pod.assert_called_once()

    assert seen == ["task-9"]
