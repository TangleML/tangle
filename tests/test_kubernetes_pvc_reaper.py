"""Tests for cloud_pipelines_backend.launchers.kubernetes_pvc_reaper.

The Kubernetes client is mocked so the tests run offline.
"""

from __future__ import annotations

import types
from unittest import mock

import kubernetes.client.exceptions

from cloud_pipelines_backend.launchers import kubernetes_launchers
from cloud_pipelines_backend.launchers import kubernetes_pvc_reaper


def _make_reaper(
    namespace: str = "test-ns",
) -> "kubernetes_pvc_reaper.KubernetesPVCReaper":
    reaper = kubernetes_pvc_reaper.KubernetesPVCReaper(
        api_client=mock.MagicMock(),
        namespace=namespace,
    )
    # Replace the real API clients built in __init__ with mocks.
    reaper._core_client = mock.MagicMock()
    reaper._batch_client = mock.MagicMock()
    return reaper


def _job_event(event_type: str, job_name: str) -> dict:
    metadata = types.SimpleNamespace(name=job_name)
    job = types.SimpleNamespace(metadata=metadata)
    return {"type": event_type, "object": job}


def _pvc_list(*pvc_names: str):
    items = [
        types.SimpleNamespace(metadata=types.SimpleNamespace(name=name))
        for name in pvc_names
    ]
    return types.SimpleNamespace(items=items)


def test_deleted_job_reaps_matching_pvc():
    reaper = _make_reaper()
    reaper._core_client.list_namespaced_persistent_volume_claim.return_value = (
        _pvc_list("tangle-ce-abc")
    )

    reaper._handle_event(_job_event("DELETED", "tangle-ce-abc"))

    # The PVC list is scoped to both the managed label and the owning Job name.
    list_kwargs = (
        reaper._core_client.list_namespaced_persistent_volume_claim.call_args.kwargs
    )
    selector = list_kwargs["label_selector"]
    assert kubernetes_launchers.PVC_MANAGED_LABEL_KEY in selector
    assert (
        f"{kubernetes_launchers.PVC_OWNER_JOB_NAME_LABEL_KEY}=tangle-ce-abc" in selector
    )

    reaper._core_client.delete_namespaced_persistent_volume_claim.assert_called_once()
    delete_kwargs = (
        reaper._core_client.delete_namespaced_persistent_volume_claim.call_args.kwargs
    )
    assert delete_kwargs["name"] == "tangle-ce-abc"
    assert delete_kwargs["namespace"] == "test-ns"


def test_non_deleted_events_are_ignored():
    reaper = _make_reaper()

    for event_type in ("ADDED", "MODIFIED", "BOOKMARK"):
        reaper._handle_event(_job_event(event_type, "tangle-ce-abc"))

    reaper._core_client.list_namespaced_persistent_volume_claim.assert_not_called()
    reaper._core_client.delete_namespaced_persistent_volume_claim.assert_not_called()


def test_no_matching_pvc_deletes_nothing():
    reaper = _make_reaper()
    reaper._core_client.list_namespaced_persistent_volume_claim.return_value = (
        _pvc_list()
    )

    reaper._handle_event(_job_event("DELETED", "some-unrelated-job"))

    reaper._core_client.delete_namespaced_persistent_volume_claim.assert_not_called()


def test_already_deleted_pvc_is_swallowed():
    reaper = _make_reaper()
    reaper._core_client.list_namespaced_persistent_volume_claim.return_value = (
        _pvc_list("tangle-ce-abc")
    )
    reaper._core_client.delete_namespaced_persistent_volume_claim.side_effect = (
        kubernetes.client.exceptions.ApiException(status=404)
    )

    # Should not raise.
    reaper._reap_pvcs_for_job("tangle-ce-abc")


def test_run_restarts_watch_after_410():
    reaper = _make_reaper()
    calls: list[int] = []

    def fake_watch_once():
        calls.append(1)
        if len(calls) == 1:
            raise kubernetes.client.exceptions.ApiException(status=410)
        # Second pass: signal stop so _run exits.
        reaper._stop_event.set()

    reaper._watch_once = fake_watch_once
    reaper._run()

    assert len(calls) == 2


def test_stop_before_start_is_safe():
    reaper = _make_reaper()
    # Should not raise even though the thread was never started.
    reaper.stop()
