"""Tests for the PENDING-deadline watchdog and kubelet reason surfacing.

Covers:
* ``_pending_deadline_exceeded`` boundary logic in the orchestrator.
* ``LaunchedKubernetesContainer.pending_diagnostics`` reason extraction.

The diagnostics tests build ``V1Pod`` fixtures directly, so they run offline
without a cluster.
"""

from __future__ import annotations

import datetime

from kubernetes import client as k8s_client_lib

from cloud_pipelines_backend import orchestrator_sql
from cloud_pipelines_backend.launchers import kubernetes_launchers

_NOW = datetime.datetime(2026, 6, 30, 12, 0, 0, tzinfo=datetime.timezone.utc)


def test_pending_deadline_disabled_when_duration_is_none():
    created_at = _NOW - datetime.timedelta(days=6)
    assert (
        orchestrator_sql._pending_deadline_exceeded(
            created_at=created_at, now=_NOW, max_pending_duration=None
        )
        is False
    )


def test_pending_deadline_skipped_when_created_at_unknown():
    assert (
        orchestrator_sql._pending_deadline_exceeded(
            created_at=None,
            now=_NOW,
            max_pending_duration=datetime.timedelta(minutes=30),
        )
        is False
    )


def test_pending_deadline_not_exceeded_under_threshold():
    created_at = _NOW - datetime.timedelta(minutes=10)
    assert (
        orchestrator_sql._pending_deadline_exceeded(
            created_at=created_at,
            now=_NOW,
            max_pending_duration=datetime.timedelta(minutes=30),
        )
        is False
    )


def test_pending_deadline_exceeded_past_threshold():
    created_at = _NOW - datetime.timedelta(minutes=31)
    assert (
        orchestrator_sql._pending_deadline_exceeded(
            created_at=created_at,
            now=_NOW,
            max_pending_duration=datetime.timedelta(minutes=30),
        )
        is True
    )


def test_pending_deadline_boundary_is_strict():
    # Exactly at the threshold is not yet exceeded.
    created_at = _NOW - datetime.timedelta(minutes=30)
    assert (
        orchestrator_sql._pending_deadline_exceeded(
            created_at=created_at,
            now=_NOW,
            max_pending_duration=datetime.timedelta(minutes=30),
        )
        is False
    )


def _make_launched_container(
    pod: k8s_client_lib.V1Pod,
) -> kubernetes_launchers.LaunchedKubernetesContainer:
    return kubernetes_launchers.LaunchedKubernetesContainer(
        pod_name="task-abc-rtdrr",
        namespace="oasis",
        output_uris={},
        log_uri="memory://log",
        debug_pod=pod,
    )


def test_pending_diagnostics_surfaces_gcsfuse_mount_wedge():
    pod = k8s_client_lib.V1Pod(
        metadata=k8s_client_lib.V1ObjectMeta(name="task-abc-rtdrr"),
        status=k8s_client_lib.V1PodStatus(
            phase="Pending",
            container_statuses=[
                k8s_client_lib.V1ContainerStatus(
                    name="main",
                    image="img",
                    image_id="",
                    ready=False,
                    restart_count=0,
                    state=k8s_client_lib.V1ContainerState(
                        waiting=k8s_client_lib.V1ContainerStateWaiting(
                            reason="CreateContainerConfigError",
                            message=(
                                "MountVolume.SetUp failed for volume"
                                ' "gcsfuse-prd-oasis-tmp": code = Unauthenticated'
                                " desc = failed to prepare storage service"
                            ),
                        )
                    ),
                )
            ],
        ),
    )
    diagnostics = _make_launched_container(pod).pending_diagnostics
    assert diagnostics is not None
    assert "CreateContainerConfigError" in diagnostics
    assert "code = Unauthenticated" in diagnostics


def test_pending_diagnostics_returns_none_without_main_container_status():
    # Before the kubelet creates a container status (e.g. an unschedulable pod),
    # there is no main-container waiting reason to surface.
    pod = k8s_client_lib.V1Pod(
        metadata=k8s_client_lib.V1ObjectMeta(name="task-abc-rtdrr"),
        status=k8s_client_lib.V1PodStatus(phase="Pending"),
    )
    assert _make_launched_container(pod).pending_diagnostics is None


def test_pending_diagnostics_returns_none_when_no_status():
    pod = k8s_client_lib.V1Pod(
        metadata=k8s_client_lib.V1ObjectMeta(name="task-abc-rtdrr"),
        status=None,
    )
    assert _make_launched_container(pod).pending_diagnostics is None
