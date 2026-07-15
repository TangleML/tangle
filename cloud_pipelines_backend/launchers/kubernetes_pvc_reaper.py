"""Event-driven cleanup of launcher-managed PersistentVolumeClaims.

Watches Kubernetes Jobs and deletes the PVC(s) bound to a Job when that Job is
deleted, so auto-provisioned volumes do not leak. PVCs are matched to their
owning Job via the labels applied by
:func:`kubernetes_launchers.create_managed_pvc`.

Watch-only: there is no periodic reconcile sweep, so a Job deletion that happens
while the watcher is down (process restart, crash) is not reaped. This is a
deliberate minimality choice; the leak surface is bounded to Jobs deleted during
watcher downtime.
"""

from __future__ import annotations

import logging
import threading

import kubernetes.client.exceptions
from kubernetes import client as k8s_client_lib
from kubernetes import watch as k8s_watch_lib

from . import kubernetes_launchers

_logger = logging.getLogger(__name__)

# Server-side watch timeout so a silently dropped connection recycles instead of
# blocking forever. The watch is simply re-established when it expires.
_WATCH_TIMEOUT_SECONDS = 300

# How long to wait for the watch thread to exit when stopping.
_STOP_JOIN_TIMEOUT_SECONDS = 10


class KubernetesPVCReaper:
    """Deletes launcher-managed PVCs when their owning Job is deleted.

    Runs a single daemon thread that streams Job events in ``namespace`` and, on
    each Job ``DELETED`` event, deletes the managed PVC(s) whose owner-job label
    matches the deleted Job.
    """

    def __init__(
        self,
        *,
        api_client: k8s_client_lib.ApiClient,
        namespace: str,
        request_timeout: int | tuple[int, int] = 10,
    ):
        self._api_client = api_client
        self._namespace = namespace
        self._request_timeout = request_timeout
        self._batch_client = k8s_client_lib.BatchV1Api(api_client)
        self._core_client = k8s_client_lib.CoreV1Api(api_client)
        self._managed_selector = (
            f"{kubernetes_launchers.PVC_MANAGED_LABEL_KEY}"
            f"={kubernetes_launchers.PVC_MANAGED_LABEL_VALUE}"
        )
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    @classmethod
    def from_launcher(
        cls,
        launcher: kubernetes_launchers._KubernetesContainerLauncherBase,
        **kwargs,
    ) -> "KubernetesPVCReaper":
        """Build a reaper reusing a launcher's Kubernetes client and namespace."""
        return cls(
            api_client=launcher._api_client,
            namespace=launcher._namespace,
            **kwargs,
        )

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run, name="kubernetes-pvc-reaper", daemon=True
        )
        self._thread.start()
        _logger.info(f"PVC reaper started for namespace {self._namespace!r}")

    def stop(self) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=_STOP_JOIN_TIMEOUT_SECONDS)
        self._thread = None

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._watch_once()
            except kubernetes.client.exceptions.ApiException as exc:
                # 410 Gone: the resourceVersion is too old; the watch is simply
                # re-established (from the latest state) on the next iteration.
                if exc.status == 410:
                    _logger.info("PVC reaper watch expired (410 Gone); restarting")
                else:
                    _logger.exception("PVC reaper watch error; restarting")
            except Exception:
                _logger.exception("PVC reaper watch error; restarting")

    def _watch_once(self) -> None:
        watch = k8s_watch_lib.Watch()
        try:
            for event in watch.stream(
                self._batch_client.list_namespaced_job,
                namespace=self._namespace,
                timeout_seconds=_WATCH_TIMEOUT_SECONDS,
            ):
                if self._stop_event.is_set():
                    break
                self._handle_event(event)
        finally:
            watch.stop()

    def _handle_event(self, event: dict) -> None:
        if event.get("type") != "DELETED":
            return
        job = event.get("object")
        metadata = getattr(job, "metadata", None)
        job_name = getattr(metadata, "name", None)
        if job_name:
            self._reap_pvcs_for_job(job_name)

    def _reap_pvcs_for_job(self, job_name: str) -> None:
        selector = (
            f"{self._managed_selector},"
            f"{kubernetes_launchers.PVC_OWNER_JOB_NAME_LABEL_KEY}={job_name}"
        )
        try:
            pvcs = self._core_client.list_namespaced_persistent_volume_claim(
                namespace=self._namespace,
                label_selector=selector,
                _request_timeout=self._request_timeout,
            )
        except Exception:
            _logger.exception(
                f"PVC reaper: failed to list PVCs for deleted Job {job_name!r}"
            )
            return

        for pvc in pvcs.items:
            pvc_name = pvc.metadata.name
            try:
                self._core_client.delete_namespaced_persistent_volume_claim(
                    name=pvc_name,
                    namespace=self._namespace,
                    _request_timeout=self._request_timeout,
                )
                _logger.info(
                    f"PVC reaper: deleted PVC {pvc_name!r} after "
                    f"Job {job_name!r} deletion"
                )
            except kubernetes.client.exceptions.ApiException as exc:
                if exc.status == 404:
                    # Already gone; nothing to do.
                    continue
                _logger.exception(f"PVC reaper: failed to delete PVC {pvc_name!r}")
            except Exception:
                _logger.exception(f"PVC reaper: failed to delete PVC {pvc_name!r}")
