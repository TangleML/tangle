import typing

from cloud_pipelines.orchestration.storage_providers import google_cloud_storage
from kubernetes import client as k8s_client_lib

from . import kubernetes_launchers

if typing.TYPE_CHECKING:
    from google.cloud import storage


class GoogleKubernetesEngine_UsingGoogleCloudStorage_KubernetesJobLauncher(
    kubernetes_launchers._KubernetesJobLauncher
):
    """Launcher that uses Google Kubernetes Engine to launch Kubernetes Jobs (uses GKE-gcsfuse driver for data passing)"""

    def __init__(
        self,
        *,
        api_client: k8s_client_lib.ApiClient,
        namespace: str = "default",
        service_account_name: str | None = None,
        request_timeout: int | tuple[int, int] = 10,
        gcs_client: "storage.Client | None" = None,
        pod_labels: dict[str, str] | None = None,
        pod_annotations: dict[str, str] | None = None,
        pod_postprocessor: kubernetes_launchers.PodPostProcessor | None = None,
    ):
        pod_postprocessors = [
            kubernetes_launchers._google_kubernetes_engine_accelerator_pod_postprocessor
        ]
        if pod_postprocessor:
            pod_postprocessors.append(pod_postprocessor)
        final_pod_postporocessor = kubernetes_launchers._create_pod_postprocessor_stack(
            pod_postprocessors
        )

        super().__init__(
            namespace=namespace,
            service_account_name=service_account_name,
            api_client=api_client,
            request_timeout=request_timeout,
            pod_labels=pod_labels,
            pod_annotations={"gke-gcsfuse/volumes": "true"} | (pod_annotations or {}),
            pod_postprocessor=final_pod_postporocessor,
            _storage_provider=google_cloud_storage.GoogleCloudStorageProvider(
                gcs_client
            ),
            _create_volume_and_volume_mount=kubernetes_launchers._create_volume_and_volume_mount_google_cloud_storage,
        )
