"""Modified orchestrator_main.py — adds env-var-driven launcher selection.

Changes vs upstream:
  * Reads $TANGLE_LAUNCHER ("kubernetes" | "kubernetes_gcs" | "skypilot")
    to choose between built-in K8s launchers and the new SkyPilot launcher.
  * Defaults to the previous behavior (kubernetes_hostpath) so existing
    deployments are unaffected.
  * SkyPilot-specific config exposed via env vars to keep the bootstrap small.

This is the smallest possible diff that lets users opt into the SkyPilot
launcher without forking the bootstrap.
"""

import logging
import os
import pathlib

import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend import orchestrator_sql
from cloud_pipelines_backend.launchers import kubernetes_launchers
from cloud_pipelines.orchestration.storage_providers import local_storage


def _build_launcher():
    """Select container launcher via TANGLE_LAUNCHER env var.

    Values:
      "kubernetes"      (default) — KubernetesWithHostPathContainerLauncher
      "kubernetes_gcs"  — KubernetesWithGcsFuseContainerLauncher (GKE)
      "skypilot"        — SkyPilotKubernetesLauncher
    """
    choice = os.environ.get("TANGLE_LAUNCHER", "kubernetes").strip().lower()

    if choice == "skypilot":
        # Lazy-import so users without the skypilot extra don't pay for it.
        from cloud_pipelines_backend.launchers.skypilot_launchers import (
            SkyPilotKubernetesLauncher,
        )
        return SkyPilotKubernetesLauncher(
            infra=os.environ.get("SKYPILOT_INFRA", "kubernetes"),
            pool=os.environ.get("SKYPILOT_POOL"),
            default_image=os.environ.get("DEFAULT_CONTAINER_IMAGE"),
            priority_class=os.environ.get("DEFAULT_PRIORITY_CLASS"),
            default_labels={"managed-by": "tangle"},
        )

    # Existing K8s launcher paths.
    from kubernetes import config as k8s_config_lib
    from kubernetes import client as k8s_client_lib
    try:
        k8s_config_lib.load_incluster_config()
    except Exception:
        k8s_config_lib.load_kube_config()
    k8s_client = k8s_client_lib.ApiClient()
    k8s_client_lib.VersionApi(k8s_client).get_code(_request_timeout=5)

    if choice == "kubernetes_gcs":
        return kubernetes_launchers.KubernetesWithGcsFuseContainerLauncher(
            api_client=k8s_client,
        )
    return kubernetes_launchers.KubernetesWithHostPathContainerLauncher(
        api_client=k8s_client,
    )


def main():
    logger = logging.getLogger(__name__)
    orchestrator_logger = logging.getLogger("cloud_pipelines_backend.orchestrator_sql")

    orchestrator_logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s")

    file_handler = logging.FileHandler("orchestrator_main.log")
    stderr_handler = logging.StreamHandler()
    file_handler.setLevel(logging.DEBUG)
    stderr_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    stderr_handler.setFormatter(formatter)

    orchestrator_logger.addHandler(file_handler)
    logger.addHandler(file_handler)
    logger.addHandler(stderr_handler)

    logger.info("Starting the orchestrator")

    DEFAULT_DATABASE_URI = "sqlite:///db.sqlite"
    database_uri = os.environ.get("DATABASE_URI", DEFAULT_DATABASE_URI)
    db_engine = sqlalchemy.create_engine(url=database_uri)

    session_factory = orm.sessionmaker(
        autocommit=False, autoflush=False, bind=db_engine
    )

    artifact_store_root_dir = (pathlib.Path.cwd() / "tmp" / "artifacts").as_posix()
    log_store_root_dir = (pathlib.Path.cwd() / "tmp" / "logs").as_posix()

    default_task_annotations = {
        kubernetes_launchers.RESOURCES_CPU_ANNOTATION_KEY: "1",
        kubernetes_launchers.RESOURCES_MEMORY_ANNOTATION_KEY: "512Mi",
    }

    launcher = _build_launcher()
    logger.info(f"Using launcher: {type(launcher).__name__}")

    orchestrator = orchestrator_sql.OrchestratorService_Sql(
        session_factory=session_factory,
        launcher=launcher,
        storage_provider=local_storage.LocalStorageProvider(),
        data_root_uri=artifact_store_root_dir,
        logs_root_uri=log_store_root_dir,
        default_task_annotations=default_task_annotations,
        sleep_seconds_between_queue_sweeps=5.0,
    )
    orchestrator.run_loop()


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s\t%(levelname)s\t%(message)s", level=logging.NOTSET
    )
    main()
