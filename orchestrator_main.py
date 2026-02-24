import logging
import os
import pathlib

import sqlalchemy
from cloud_pipelines.orchestration.storage_providers import local_storage
from sqlalchemy import orm

from cloud_pipelines_backend import orchestrator_sql
from cloud_pipelines_backend.launchers import kubernetes_launchers


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
    # TODO: Disable the default logger instead of not adding a new one
    # orchestrator_logger.addHandler(stderr_handler)

    logger.addHandler(file_handler)
    logger.addHandler(stderr_handler)

    logger.info("Starting the orchestrator")

    DEFAULT_DATABASE_URI = "sqlite:///db.sqlite"
    database_uri = os.environ.get("DATABASE_URI", DEFAULT_DATABASE_URI)
    db_engine = sqlalchemy.create_engine(url=database_uri)
    logger.info("Completed sqlalchemy.create_engine")

    # With autobegin=False you always need to beging a transaction, even to query the DB.
    session_factory = orm.sessionmaker(autocommit=False, autoflush=False, bind=db_engine)

    artifact_store_root_dir = (pathlib.Path.cwd() / "tmp" / "artifacts").as_posix()
    log_store_root_dir = (pathlib.Path.cwd() / "tmp" / "logs").as_posix()

    from kubernetes import client as k8s_client_lib
    from kubernetes import config as k8s_config_lib

    try:
        k8s_config_lib.load_incluster_config()
    except Exception:
        k8s_config_lib.load_kube_config()
    k8s_client = k8s_client_lib.ApiClient()

    k8s_client_lib.VersionApi(k8s_client).get_code(_request_timeout=5)
    logger.info("Kubernetes works")

    default_task_annotations = {
        kubernetes_launchers.RESOURCES_CPU_ANNOTATION_KEY: "1",
        kubernetes_launchers.RESOURCES_MEMORY_ANNOTATION_KEY: "512Mi",
    }

    # launcher = kubernetes_launchers.KubernetesWithGcsFuseContainerLauncher(
    launcher = kubernetes_launchers.KubernetesWithHostPathContainerLauncher(
        api_client=k8s_client,
    )

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
    # This sets the root logger to write to stdout (your console).
    # Your script/app needs to call this somewhere at least once.
    # logging.basicConfig()
    logging.basicConfig(format="%(asctime)s\t%(levelname)s\t%(message)s", level=logging.NOTSET)

    # # By default the root logger is set to WARNING and all loggers you define
    # # inherit that value. Here we set the root logger to NOTSET. This logging
    # # level is automatically inherited by all existing and new sub-loggers
    # # that do not set a less verbose level.
    # logging.root.setLevel(logging.NOTSET)

    # # The following line sets the root logger level as well.
    # # It's equivalent to both previous statements combined:
    # logging.basicConfig(level=logging.NOTSET)

    main()
