"""Local Tangle launcher using SkyPilot as the container backend.

Drop-in replacement for start_local.py that swaps the Docker launcher for
SkyPilotKubernetesLauncher. Useful for:
  - Browsing the Tangle UI without needing Docker installed.
  - Demoing the SkyPilot launcher integration end-to-end.

Run:
    /home/sky/.venv/bin/uvicorn start_local_skypilot:app --host 0.0.0.0 --port 8000

Or:
    /home/sky/.venv/bin/python -m uvicorn start_local_skypilot:app --host 0.0.0.0 --port 8000

The Tangle frontend (cloned to ./ui_build) is served at http://localhost:8000/.
Pipelines submitted from the UI will be dispatched through SkyPilot — they need
a configured infra (e.g. a Kubernetes context) to actually execute, but the UI
itself is fully browsable without one.
"""

from __future__ import annotations

import contextlib
import logging
import logging.config
import os
import pathlib
import threading
import traceback

import fastapi
import sqlalchemy
from fastapi import staticfiles
from sqlalchemy import orm

# region: Paths
root_data_dir = (
    os.environ.get("CLOUD_PIPELINES_BACKEND_DATA_DIR")
    or os.environ.get("TANGLE_BACKEND_DATA_DIR")
    or "data"
)
root_data_dir_path = pathlib.Path(root_data_dir).expanduser()
artifacts_dir_path = root_data_dir_path / "artifacts"
logs_dir_path = root_data_dir_path / "logs"

root_data_dir_path.mkdir(parents=True, exist_ok=True)
artifacts_dir_path.mkdir(parents=True, exist_ok=True)
logs_dir_path.mkdir(parents=True, exist_ok=True)
# endregion

# region: DB
database_path = root_data_dir_path / "db.sqlite"
database_uri = f"sqlite:///{database_path}"
print(f"{database_uri=}")
# endregion

# region: Storage
# Choose between LocalStorageProvider (default) and a cloud StorageProvider
# via the TANGLE_STORAGE_BUCKET env var. SkyPilot's file_mounts can mount
# cloud URIs (gs://, s3://, abfs://) but cannot represent relative local
# paths, so multi-step pipelines need a cloud StorageProvider.
#
# TANGLE_STORAGE_BUCKET must include a URI scheme so we can pick the
# matching StorageProvider (e.g. "gs://my-bucket", "s3://my-bucket").
storage_bucket = os.environ.get("TANGLE_STORAGE_BUCKET")
if storage_bucket:
    bucket_uri = storage_bucket.rstrip("/")
    if "://" not in bucket_uri:
        raise ValueError(
            f"TANGLE_STORAGE_BUCKET={storage_bucket!r} must include a URI "
            "scheme so the storage provider can be deduced (e.g. "
            "'gs://my-bucket', 's3://my-bucket')."
        )
    scheme = bucket_uri.split("://", 1)[0]
    if scheme == "gs":
        from cloud_pipelines.orchestration.storage_providers import google_cloud_storage
        storage_provider = google_cloud_storage.GoogleCloudStorageProvider()
    else:
        raise ValueError(
            f"TANGLE_STORAGE_BUCKET scheme {scheme!r} is not supported by "
            f"start_local_skypilot.py yet (got {storage_bucket!r}). Supported: "
            "'gs://'."
        )
    artifacts_root_uri = bucket_uri + "/artifacts"
    logs_root_uri = bucket_uri + "/logs"
else:
    from cloud_pipelines.orchestration.storage_providers import local_storage
    storage_provider = local_storage.LocalStorageProvider()
    artifacts_root_uri = artifacts_dir_path.as_posix()
    logs_root_uri = logs_dir_path.as_posix()
# endregion

# region: Launcher — SkyPilot instead of Docker
from cloud_pipelines_backend.launchers.skypilot_launchers import (
    SkyPilotKubernetesLauncher,
)

_infra_env = os.environ.get("SKYPILOT_INFRA")
launcher = SkyPilotKubernetesLauncher(
    # Empty string -> None (let optimizer pick / use API server's in-cluster).
    infra=_infra_env if _infra_env else None,
    pool=os.environ.get("SKYPILOT_POOL"),
    default_image=os.environ.get(
        "DEFAULT_CONTAINER_IMAGE", "python:3.11-slim"
    ),
    default_labels={"managed-by": "tangle"},
    annotation_to_label_keys={
        # Propagate the priority-class annotation through as a K8s pod label
        # so Kueue (or another admission controller) can route accordingly.
        "ml.shopify.io/priority-class": "ml.shopify.io/priority-class",
    },
    priority_class=os.environ.get("DEFAULT_PRIORITY_CLASS"),
    # Pass the storage provider so upload_log() can mirror SkyPilot logs to
    # log_uri and the Tangle UI's /api/.../log endpoint serves them.
    storage_provider=storage_provider,
)
# endregion

# region: Auth (single-user placeholder — same as upstream start_local.py)
from cloud_pipelines_backend import api_router

ADMIN_USER_NAME = "admin"
default_component_library_owner_username = ADMIN_USER_NAME


def get_user_details(request: fastapi.Request):
    return api_router.UserDetails(
        name=ADMIN_USER_NAME,
        permissions=api_router.Permissions(read=True, write=True, admin=True),
    )
# endregion

# region: Logging
from cloud_pipelines_backend.instrumentation import structured_logging

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"},
        "with_context": {"()": structured_logging.ContextAwareFormatter},
    },
    "filters": {
        "context_filter": {"()": structured_logging.LoggingContextFilter},
    },
    "handlers": {
        "default": {
            "level": "INFO",
            "formatter": "with_context",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
            "filters": ["context_filter"],
        },
    },
    "loggers": {
        "": {"level": "INFO", "handlers": ["default"], "propagate": False},
        "uvicorn.error": {"level": "DEBUG", "handlers": ["default"], "propagate": False},
        "uvicorn.access": {"level": "DEBUG", "handlers": ["default"]},
        "watchfiles.main": {"level": "WARNING", "handlers": ["default"]},
    },
}
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)
# endregion

# region: OpenTelemetry (no-op if not configured)
from cloud_pipelines_backend.instrumentation import opentelemetry as otel
otel.setup_providers()
# endregion

# region: DB engine
from cloud_pipelines_backend import database_ops

db_engine = database_ops.create_db_engine(database_uri=database_uri)
# endregion

# region: Orchestrator
from cloud_pipelines_backend import orchestrator_sql


def run_configured_orchestrator():
    logger.info("Starting orchestrator (SkyPilot launcher)")
    session_factory = orm.sessionmaker(
        autocommit=False, autoflush=False, bind=db_engine
    )
    orchestrator = orchestrator_sql.OrchestratorService_Sql(
        session_factory=session_factory,
        launcher=launcher,
        storage_provider=storage_provider,
        data_root_uri=artifacts_root_uri,
        logs_root_uri=logs_root_uri,
        default_task_annotations={},
        sleep_seconds_between_queue_sweeps=5.0,
    )
    orchestrator.run_loop()
# endregion

# region: API server
from cloud_pipelines_backend.instrumentation import api_tracing
from cloud_pipelines_backend.instrumentation import contextual_logging


@contextlib.asynccontextmanager
async def lifespan(app: fastapi.FastAPI):
    database_ops.initialize_and_migrate_db(db_engine=db_engine)
    threading.Thread(target=run_configured_orchestrator, daemon=True).start()
    logger.info("Tangle UI: open http://localhost:8000/ in a browser")
    yield


app = fastapi.FastAPI(
    title="Cloud Pipelines API (SkyPilot launcher)",
    version="0.0.1",
    separate_input_output_schemas=False,
    lifespan=lifespan,
)
otel.instrument_fastapi(app)
app.add_middleware(api_tracing.RequestContextMiddleware)


@app.exception_handler(Exception)
def handle_error(request: fastapi.Request, exc: BaseException):
    exception_str = traceback.format_exception(type(exc), exc, exc.__traceback__)
    response = fastapi.responses.JSONResponse(
        status_code=503, content={"exception": exception_str},
    )
    request_id = contextual_logging.get_context_metadata("request_id")
    if request_id:
        response.headers["x-tangle-request-id"] = request_id
    return response


api_router.setup_routes(
    app=app,
    db_engine=db_engine,
    user_details_getter=get_user_details,
    container_launcher_for_log_streaming=launcher,
    default_component_library_owner_username=default_component_library_owner_username,
)


@app.get("/services/ping")
def health_check():
    return {}


# Mount the prebuilt frontend (cloned from TangleML/tangle-ui to ./ui_build).
this_dir = pathlib.Path(__file__).parent
web_app_search_dirs = [
    this_dir / "ui_build",
    this_dir / ".." / "ui_build",
    this_dir / ".." / "tangle-ui" / "dist",
]
mounted = False
for web_app_dir in web_app_search_dirs:
    if web_app_dir.exists():
        logger.info(f"Mounting frontend from {web_app_dir}")
        app.mount(
            "/tangle-ui/",
            staticfiles.StaticFiles(directory=web_app_dir, html=True),
            name="static-tangle-ui",
        )
        app.mount(
            "/pipeline-studio-app/",
            staticfiles.StaticFiles(directory=web_app_dir, html=True),
            name="static-studio",
        )
        app.mount(
            "/",
            staticfiles.StaticFiles(directory=web_app_dir, html=True),
            name="static-root",
        )
        mounted = True
        break
if not mounted:
    logger.warning("Frontend build files not found; UI will not be available.")
# endregion
