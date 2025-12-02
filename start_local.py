import logging
import os
import pathlib

import fastapi

# region Paths configuration

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

# region: DB Configuration
database_path = root_data_dir_path / "db.sqlite"
database_uri = f"sqlite:///{database_path}"
print(f"{database_uri=}")
# endregion

# region: Storage configuration
from cloud_pipelines.orchestration.storage_providers import local_storage

storage_provider = local_storage.LocalStorageProvider()

artifacts_root_uri = artifacts_dir_path.as_posix()
logs_root_uri = logs_dir_path.as_posix()
# endregion

# region: Launcher configuration
import docker
from cloud_pipelines_backend.launchers import local_docker_launchers

docker_client = docker.DockerClient.from_env(timeout=5)
_ = docker_client.version()

launcher = local_docker_launchers.DockerContainerLauncher(
    client=docker_client,
)
# endregion

# region: Orchestrator configuration
default_task_annotations = {}
sleep_seconds_between_queue_sweeps: float = 1.0
# endregion

# region: Authentication configuration
import fastapi

ADMIN_USER_NAME = "admin"
default_component_library_owner_username = ADMIN_USER_NAME


# ! This function is just a placeholder for user authentication and authorization so that every request has a user name and permissions.
# ! This placeholder function authenticates the user as user with name "admin" and read/write/admin permissions.
# ! In a real multi-user deployment, the `get_user_details` function MUST be replaced with real authentication/authorization based on OAuth or another auth system.
def get_user_details(request: fastapi.Request):
    return api_router.UserDetails(
        name=ADMIN_USER_NAME,
        permissions=api_router.Permissions(
            read=True,
            write=True,
            admin=True,
        ),
    )


# endregion


# region: Logging configuration
import logging.config

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"},
    },
    "handlers": {
        "default": {
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
        },
    },
    "loggers": {
        # root logger
        "": {
            "level": "INFO",
            "handlers": ["default"],
            "propagate": False,
        },
        "uvicorn.error": {
            "level": "DEBUG",
            "handlers": ["default"],
            # Fix triplicated log messages
            "propagate": False,
        },
        "uvicorn.access": {
            "level": "DEBUG",
            "handlers": ["default"],
        },
        "watchfiles.main": {
            "level": "WARNING",
            "handlers": ["default"],
        },
    },
}

logging.config.dictConfig(LOGGING_CONFIG)

logger = logging.getLogger(__name__)
# endregion

# region: Database engine initialization
from cloud_pipelines_backend import database_ops

db_engine = database_ops.create_db_engine(
    database_uri=database_uri,
)
# endregion


# region: Orchestrator initialization

import logging
import pathlib

import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines.orchestration.storage_providers import (
    interfaces as storage_interfaces,
)
from cloud_pipelines_backend import orchestrator_sql


def run_orchestrator(
    db_engine: sqlalchemy.Engine,
    storage_provider: storage_interfaces.StorageProvider,
    data_root_uri: str,
    logs_root_uri: str,
    sleep_seconds_between_queue_sweeps: float = 1.0,
):
    # logger = logging.getLogger(__name__)
    # orchestrator_logger = logging.getLogger("cloud_pipelines_backend.orchestrator_sql")

    # orchestrator_logger.setLevel(logging.DEBUG)
    # formatter = logging.Formatter("%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s")

    # stderr_handler = logging.StreamHandler()
    # stderr_handler.setLevel(logging.INFO)
    # stderr_handler.setFormatter(formatter)

    # # TODO: Disable the default logger instead of not adding a new one
    # # orchestrator_logger.addHandler(stderr_handler)
    # logger.addHandler(stderr_handler)

    logger.info("Starting the orchestrator")

    # With autobegin=False you always need to begin a transaction, even to query the DB.
    session_factory = orm.sessionmaker(
        autocommit=False, autoflush=False, bind=db_engine
    )

    orchestrator = orchestrator_sql.OrchestratorService_Sql(
        session_factory=session_factory,
        launcher=launcher,
        storage_provider=storage_provider,
        data_root_uri=data_root_uri,
        logs_root_uri=logs_root_uri,
        default_task_annotations=default_task_annotations,
        sleep_seconds_between_queue_sweeps=sleep_seconds_between_queue_sweeps,
    )
    orchestrator.run_loop()


run_configured_orchestrator = lambda: run_orchestrator(
    db_engine=db_engine,
    storage_provider=storage_provider,
    data_root_uri=artifacts_root_uri,
    logs_root_uri=logs_root_uri,
    sleep_seconds_between_queue_sweeps=sleep_seconds_between_queue_sweeps,
)
# endregion


# region: API Server initialization
import contextlib
import threading
import traceback

import fastapi
from fastapi import staticfiles

from cloud_pipelines_backend import api_router
from cloud_pipelines_backend import database_ops


@contextlib.asynccontextmanager
async def lifespan(app: fastapi.FastAPI):
    database_ops.initialize_and_migrate_db(db_engine=db_engine)
    threading.Thread(
        target=run_configured_orchestrator,
        daemon=True,
    ).start()
    if os.environ.get("GOOGLE_CLOUD_SHELL") == "true":
        # TODO: Find a way to get fastapi/starlette/uvicorn port
        port = 8000
        logger.info(
            f"View app at: https://shell.cloud.google.com/devshell/proxy?port={port}"
        )
    yield


app = fastapi.FastAPI(
    title="Cloud Pipelines API",
    version="0.0.1",
    separate_input_output_schemas=False,
    lifespan=lifespan,
)


@app.exception_handler(Exception)
def handle_error(request: fastapi.Request, exc: BaseException):
    exception_str = traceback.format_exception(type(exc), exc, exc.__traceback__)
    return fastapi.responses.JSONResponse(
        status_code=503,
        content={"exception": exception_str},
    )


api_router.setup_routes(
    app=app,
    db_engine=db_engine,
    user_details_getter=get_user_details,
    container_launcher_for_log_streaming=launcher,
    default_component_library_owner_username=default_component_library_owner_username,
)


# Health check needed by the Web app
@app.get("/services/ping")
def health_check():
    return {}


# Mounting the web app if the files exist
this_dir = pathlib.Path(__file__).parent
web_app_search_dirs = [
    this_dir / ".." / "pipeline-studio-app" / "build",
    this_dir / ".." / "frontend" / "build",
    this_dir / ".." / "frontend_build",
    this_dir / ".." / "tangle-ui" / "dist",
    this_dir / ".." / "ui_build",
    this_dir / "pipeline-studio-app" / "build",
    this_dir / "ui_build",
]
found_frontend_build_files = False
for web_app_dir in web_app_search_dirs:
    if web_app_dir.exists():
        found_frontend_build_files = True
        logger.info(
            f"Found the Web app static files at {str(web_app_dir)}. Mounting them."
        )
        # The Web app base URL is currently static and hardcoded.
        # TODO: Remove this mount once the base URL becomes relative.
        app.mount(
            "/tangle-ui/",
            staticfiles.StaticFiles(directory=web_app_dir, html=True),
            name="static",
        )
        app.mount(
            "/pipeline-studio-app/",
            staticfiles.StaticFiles(directory=web_app_dir, html=True),
            name="static",
        )
        app.mount(
            "/",
            staticfiles.StaticFiles(directory=web_app_dir, html=True),
            name="static",
        )
if not found_frontend_build_files:
    logger.warning("The Web app files were not found. Skipping.")
# endregion
