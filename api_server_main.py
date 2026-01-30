import os
import traceback

import fastapi

from cloud_pipelines_backend import api_router
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend.instrumentation import api_tracing
from cloud_pipelines_backend.instrumentation import contextual_logging
from cloud_pipelines_backend.instrumentation import otel_tracing

app = fastapi.FastAPI(
    title="Cloud Pipelines API",
    version="0.0.1",
    separate_input_output_schemas=False,
)

# Configure OpenTelemetry tracing
otel_tracing.setup_api_tracing(app)

# Add request context middleware for automatic request_id generation
app.add_middleware(api_tracing.RequestContextMiddleware)


@app.exception_handler(Exception)
def handle_error(request: fastapi.Request, exc: BaseException):
    exception_str = traceback.format_exception(type(exc), exc, exc.__traceback__)
    response = fastapi.responses.JSONResponse(
        status_code=503,
        content={"exception": exception_str},
    )
    request_id = contextual_logging.get_context_metadata("request_id")
    if request_id:
        response.headers["x-tangle-request-id"] = request_id
    return response


DEFAULT_DATABASE_URI = "sqlite:///db.sqlite"
database_uri = (
    os.environ.get("DATABASE_URI")
    or os.environ.get("DATABASE_URL")
    or DEFAULT_DATABASE_URI
)

db_engine = database_ops.create_db_engine_and_migrate_db(
    database_uri=database_uri,
)


ADMIN_USER_NAME = "admin"


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


api_router.setup_routes(
    app=app,
    db_engine=db_engine,
    user_details_getter=get_user_details,
    default_component_library_owner_username=ADMIN_USER_NAME,
)
