from collections import abc
import contextlib
import dataclasses
import typing
import typing_extensions

import fastapi
import sqlalchemy
from sqlalchemy import orm
import starlette.types


from . import api_server_sql
from . import backend_types_sql
from . import component_library_api_server as components_api
from . import database_ops
from . import errors
from .instrumentation import contextual_logging

if typing.TYPE_CHECKING:
    from .launchers import interfaces as launcher_interfaces


class Permissions(typing_extensions.TypedDict):
    read: bool
    write: bool
    admin: bool


@dataclasses.dataclass
class UserDetails:
    name: str | None
    permissions: Permissions


def setup_routes(
    app: fastapi.FastAPI,
    db_engine: sqlalchemy.Engine,
    user_details_getter: typing.Callable[..., UserDetails],
    container_launcher_for_log_streaming: "launcher_interfaces.ContainerTaskLauncher[launcher_interfaces.LaunchedContainer] | None" = None,
    default_component_library_owner_username: str = "admin",
):
    def get_session():
        with orm.Session(autocommit=False, autoflush=False, bind=db_engine) as session:
            yield session

    def create_db_and_tables():
        database_ops.initialize_and_migrate_db(db_engine=db_engine)

        # The default library must be initialized here, not when adding the Component Library routes.
        # Otherwise the tables won't yet exist when initialization is performed.
        component_library_service = components_api.ComponentLibraryService()
        with orm.Session(bind=db_engine) as session:
            component_library_service._initialize_empty_default_library_if_missing(
                session=session,
                published_by=default_component_library_owner_username,
            )

    @contextlib.asynccontextmanager
    async def lifespan(app: fastapi.FastAPI):
        create_db_and_tables()
        yield

    get_launcher = (
        (lambda: container_launcher_for_log_streaming)
        if container_launcher_for_log_streaming
        else None
    )

    _setup_routes_internal(
        app=app,
        get_session=get_session,
        user_details_getter=user_details_getter,
        get_launcher=get_launcher,
        lifespan=lifespan,
    )


def _setup_routes_internal(
    app: fastapi.FastAPI,
    get_session: (
        typing.Callable[..., orm.Session]
        | typing.Callable[..., abc.Iterator[orm.Session]]
    ),
    user_details_getter: typing.Callable[..., UserDetails],
    get_launcher: "typing.Callable[..., launcher_interfaces.ContainerTaskLauncher[launcher_interfaces.LaunchedContainer]] | None" = None,
    lifespan: starlette.types.Lifespan[typing.Any] | None = None,
    pipeline_run_creation_hook: (
        typing.Callable[..., typing.Any]
        | typing.Callable[..., abc.Iterator[typing.Any]]
        | None
    ) = None,
):
    # We request `app: fastapi.FastAPI` instead of just returning the router
    # because we want to add exception handler which is only supported for `FastAPI`.

    @app.exception_handler(errors.ItemNotFoundError)
    def handle_not_found_error(request: fastapi.Request, exc: errors.ItemNotFoundError):
        response = fastapi.responses.JSONResponse(
            status_code=404,
            content={"message": str(exc)},
        )
        request_id = contextual_logging.get_context_metadata("request_id")
        if request_id:
            response.headers["x-tangle-request-id"] = request_id
        return response

    @app.exception_handler(errors.PermissionError)
    def handle_permission_error(request: fastapi.Request, exc: errors.PermissionError):
        response = fastapi.responses.JSONResponse(
            status_code=403,
            content={"message": str(exc)},
        )
        request_id = contextual_logging.get_context_metadata("request_id")
        if request_id:
            response.headers["x-tangle-request-id"] = request_id
        return response

    @app.exception_handler(errors.ItemAlreadyExistsError)
    def handle_item_already_exists_error(
        request: fastapi.Request, exc: errors.ItemAlreadyExistsError
    ):
        return fastapi.responses.JSONResponse(
            status_code=409,
            content={"message": str(exc)},
        )

    get_user_details_dependency = fastapi.Depends(user_details_getter)

    def get_user_name(
        user_details: typing.Annotated[UserDetails, get_user_details_dependency],
    ) -> str | None:
        return user_details.name

    get_user_name_dependency = fastapi.Depends(get_user_name)

    def user_has_admin_permission(
        user_details: typing.Annotated[UserDetails, get_user_details_dependency],
    ):
        return user_details.permissions.get("admin") == True

    user_has_admin_permission_dependency = fastapi.Depends(user_has_admin_permission)

    def ensure_admin_user(
        user_details: typing.Annotated[UserDetails, get_user_details_dependency],
    ):
        if not user_details.permissions.get("admin"):
            raise fastapi.HTTPException(
                status_code=fastapi.status.HTTP_403_FORBIDDEN,
                detail=f"User {user_details.name} is not an admin user",
            )

    ensure_admin_user_dependency = fastapi.Depends(ensure_admin_user)

    def ensure_user_can_write(
        user_details: typing.Annotated[UserDetails, get_user_details_dependency],
    ):
        if not user_details.permissions.get("write"):
            raise fastapi.HTTPException(
                status_code=fastapi.status.HTTP_403_FORBIDDEN,
                detail=f"User {user_details.name} does not have write permission",
            )

    ensure_user_can_write_dependency = fastapi.Depends(ensure_user_can_write)

    def inject_user_name(func: typing.Callable, parameter_name: str = "user_name"):
        # The `user_name` parameter value now comes from a Dependency (instead of request)
        return add_parameter_annotation_metadata(
            func,
            parameter_name=parameter_name,
            annotation_metadata=get_user_name_dependency,
        )

    SessionDep = typing.Annotated[orm.Session, fastapi.Depends(get_session)]

    def inject_session_dependency(func: typing.Callable) -> typing.Callable:
        return replace_annotations(
            func, original_annotation=orm.Session, new_annotation=SessionDep
        )

    artifact_service = api_server_sql.ArtifactNodesApiService_Sql()
    execution_service = api_server_sql.ExecutionNodesApiService_Sql()
    pipeline_run_service = api_server_sql.PipelineRunsApiService_Sql()

    # Can be used by admins to disable methods that modify the DB
    is_read_only = False

    def check_not_readonly():
        if is_read_only:
            raise fastapi.HTTPException(
                status_code=503, detail="The server is in read-only mode."
            )

    ensure_user_can_write_dependencies = [
        fastapi.Depends(check_not_readonly),
        ensure_user_can_write_dependency,
    ]

    # === API ===

    router = fastapi.APIRouter(
        lifespan=lifespan,
    )

    default_config: dict = dict(
        response_model_exclude_defaults=True,
        response_model_exclude_none=True,
    )

    router.get("/api/artifacts/{id}", tags=["artifacts"], **default_config)(
        # functools.partial(print_annotations(artifact_service.get), session=fastapi.Depends(get_session))
        inject_session_dependency(artifact_service.get)
    )
    router.get("/api/executions/{id}/details", tags=["executions"], **default_config)(
        inject_session_dependency(execution_service.get)
    )
    get_graph_execution_state = inject_session_dependency(
        execution_service.get_graph_execution_state
    )
    # Deprecated
    router.get("/api/executions/{id}/state", tags=["executions"], **default_config)(
        get_graph_execution_state
    )
    router.get(
        "/api/executions/{id}/graph_execution_state",
        tags=["executions"],
        **default_config,
    )(get_graph_execution_state)
    router.get(
        "/api/executions/{id}/container_state", tags=["executions"], **default_config
    )(inject_session_dependency(execution_service.get_container_execution_state))
    router.get("/api/executions/{id}/artifacts", tags=["executions"], **default_config)(
        inject_session_dependency(execution_service.get_artifacts)
    )

    LauncherDep = typing.Annotated[
        "launcher_interfaces.ContainerTaskLauncher[launcher_interfaces.LaunchedContainer]",
        fastapi.Depends(get_launcher),
    ]

    @router.get(
        "/api/executions/{id}/container_log", tags=["executions"], **default_config
    )
    def get_container_log(
        id: backend_types_sql.IdType,
        session: SessionDep,
        container_launcher: LauncherDep,
    ) -> api_server_sql.GetContainerExecutionLogResponse:
        return execution_service.get_container_execution_log(
            id=id,
            session=session,
            container_launcher=container_launcher,
        )

    if get_launcher:

        @router.get(
            "/api/executions/{id}/stream_container_log",
            tags=["executions"],
            **default_config,
        )
        def stream_container_log(
            id: backend_types_sql.IdType,
            session: SessionDep,
            container_launcher: LauncherDep,
        ):
            iterator = execution_service.stream_container_execution_log(
                session=session,
                container_launcher=container_launcher,
                execution_id=id,
            )
            return fastapi.responses.StreamingResponse(
                iterator,
                headers={"X-Content-Type-Options": "nosniff"},
                media_type="text/event-stream",
            )

    list_pipeline_runs_func = pipeline_run_service.list
    if get_user_name_dependency:
        # The `created_by` parameter value now comes from a Dependency (instead of request)
        list_pipeline_runs_func = add_parameter_annotation_metadata(
            list_pipeline_runs_func,
            parameter_name="current_user",
            annotation_metadata=get_user_name_dependency,
        )

    router.get("/api/pipeline_runs/", tags=["pipelineRuns"], **default_config)(
        inject_session_dependency(list_pipeline_runs_func)
    )
    router.get("/api/pipeline_runs/{id}", tags=["pipelineRuns"], **default_config)(
        inject_session_dependency(pipeline_run_service.get)
    )

    create_run_func = pipeline_run_service.create
    # The `session` parameter value now comes from a Dependency (instead of request)
    create_run_func = inject_session_dependency(create_run_func)
    # The `created_by` parameter value now comes from a Dependency (instead of request)
    create_run_func = add_parameter_annotation_metadata(
        create_run_func,
        parameter_name="created_by",
        annotation_metadata=get_user_name_dependency,
    )

    pipeline_run_creation_dependencies = ensure_user_can_write_dependencies
    if pipeline_run_creation_hook:
        pipeline_run_creation_dependencies += [
            fastapi.Depends(pipeline_run_creation_hook)
        ]
    router.post(
        "/api/pipeline_runs/",
        tags=["pipelineRuns"],
        dependencies=pipeline_run_creation_dependencies,
        **default_config,
    )(create_run_func)

    router.get(
        "/api/artifacts/{id}/signed_artifact_url", tags=["artifacts"], **default_config
    )(inject_session_dependency(artifact_service.get_signed_artifact_url))

    # The `terminated_by` parameter value now comes from a Dependency (instead of request)
    # We also allow admin users to cancel any run
    def pipeline_run_cancel(
        session: SessionDep,
        id: backend_types_sql.IdType,
        user_details: typing.Annotated[UserDetails, get_user_details_dependency],
    ):
        terminated_by = user_details.name
        if user_details and user_details and user_details.permissions.get("admin"):
            skip_user_check = True
        else:
            skip_user_check = False
        pipeline_run_service.terminate(
            session=session,
            id=id,
            terminated_by=terminated_by,
            skip_user_check=skip_user_check,
        )

    router.post(
        "/api/pipeline_runs/{id}/cancel",
        tags=["pipelineRuns"],
        dependencies=[fastapi.Depends(check_not_readonly)],
        **default_config,
    )(pipeline_run_cancel)

    ### Pipeline run annotations routes

    router.get(
        "/api/pipeline_runs/{id}/annotations/", tags=["pipelineRuns"], **default_config
    )(inject_session_dependency(pipeline_run_service.list_annotations))

    pipeline_run_set_annotation_func = inject_session_dependency(
        pipeline_run_service.set_annotation
    )
    pipeline_run_set_annotation_func = inject_user_name(
        func=pipeline_run_set_annotation_func
    )
    pipeline_run_set_annotation_func = add_parameter_annotation_metadata(
        pipeline_run_set_annotation_func,
        parameter_name="skip_user_check",
        annotation_metadata=user_has_admin_permission_dependency,
    )
    router.put(
        "/api/pipeline_runs/{id}/annotations/{key}",
        tags=["pipelineRuns"],
        dependencies=ensure_user_can_write_dependencies,
        **default_config,
    )(pipeline_run_set_annotation_func)

    pipeline_run_delete_annotation_func = inject_session_dependency(
        pipeline_run_service.delete_annotation
    )
    pipeline_run_delete_annotation_func = inject_user_name(
        func=pipeline_run_delete_annotation_func
    )
    pipeline_run_delete_annotation_func = add_parameter_annotation_metadata(
        pipeline_run_delete_annotation_func,
        parameter_name="skip_user_check",
        annotation_metadata=user_has_admin_permission_dependency,
    )
    router.delete(
        "/api/pipeline_runs/{id}/annotations/{key}",
        tags=["pipelineRuns"],
        dependencies=ensure_user_can_write_dependencies,
        **default_config,
    )(pipeline_run_delete_annotation_func)

    ### Users

    @dataclasses.dataclass
    class GetUserResponse:
        id: str | None
        permissions: list[str]

    @router.get("/api/users/me", tags=["users"])
    def get_current_user(
        user_details: typing.Annotated[UserDetails | None, get_user_details_dependency],
    ) -> GetUserResponse | None:
        if not user_details:
            return None
        permissions = list(
            permission
            for permission, is_granted in (user_details.permissions or {}).items()
            if is_granted == True
        )
        return GetUserResponse(
            id=user_details.name,
            permissions=permissions,
        )

    ### Secrets routes
    secrets_service = api_server_sql.SecretsApiService()

    router.get("/api/secrets/", tags=["secrets"], **default_config)(
        inject_session_dependency(
            inject_user_name(secrets_service.list_secrets, parameter_name="user_id")
        )
    )
    router.post("/api/secrets/", tags=["secrets"], **default_config)(
        add_parameter_annotation_metadata(
            inject_session_dependency(
                inject_user_name(
                    secrets_service.create_secret, parameter_name="user_id"
                )
            ),
            parameter_name="secret_value",
            annotation_metadata=fastapi.Body(embed=True),
        )
    )
    router.put("/api/secrets/{secret_name}", tags=["secrets"], **default_config)(
        add_parameter_annotation_metadata(
            inject_session_dependency(
                inject_user_name(
                    secrets_service.update_secret, parameter_name="user_id"
                )
            ),
            parameter_name="secret_value",
            annotation_metadata=fastapi.Body(embed=True),
        )
    )
    router.delete("/api/secrets/{secret_name}", tags=["secrets"], **default_config)(
        inject_session_dependency(
            inject_user_name(secrets_service.delete_secret, parameter_name="user_id")
        )
    )

    ### Component library routes

    component_service = components_api.ComponentService()
    published_component_service = components_api.PublishedComponentService()
    component_library_service = components_api.ComponentLibraryService()
    user_service = components_api.UserService()

    router.get("/api/components/{digest}", tags=["components"], **default_config)(
        inject_session_dependency(component_service.get)
    )
    # router.post("/api/components/", tags=["components"])(
    #     inject_session_dependency(component_service.add_from_text)
    # )

    router.get("/api/published_components/", tags=["components"], **default_config)(
        inject_session_dependency(published_component_service.list)
    )
    router.post("/api/published_components/", tags=["components"], **default_config)(
        inject_user_name(inject_session_dependency(published_component_service.publish))
    )
    router.put(
        "/api/published_components/{digest}", tags=["components"], **default_config
    )(inject_user_name(inject_session_dependency(published_component_service.update)))

    router.get("/api/component_libraries/", tags=["components"], **default_config)(
        inject_session_dependency(component_library_service.list)
    )
    router.get("/api/component_libraries/{id}", tags=["components"], **default_config)(
        inject_session_dependency(component_library_service.get)
    )
    router.post("/api/component_libraries/", tags=["components"], **default_config)(
        inject_user_name(inject_session_dependency(component_library_service.create))
    )
    router.put("/api/component_libraries/{id}", tags=["components"], **default_config)(
        inject_user_name(inject_session_dependency(component_library_service.replace))
    )

    router.get(
        "/api/component_library_pins/me/", tags=["components"], **default_config
    )(
        inject_user_name(
            inject_session_dependency(user_service.get_component_library_pins)
        )
    )
    router.put(
        "/api/component_library_pins/me/", tags=["components"], **default_config
    )(
        inject_user_name(
            inject_session_dependency(user_service.set_component_library_pins)
        )
    )

    ### Admin routes

    @router.put(
        "/api/admin/set_read_only_model",
        tags=["admin"],
        # Hiding the admin methods from the public schema.
        # include_in_schema=False,
        dependencies=[ensure_admin_user_dependency],
        **default_config,
    )
    def admin_set_read_only_model(read_only: bool):
        nonlocal is_read_only
        is_read_only = read_only

    @router.put(
        "/api/admin/execution_node/{id}/status",
        tags=["admin"],
        # Hiding the admin methods from the public schema.
        # include_in_schema=False,
        dependencies=[ensure_admin_user_dependency],
        **default_config,
    )
    def admin_set_execution_node_status(
        id: backend_types_sql.IdType,
        status: backend_types_sql.ContainerExecutionStatus,
        session: typing.Annotated[orm.Session, fastapi.Depends(get_session)],
    ):
        with session.begin():
            execution_node = session.get(backend_types_sql.ExecutionNode, id)
            execution_node.container_execution_status = status

    @router.get("/api/admin/sql_engine_connection_pool_status")
    async def get_sql_engine_connection_pool_status(
        session: typing.Annotated[orm.Session, fastapi.Depends(get_session)],
    ) -> str:
        return session.get_bind().pool.status()

    # # Needs to be called after all routes have been added to the router
    # app.include_router(router)

    app.include_router(router=router)


# def partial_wraps(func, **kwargs):
#     import inspect
#
#     # return functools.wraps(func)(functools.partial(func, *args, **kwargs))
#     partial_func = functools.partial(func, **kwargs)
#     param_names = set(kwargs)
#     signature = inspect.signature(func)
#     partial_signature = signature.replace(
#         parameters=[
#             parameter
#             for parameter_name, parameter in signature.parameters.items()
#             if parameter_name not in param_names
#         ]
#     )
#     print(f"{partial_signature=}")
#     partial_func.__signature__ = partial_signature
#     return partial_func


# Super hack to avoid duplicating functions in Fast API
# Normal functions take `session: orm.Session`.
# Those functions don't depend on FastApi, so we don't want to change those signatures.
# But FastApi needs `session: orm.Session = fastapi.Depends(get_session)`.
# functools.partial can add `session=fastapi.Depends(get_session)`, but destroys the signature.
# My partial_wraps attempt did not succeed.
# So the solution is to use `Annotated` method of dependency injection.
# https://fastapi.tiangolo.com/tutorial/dependencies
# We'll replace the original function annotations.
# It works!!!
def replace_annotations(func, original_annotation, new_annotation):
    for name in list(func.__annotations__):
        if func.__annotations__[name] == original_annotation:
            func.__annotations__[name] = new_annotation
    return func


def add_parameter_annotation_metadata(func, parameter_name: str, annotation_metadata):
    func.__annotations__[parameter_name] = typing.Annotated[
        func.__annotations__[parameter_name], annotation_metadata
    ]
    return func
