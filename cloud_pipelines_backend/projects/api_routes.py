"""HTTP routes for workspaces, projects and project resources.

Route inventory:
- GET    /api/workspaces/                                  -- the workspaces on this deployment.
- GET    /api/workspaces/{workspace_id}                    -- one of them.
- POST   /api/workspaces/                                  -- create one. **Admin only.**
- PATCH  /api/workspaces/{workspace_id}                    -- edit one. **Admin only.**
- DELETE /api/workspaces/{workspace_id}                    -- delete an empty one. **Admin only.**
- POST   /api/projects/                                    -- create a project in a workspace.
- GET    /api/projects/                                    -- every project, filtered and paged.
- GET    /api/projects/{project_id}                        -- one project, with resource counts.
- PATCH  /api/projects/{project_id}                        -- edit name, description or data.
- DELETE /api/projects/{project_id}                        -- hard delete, cascading resources.
- GET    /api/projects/{project_id}/runs                   -- runs submitted for this project.
- POST   /api/projects/{project_id}/resources/             -- attach a resource.
- GET    /api/projects/{project_id}/resources/             -- list them, filtered and paged.
- GET    /api/projects/{project_id}/resources/{id}         -- one resource, `payload` included.
- PATCH  /api/projects/{project_id}/resources/{id}         -- edit name, payload or data.
- DELETE /api/projects/{project_id}/resources/{id}         -- hard delete.

Conventions follow `user_pipelines/api_routes.py`: snake_case, cursor pagination through
`page_token` / `next_page_token`, trailing slashes on collection routes, and `extra="forbid"`
on every request body.

401 and 403 are always about the caller's permissions, never the target -- there is no ownership
403. Every PATCH body is read with `model_dump(exclude_unset=True)`, so an omitted key leaves the
column alone and an explicit `null` clears it; a `null` for a NOT NULL column is a 422.

`GET /api/projects/{project_id}/runs` reads `pipeline_run`, not `project_resource`: a run reaches
a project through an annotation its submitter sets. See
`user_pipelines.pipeline_run_annotations` for the key's shape.
"""

import collections.abc
import datetime
from typing import Annotated, Any, Final, Literal

import fastapi
import fastapi.responses
import pydantic
from cloud_pipelines_backend import api_router
from cloud_pipelines_backend.projects import db_models, errors, services
from sqlalchemy import orm
from starlette import status

_TAG: Final[str] = "projects"
_WORKSPACES_BASE: Final[str] = "/api/workspaces"
_PROJECTS_BASE: Final[str] = "/api/projects"
_CURSOR_SEPARATOR: Final[str] = "~"

# Said on the endpoint because somebody will eventually paste a credential into a note.
_PUBLIC_READ_POLICY: Final[str] = (
    "Projects, `data`, and resource payloads are public to authenticated readers with read "
    "permission. `created_by` is attribution and is never access control. Secret values must be "
    "stored in the secrets service and referenced; this resource is not a secret store."
)

# In `description=` rather than a docstring: only that reaches the generated schema.
_DATA_POLICY: Final[str] = (
    "`data` is an unstructured object owned by the client. It is checked for shape and nothing "
    "else -- that it is a JSON object, and that it serializes -- so either of those is a 422. "
    "Nothing in this service reads, queries or migrates what is inside it: its keys are a "
    "convention between clients, not part of this contract. A `document`'s content belongs in "
    "`payload`, never here."
)

_RESOURCE_POLICY: Final[str] = f"{_PUBLIC_READ_POLICY} {_DATA_POLICY}"

# On the three workspace write paths, so the restriction shows in the schema.
_ADMIN_WORKSPACE_POLICY: Final[str] = (
    "Requires `admin` permission as well as `write`. Workspaces are the container every "
    "project's `workspace_id` names and that deployment configuration refers to by id, so they "
    "are administered rather than created by users. Reading them needs only `read`."
)

_Name = Annotated[str, pydantic.StringConstraints(strip_whitespace=True, min_length=1)]
_TrimmedText = Annotated[str, pydantic.StringConstraints(strip_whitespace=True)]


# --- Request models -------------------------------------------------------------------------


class _RequestModel(pydantic.BaseModel):
    """The base every request body inherits, for `extra="forbid"`: an unknown field is a 422."""

    model_config = pydantic.ConfigDict(extra="forbid")


class CreateWorkspaceRequest(_RequestModel):
    """A new workspace, at a server-minted id. Admin only. `created_by` is never accepted."""

    name: _Name
    description: _TrimmedText | None = None
    # Settable, so a workspace can be provisioned already retired.
    is_active: bool = True
    data: dict[str, Any] | None = None


class UpdateWorkspaceRequest(_RequestModel):
    """An edit. Admin only. `data` is replaced wholesale, never merged."""

    name: _Name | None = None
    description: _TrimmedText | None = None
    is_active: bool | None = None
    data: dict[str, Any] | None = None


class CreateProjectRequest(_RequestModel):
    """A new project. `created_by` is stamped from the caller and never accepted here."""

    workspace_id: str
    name: _Name
    description: _TrimmedText | None = None
    data: dict[str, Any] | None = None
    # Declared by the caller: an agent reaches this API with the same credentials a person does.
    origin: db_models.ProjectOrigin = db_models.ProjectOrigin.USER


class UpdateProjectRequest(_RequestModel):
    """An edit. `workspace_id` is absent on purpose -- a project's workspace is fixed at
    creation. `data` is replaced wholesale, never merged."""

    name: _Name | None = None
    description: _TrimmedText | None = None
    data: dict[str, Any] | None = None


class CreateResourceRequest(_RequestModel):
    """A resource to attach. `project_id` comes from the path.

    `entity` decides which of the other two fields is required; `services.validate_entity_shape`
    holds the rule. `entity_id` is opaque and never joined.
    """

    entity: db_models.ProjectResourceEntity
    name: _TrimmedText | None = None
    entity_id: str | None = None
    payload: dict[str, Any] | None = None
    data: dict[str, Any] | None = None


class UpdateResourceRequest(_RequestModel):
    """An edit. `entity`, `entity_id` and `project_id` are immutable. `payload` and `data` are
    each replaced wholesale, never merged."""

    name: _TrimmedText | None = None
    payload: dict[str, Any] | None = None
    data: dict[str, Any] | None = None


# --- Response models ------------------------------------------------------------------------


class WorkspaceResponse(pydantic.BaseModel):
    """One workspace. `is_active: false` is retired: it still lists and resolves, so a picker
    greys it out rather than hiding it, but it refuses new projects with a 422."""

    id: str
    name: str
    description: str | None
    is_active: bool
    created_by: str | None
    data: dict[str, Any] | None
    created_at: datetime.datetime
    updated_at: datetime.datetime


class WorkspaceListResponse(pydantic.BaseModel):
    """Not paginated -- a deployment holds a handful of these."""

    workspaces: list[WorkspaceResponse]
    total_count: int


class ProjectSummaryResponse(pydantic.BaseModel):
    """A project as the list returns it.

    `resource_counts` is keyed by entity, one with no rows being absent rather than zero.
    """

    id: str
    workspace_id: str
    name: str
    description: str | None
    created_by: str | None
    origin: str
    data: dict[str, Any] | None
    created_at: datetime.datetime
    updated_at: datetime.datetime
    resource_counts: dict[str, int]


class ProjectResponse(ProjectSummaryResponse):
    """Identical to the summary today, kept distinct so a read-only-affordable field has
    somewhere to go."""


class ProjectListResponse(pydantic.BaseModel):
    projects: list[ProjectSummaryResponse]
    total_count: int
    next_page_token: str | None = None


class DeleteProjectResponse(pydantic.BaseModel):
    """What the hard delete removed, rather than a bare 204, so the UI can name it."""

    id: str
    deleted_resource_counts: dict[str, int]
    deleted_resource_total: int


class ProjectRunResponse(pydantic.BaseModel):
    """One run in a project's feed, in the shape `/api/pipeline_runs/` already returns it.

    Copied from the core `api_server_sql.PipelineRunResponse` rather than re-exported, so
    this contract does not move when that dataclass does.
    """

    id: str
    root_execution_id: str
    annotations: dict[str, Any] | None
    created_by: str | None
    created_at: datetime.datetime | None
    pipeline_name: str | None


class ProjectRunListResponse(pydantic.BaseModel):
    """A page of runs. No `total_count`: a COUNT over the correlated `EXISTS` this filter
    compiles to would be a second full pass per page."""

    runs: list[ProjectRunResponse]
    next_page_token: str | None = None


class ProjectResourceSummaryResponse(pydantic.BaseModel):
    """A resource as the list returns it.

    `payload` is absent; `GET /{resource_id}` has it. `entity` is a `str` so reads stay permissive
    where writes are strict -- a rollback to a deploy predating a new entity returns the row.
    """

    id: str
    project_id: str
    entity: str
    name: str | None
    entity_id: str | None
    data: dict[str, Any] | None
    created_by: str | None
    created_at: datetime.datetime
    updated_at: datetime.datetime


class ProjectResourceResponse(ProjectResourceSummaryResponse):
    payload: dict[str, Any] | None


class ProjectResourceListResponse(pydantic.BaseModel):
    resources: list[ProjectResourceSummaryResponse]
    total_count: int
    next_page_token: str | None = None


# --- Cursors --------------------------------------------------------------------------------


def _encode_cursor(*, timestamp: datetime.datetime, row_id: str) -> str:
    """The keyset convention the rest of this API already speaks: `<timestamp>~<id>`.

    Naive timestamps are stamped UTC rather than rejected -- SQLite hands back naive datetimes on
    some paths, and a cursor that changed meaning by dialect would be worse.
    """
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)
    return f"{timestamp.isoformat()}{_CURSOR_SEPARATOR}{row_id}"


def _decode_cursor(
    *, page_token: str, timestamp_field: str
) -> tuple[datetime.datetime, str]:
    """Parse a page token, or 422."""
    try:
        timestamp_text, row_id = page_token.split(_CURSOR_SEPARATOR, 1)
        timestamp = datetime.datetime.fromisoformat(timestamp_text)
        if timestamp.tzinfo is None:
            # Mirrors `_encode_cursor`: a naive half is read as the UTC this system writes.
            timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)
        else:
            # Inside the `try` for `OverflowError`: normalizing subtracts the offset, so a date
            # on either `datetime` boundary leaves the representable range.
            timestamp = timestamp.astimezone(datetime.timezone.utc)
    except (TypeError, ValueError, OverflowError) as exc:
        raise fastapi.HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"Unrecognized page_token format: {page_token!r}. Expected '{timestamp_field}~id' cursor."
            ),
        ) from exc
    return timestamp, row_id


# --- Serialization --------------------------------------------------------------------------


def _to_workspace_response(workspace: db_models.Workspace) -> WorkspaceResponse:
    return WorkspaceResponse(
        id=workspace.id,
        name=workspace.name,
        description=workspace.description,
        is_active=workspace.is_active,
        created_by=workspace.created_by,
        data=workspace.data,
        created_at=workspace.created_at,
        updated_at=workspace.updated_at,
    )


def _to_project_summary(
    *,
    project: db_models.Project,
    resource_counts: dict[str, int],
) -> ProjectSummaryResponse:
    return ProjectSummaryResponse(
        id=project.id,
        workspace_id=project.workspace_id,
        name=project.name,
        description=project.description,
        created_by=project.created_by,
        origin=project.origin,
        data=project.data,
        created_at=project.created_at,
        updated_at=project.updated_at,
        resource_counts=resource_counts,
    )


def _to_project_response(
    *,
    project: db_models.Project,
    resource_counts: dict[str, int],
) -> ProjectResponse:
    return ProjectResponse(
        **_to_project_summary(
            project=project, resource_counts=resource_counts
        ).model_dump()
    )


def _to_resource_summary(
    resource: db_models.ProjectResource,
) -> ProjectResourceSummaryResponse:
    """Serialize without touching `.payload` or `.extra_data`.

    A requirement, not an optimization: `services.list_resources` defers both with
    `raiseload=True`, so reading one here raises rather than fetching per row.
    """
    return ProjectResourceSummaryResponse(
        id=resource.id,
        project_id=resource.project_id,
        entity=resource.entity,
        name=resource.name,
        entity_id=resource.entity_id,
        data=resource.data,
        created_by=resource.created_by,
        created_at=resource.created_at,
        updated_at=resource.updated_at,
    )


def _to_resource_response(
    resource: db_models.ProjectResource,
) -> ProjectResourceResponse:
    return ProjectResourceResponse(
        **_to_resource_summary(resource).model_dump(),
        payload=resource.payload,
    )


# --- Error responses ------------------------------------------------------------------------


def _register_exception_handlers(*, app: fastapi.FastAPI) -> None:
    """Map the domain errors in `projects.errors` to their public HTTP responses.

    Here rather than beside the exceptions, so `errors` stays free of FastAPI. Every route
    reports the same shape: `{"detail": "..."}`.
    """

    @app.exception_handler(errors.ProjectNotFoundError)
    async def handle_project_not_found(
        request: fastapi.Request,
        exc: errors.ProjectNotFoundError,
    ) -> fastapi.responses.JSONResponse:
        del request
        return fastapi.responses.JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"detail": str(exc)},
        )

    @app.exception_handler(errors.ProjectValidationError)
    async def handle_project_validation_error(
        request: fastapi.Request,
        exc: errors.ProjectValidationError,
    ) -> fastapi.responses.JSONResponse:
        del request
        return fastapi.responses.JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            content={"detail": str(exc)},
        )

    @app.exception_handler(errors.DuplicateResourceError)
    async def handle_duplicate_resource(
        request: fastapi.Request,
        exc: errors.DuplicateResourceError,
    ) -> fastapi.responses.JSONResponse:
        del request
        return fastapi.responses.JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"detail": str(exc)},
        )

    @app.exception_handler(errors.WorkspaceNotEmptyError)
    async def handle_workspace_not_empty(
        request: fastapi.Request,
        exc: errors.WorkspaceNotEmptyError,
    ) -> fastapi.responses.JSONResponse:
        del request
        return fastapi.responses.JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"detail": str(exc)},
        )


def setup_project_routes(
    *,
    app: fastapi.FastAPI,
    get_session: (
        collections.abc.Callable[..., orm.Session]
        | collections.abc.Callable[..., collections.abc.Iterator[orm.Session]]
    ),
    user_details_getter: collections.abc.Callable[..., api_router.UserDetails],
    is_read_only: bool = False,
) -> None:
    """Mount the workspace, project and resource routes on `app`.

    `is_read_only` mirrors `notices.api_routes.setup_notice_routes` and carries the same known
    limitation: the admin toggle is a `nonlocal` inside `api_router.setup_routes`, so no module
    mounted from `app.py` can observe it. This is the *static* setting, False in every current
    deployment, with the dependency wired so honouring the flag is a one-line change.
    """
    _register_exception_handlers(app=app)

    SessionDep = Annotated[orm.Session, fastapi.Depends(get_session)]
    UserDep = Annotated[api_router.UserDetails, fastapi.Depends(user_details_getter)]

    router = fastapi.APIRouter()
    project_service = services.ProjectService()

    def _authenticated_user(
        user_details: api_router.UserDetails,
        *,
        required_permissions: tuple[Literal["read", "write", "admin"], ...] = ("read",),
    ) -> str:
        """The caller's name, or 401/403. Copied in shape from `user_pipelines`.

        `admin` goes through the same loop as `read` and `write` rather than a separate
        dependency: it is a key in the same `Permissions` mapping, and one code path means one
        403 body for all three.
        """
        if not user_details.name:
            raise fastapi.HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication required",
            )
        for required_permission in required_permissions:
            if not user_details.permissions.get(required_permission):
                raise fastapi.HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"User {user_details.name} does not have {required_permission} permission",
                )
        return user_details.name

    def check_not_readonly() -> None:
        if is_read_only:
            raise fastapi.HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="The server is in read-only mode.",
            )

    write_dependencies = [fastapi.Depends(check_not_readonly)]

    def _counts_for(*, session: orm.Session, project_id: str) -> dict[str, int]:
        return project_service.count_resources(
            session=session, project_ids=[project_id]
        ).get(project_id, {})

    ### Workspaces

    @router.get(f"{_WORKSPACES_BASE}/", tags=[_TAG])
    def list_workspaces(
        session: SessionDep,
        user_details: UserDep,
    ) -> WorkspaceListResponse:
        """The workspaces available on this deployment."""
        _authenticated_user(user_details)
        workspaces = project_service.list_workspaces(session=session)
        return WorkspaceListResponse(
            workspaces=[_to_workspace_response(workspace) for workspace in workspaces],
            total_count=len(workspaces),
        )

    @router.get(f"{_WORKSPACES_BASE}/{{workspace_id}}", tags=[_TAG])
    def get_workspace(
        workspace_id: str,
        session: SessionDep,
        user_details: UserDep,
    ) -> WorkspaceResponse:
        _authenticated_user(user_details)
        return _to_workspace_response(
            project_service.get_workspace(session=session, workspace_id=workspace_id)
        )

    @router.post(
        f"{_WORKSPACES_BASE}/",
        status_code=status.HTTP_201_CREATED,
        tags=[_TAG],
        dependencies=write_dependencies,
        description=_ADMIN_WORKSPACE_POLICY,
    )
    def create_workspace(
        request: CreateWorkspaceRequest,
        session: SessionDep,
        user_details: UserDep,
    ) -> WorkspaceResponse:
        """Create a workspace at a server-minted id. Admin only.

        Not idempotent: `name` is not unique, so this twice makes two workspaces.
        """
        created_by = _authenticated_user(
            user_details, required_permissions=("write", "admin")
        )
        workspace = project_service.create_workspace(
            session=session,
            name=request.name,
            description=request.description,
            is_active=request.is_active,
            data=request.data,
            created_by=created_by,
        )
        return _to_workspace_response(workspace)

    @router.patch(
        f"{_WORKSPACES_BASE}/{{workspace_id}}",
        tags=[_TAG],
        dependencies=write_dependencies,
        description=_ADMIN_WORKSPACE_POLICY,
    )
    def update_workspace(
        request: UpdateWorkspaceRequest,
        workspace_id: str,
        session: SessionDep,
        user_details: UserDep,
    ) -> WorkspaceResponse:
        """Edit `name`, `description`, `is_active` or `data`. Admin only.

        `is_active: false` retires the workspace: it still serves the projects already in it but
        refuses new ones, and is the only way to take one out of use while it holds any.
        """
        _authenticated_user(user_details, required_permissions=("write", "admin"))
        workspace = project_service.update_workspace(
            session=session,
            workspace_id=workspace_id,
            updates=request.model_dump(exclude_unset=True),
        )
        return _to_workspace_response(workspace)

    @router.delete(
        f"{_WORKSPACES_BASE}/{{workspace_id}}",
        status_code=status.HTTP_204_NO_CONTENT,
        tags=[_TAG],
        dependencies=write_dependencies,
        description=_ADMIN_WORKSPACE_POLICY,
    )
    def delete_workspace(
        workspace_id: str,
        session: SessionDep,
        user_details: UserDep,
    ) -> None:
        """Hard delete, refused with a 409 while the workspace still holds projects. Admin only.

        Never a cascade. To take one out of use instead, PATCH `is_active: false`.
        """
        _authenticated_user(user_details, required_permissions=("write", "admin"))
        project_service.delete_workspace(session=session, workspace_id=workspace_id)

    ### Projects

    @router.post(
        f"{_PROJECTS_BASE}/",
        status_code=status.HTTP_201_CREATED,
        tags=[_TAG],
        dependencies=write_dependencies,
        description=_PUBLIC_READ_POLICY,
    )
    def create_project(
        request: CreateProjectRequest,
        session: SessionDep,
        user_details: UserDep,
    ) -> ProjectResponse:
        created_by = _authenticated_user(user_details, required_permissions=("write",))
        project = project_service.create_project(
            session=session,
            workspace_id=request.workspace_id,
            name=request.name,
            description=request.description,
            data=request.data,
            created_by=created_by,
            origin=request.origin,
        )
        # A new project has no resources, so `{}` without a query.
        return _to_project_response(project=project, resource_counts={})

    @router.get(f"{_PROJECTS_BASE}/", tags=[_TAG], description=_PUBLIC_READ_POLICY)
    def list_projects(
        session: SessionDep,
        user_details: UserDep,
        page_size: int = fastapi.Query(default=20, ge=1, le=100),
        page_token: str | None = fastapi.Query(default=None),
        created_by: str | None = fastapi.Query(default=None),
        workspace_id: str | None = fastapi.Query(default=None),
    ) -> ProjectListResponse:
        """Every project, most recently updated first.

        Unfiltered by default -- there is no implicit created-by-me scope. `created_by` and
        `workspace_id` narrow it and compose with AND.
        """
        _authenticated_user(user_details)
        # `is not None`, not truthiness: `?page_token=` present and empty is a malformed token.
        cursor = (
            _decode_cursor(page_token=page_token, timestamp_field="updated_at")
            if page_token is not None
            else None
        )
        page = project_service.list_projects(
            session=session,
            page_size=page_size,
            cursor=cursor,
            created_by=created_by,
            workspace_id=workspace_id,
        )
        counts = project_service.count_resources(
            session=session,
            project_ids=[project.id for project in page.rows],
        )
        return ProjectListResponse(
            projects=[
                _to_project_summary(
                    project=project, resource_counts=counts.get(project.id, {})
                )
                for project in page.rows
            ],
            total_count=page.total_count,
            next_page_token=(
                _encode_cursor(
                    timestamp=page.rows[-1].updated_at, row_id=page.rows[-1].id
                )
                if page.has_more
                else None
            ),
        )

    @router.get(
        f"{_PROJECTS_BASE}/{{project_id}}",
        tags=[_TAG],
        description=_PUBLIC_READ_POLICY,
    )
    def get_project(
        project_id: str,
        session: SessionDep,
        user_details: UserDep,
    ) -> ProjectResponse:
        _authenticated_user(user_details)
        project = project_service.get_project(session=session, project_id=project_id)
        return _to_project_response(
            project=project,
            resource_counts=_counts_for(session=session, project_id=project.id),
        )

    @router.patch(
        f"{_PROJECTS_BASE}/{{project_id}}",
        tags=[_TAG],
        dependencies=write_dependencies,
        description=_PUBLIC_READ_POLICY,
    )
    def update_project(
        request: UpdateProjectRequest,
        project_id: str,
        session: SessionDep,
        user_details: UserDep,
    ) -> ProjectResponse:
        """Edit `name`, `description` or `data`.

        `workspace_id` is rejected by `extra="forbid"` before this body runs, and by the service
        after it -- a project's workspace is fixed at creation.
        """
        _authenticated_user(user_details, required_permissions=("write",))
        project = project_service.update_project(
            session=session,
            project_id=project_id,
            updates=request.model_dump(exclude_unset=True),
        )
        return _to_project_response(
            project=project,
            resource_counts=_counts_for(session=session, project_id=project.id),
        )

    @router.delete(
        f"{_PROJECTS_BASE}/{{project_id}}",
        tags=[_TAG],
        dependencies=write_dependencies,
    )
    def delete_project(
        project_id: str,
        session: SessionDep,
        user_details: UserDep,
    ) -> DeleteProjectResponse:
        """Hard delete. The project and every resource on it are gone, with no undo."""
        _authenticated_user(user_details, required_permissions=("write",))
        counts = project_service.delete_project(session=session, project_id=project_id)
        return DeleteProjectResponse(
            id=project_id,
            deleted_resource_counts=counts,
            deleted_resource_total=sum(counts.values()),
        )

    @router.get(
        f"{_PROJECTS_BASE}/{{project_id}}/runs",
        tags=[_TAG],
        description=_PUBLIC_READ_POLICY,
    )
    def list_project_runs(
        project_id: str,
        session: SessionDep,
        user_details: UserDep,
        page_token: str | None = fastapi.Query(default=None),
        since: datetime.datetime | None = fastapi.Query(default=None),
        until: datetime.datetime | None = fastapi.Query(default=None),
    ) -> ProjectRunListResponse:
        """The runs submitted for this project, newest first.

        Both bounds compare against the run's `created_at` and must carry a timezone offset.
        `since` defaults to 30 days before `until`, or before now -- a default rather than a cap.
        No `page_size`: the page and `next_page_token` are the run service's, passed back
        unchanged.
        """
        _authenticated_user(user_details)
        page = project_service.list_project_runs(
            session=session,
            project_id=project_id,
            page_token=page_token,
            since=since,
            until=until,
        )
        return ProjectRunListResponse(
            runs=[
                ProjectRunResponse(
                    id=run.id,
                    root_execution_id=run.root_execution_id,
                    annotations=run.annotations,
                    created_by=run.created_by,
                    created_at=run.created_at,
                    pipeline_name=run.pipeline_name,
                )
                for run in page.pipeline_runs
            ],
            next_page_token=page.next_page_token,
        )

    ### Project resources

    @router.post(
        f"{_PROJECTS_BASE}/{{project_id}}/resources/",
        status_code=status.HTTP_201_CREATED,
        tags=[_TAG],
        dependencies=write_dependencies,
        description=_RESOURCE_POLICY,
    )
    def create_resource(
        request: CreateResourceRequest,
        project_id: str,
        session: SessionDep,
        user_details: UserDep,
    ) -> ProjectResourceResponse:
        """Attach a resource to the project named in the path."""
        created_by = _authenticated_user(user_details, required_permissions=("write",))
        resource = project_service.create_resource(
            session=session,
            project_id=project_id,
            entity=request.entity,
            name=request.name,
            entity_id=request.entity_id,
            payload=request.payload,
            data=request.data,
            created_by=created_by,
        )
        return _to_resource_response(resource)

    @router.get(
        f"{_PROJECTS_BASE}/{{project_id}}/resources/",
        tags=[_TAG],
        description=_RESOURCE_POLICY,
    )
    def list_resources(
        project_id: str,
        session: SessionDep,
        user_details: UserDep,
        page_size: int = fastapi.Query(default=20, ge=1, le=100),
        page_token: str | None = fastapi.Query(default=None),
        entity: list[db_models.ProjectResourceEntity] | None = fastapi.Query(
            default=None
        ),
    ) -> ProjectResourceListResponse:
        """A page of the project's resources, newest first.

        `entity` is repeatable: `?entity=document&entity=pipeline` matches either.
        """
        _authenticated_user(user_details)
        # `is not None`, as in `list_projects`.
        cursor = (
            _decode_cursor(page_token=page_token, timestamp_field="created_at")
            if page_token is not None
            else None
        )
        page = project_service.list_resources(
            session=session,
            project_id=project_id,
            page_size=page_size,
            cursor=cursor,
            entities=entity,
        )
        return ProjectResourceListResponse(
            resources=[_to_resource_summary(resource) for resource in page.rows],
            total_count=page.total_count,
            next_page_token=(
                _encode_cursor(
                    timestamp=page.rows[-1].created_at, row_id=page.rows[-1].id
                )
                if page.has_more
                else None
            ),
        )

    @router.get(
        f"{_PROJECTS_BASE}/{{project_id}}/resources/{{resource_id}}",
        tags=[_TAG],
        description=_RESOURCE_POLICY,
    )
    def get_resource(
        project_id: str,
        resource_id: str,
        session: SessionDep,
        user_details: UserDep,
    ) -> ProjectResourceResponse:
        _authenticated_user(user_details)
        return _to_resource_response(
            project_service.get_resource(
                session=session, project_id=project_id, resource_id=resource_id
            )
        )

    @router.patch(
        f"{_PROJECTS_BASE}/{{project_id}}/resources/{{resource_id}}",
        tags=[_TAG],
        dependencies=write_dependencies,
        description=_RESOURCE_POLICY,
    )
    def update_resource(
        request: UpdateResourceRequest,
        project_id: str,
        resource_id: str,
        session: SessionDep,
        user_details: UserDep,
    ) -> ProjectResourceResponse:
        _authenticated_user(user_details, required_permissions=("write",))
        return _to_resource_response(
            project_service.update_resource(
                session=session,
                project_id=project_id,
                resource_id=resource_id,
                updates=request.model_dump(exclude_unset=True),
            )
        )

    @router.delete(
        f"{_PROJECTS_BASE}/{{project_id}}/resources/{{resource_id}}",
        status_code=status.HTTP_204_NO_CONTENT,
        tags=[_TAG],
        dependencies=write_dependencies,
    )
    def delete_resource(
        project_id: str,
        resource_id: str,
        session: SessionDep,
        user_details: UserDep,
    ) -> None:
        """Hard delete. 204 rather than a body: a resource takes nothing with it."""
        _authenticated_user(user_details, required_permissions=("write",))
        project_service.delete_resource(
            session=session, project_id=project_id, resource_id=resource_id
        )

    app.include_router(router)
