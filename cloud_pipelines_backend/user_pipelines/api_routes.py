"""HTTP routes for versioned user pipelines.

Route inventory:
- PUT /api/users/me/pipelines — create or replace the caller's pipeline definition by file path.
- PATCH /api/users/me/pipelines/{pipeline_id}/properties — update properties of the caller's pipeline.
- DELETE /api/users/me/pipelines — delete the caller's pipeline by file path.
- GET /api/users/me/pipelines — read the caller's pipeline by file path.
- GET /api/users/me/pipelines/all — list with pagination and optional file-path prefix.
- GET /api/pipelines — read a public pipeline by the user/file alternate key.
- GET /api/pipelines/search — search current saved definitions across owners.
- POST /api/pipelines/search — search with filters and continuation tokens in a JSON body.
- GET /api/pipelines/{pipeline_id} — read a public pipeline by stable UUID.
- GET /api/pipelines/{pipeline_id}/versions — list versions by stable UUID.
- POST /api/pipeline_runs/from_pipeline/{pipeline_id} — run a saved version by UUID.
- POST /api/pipeline_runs/from_pipeline — run a saved version by user/file key.
"""

import collections.abc
import datetime
import uuid
from typing import Any, Final, Literal

import fastapi
import pydantic
from cloud_pipelines_backend import (
    api_router,
    api_server_sql,
    component_structures,
)
from cloud_pipelines_backend import errors as backend_errors
from cloud_pipelines_backend.user_pipelines import db_models, errors, services
from cloud_pipelines_backend.user_pipelines.search import (
    service as pipeline_search,
)
from sqlalchemy import orm
from starlette import status

_TAG: Final[str] = "user_pipelines"
_CURRENT_USER_BASE: Final[str] = "/api/users/me/pipelines"
_ALL_PIPELINES_BASE: Final[str] = "/api/pipelines"
_PIPELINE_RUNS_FROM_PIPELINE_BASE: Final[str] = "/api/pipeline_runs/from_pipeline"
_CURSOR_SEPARATOR: Final[str] = "~"
_PUBLIC_READ_POLICY: Final[str] = (
    "Saved pipeline definitions and annotations are public to authenticated readers "
    "with read permission. Secret values must be stored in "
    "the secrets service and referenced by the pipeline; this resource is not a "
    "secret store."
)
_SAVED_PIPELINE_RUN_POLICY: Final[str] = (
    "Creates a run from a resolved saved-pipeline definition. An omitted version "
    "snapshots current content; an explicit digest selects the pointed current "
    "content or immutable history. The server adds authoritative provenance for "
    "the stable pipeline UUID, exact content digest, owner, and stored file path; "
    "clients cannot override it. To file the run under a project, send a run annotation "
    "whose *key* is `tangleml.com/project/id/<project-id>` (the project's id in the key "
    'itself) and whose value is the marker `"true"`. It is not inherited from the '
    "pipeline's stored annotations. "
    "`POST /api/pipeline_runs/` takes the same annotation, but not identically: only this "
    "route validates it, so here a key with no id after the prefix is a 422, more than one "
    "project key is a 422, and the value is normalized to the marker. On the generic route "
    "the key is stored exactly as sent."
)


class PipelineWriteRequest(pydantic.BaseModel):
    root_pipeline_task: dict[str, Any]
    pipeline_run_annotations: dict[str, str] | None = None
    versioning_mode: db_models.PipelineVersioningMode | None = None


class PipelinePropertyPatchRequest(pydantic.BaseModel):
    # Reject unsupported PATCH properties instead of silently ignoring them.
    model_config = pydantic.ConfigDict(extra="forbid")

    versioning_mode: db_models.PipelineVersioningMode


class PipelineSearchRequest(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    filter_query: str | None = None
    page_size: int = pydantic.Field(default=25, ge=1, le=100)
    page_token: str | None = None
    sort_field: pipeline_search.SortField | None = None
    sort_direction: pipeline_search.SortDirection | None = None


class SavedPipelineRunRequest(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    run_arguments: dict[str, component_structures.ArgumentType] | None = None
    # Carries the project too, under a `pipeline_run_annotations.project_run_key`. Not a typed
    # `project_id` field: `POST /api/pipeline_runs/` has only an annotations map, and one intent
    # travelling two ways would be worse than one mechanism on both routes.
    pipeline_run_annotations: dict[str, str] | None = None


class PipelineResponse(pydantic.BaseModel):
    id: uuid.UUID
    user_id: str
    file_path: str
    pipeline_name: str | None = None
    created_at: datetime.datetime
    updated_at: datetime.datetime
    version: str
    current_version: str
    versioning_mode: db_models.PipelineVersioningMode
    version_created_at: datetime.datetime
    root_pipeline_task: dict[str, Any]
    pipeline_run_annotations: dict[str, str]


class PipelineWriteResponse(PipelineResponse):
    message: str
    updated: bool
    reused_version: bool


class PipelineSummaryResponse(pydantic.BaseModel):
    id: uuid.UUID
    user_id: str
    file_path: str
    pipeline_name: str | None = None
    created_at: datetime.datetime
    updated_at: datetime.datetime
    current_version: str
    versioning_mode: db_models.PipelineVersioningMode


class PipelineListResponse(pydantic.BaseModel):
    pipelines: list[PipelineSummaryResponse]
    total_count: int
    next_page_token: str | None = None


class PipelineVersionResponse(pydantic.BaseModel):
    version: str
    created_at: datetime.datetime
    is_current: bool


class PipelineVersionListResponse(pydantic.BaseModel):
    id: uuid.UUID
    user_id: str
    file_path: str
    current_version: str
    versioning_mode: db_models.PipelineVersioningMode
    versions: list[PipelineVersionResponse]
    total_count: int
    next_page_token: str | None = None


def _encode_cursor(*, updated_at: datetime.datetime, pipeline_id: str) -> str:
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=datetime.timezone.utc)
    return f"{updated_at.isoformat()}{_CURSOR_SEPARATOR}{pipeline_id}"


def _decode_cursor(*, page_token: str) -> tuple[datetime.datetime, str]:
    try:
        updated_at_text, pipeline_id = page_token.split(_CURSOR_SEPARATOR, 1)
        updated_at = datetime.datetime.fromisoformat(updated_at_text)
        pipeline_id = services.normalize_pipeline_id(pipeline_id)
    except (TypeError, ValueError, errors.PipelineValidationError) as exc:
        raise fastapi.HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"Unrecognized page_token format: {page_token!r}. Expected 'updated_at~pipeline_id' cursor."
            ),
        ) from exc
    if updated_at.tzinfo is not None:
        updated_at = updated_at.astimezone(datetime.timezone.utc)
    return updated_at, pipeline_id


def _encode_version_cursor(*, created_at: datetime.datetime, digest: str) -> str:
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=datetime.timezone.utc)
    return f"{created_at.isoformat()}{_CURSOR_SEPARATOR}{digest}"


def _decode_version_cursor(*, page_token: str) -> tuple[datetime.datetime, str]:
    try:
        created_at_text, digest = page_token.split(_CURSOR_SEPARATOR, 1)
        created_at = datetime.datetime.fromisoformat(created_at_text)
        if len(digest) != db_models.DIGEST_LENGTH:
            raise ValueError("invalid digest length")
        int(digest, 16)
    except (TypeError, ValueError) as exc:
        raise fastapi.HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"Unrecognized page_token format: {page_token!r}. Expected 'created_at~digest' cursor."
            ),
        ) from exc
    if created_at.tzinfo is not None:
        created_at = created_at.astimezone(datetime.timezone.utc)
    return created_at, digest


def _pipeline_name_from_extra_data(
    extra_data: dict[str, Any] | None,
) -> str | None:
    return (extra_data or {}).get("pipeline_name")


def _pipeline_name(version: db_models.UserPipelineVersion) -> str | None:
    return _pipeline_name_from_extra_data(version.extra_data)


def _pipeline_api_id(pipeline: db_models.UserPipeline) -> uuid.UUID:
    """Convert the portable String(36) persistence ID to the API's UUID type."""
    return uuid.UUID(pipeline.id)


def _to_pipeline_response(
    *,
    pipeline: db_models.UserPipeline,
    version: db_models.UserPipelineVersion,
    current_version: db_models.UserPipelineVersion,
) -> PipelineResponse:
    return PipelineResponse(
        id=_pipeline_api_id(pipeline),
        user_id=pipeline.user_id,
        file_path=pipeline.file_path,
        pipeline_name=_pipeline_name(version),
        created_at=pipeline.created_at,
        updated_at=pipeline.updated_at,
        version=version.content_digest,
        current_version=current_version.content_digest,
        versioning_mode=pipeline.versioning_mode,
        version_created_at=version.created_at,
        root_pipeline_task=version.root_pipeline_task,
        pipeline_run_annotations=version.pipeline_run_annotations,
    )


def setup_user_pipeline_routes(
    *,
    app: fastapi.FastAPI,
    get_session: (
        collections.abc.Callable[..., orm.Session]
        | collections.abc.Callable[..., collections.abc.Iterator[orm.Session]]
    ),
    user_details_getter: collections.abc.Callable[..., api_router.UserDetails],
    service: services.UserPipelineService | None = None,
) -> None:
    errors.register_pipeline_exception_handlers(app=app)
    router = fastapi.APIRouter()
    if service is None:
        service = services.UserPipelineService()

    def _authenticated_user_id(
        user_details: api_router.UserDetails,
        *,
        required_permissions: tuple[Literal["read", "write"], ...] = ("read",),
    ) -> str:
        if not user_details.name:
            raise fastapi.HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication required",
            )
        for required_permission in required_permissions:
            if not user_details.permissions.get(required_permission):
                raise fastapi.HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=(
                        f"User {user_details.name} does not have {required_permission} permission"
                    ),
                )
        return user_details.name

    def _create_pipeline_run(
        *,
        request: SavedPipelineRunRequest,
        session: orm.Session,
        pipeline_id: str | None,
        user_id: str | None,
        file_path: str | None,
        version: str | None,
        created_by: str,
    ) -> api_server_sql.PipelineRunResponse:
        """Resolve a saved version; the service injects authoritative provenance."""
        try:
            return service.create_from_pipeline(
                session=session,
                pipeline_id=pipeline_id,
                user_id=user_id,
                file_path=file_path,
                version=version,
                run_arguments=request.run_arguments,
                pipeline_run_annotations=request.pipeline_run_annotations,
                created_by=created_by,
            )
        except (
            backend_errors.ApiValidationError,
            api_server_sql.ApiServiceError,
        ) as exc:
            raise fastapi.HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail=str(exc),
            ) from exc

    def _get(
        *,
        session: orm.Session,
        pipeline_id: str | None,
        user_id: str | None,
        file_path: str | None,
        version: str | None,
    ) -> PipelineResponse:
        pipeline, version_row, current_row = service.get_pipeline_and_version(
            session=session,
            pipeline_id=pipeline_id,
            user_id=user_id,
            file_path=file_path,
            version=version,
        )
        return _to_pipeline_response(
            pipeline=pipeline,
            version=version_row,
            current_version=current_row,
        )

    def _list(
        *,
        session: orm.Session,
        user_id: str,
        page_size: int,
        page_token: str | None,
        file_path: str | None,
    ) -> PipelineListResponse:
        cursor = _decode_cursor(page_token=page_token) if page_token else None
        rows, total_count, has_more = service.list_pipelines(
            session=session,
            user_id=user_id,
            page_size=page_size,
            cursor=cursor,
            file_path_prefix=file_path,
        )
        return PipelineListResponse(
            pipelines=[
                PipelineSummaryResponse(
                    id=_pipeline_api_id(pipeline),
                    user_id=pipeline.user_id,
                    file_path=pipeline.file_path,
                    pipeline_name=_pipeline_name_from_extra_data(version_extra_data),
                    created_at=pipeline.created_at,
                    updated_at=pipeline.updated_at,
                    current_version=version_digest,
                    versioning_mode=pipeline.versioning_mode,
                )
                for pipeline, version_digest, version_extra_data in rows
            ],
            total_count=total_count,
            next_page_token=(
                _encode_cursor(
                    updated_at=rows[-1][0].updated_at,
                    pipeline_id=rows[-1][0].id,
                )
                if has_more
                else None
            ),
        )

    def _list_versions(
        *,
        session: orm.Session,
        pipeline_id: str | None,
        user_id: str | None,
        file_path: str | None,
        page_size: int,
        page_token: str | None,
    ) -> PipelineVersionListResponse:
        cursor = _decode_version_cursor(page_token=page_token) if page_token else None
        (
            pipeline,
            current_content_digest,
            versions,
            total_count,
            has_more,
        ) = service.list_versions(
            session=session,
            pipeline_id=pipeline_id,
            user_id=user_id,
            file_path=file_path,
            page_size=page_size,
            cursor=cursor,
        )
        return PipelineVersionListResponse(
            id=_pipeline_api_id(pipeline),
            user_id=pipeline.user_id,
            file_path=pipeline.file_path,
            current_version=current_content_digest,
            versioning_mode=pipeline.versioning_mode,
            versions=[
                PipelineVersionResponse(
                    version=digest,
                    created_at=created_at,
                    is_current=(
                        pipeline.versioning_mode
                        is db_models.PipelineVersioningMode.FULL
                        and digest == current_content_digest
                    ),
                )
                for digest, created_at in versions
            ],
            total_count=total_count,
            next_page_token=(
                _encode_version_cursor(
                    created_at=versions[-1][1],
                    digest=versions[-1][0],
                )
                if has_more
                else None
            ),
        )

    @router.put(_CURRENT_USER_BASE, tags=[_TAG])
    def set_pipeline(
        request: PipelineWriteRequest,
        file_path: str = fastapi.Query(),
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> PipelineWriteResponse:
        user_id = _authenticated_user_id(
            user_details,
            required_permissions=("write",),
        )
        result = service.set_pipeline(
            session=session,
            user_id=user_id,
            file_path=file_path,
            root_pipeline_task=request.root_pipeline_task,
            pipeline_run_annotations=request.pipeline_run_annotations,
            versioning_mode=request.versioning_mode,
        )
        response = _to_pipeline_response(
            pipeline=result.pipeline,
            version=result.version,
            current_version=result.version,
        )
        return PipelineWriteResponse(
            **response.model_dump(),
            message=result.message,
            updated=result.updated,
            reused_version=result.reused_version,
        )

    @router.patch(
        f"{_CURRENT_USER_BASE}/{{pipeline_id}}/properties",
        tags=[_TAG],
    )
    def patch_pipeline_properties(
        request: PipelinePropertyPatchRequest,
        pipeline_id: uuid.UUID = fastapi.Path(),
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> PipelineWriteResponse:
        user_id = _authenticated_user_id(
            user_details,
            required_permissions=("write",),
        )
        result = service.patch_pipeline_properties(
            session=session,
            user_id=user_id,
            pipeline_id=str(pipeline_id),
            versioning_mode=request.versioning_mode,
        )
        response = _to_pipeline_response(
            pipeline=result.pipeline,
            version=result.version,
            current_version=result.version,
        )
        return PipelineWriteResponse(
            **response.model_dump(),
            message=result.message,
            updated=result.updated,
            reused_version=result.reused_version,
        )

    def _search(
        *,
        request: PipelineSearchRequest,
        session: orm.Session,
        user_details: api_router.UserDetails,
    ) -> PipelineListResponse:
        current_user = _authenticated_user_id(user_details)
        rows, total_count, next_page_token = pipeline_search.search_pipelines(
            session=session,
            current_user=current_user,
            filter_query=request.filter_query,
            page_size=request.page_size,
            page_token=request.page_token,
            sort_field=request.sort_field,
            sort_direction=request.sort_direction,
        )
        return PipelineListResponse(
            pipelines=[PipelineSummaryResponse(**row) for row in rows],
            total_count=total_count,
            next_page_token=next_page_token,
        )

    # Keep filters and full name boundaries out of URL length limits.
    @router.post(
        f"{_ALL_PIPELINES_BASE}/search",
        tags=[_TAG],
        description=(
            _PUBLIC_READ_POLICY
            + " Searches active pipelines' current versions, newest update first by default. "
            "Use this method for pagination: filters and continuation tokens stay in the JSON body. "
            "Tokens from either search method can be continued here. Requires read permission only."
        ),
    )
    def search_pipelines_with_body(
        request: PipelineSearchRequest,
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> PipelineListResponse:
        return _search(request=request, session=session, user_details=user_details)

    # Register this static path before the UUID detail route below.
    @router.get(
        f"{_ALL_PIPELINES_BASE}/search",
        tags=[_TAG],
        description=(
            _PUBLIC_READ_POLICY
            + " Searches active pipelines' current versions, newest update first by default. "
            "For pagination, prefer POST on this path with page_token in its JSON body; "
            "names and filters can make tokens exceed URL length limits."
        ),
    )
    def search_pipelines(
        filter_query: str | None = fastapi.Query(default=None),
        page_size: int = fastapi.Query(default=25, ge=1, le=100),
        page_token: str | None = fastapi.Query(default=None),
        sort_field: pipeline_search.SortField | None = fastapi.Query(
            default=None,
            description="Sort by name or updated_at; defaults to updated_at, or the token's sort field.",
        ),
        sort_direction: pipeline_search.SortDirection | None = fastapi.Query(
            default=None,
            description="Sort asc or desc; defaults to desc, or the token's direction.",
        ),
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> PipelineListResponse:
        return _search(
            request=PipelineSearchRequest(
                filter_query=filter_query,
                page_size=page_size,
                page_token=page_token,
                sort_field=sort_field,
                sort_direction=sort_direction,
            ),
            session=session,
            user_details=user_details,
        )

    @router.get(f"{_CURRENT_USER_BASE}/all", tags=[_TAG])
    def list_current_user_pipelines(
        page_size: int = fastapi.Query(default=10, ge=1, le=100),
        page_token: str | None = fastapi.Query(default=None),
        file_path: str | None = fastapi.Query(default=None),
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> PipelineListResponse:
        return _list(
            session=session,
            user_id=_authenticated_user_id(user_details),
            page_size=page_size,
            page_token=page_token,
            file_path=file_path,
        )

    @router.get(
        f"{_ALL_PIPELINES_BASE}/{{pipeline_id}}/versions",
        tags=[_TAG],
        description=_PUBLIC_READ_POLICY,
    )
    def list_pipeline_versions_by_id(
        pipeline_id: uuid.UUID = fastapi.Path(),
        page_size: int = fastapi.Query(default=10, ge=1, le=100),
        page_token: str | None = fastapi.Query(default=None),
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> PipelineVersionListResponse:
        _authenticated_user_id(user_details)
        return _list_versions(
            session=session,
            pipeline_id=str(pipeline_id),
            user_id=None,
            file_path=None,
            page_size=page_size,
            page_token=page_token,
        )

    @router.get(_CURRENT_USER_BASE, tags=[_TAG])
    def get_current_user_pipeline(
        file_path: str = fastapi.Query(),
        version: str | None = fastapi.Query(default=None),
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> PipelineResponse:
        return _get(
            session=session,
            pipeline_id=None,
            user_id=_authenticated_user_id(user_details),
            file_path=file_path,
            version=version,
        )

    @router.get(
        _ALL_PIPELINES_BASE,
        tags=[_TAG],
        description=_PUBLIC_READ_POLICY,
    )
    def get_pipeline_by_alternate_key(
        user_id: str = fastapi.Query(),
        file_path: str = fastapi.Query(),
        version: str | None = fastapi.Query(default=None),
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> PipelineResponse:
        _authenticated_user_id(user_details)
        return _get(
            session=session,
            pipeline_id=None,
            user_id=user_id,
            file_path=file_path,
            version=version,
        )

    @router.get(
        f"{_ALL_PIPELINES_BASE}/{{pipeline_id}}",
        tags=[_TAG],
        description=_PUBLIC_READ_POLICY,
    )
    def get_pipeline_by_id(
        pipeline_id: uuid.UUID = fastapi.Path(),
        version: str | None = fastapi.Query(default=None),
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> PipelineResponse:
        _authenticated_user_id(user_details)
        return _get(
            session=session,
            pipeline_id=str(pipeline_id),
            user_id=None,
            file_path=None,
            version=version,
        )

    @router.post(
        f"{_PIPELINE_RUNS_FROM_PIPELINE_BASE}/{{pipeline_id}}",
        tags=["pipelineRuns"],
        description=_SAVED_PIPELINE_RUN_POLICY,
    )
    def create_pipeline_run_by_id(
        request: SavedPipelineRunRequest,
        pipeline_id: uuid.UUID = fastapi.Path(),
        version: str | None = fastapi.Query(default=None),
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> api_server_sql.PipelineRunResponse:
        return _create_pipeline_run(
            request=request,
            session=session,
            pipeline_id=str(pipeline_id),
            user_id=None,
            file_path=None,
            version=version,
            created_by=_authenticated_user_id(
                user_details,
                required_permissions=("read", "write"),
            ),
        )

    @router.post(
        _PIPELINE_RUNS_FROM_PIPELINE_BASE,
        tags=["pipelineRuns"],
        description=_SAVED_PIPELINE_RUN_POLICY,
    )
    def create_pipeline_run_by_alternate_key(
        request: SavedPipelineRunRequest,
        user_id: str = fastapi.Query(),
        file_path: str = fastapi.Query(),
        version: str | None = fastapi.Query(default=None),
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> api_server_sql.PipelineRunResponse:
        return _create_pipeline_run(
            request=request,
            session=session,
            pipeline_id=None,
            user_id=user_id,
            file_path=file_path,
            version=version,
            created_by=_authenticated_user_id(
                user_details,
                required_permissions=("read", "write"),
            ),
        )

    @router.delete(
        _CURRENT_USER_BASE,
        status_code=status.HTTP_204_NO_CONTENT,
        tags=[_TAG],
    )
    def delete_pipeline(
        file_path: str = fastapi.Query(),
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> None:
        service.delete_pipeline(
            session=session,
            user_id=_authenticated_user_id(
                user_details,
                required_permissions=("write",),
            ),
            file_path=file_path,
        )

    app.include_router(router)
