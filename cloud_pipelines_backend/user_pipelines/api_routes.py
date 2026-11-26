import collections.abc
import datetime
import uuid
from typing import Any, Final

import fastapi
import pydantic
from sqlalchemy import orm
from starlette import status

from .. import api_router, api_server_sql, component_structures
from .. import errors as backend_errors
from ..access_control import checks as access_control_checks
from . import db_models, errors, services
from . import search as pipeline_search

_TAG: Final[str] = "user_pipelines"
_MY_PIPELINES: Final[str] = "/api/users/me/pipelines"
_PIPELINES: Final[str] = "/api/pipelines"
_CURSOR_SEPARATOR: Final[str] = "~"


class PipelineWriteRequest(pydantic.BaseModel):
    root_pipeline_task: dict[str, Any]
    pipeline_run_annotations: dict[str, str] | None = None
    versioning_mode: db_models.PipelineVersioningMode | None = None


class PipelinePropertyPatchRequest(pydantic.BaseModel):
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
    pipeline_run_annotations: dict[str, str] | None = None


class PipelineSummaryResponse(pydantic.BaseModel):
    id: uuid.UUID
    user_id: str
    file_path: str
    pipeline_name: str | None = None
    created_at: datetime.datetime
    updated_at: datetime.datetime
    current_version: str
    versioning_mode: db_models.PipelineVersioningMode


class PipelineResponse(PipelineSummaryResponse):
    version: str
    version_created_at: datetime.datetime
    root_pipeline_task: dict[str, Any]
    pipeline_run_annotations: dict[str, str]


class PipelineWriteResponse(PipelineResponse):
    message: str
    updated: bool
    reused_version: bool


class PipelineSearchResponse(pydantic.BaseModel):
    pipelines: list[PipelineSummaryResponse]
    next_page_token: str | None = None


class PipelineListResponse(PipelineSearchResponse):
    total_count: int


def _encode_cursor(*, updated_at: datetime.datetime, pipeline_id: str) -> str:
    """Encodes a list position as `updated_at~id`."""
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=datetime.timezone.utc)
    return f"{updated_at.isoformat()}{_CURSOR_SEPARATOR}{pipeline_id}"


def _decode_cursor(page_token: str) -> tuple[datetime.datetime, str]:
    """Decodes a list position written by `_encode_cursor`."""
    try:
        updated_at_text, pipeline_id = page_token.split(_CURSOR_SEPARATOR, 1)
        updated_at = datetime.datetime.fromisoformat(updated_at_text)
        pipeline_id = services.normalize_pipeline_id(pipeline_id)
    except (ValueError, errors.PipelineValidationError) as exc:
        raise errors.PipelineValidationError(
            f"Unrecognized page_token: {page_token!r}."
        ) from exc
    if updated_at.tzinfo is not None:
        updated_at = updated_at.astimezone(datetime.timezone.utc)
    return updated_at, pipeline_id


def _to_response(
    *,
    pipeline: db_models.UserPipeline,
    version: db_models.UserPipelineVersion,
    current: db_models.UserPipelineVersion,
) -> PipelineResponse:
    """Builds the API view of a pipeline at one version."""
    return PipelineResponse(
        id=uuid.UUID(pipeline.id),
        user_id=pipeline.user_id,
        file_path=pipeline.file_path,
        pipeline_name=(version.extra_data or {}).get("pipeline_name"),
        created_at=pipeline.created_at,
        updated_at=pipeline.updated_at,
        current_version=current.content_digest,
        versioning_mode=pipeline.versioning_mode,
        version=version.content_digest,
        version_created_at=version.created_at,
        root_pipeline_task=version.root_pipeline_task,
        pipeline_run_annotations=version.pipeline_run_annotations,
    )


def _to_write_response(result: services.PipelineWriteResult) -> PipelineWriteResponse:
    """Builds the API view of a write."""
    response = _to_response(
        pipeline=result.pipeline, version=result.version, current=result.version
    )
    return PipelineWriteResponse(
        **response.model_dump(),
        message=result.message,
        updated=result.updated,
        reused_version=result.reused_version,
    )


def _require(user_details: api_router.UserDetails, *permissions: str) -> str:
    """Returns the caller's name, or raises 401/403."""
    if not user_details.name:
        raise fastapi.HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required"
        )
    for permission in permissions:
        if not user_details.permissions.get(permission):
            raise fastapi.HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"User {user_details.name} does not have {permission} permission",
            )
    return user_details.name


def setup_user_pipeline_routes(
    *,
    app: fastapi.FastAPI,
    get_session: (
        collections.abc.Callable[..., orm.Session]
        | collections.abc.Callable[..., collections.abc.Iterator[orm.Session]]
    ),
    user_details_getter: collections.abc.Callable[..., api_router.UserDetails],
) -> None:
    """Mounts the saved-pipeline routes."""
    errors.register_pipeline_exception_handlers(app=app)
    router = fastapi.APIRouter(tags=[_TAG])
    runs_router = fastapi.APIRouter(tags=["pipelineRuns"])
    service = services.UserPipelineService()
    session_dependency = fastapi.Depends(get_session)
    user_dependency = fastapi.Depends(user_details_getter)

    def _get(
        *,
        session: orm.Session,
        pipeline_id: str | None = None,
        user_id: str | None = None,
        file_path: str | None = None,
        version: str | None,
    ) -> PipelineResponse:
        pipeline, selected, current = service.get_pipeline_and_version(
            session=session,
            pipeline_id=pipeline_id,
            user_id=user_id,
            file_path=file_path,
            version=version,
        )
        return _to_response(pipeline=pipeline, version=selected, current=current)

    def _list(
        *,
        session: orm.Session,
        user_id: str,
        page_size: int,
        page_token: str | None,
        file_path: str | None,
    ) -> PipelineListResponse:
        rows, total_count, has_more = service.list_pipelines(
            session=session,
            user_id=user_id,
            page_size=page_size,
            cursor=_decode_cursor(page_token) if page_token else None,
            file_path_prefix=file_path,
        )
        return PipelineListResponse(
            pipelines=[
                PipelineSummaryResponse(
                    id=uuid.UUID(pipeline.id),
                    user_id=pipeline.user_id,
                    file_path=pipeline.file_path,
                    pipeline_name=(extra_data or {}).get("pipeline_name"),
                    created_at=pipeline.created_at,
                    updated_at=pipeline.updated_at,
                    current_version=digest,
                    versioning_mode=pipeline.versioning_mode,
                )
                for pipeline, digest, extra_data in rows
            ],
            total_count=total_count,
            next_page_token=(
                _encode_cursor(
                    updated_at=rows[-1][0].updated_at, pipeline_id=rows[-1][0].id
                )
                if has_more
                else None
            ),
        )

    def _owner_and_file_path(
        *, session: orm.Session, pipeline_id: uuid.UUID
    ) -> tuple[str, str]:
        """The owner and file path of a live pipeline, read outside the write transaction."""
        pipeline = services.get_live_owned_pipeline(
            session=session, pipeline_id=str(pipeline_id), user_id=None
        )
        owner_and_file_path = pipeline.user_id, pipeline.file_path
        session.rollback()
        return owner_and_file_path

    @router.put(_MY_PIPELINES)
    def set_pipeline(
        request: PipelineWriteRequest,
        file_path: str = fastapi.Query(),
        session: orm.Session = session_dependency,
        user_details: api_router.UserDetails = user_dependency,
    ) -> PipelineWriteResponse:
        return _to_write_response(
            service.set_pipeline(
                session=session,
                user_id=_require(user_details, "write"),
                file_path=file_path,
                root_pipeline_task=request.root_pipeline_task,
                pipeline_run_annotations=request.pipeline_run_annotations,
                versioning_mode=request.versioning_mode,
            )
        )

    @router.get(_MY_PIPELINES)
    def get_current_user_pipeline(
        file_path: str = fastapi.Query(),
        version: str | None = fastapi.Query(default=None),
        session: orm.Session = session_dependency,
        user_details: api_router.UserDetails = user_dependency,
    ) -> PipelineResponse:
        return _get(
            session=session,
            user_id=_require(user_details),
            file_path=file_path,
            version=version,
        )

    @router.delete(_MY_PIPELINES, status_code=status.HTTP_204_NO_CONTENT)
    def delete_pipeline(
        file_path: str = fastapi.Query(),
        session: orm.Session = session_dependency,
        user_details: api_router.UserDetails = user_dependency,
    ) -> None:
        service.delete_pipeline(
            session=session,
            user_id=_require(user_details, "write"),
            file_path=file_path,
        )

    @router.patch(f"{_MY_PIPELINES}/{{pipeline_id}}/properties")
    def patch_pipeline_properties(
        request: PipelinePropertyPatchRequest,
        pipeline_id: uuid.UUID = fastapi.Path(),
        session: orm.Session = session_dependency,
        user_details: api_router.UserDetails = user_dependency,
    ) -> PipelineWriteResponse:
        return _to_write_response(
            service.patch_pipeline_properties(
                session=session,
                user_id=_require(user_details, "write"),
                pipeline_id=str(pipeline_id),
                versioning_mode=request.versioning_mode,
            )
        )

    @router.get(f"{_MY_PIPELINES}/all")
    def list_current_user_pipelines(
        page_size: int = fastapi.Query(default=10, ge=1, le=100),
        page_token: str | None = fastapi.Query(default=None),
        file_path: str | None = fastapi.Query(default=None),
        session: orm.Session = session_dependency,
        user_details: api_router.UserDetails = user_dependency,
    ) -> PipelineListResponse:
        return _list(
            session=session,
            user_id=_require(user_details),
            page_size=page_size,
            page_token=page_token,
            file_path=file_path,
        )

    @router.get("/api/users/{user_id}/pipelines/all")
    def list_user_pipelines(
        user_id: str = fastapi.Path(),
        page_size: int = fastapi.Query(default=10, ge=1, le=100),
        page_token: str | None = fastapi.Query(default=None),
        file_path: str | None = fastapi.Query(default=None),
        session: orm.Session = session_dependency,
        user_details: api_router.UserDetails = user_dependency,
    ) -> PipelineListResponse:
        _require(user_details)
        return _list(
            session=session,
            user_id=user_id,
            page_size=page_size,
            page_token=page_token,
            file_path=file_path,
        )

    @router.get(_PIPELINES)
    def get_pipeline_by_owner_and_file_path(
        user_id: str = fastapi.Query(),
        file_path: str = fastapi.Query(),
        version: str | None = fastapi.Query(default=None),
        session: orm.Session = session_dependency,
        user_details: api_router.UserDetails = user_dependency,
    ) -> PipelineResponse:
        _require(user_details)
        return _get(
            session=session, user_id=user_id, file_path=file_path, version=version
        )

    @router.post(f"{_PIPELINES}/search")
    def search_pipelines(
        request: PipelineSearchRequest,
        session: orm.Session = session_dependency,
        user_details: api_router.UserDetails = user_dependency,
    ) -> PipelineSearchResponse:
        rows, next_page_token = pipeline_search.search_pipelines(
            session=session,
            current_user=_require(user_details),
            filter_query=request.filter_query,
            page_size=request.page_size,
            page_token=request.page_token,
            sort_field=request.sort_field,
            sort_direction=request.sort_direction,
        )
        return PipelineSearchResponse(
            pipelines=[PipelineSummaryResponse(**row) for row in rows],
            next_page_token=next_page_token,
        )

    @router.get(f"{_PIPELINES}/{{pipeline_id}}")
    def get_pipeline_by_id(
        pipeline_id: uuid.UUID = fastapi.Path(),
        version: str | None = fastapi.Query(default=None),
        session: orm.Session = session_dependency,
        user_details: api_router.UserDetails = user_dependency,
    ) -> PipelineResponse:
        _require(user_details)
        return _get(session=session, pipeline_id=str(pipeline_id), version=version)

    @router.put(f"{_PIPELINES}/{{pipeline_id}}")
    def set_pipeline_by_id(
        request: PipelineWriteRequest,
        pipeline_id: uuid.UUID = fastapi.Path(),
        session: orm.Session = session_dependency,
        user_details: api_router.UserDetails = user_dependency,
    ) -> PipelineWriteResponse:
        """Saves into another user's pipeline; only its owner or an admin may change secrets."""
        caller = access_control_checks.Caller(
            email=_require(user_details, "write"),
            is_admin=bool(user_details.permissions.get("admin")),
        )
        owner, file_path = _owner_and_file_path(
            session=session, pipeline_id=pipeline_id
        )
        return _to_write_response(
            service.set_pipeline(
                session=session,
                user_id=owner,
                file_path=file_path,
                root_pipeline_task=request.root_pipeline_task,
                pipeline_run_annotations=request.pipeline_run_annotations,
                versioning_mode=request.versioning_mode,
                may_change_secrets=caller.is_admin
                or access_control_checks.is_owner(owner=owner, caller=caller),
            )
        )

    @router.delete(
        f"{_PIPELINES}/{{pipeline_id}}", status_code=status.HTTP_204_NO_CONTENT
    )
    def delete_pipeline_by_id(
        pipeline_id: uuid.UUID = fastapi.Path(),
        session: orm.Session = session_dependency,
        user_details: api_router.UserDetails = user_dependency,
    ) -> None:
        _require(user_details, "write")
        owner, file_path = _owner_and_file_path(
            session=session, pipeline_id=pipeline_id
        )
        service.delete_pipeline(session=session, user_id=owner, file_path=file_path)

    @runs_router.post("/api/pipeline_runs/from_pipeline/{pipeline_id}")
    def create_pipeline_run_by_id(
        request: SavedPipelineRunRequest,
        pipeline_id: uuid.UUID = fastapi.Path(),
        version: str | None = fastapi.Query(default=None),
        session: orm.Session = session_dependency,
        user_details: api_router.UserDetails = user_dependency,
    ) -> api_server_sql.PipelineRunResponse:
        created_by = _require(user_details, "read", "write")
        try:
            return service.create_from_pipeline(
                session=session,
                pipeline_id=str(pipeline_id),
                user_id=None,
                file_path=None,
                version=version,
                run_arguments=request.run_arguments,
                pipeline_run_annotations=request.pipeline_run_annotations,
                created_by=created_by,
            )
        except (
            backend_errors.ApiValidationError,
            api_server_sql.ApiServiceError,
        ) as exc:
            raise errors.PipelineValidationError(str(exc)) from exc

    app.include_router(router)
    app.include_router(runs_router)
