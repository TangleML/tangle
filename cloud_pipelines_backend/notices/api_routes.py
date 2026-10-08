"""Notice HTTP routes with injected sessions and user authentication.

Admin operations use the same permission and read-only checks as the core API.
Responses retain nullable fields for the notice contract.
"""

import collections.abc
import typing

import fastapi
from cloud_pipelines_backend import api_router
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.notices import service
from sqlalchemy import orm


def setup_notice_routes(
    *,
    app: fastapi.FastAPI,
    get_session: (
        collections.abc.Callable[..., orm.Session]
        | collections.abc.Callable[..., collections.abc.Iterator[orm.Session]]
    ),
    user_details_getter: collections.abc.Callable[..., api_router.UserDetails],
    is_read_only: bool = False,
) -> None:
    get_user_details_dependency = fastapi.Depends(user_details_getter)

    def get_user_name(
        user_details: typing.Annotated[
            api_router.UserDetails, get_user_details_dependency
        ],
    ) -> str | None:
        return user_details.name

    get_user_name_dependency = fastapi.Depends(get_user_name)

    def ensure_admin_user(
        user_details: typing.Annotated[
            api_router.UserDetails, get_user_details_dependency
        ],
    ):
        if not user_details.permissions.get("admin"):
            raise fastapi.HTTPException(
                status_code=fastapi.status.HTTP_403_FORBIDDEN,
                detail=f"User {user_details.name} is not an admin user",
            )

    ensure_admin_user_dependency = fastapi.Depends(ensure_admin_user)

    def check_not_readonly():
        if is_read_only:
            raise fastapi.HTTPException(
                status_code=503, detail="The server is in read-only mode."
            )

    SessionDep = typing.Annotated[orm.Session, fastapi.Depends(get_session)]

    router = fastapi.APIRouter()

    ### Notice routes

    notice_service = service.NoticesApiService_Sql()

    @router.get("/api/notices/active", tags=["notices"])
    def get_active_notices(
        session: SessionDep,
        response: fastapi.Response,
    ) -> service.ListNoticesResponse:
        # A notice can start or expire at any moment, so responses must not be cached.
        response.headers["Cache-Control"] = "no-store"
        return notice_service.list_active(session=session)

    admin_notice_write_dependencies = [
        ensure_admin_user_dependency,
        fastapi.Depends(check_not_readonly),
    ]

    @router.get(
        "/api/admin/notices",
        tags=["notices"],
        dependencies=[ensure_admin_user_dependency],
    )
    def admin_list_notices(
        session: SessionDep,
        include_deleted: bool = False,
    ) -> service.ListAdminNoticesResponse:
        return notice_service.list_all(session=session, include_deleted=include_deleted)

    @router.post(
        "/api/admin/notices",
        tags=["notices"],
        dependencies=admin_notice_write_dependencies,
    )
    def admin_create_notice(
        session: SessionDep,
        notice: service.CreateNoticeRequest,
        user_name: typing.Annotated[str | None, get_user_name_dependency],
    ) -> service.AdminNoticeResponse:
        return notice_service.create(
            session=session, notice=notice, user_name=user_name
        )

    @router.get(
        "/api/admin/notices/{id}",
        tags=["notices"],
        dependencies=[ensure_admin_user_dependency],
    )
    def admin_get_notice(
        session: SessionDep,
        id: bts.IdType,
    ) -> service.AdminNoticeResponse:
        return notice_service.get(session=session, id=id)

    @router.patch(
        "/api/admin/notices/{id}",
        tags=["notices"],
        dependencies=admin_notice_write_dependencies,
    )
    def admin_update_notice(
        session: SessionDep,
        id: bts.IdType,
        notice: service.UpdateNoticeRequest,
        user_name: typing.Annotated[str | None, get_user_name_dependency],
    ) -> service.AdminNoticeResponse:
        return notice_service.update(
            session=session, id=id, notice=notice, user_name=user_name
        )

    @router.delete(
        "/api/admin/notices/{id}",
        tags=["notices"],
        dependencies=admin_notice_write_dependencies,
    )
    def admin_delete_notice(
        session: SessionDep,
        id: bts.IdType,
        user_name: typing.Annotated[str | None, get_user_name_dependency],
    ) -> service.AdminNoticeResponse:
        return notice_service.delete(session=session, id=id, user_name=user_name)

    app.include_router(router)
