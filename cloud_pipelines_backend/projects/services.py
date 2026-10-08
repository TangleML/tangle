"""Project, workspace and project-resource writes and reads.

Every method takes the caller's session; write methods open `session.begin()` first and commit
on the way out, like `user_pipelines.services.UserPipelineService.set_pipeline`.

`workspace_id`, `entity`, `entity_id` and `project_id` are immutable. Guarded here as well as at
the HTTP edge, so a script or a test is refused too.
"""

import base64
import collections.abc
import dataclasses
import datetime
import json
from typing import Any, Final, Generic, TypeVar

import pydantic
import sqlalchemy as sql
from sqlalchemy import orm

from cloud_pipelines_backend import (
    api_server_sql,
    filter_query_models,
    filter_query_sql,
)
from cloud_pipelines_backend.projects import db_models, errors
from cloud_pipelines_backend.user_pipelines import (
    db_models as user_pipeline_db_models,
)
from cloud_pipelines_backend.user_pipelines import pipeline_run_annotations
from cloud_pipelines_backend.utils import db as db_utils

# What PATCH may touch, named here rather than inferred from the request models so a non-route
# caller is refused too.
_WORKSPACE_MUTABLE_FIELDS: Final[frozenset[str]] = frozenset(
    {"name", "description", "is_active", "data"}
)
_PROJECT_MUTABLE_FIELDS: Final[frozenset[str]] = frozenset(
    {"name", "description", "data"}
)
_RESOURCE_MUTABLE_FIELDS: Final[frozenset[str]] = frozenset({"name", "payload", "data"})

# How far back the run feed reaches when the caller names no window. A default, not a cap. The
# feed's `key` predicate cannot seek -- `pipeline_run_annotation` is indexed
# `(pipeline_run_id, key, value)` -- so this bound is what keeps the scan a range. Issue #649.
_DEFAULT_RUN_WINDOW: Final[datetime.timedelta] = datetime.timedelta(days=30)

# Read at import, so a rename in the core API is a startup failure naming these lines rather
# than a "your token is foreign" 422 on every continuation.
_PAGE_TOKEN_FILTER_QUERY_KEY: Final[str] = filter_query_sql._PAGE_TOKEN_FILTER_QUERY_KEY
_PAGE_TOKEN_OFFSET_KEY: Final[str] = filter_query_sql._PAGE_TOKEN_OFFSET_KEY
_RUN_CREATED_AT_KEY: Final[str] = (
    filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_AT.value
)

# An allow-list, because the core reader honours slots this module does not compile -- its
# legacy `filter` can replace the filter query outright -- and tokens are unauthenticated base64
# JSON anyone can mint.
_ALLOWED_PAGE_TOKEN_KEYS: Final[frozenset[str]] = frozenset(
    {_PAGE_TOKEN_FILTER_QUERY_KEY, _PAGE_TOKEN_OFFSET_KEY}
)

# The core pager puts `offset` straight into `OFFSET`, where an oversized one is a driver
# error rather than a 422.
_MAX_PAGE_TOKEN_OFFSET: Final[int] = 1_000_000


_RowT = TypeVar("_RowT", db_models.Project, db_models.ProjectResource)


@dataclasses.dataclass(frozen=True, kw_only=True)
class Page(Generic[_RowT]):
    """One page of rows. `has_more` comes from a `page_size + 1` probe."""

    rows: list[_RowT]
    total_count: int
    has_more: bool


def _normalized_name(*, value: str, field: str) -> str:
    """Trim a required display name, refusing one that is only whitespace."""
    normalized = value.strip()
    if not normalized:
        raise errors.ProjectValidationError(f"{field} must not be empty.")
    return normalized


def _optional_text(*, value: str | None) -> str | None:
    """A nullable text column's value, trimmed. Whitespace-only clears it, as `None` does."""
    if value is None:
        return None
    return value.strip() or None


def _reject_immutable_fields(
    *, updates: dict[str, Any], mutable: frozenset[str], subject: str
) -> None:
    """Refuse a patch naming anything outside `mutable`, listing every offender at once."""
    illegal = sorted(set(updates) - mutable)
    if illegal:
        raise errors.ProjectValidationError(
            f"{', '.join(illegal)} cannot be changed after a {subject} is created. "
            f"Editable fields: {', '.join(sorted(mutable))}."
        )


def _reject_unstorable_json(*, value: dict[str, Any], field: str) -> None:
    """Refuse a JSON object the database could not store.

    `allow_nan=False` because MySQL's JSON column refuses NaN and Infinity while SQLite stores
    them.
    """
    try:
        json.dumps(value, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise errors.ProjectValidationError(
            f"{field} must be JSON-serializable: {exc}"
        ) from exc


def _normalized_json_object(
    *, value: dict[str, Any] | None, field: str
) -> dict[str, Any] | None:
    """A nullable JSON object column's value, checked for shape.

    `{}` is kept rather than folded to null, unlike `_optional_text`'s empty string.
    """
    if value is None:
        return None
    if not isinstance(value, dict):
        # A list or string stores fine and then 500s on every read, the response model typing
        # the field as an object.
        raise errors.ProjectValidationError(
            f"{field} must be a JSON object or null, not {type(value).__name__}."
        )
    _reject_unstorable_json(value=value, field=field)
    return value


def validate_entity_shape(
    *,
    entity: db_models.ProjectResourceEntity,
    entity_id: str | None,
    payload: dict[str, Any] | None,
) -> None:
    """Refuse a resource that is neither a reference nor content, or names an id it cannot have.

    A *payload* entity (`db_models.PAYLOAD_ENTITIES`) requires a `payload` and must not name an
    `entity_id`; every other entity requires an `entity_id` and may also carry a `payload`. The
    CHECK constraint cannot express the entity-dependent half without naming an entity in DDL.
    """
    if payload is not None and not isinstance(payload, dict):
        raise errors.ProjectValidationError(
            f"payload must be a JSON object or null, not {type(payload).__name__}."
        )
    expects_payload = entity in db_models.PAYLOAD_ENTITIES
    if expects_payload and entity_id is not None:
        raise errors.ProjectValidationError(
            f"A {entity.value!r} resource carries its content in `payload` and must not name an "
            "`entity_id`: there is no other record for it to point at."
        )
    if expects_payload and payload is None:
        raise errors.ProjectValidationError(
            f"A {entity.value!r} resource requires a `payload`."
        )
    # A reference may carry metadata alongside the id it points with, so no rule here.
    if not expects_payload and entity_id is None:
        raise errors.ProjectValidationError(
            f"A {entity.value!r} resource requires an `entity_id`."
        )
    if payload is not None:
        _reject_unstorable_json(value=payload, field="payload")


def _run_window(
    *,
    since: datetime.datetime | None,
    until: datetime.datetime | None,
) -> tuple[datetime.datetime, datetime.datetime | None]:
    """The run feed's `created_at` bounds, defaulted and checked.

    The default window is anchored at `until`, so `?until=` alone reads as "the window ending
    there". Naive input is refused rather than read as UTC, and refused here so it is a 422
    naming the field rather than a pydantic error from inside the core filter model.
    """
    for value, field in ((since, "since"), (until, "until")):
        if value is not None and value.tzinfo is None:
            raise errors.ProjectValidationError(
                f"{field} must carry a timezone offset, e.g. '2026-01-01T00:00:00Z'."
            )
    if since is None:
        anchor = until if until is not None else db_utils.utc_now()
        try:
            return anchor - _DEFAULT_RUN_WINDOW, until
        except OverflowError as exc:
            # No representable lower bound. Reached by Go's zero `time.Time`, which marshals
            # to `0001-01-01T00:00:00Z`.
            raise errors.ProjectValidationError(
                f"until is too far in the past to anchor the default {_DEFAULT_RUN_WINDOW.days}-day "
                "window; send an explicit `since` with it."
            ) from exc
    if until is not None and since >= until:
        # Equal is refused as well as backwards: the upper bound compiles exclusively, so an
        # equal pair answers as a project with no runs.
        raise errors.ProjectValidationError(
            "since must be strictly before until; `until` is exclusive, so an equal pair selects nothing."
        )
    return since, until


def _reject_foreign_run_page_token(*, page_token: str, project_id: str) -> None:
    """Refuse a run-feed page token this feed did not issue.

    The core run list prefers the token's embedded filter over anything the caller sends
    (`filter_query_sql._resolve_filter_value`), so past the first page the token *is* the query --
    and tokens are unauthenticated base64 JSON. Unchecked, a token from project A replays A's
    filter under B's URL, and one from `GET /api/pipeline_runs/` serves every run on the
    deployment.

    Matched exactly -- slots and predicates, shape and types. Containment alone would let a
    hand-built token AND extra predicates on behind a 200, an unexpected slot could carry the
    legacy `filter` that replaces the filter query outright, and a wrong type downstream is a 500
    rather than the 422 this raises.
    """
    expected_key = pipeline_run_annotations.project_run_key(project_id)
    try:
        token = json.loads(base64.b64decode(page_token))
    except (TypeError, ValueError) as exc:
        raise errors.ProjectValidationError(
            "page_token is malformed; it is not a page token this API issued."
        ) from exc
    if isinstance(token, dict) and set(token) - _ALLOWED_PAGE_TOKEN_KEYS:
        raise errors.ProjectValidationError(
            "page_token was not issued by this project's run feed."
        )
    if not isinstance(token, dict) or _PAGE_TOKEN_FILTER_QUERY_KEY not in token:
        # The core encoder always writes this slot, so its absence is format drift rather
        # than a wrong token.
        raise errors.ProjectValidationError(
            "page_token is malformed; it is not a page token this API issued. If pagination "
            "stopped working after a deployment, the run list's page-token format has changed."
        )
    offset = token.get(_PAGE_TOKEN_OFFSET_KEY, 0)
    if (
        not isinstance(offset, int)
        or isinstance(offset, bool)
        or not 0 <= offset <= _MAX_PAGE_TOKEN_OFFSET
    ):
        # Absent is fine; the core reader defaults it to 0.
        raise errors.ProjectValidationError(
            "page_token was not issued by this project's run feed."
        )
    try:
        # `None` here is the unfiltered `GET /api/pipeline_runs/` token -- legitimately foreign.
        predicates = json.loads(token[_PAGE_TOKEN_FILTER_QUERY_KEY])["and"]
        matches_shape = _is_project_run_filter(
            predicates=predicates, expected_key=expected_key
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise errors.ProjectValidationError(
            "page_token was not issued by this project's run feed."
        ) from exc
    if not matches_shape:
        raise errors.ProjectValidationError(
            "page_token was not issued by this project's run feed."
        )


def _is_project_run_filter(*, predicates: Any, expected_key: str) -> bool:
    """Whether a token's embedded predicates are exactly what this project's feed compiles.

    One `key_exists` on this project's key, one bounded `created_at` `time_range`, nothing else.
    """
    if not isinstance(predicates, list) or len(predicates) != 2:
        return False
    seen: list[str] = []
    for predicate in predicates:
        if not isinstance(predicate, dict) or len(predicate) != 1:
            return False
        ((name, body),) = predicate.items()
        if not isinstance(body, dict):
            return False
        if (
            name == "key_exists"
            and set(body) == {"key"}
            and body["key"] == expected_key
        ):
            seen.append(name)
        elif name == "time_range" and _is_bounded_run_window(body):
            seen.append(name)
        else:
            return False
    # One of each, in either order.
    return sorted(seen) == ["key_exists", "time_range"]


def _is_bounded_run_window(body: dict[str, Any]) -> bool:
    """Whether a `time_range` slot in a page token is one this feed could have issued.

    `start_time` is required -- without it the query runs back to the beginning of time.
    Validated with `TimeRange`, the model the filter compiler uses, so a malformed bound is a 422
    here rather than a 500 out of the compiler.
    """
    try:
        window = filter_query_models.TimeRange.model_validate(body)
    except pydantic.ValidationError:
        return False
    return window.key == _RUN_CREATED_AT_KEY and window.start_time is not None


def _hide_deleted_pipelines() -> sql.ColumnElement[bool]:
    """The WHERE term that drops resources pointing at a soft-deleted pipeline.

    Hidden rather than deleted, because `user_pipelines` revives a row on a later `PUT` to the
    same path. Applied by both `list_resources` and `count_resources`, so a card cannot say
    "2 pipelines" above a list showing one.
    """
    is_deleted = (
        sql.select(1)
        .where(
            user_pipeline_db_models.UserPipeline.id
            == db_models.ProjectResource.entity_id,
            user_pipeline_db_models.UserPipeline.deleted_at.is_not(None),
        )
        .exists()
    )
    return sql.not_(
        sql.and_(
            db_models.ProjectResource.entity
            == db_models.ProjectResourceEntity.PIPELINE.value,
            is_deleted,
        )
    )


def _attached_resource_id(
    *,
    session: orm.Session,
    project_id: str,
    entity: db_models.ProjectResourceEntity,
    entity_id: str | None,
) -> str | None:
    """The id of the resource already holding this reference, if one does.

    Unfiltered on purpose: the unique index spans rows `list_resources` hides, so a resource
    pointing at a soft-deleted pipeline still blocks an attach and still has to be nameable.
    """
    return session.scalar(
        sql.select(db_models.ProjectResource.id).where(
            db_models.ProjectResource.project_id == project_id,
            db_models.ProjectResource.entity == entity.value,
            db_models.ProjectResource.entity_id == entity_id,
        )
    )


def _duplicate_resource_error(
    *,
    project_id: str,
    entity: db_models.ProjectResourceEntity,
    entity_id: str | None,
    blocking_id: str | None,
) -> errors.DuplicateResourceError:
    """The 409 for a reference this project already holds.

    `blocking_id` is None only when the row went away between the insert that failed on it and
    the read for it. The client is still owed an answer, so the id drops out of the message
    rather than the refusal.
    """
    detach = (
        f"Resource {blocking_id!r} already holds that reference; delete it to "
        "attach a new one."
        if blocking_id is not None
        else "Delete the existing resource to attach a new one."
    )
    return errors.DuplicateResourceError(
        f"Project {project_id!r} already references {entity.value!r} "
        f"{entity_id!r}. {detach}"
    )


def _is_entity_collision(*, error: sql.exc.IntegrityError) -> bool:
    """Whether this integrity error is the duplicate-reference constraint and not another.

    The strings come off the constraint object, so renaming it cannot leave this silently
    matching nothing. MySQL quotes the key name, SQLite lists the qualified columns.
    """
    constraint = db_models.PROJECT_RESOURCE_ENTITY_CONSTRAINT
    message = str(error.orig) if error.orig is not None else str(error)
    if constraint.name and constraint.name in message:
        return True
    return all(
        f"{column.table.name}.{column.name}" in message for column in constraint.columns
    )


class ProjectService:
    """Reads and writes for the three project tables."""

    # --- Workspaces -------------------------------------------------------------------

    def list_workspaces(self, *, session: orm.Session) -> list[db_models.Workspace]:
        """The workspaces available on this deployment, unpaginated -- a deployment holds few."""
        return list(
            session.scalars(
                sql.select(db_models.Workspace)
                .options(orm.defer(db_models.Workspace.extra_data, raiseload=True))
                .order_by(db_models.Workspace.name)
            ).all()
        )

    def get_workspace(
        self, *, session: orm.Session, workspace_id: str
    ) -> db_models.Workspace:
        workspace = session.get(db_models.Workspace, workspace_id)
        if workspace is None:
            raise errors.ProjectNotFoundError(
                f"Workspace {workspace_id!r} was not found."
            )
        return workspace

    def create_workspace(
        self,
        *,
        session: orm.Session,
        name: str,
        description: str | None = None,
        is_active: bool = True,
        data: dict[str, Any] | None = None,
        created_by: str | None = None,
    ) -> db_models.Workspace:
        """Create a workspace at a server-minted id.

        `name` is a label, not a key: two calls naming one `name` make two workspaces.
        """
        name = _normalized_name(value=name, field="name")
        description = _optional_text(value=description)
        data = _normalized_json_object(value=data, field="data")

        with session.begin():
            workspace = db_models.Workspace(
                name=name,
                description=description,
                is_active=is_active,
                data=data,
                created_by=created_by,
            )
            session.add(workspace)
            session.flush()
        return workspace

    def update_workspace(
        self,
        *,
        session: orm.Session,
        workspace_id: str,
        updates: dict[str, Any],
    ) -> db_models.Workspace:
        """Apply a patch to a workspace's editable fields.

        `updates` is the caller's *set* fields: an omitted key leaves the column alone, an
        explicit null clears it.
        """
        _reject_immutable_fields(
            updates=updates,
            mutable=_WORKSPACE_MUTABLE_FIELDS,
            subject="workspace",
        )

        with session.begin():
            workspace = session.get(db_models.Workspace, workspace_id)
            if workspace is None:
                raise errors.ProjectNotFoundError(
                    f"Workspace {workspace_id!r} was not found."
                )
            if "name" in updates:
                if updates["name"] is None:
                    # NOT NULL, so "explicit null clears it" cannot apply here.
                    raise errors.ProjectValidationError(
                        "name cannot be null; a workspace always has a name."
                    )
                workspace.name = _normalized_name(value=updates["name"], field="name")
            if "description" in updates:
                workspace.description = _optional_text(value=updates["description"])
            if "is_active" in updates:
                if updates["is_active"] is None:
                    raise errors.ProjectValidationError(
                        "is_active cannot be null; a workspace is either accepting new projects or not."
                    )
                if not isinstance(updates["is_active"], bool):
                    # Checked rather than coerced: `bool("false")` is `True`, so a non-route
                    # caller could retire a workspace by asking to activate it.
                    raise errors.ProjectValidationError(
                        "is_active must be true or false, not "
                        f"{type(updates['is_active']).__name__}."
                    )
                workspace.is_active = updates["is_active"]
            if "data" in updates:
                # Replaced wholesale, never merged: nothing here inspects the object, so there is
                # no basis for deciding what a partial update would mean.
                workspace.data = _normalized_json_object(
                    value=updates["data"],
                    field="data",
                )
            session.flush()
        return workspace

    def delete_workspace(self, *, session: orm.Session, workspace_id: str) -> None:
        """Hard-delete an empty workspace, refused with a 409 while it still holds projects.

        Never a cascade. Counted here rather than left to RESTRICT, so the refusal names the
        count and happens on SQLite too, where foreign keys are inert by default.
        """
        with session.begin():
            workspace = session.get(db_models.Workspace, workspace_id)
            if workspace is None:
                raise errors.ProjectNotFoundError(
                    f"Workspace {workspace_id!r} was not found."
                )
            # In the same transaction as the delete, so a project created in between cannot be
            # orphaned by a check that passed a moment earlier.
            project_count = (
                session.scalar(
                    sql.select(sql.func.count())
                    .select_from(db_models.Project)
                    .where(db_models.Project.workspace_id == workspace_id)
                )
                or 0
            )
            if project_count:
                raise errors.WorkspaceNotEmptyError(
                    f"Workspace {workspace_id!r} still holds {project_count} project(s). Delete them "
                    "first, or retire the workspace with `is_active: false` to stop it taking new ones."
                )
            session.delete(workspace)

    # --- Projects ---------------------------------------------------------------------

    def create_project(
        self,
        *,
        session: orm.Session,
        workspace_id: str,
        name: str,
        description: str | None = None,
        data: dict[str, Any] | None = None,
        created_by: str | None = None,
        origin: db_models.ProjectOrigin = db_models.ProjectOrigin.USER,
    ) -> db_models.Project:
        """Create a project inside an existing, active workspace.

        An unknown workspace is a 404; a retired one is a 422, since it exists and still serves
        the projects already in it.
        """
        name = _normalized_name(value=name, field="name")
        description = _optional_text(value=description)
        data = _normalized_json_object(value=data, field="data")

        with session.begin():
            workspace = session.get(db_models.Workspace, workspace_id)
            if workspace is None:
                raise errors.ProjectNotFoundError(
                    f"Workspace {workspace_id!r} was not found."
                )
            if not workspace.is_active:
                raise errors.ProjectValidationError(
                    f"Workspace {workspace.name!r} is no longer accepting new projects."
                )
            project = db_models.Project(
                workspace_id=workspace_id,
                name=name,
                description=description,
                data=data,
                created_by=created_by,
                origin=origin.value,
            )
            session.add(project)
            # Explicit: the app's sessions are `autoflush=False`, and the flush is what populates
            # the `init=False` timestamps the route serializes.
            session.flush()
        return project

    def get_project(
        self, *, session: orm.Session, project_id: str
    ) -> db_models.Project:
        project = session.get(db_models.Project, project_id)
        if project is None:
            raise errors.ProjectNotFoundError(f"Project {project_id!r} was not found.")
        return project

    def list_projects(
        self,
        *,
        session: orm.Session,
        page_size: int,
        cursor: tuple[datetime.datetime, str] | None = None,
        created_by: str | None = None,
        workspace_id: str | None = None,
    ) -> Page[db_models.Project]:
        """A page of projects, most recently updated first.

        Returns **every** project, not the caller's: `created_by` is attribution, not access
        control. The keyset runs on `updated_at`, which a `PATCH` moves, so a walk is not a
        snapshot.
        """
        filters = []
        if workspace_id is not None:
            filters.append(db_models.Project.workspace_id == workspace_id)
        if created_by is not None:
            # Matched exactly: the auth layer mints this, so folding its case would invent a
            # rule its writer never applied.
            filters.append(db_models.Project.created_by == created_by)

        total_count = (
            session.scalar(
                sql.select(sql.func.count())
                .select_from(db_models.Project)
                .where(*filters)
            )
            or 0
        )

        query = (
            sql.select(db_models.Project)
            .options(orm.defer(db_models.Project.extra_data, raiseload=True))
            .where(*filters)
        )
        if cursor is not None:
            cursor_updated_at, cursor_id = cursor
            # `(updated_at, id) < (?, ?)` -- "strictly after this row in the sort order". The
            # equivalent `updated_at < ? OR (updated_at = ? AND id < ?)` does not match the
            # `(updated_at, id)` index as a range scan; this form does.
            query = query.where(
                sql.tuple_(db_models.Project.updated_at, db_models.Project.id)
                < sql.tuple_(cursor_updated_at, cursor_id)
            )
        rows = list(
            session.scalars(
                query.order_by(
                    db_models.Project.updated_at.desc(),
                    db_models.Project.id.desc(),
                ).limit(page_size + 1)
            ).all()
        )
        has_more = len(rows) > page_size
        return Page(rows=rows[:page_size], total_count=total_count, has_more=has_more)

    def update_project(
        self,
        *,
        session: orm.Session,
        project_id: str,
        updates: dict[str, Any],
    ) -> db_models.Project:
        """Apply a patch to a project's editable fields.

        `updates` is the caller's *set* fields; anything outside `_PROJECT_MUTABLE_FIELDS` is a
        422.
        """
        _reject_immutable_fields(
            updates=updates, mutable=_PROJECT_MUTABLE_FIELDS, subject="project"
        )

        with session.begin():
            project = session.get(db_models.Project, project_id)
            if project is None:
                raise errors.ProjectNotFoundError(
                    f"Project {project_id!r} was not found."
                )
            if "name" in updates:
                if updates["name"] is None:
                    # The one editable field that is NOT NULL.
                    raise errors.ProjectValidationError(
                        "name cannot be null; a project always has a name."
                    )
                project.name = _normalized_name(value=updates["name"], field="name")
            if "description" in updates:
                project.description = _optional_text(value=updates["description"])
            if "data" in updates:
                project.data = _normalized_json_object(
                    value=updates["data"], field="data"
                )
            session.flush()
        return project

    def delete_project(
        self, *, session: orm.Session, project_id: str
    ) -> dict[str, int]:
        """Hard-delete a project and everything attached to it.

        Returns the *visible* resource counts, so a row pointing at a deleted pipeline is removed
        but not reported. Deleted explicitly rather than by `ON DELETE CASCADE`, which SQLite
        ignores without a per-connection pragma.
        """
        with session.begin():
            project = session.get(db_models.Project, project_id)
            if project is None:
                raise errors.ProjectNotFoundError(
                    f"Project {project_id!r} was not found."
                )
            counts = self.count_resources(
                session=session, project_ids=[project_id]
            ).get(project_id, {})
            session.execute(
                sql.delete(db_models.ProjectResource).where(
                    db_models.ProjectResource.project_id == project_id
                )
            )
            session.delete(project)
        return counts

    def count_resources(
        self,
        *,
        session: orm.Session,
        project_ids: collections.abc.Sequence[str],
    ) -> dict[str, dict[str, int]]:
        """Resource counts per project, grouped by entity, in **one** query.

        Taking the whole page is what makes the per-card N+1 impossible. Projects with no
        resources are absent rather than empty.
        """
        if not project_ids:
            return {}
        rows = session.execute(
            sql.select(
                db_models.ProjectResource.project_id,
                db_models.ProjectResource.entity,
                sql.func.count().label("count"),
            )
            .where(
                db_models.ProjectResource.project_id.in_(list(project_ids)),
                _hide_deleted_pipelines(),
            )
            .group_by(
                db_models.ProjectResource.project_id,
                db_models.ProjectResource.entity,
            )
        ).all()
        counts: dict[str, dict[str, int]] = {}
        for row_project_id, entity, count in rows:
            counts.setdefault(row_project_id, {})[entity] = count
        return counts

    def list_project_runs(
        self,
        *,
        session: orm.Session,
        project_id: str,
        page_token: str | None = None,
        since: datetime.datetime | None = None,
        until: datetime.datetime | None = None,
        include_pipeline_names: bool = True,
    ) -> api_server_sql.ListPipelineJobsResponse:
        """The runs submitted for this project, newest first, within a `created_at` window.

        A run is never a `project_resource` row: the link is an annotation, and the feed is a
        `filter_query` against the existing run list, so the page size and OFFSET token are
        inherited. The token carries the compiled filter -- see `_reject_foreign_run_page_token`.
        """
        if session.get(db_models.Project, project_id) is None:
            raise errors.ProjectNotFoundError(f"Project {project_id!r} was not found.")
        if page_token is not None:
            _reject_foreign_run_page_token(page_token=page_token, project_id=project_id)

        resolved_since, resolved_until = _run_window(since=since, until=until)
        # On the `created_at` system key this compiles to a plain `pipeline_run.created_at`
        # comparison rather than an `EXISTS`, which is what lets it bound the scan.
        time_range: dict[str, Any] = {
            "key": filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_AT.value,
            "start_time": resolved_since.isoformat(),
        }
        if resolved_until is not None:
            time_range["end_time"] = resolved_until.isoformat()

        # Through `FilterQuery` rather than hand-rolled JSON, so a typo raises here instead of
        # compiling to a filter that matches everything. `key_exists`, not `value_equals`,
        # because the project id is in the key.
        filter_query = filter_query_models.FilterQuery.model_validate(
            {
                "and": [
                    {
                        "key_exists": {
                            "key": pipeline_run_annotations.project_run_key(project_id)
                        }
                    },
                    {"time_range": time_range},
                ]
            }
        )
        return api_server_sql.PipelineRunsApiService_Sql().list(
            session=session,
            # One or the other, never both: `list` prefers the token's embedded filter.
            page_token=page_token,
            filter_query=(
                None
                if page_token
                else filter_query.model_dump_json(by_alias=True, exclude_none=True)
            ),
            include_pipeline_names=include_pipeline_names,
        )

    # --- Project resources ------------------------------------------------------------

    def create_resource(
        self,
        *,
        session: orm.Session,
        project_id: str,
        entity: db_models.ProjectResourceEntity,
        name: str | None = None,
        entity_id: str | None = None,
        payload: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        created_by: str | None = None,
    ) -> db_models.ProjectResource:
        """Attach a resource to a project.

        Two duplicate checks rather than one. A read answers the ordinary repeat attach, and
        the unique index catches the race a read cannot see. The 409 names the blocking row
        either way, because the index spans rows `list_resources` hides.
        """
        validate_entity_shape(entity=entity, entity_id=entity_id, payload=payload)
        name = _optional_text(value=name)
        data = _normalized_json_object(value=data, field="data")

        with session.begin():
            project = session.get(db_models.Project, project_id)
            if project is None:
                raise errors.ProjectNotFoundError(
                    f"Project {project_id!r} was not found."
                )
            # Read before insert. A duplicate INSERT that fails on the unique index still
            # takes a lock on the record it collided with, so letting every repeat attach
            # reach the index amplifies lock contention. The ordinary "already attached"
            # answer is now a plain SELECT that locks nothing.
            #
            # Documents skip it: their `entity_id` is NULL, which UNIQUE treats as distinct,
            # so they never collide and the read would always miss.
            if entity_id is not None:
                blocking_id = _attached_resource_id(
                    session=session,
                    project_id=project_id,
                    entity=entity,
                    entity_id=entity_id,
                )
                if blocking_id is not None:
                    raise _duplicate_resource_error(
                        project_id=project_id,
                        entity=entity,
                        entity_id=entity_id,
                        blocking_id=blocking_id,
                    )

            resource = db_models.ProjectResource(
                project_id=project.id,
                entity=entity.value,
                name=name,
                entity_id=entity_id,
                payload=payload,
                data=data,
                created_by=created_by,
            )
            try:
                with session.begin_nested():
                    session.add(resource)
                    session.flush()
            except sql.exc.IntegrityError as insert_error:
                # Still required, and not just for safety: the read above is a consistent one,
                # so a row committed after this transaction's snapshot is invisible to it. The
                # index is what settles that race; the read only keeps the settled case away
                # from it.
                if _is_entity_collision(error=insert_error):
                    # After the savepoint rolled back, so this finds the committed row that
                    # blocked the insert rather than our own failed add.
                    raise _duplicate_resource_error(
                        project_id=project_id,
                        entity=entity,
                        entity_id=entity_id,
                        blocking_id=_attached_resource_id(
                            session=session,
                            project_id=project_id,
                            entity=entity,
                            entity_id=entity_id,
                        ),
                    ) from insert_error
                raise
        return resource

    def get_resource(
        self,
        *,
        session: orm.Session,
        project_id: str,
        resource_id: str,
    ) -> db_models.ProjectResource:
        """One resource, addressed through its project.

        The path's `project_id` is part of the WHERE clause, so a resource belonging to a
        different project is a 404. This keeps the route's two path segments consistent with each
        other; it is not an authorization check.
        """
        resource = session.scalar(
            sql.select(db_models.ProjectResource).where(
                db_models.ProjectResource.id == resource_id,
                db_models.ProjectResource.project_id == project_id,
            )
        )
        if resource is None:
            raise errors.ProjectNotFoundError(
                f"Resource {resource_id!r} was not found on project {project_id!r}."
            )
        return resource

    def list_resources(
        self,
        *,
        session: orm.Session,
        project_id: str,
        page_size: int,
        cursor: tuple[datetime.datetime, str] | None = None,
        entities: (
            collections.abc.Sequence[db_models.ProjectResourceEntity] | None
        ) = None,
    ) -> Page[db_models.ProjectResource]:
        """A page of one project's resources, newest first, optionally narrowed by entity.

        `payload` is left out of the SELECT, not just the response model, and `raiseload=True`
        makes touching it here an error rather than a query per row. `data` *is* read.
        """
        if session.get(db_models.Project, project_id) is None:
            raise errors.ProjectNotFoundError(f"Project {project_id!r} was not found.")

        filters = [db_models.ProjectResource.project_id == project_id]
        if entities is None or db_models.ProjectResourceEntity.PIPELINE in entities:
            # Skipped when the entity filter admits no pipeline, where the term could not hide
            # a row from this page anyway.
            filters.append(_hide_deleted_pipelines())
        if entities is not None:
            # `is not None`, not truthiness: an empty sequence means "match nothing".
            filters.append(
                db_models.ProjectResource.entity.in_(
                    [entity.value for entity in entities]
                )
            )

        total_count = (
            session.scalar(
                sql.select(sql.func.count())
                .select_from(db_models.ProjectResource)
                .where(*filters)
            )
            or 0
        )
        query = (
            sql.select(db_models.ProjectResource)
            .options(
                orm.defer(db_models.ProjectResource.payload, raiseload=True),
                orm.defer(db_models.ProjectResource.extra_data, raiseload=True),
            )
            .where(*filters)
        )
        if cursor is not None:
            cursor_created_at, cursor_id = cursor
            # Row-value comparison, as in `list_projects`.
            query = query.where(
                sql.tuple_(
                    db_models.ProjectResource.created_at,
                    db_models.ProjectResource.id,
                )
                < sql.tuple_(cursor_created_at, cursor_id)
            )
        rows = list(
            session.scalars(
                query.order_by(
                    db_models.ProjectResource.created_at.desc(),
                    db_models.ProjectResource.id.desc(),
                ).limit(page_size + 1)
            ).all()
        )
        return Page(
            rows=rows[:page_size],
            total_count=total_count,
            has_more=len(rows) > page_size,
        )

    def update_resource(
        self,
        *,
        session: orm.Session,
        project_id: str,
        resource_id: str,
        updates: dict[str, Any],
    ) -> db_models.ProjectResource:
        """Apply a patch to a resource's editable fields.

        `entity`, `entity_id` and `project_id` are immutable -- a movable pointer would slide a
        row out from under the unique index. `payload` and `data` are replaced wholesale.
        """
        _reject_immutable_fields(
            updates=updates,
            mutable=_RESOURCE_MUTABLE_FIELDS,
            subject="resource",
        )
        with session.begin():
            resource = self.get_resource(
                session=session, project_id=project_id, resource_id=resource_id
            )
            if "name" in updates:
                resource.name = _optional_text(value=updates["name"])
            if "payload" in updates:
                # Re-checked: a patch can break a shape that was legal at creation, clearing a
                # document's content to leave a row that is neither a reference nor content.
                try:
                    entity = db_models.ProjectResourceEntity(resource.entity)
                except ValueError as exc:
                    # `entity` is a CHECK-less VARCHAR, so a rollback can be reading rows a newer
                    # deploy wrote. Listing them works; this path has to cast.
                    raise errors.ProjectValidationError(
                        f"Resource entity {resource.entity!r} is not known to this deploy, "
                        "so its payload cannot be validated or edited here."
                    ) from exc
                validate_entity_shape(
                    entity=entity,
                    entity_id=resource.entity_id,
                    payload=updates["payload"],
                )
                resource.payload = updates["payload"]
            if "data" in updates:
                resource.data = _normalized_json_object(
                    value=updates["data"],
                    field="data",
                )
            session.flush()
        return resource

    def delete_resource(
        self, *, session: orm.Session, project_id: str, resource_id: str
    ) -> None:
        """Hard-delete one resource. No tombstone, so its `(entity, entity_id)` slot frees at once.

        A soft-deleted row would keep its slot in the unique index, leaving the write path to
        decide whether a colliding insert is a duplicate or a revival.
        """
        with session.begin():
            resource = self.get_resource(
                session=session, project_id=project_id, resource_id=resource_id
            )
            session.delete(resource)
