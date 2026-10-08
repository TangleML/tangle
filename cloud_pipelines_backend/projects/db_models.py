"""Database models for workspaces, projects and project resources.

    workspace  1 --< project  1 --< project_resource

Registered on the shared `bts._TableBase.metadata`, so `create_all()` adds these tables to an
existing database and alters nothing else -- which is also why no column here can be added to a
table that already ships.

Every table carries two JSON columns: `data` is the client's, stored opaquely and never
inspected here, and `extra_data` is a backend-only escape hatch that stays off the HTTP contract
in both directions.

Column types inherited from `bts._TableBase.type_annotation_map`:
  str -> String(255), datetime -> UtcDateTime, dict -> MutableDict(JSON)
Explicit types below only where overriding the default.
"""

import datetime
import enum
import logging
from typing import Any, Final

import sqlalchemy as sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.utils import db as db_utils
from sqlalchemy import orm
from sqlalchemy.ext import mutable

logger = logging.getLogger(__name__)


def _json_object_column(*, none_as_null: bool = True) -> sql.types.TypeEngine[Any]:
    """A nullable JSON object column that stores SQL NULL and notices in-place edits.

    `none_as_null` keeps Python `None` out of the JSON document `null`, which `IS NULL` does not
    match -- load-bearing for `payload`, whose CHECK constraint asks exactly that. Naming a type
    opts out of `type_annotation_map`, so `MutableDict` has to be re-applied by hand.
    """
    return mutable.MutableDict.as_mutable(sql.JSON(none_as_null=none_as_null))


def _created_at_column() -> orm.MappedColumn[datetime.datetime]:
    """The row's creation stamp."""
    return orm.mapped_column(init=False, insert_default=db_utils.utc_now)


def _updated_at_column() -> orm.MappedColumn[datetime.datetime]:
    """The row's last-write stamp, equal to `created_at` until something writes to the row.

    Reads the value the same INSERT is giving `created_at` rather than calling the clock twice,
    which would leave a new row looking edited by a few microseconds.
    """
    return orm.mapped_column(
        init=False,
        insert_default=lambda context: context.get_current_parameters()["created_at"],
        onupdate=db_utils.utc_now,
    )


def _id_column() -> orm.MappedColumn[bts.IdType]:
    """A primary key minted by the repo's shared generator."""
    return orm.mapped_column(
        sql.String(db_utils.ID_LENGTH),
        primary_key=True,
        init=False,
        insert_default=bts.generate_unique_id,
    )


class ProjectResourceEntity(str, enum.Enum):
    """What a `project_resource` row points at.

    A member is either a *reference*, carrying `entity_id` and optionally a `payload` of metadata
    about it, or a *payload* entity, carrying its content in `payload` and no `entity_id`.
    `PAYLOAD_ENTITIES` divides them; `services.validate_entity_shape` enforces it.
    """

    PIPELINE = "pipeline"
    AGENT_SESSION = "agent_session"
    DOCUMENT = "document"


PAYLOAD_ENTITIES: Final[frozenset[ProjectResourceEntity]] = frozenset(
    {ProjectResourceEntity.DOCUMENT}
)


class ProjectOrigin(str, enum.Enum):
    """Who made a project. Attribution about the *creator's nature*, not the creator."""

    USER = "user"
    AGENT = "agent"


class Workspace(bts._TableBase):
    """A container for projects, administered rather than created by users.

    Written only through the admin-gated routes in `api_routes`. A deployment is expected to hold
    a handful.
    """

    __tablename__ = "workspace"

    id: orm.Mapped[bts.IdType] = _id_column()
    name: orm.Mapped[str] = orm.mapped_column()
    description: orm.Mapped[str | None] = orm.mapped_column(default=None)
    # Retires a workspace without deleting it: it still lists and resolves, it just takes no new
    # projects.
    is_active: orm.Mapped[bool] = orm.mapped_column(default=True)
    # Attribution, never access control.
    created_by: orm.Mapped[str | None] = orm.mapped_column(default=None)
    data: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(
        _json_object_column(), default=None
    )
    extra_data: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(
        _json_object_column(), default=None
    )
    created_at: orm.Mapped[datetime.datetime] = _created_at_column()
    updated_at: orm.Mapped[datetime.datetime] = _updated_at_column()


class Project(bts._TableBase):
    """A project: a named grouping of resources inside exactly one workspace."""

    __tablename__ = "project"

    id: orm.Mapped[bts.IdType] = _id_column()
    # Immutable after insert. No `ondelete`, so RESTRICT: a workspace delete must not take its
    # projects with it. `services.delete_workspace` returns a 409 before the flush reaches this,
    # which it must -- SQLite does not enforce foreign keys by default.
    workspace_id: orm.Mapped[bts.IdType] = orm.mapped_column(
        sql.String(db_utils.ID_LENGTH),
        sql.ForeignKey(Workspace.id, name="fk_project_workspace_id"),
    )
    name: orm.Mapped[str] = orm.mapped_column()
    description: orm.Mapped[str | None] = orm.mapped_column(default=None)
    # Attribution, never access control.
    created_by: orm.Mapped[str | None] = orm.mapped_column(default=None)
    # A `ProjectOrigin` value, VARCHAR for the same reason `entity` is.
    origin: orm.Mapped[str] = orm.mapped_column(
        insert_default=ProjectOrigin.USER.value,
        default=ProjectOrigin.USER.value,
    )
    # Project notes live here.
    data: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(
        _json_object_column(), default=None
    )
    extra_data: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(
        _json_object_column(), default=None
    )
    created_at: orm.Mapped[datetime.datetime] = _created_at_column()
    updated_at: orm.Mapped[datetime.datetime] = _updated_at_column()

    # The listing route's access paths: keyset `(updated_at, id)`, with each filter combination
    # pushed in front of it so the seek and the sort are one index read. A filter left of the
    # sort columns is not a prefix of the bare index, which is why each combination gets its own.
    __table_args__ = (
        sql.Index("ix_project_updated_at_id", updated_at, id),
        sql.Index(
            "ix_project_workspace_id_updated_at_id",
            workspace_id,
            updated_at,
            id,
        ),
        sql.Index(
            "ix_project_created_by_updated_at_id",
            created_by,
            updated_at,
            id,
        ),
        sql.Index(
            "ix_project_workspace_id_created_by_updated_at_id",
            workspace_id,
            created_by,
            updated_at,
            id,
        ),
    )


class ProjectResource(bts._TableBase):
    """One thing attached to a project: a document, or a reference to something external."""

    __tablename__ = "project_resource"

    id: orm.Mapped[bts.IdType] = _id_column()
    project_id: orm.Mapped[bts.IdType] = orm.mapped_column(
        sql.String(db_utils.ID_LENGTH),
        sql.ForeignKey(
            Project.id,
            ondelete="CASCADE",
            name="fk_project_resource_project_id",
        ),
    )
    # A `ProjectResourceEntity` value, typed `str` rather than `Mapped[ProjectResourceEntity]`:
    # the latter emits a CHECK or a native enum, the migration this column exists to avoid.
    entity: orm.Mapped[str] = orm.mapped_column()
    # Nullable because a `document` has no referent. Opaque: no foreign key and no existence
    # check even for `pipeline`, since `user_pipelines` deletes softly and revives, so a foreign
    # key would either block that delete or cascade the membership away.
    entity_id: orm.Mapped[str | None] = orm.mapped_column(default=None)
    name: orm.Mapped[str | None] = orm.mapped_column(default=None)
    # Stored verbatim, never inspected. `none_as_null` matters here specifically: the CHECK below
    # asks `payload IS NOT NULL`, which the JSON document `null` would satisfy.
    payload: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(
        _json_object_column(),
        default=None,
    )
    data: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(
        _json_object_column(), default=None
    )
    extra_data: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(
        _json_object_column(), default=None
    )
    created_by: orm.Mapped[str | None] = orm.mapped_column(default=None)
    created_at: orm.Mapped[datetime.datetime] = _created_at_column()
    updated_at: orm.Mapped[datetime.datetime] = _updated_at_column()

    # Stops the same thing being attached to one project twice. Documents are exempt without a
    # partial index, by the SQL rule that UNIQUE treats NULLs as distinct.
    _entity_constraint = sql.UniqueConstraint(
        project_id,
        entity,
        entity_id,
        name="uq_project_resource_project_id_entity_entity_id",
    )

    # A resource must point at something or carry something. Deliberately weaker than
    # `services.validate_entity_shape`, which also decides *which* entities may omit an
    # `entity_id`: naming an entity in DDL is a migration every time the set grows.
    _reference_or_payload_constraint = sql.CheckConstraint(
        sql.or_(entity_id.isnot(None), payload.isnot(None)),
        name="ck_project_resource_reference_or_payload",
    )

    __table_args__ = (
        _entity_constraint,
        _reference_or_payload_constraint,
        # The resource list's access paths, one per filter shape: `entity` sits left of the sort
        # columns, so it cannot be served by a prefix of the unfiltered index. The grouped count
        # needs neither -- it is the leftmost prefix of `_entity_constraint`.
        sql.Index(
            "ix_project_resource_project_id_created_at_id",
            project_id,
            created_at,
            id,
        ),
        sql.Index(
            "ix_project_resource_project_id_entity_created_at_id",
            project_id,
            entity,
            created_at,
            id,
        ),
        # The reverse lookup -- "which projects reference this thing". Nothing reads it yet;
        # cheap now, expensive to add once the table is large.
        sql.Index("ix_project_resource_entity_entity_id", entity, entity_id),
    )


# Re-exported so the service can match a violation on `.name` and `.columns` and turn it into a
# 409. Declared inside the class because it references the class-body columns.
PROJECT_RESOURCE_ENTITY_CONSTRAINT: Final[sql.UniqueConstraint] = (
    ProjectResource._entity_constraint
)


def register_db_tables() -> None:
    """Ensure the models are imported before shared metadata is created."""
    logger.info(
        "Project tables registered: %s, %s, %s",
        Workspace.__tablename__,
        Project.__tablename__,
        ProjectResource.__tablename__,
    )
