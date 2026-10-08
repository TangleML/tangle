"""Schema-level assertions: the guards that live in DDL rather than in a route."""

import sqlalchemy
from cloud_pipelines_backend.projects import db_models
from sqlalchemy import orm
from sqlalchemy.dialects import mysql

from tests.projects.conftest import SANDBOX

_TABLE_NAMES = ("workspace", "project", "project_resource")


def _mysql_ddl(table: sqlalchemy.Table) -> str:
    return str(sqlalchemy.schema.CreateTable(table).compile(dialect=mysql.dialect()))


def _table(name: str) -> sqlalchemy.Table:
    return {
        "workspace": db_models.Workspace,
        "project": db_models.Project,
        "project_resource": db_models.ProjectResource,
    }[name].__table__


class TestMetadataColumns:
    """`data` is the client's; `extra_data` is the backend's and stays off the wire."""

    def test_every_table_carries_both(self) -> None:
        for table_name in _TABLE_NAMES:
            columns = _table(table_name).c
            assert "data" in columns, table_name
            assert "extra_data" in columns, table_name

    def test_extra_data_is_writable_below_the_api(self, session: orm.Session) -> None:
        """No route writes it, so this is the only place it is exercised. It is kept for a
        backend that needs somewhere to put something; if that never happens it is dead weight,
        not a bug."""
        project = _add_project(session)
        project.extra_data = {"reason": "backfill"}
        session.flush()
        session.expire(project)
        assert project.extra_data == {"reason": "backfill"}


class TestResourcesReachTheirWorkspaceThroughTheProject:
    def test_a_resource_carries_no_workspace_of_its_own(self) -> None:
        """One fact, one column.

        A `workspace_id` here would copy `project.workspace_id` with nothing keeping the two in
        step -- harmless while a project's workspace is immutable, silently wrong the first time
        one can be moved, and buying nothing over a primary-key join. If it comes back it needs a
        composite foreign key `(project_id, workspace_id) REFERENCES project(id, workspace_id)`,
        so this test failing is a design decision to re-take, not a line to delete.
        """
        assert "workspace_id" not in _table("project_resource").c

    def test_the_only_path_from_a_resource_to_a_workspace_is_its_project(
        self,
    ) -> None:
        referenced = {
            constraint.referred_table.name
            for constraint in _table("project_resource").foreign_key_constraints
        }
        assert referenced == {"project"}


class TestUniqueReferenceConstraint:
    """`(project_id, entity, entity_id)`, and the NULL semantics that make documents exempt."""

    def test_the_same_reference_cannot_be_attached_twice(
        self, session: orm.Session
    ) -> None:
        project = _add_project(session)
        entity_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        for _ in range(2):
            session.add(
                db_models.ProjectResource(
                    project_id=project.id,
                    entity=db_models.ProjectResourceEntity.PIPELINE.value,
                    entity_id=entity_id,
                )
            )
        try:
            session.flush()
        except sqlalchemy.exc.IntegrityError:
            return
        raise AssertionError("the unique constraint did not fire")

    def test_documents_are_exempt_without_a_partial_index(
        self, session: orm.Session
    ) -> None:
        """Any number of rows may share `(project_id, 'document', NULL)`.

        Not a loophole: UNIQUE treats NULLs as distinct everywhere, which is the behaviour
        wanted -- two documents with the same content are two documents.
        """
        project = _add_project(session)
        for _ in range(3):
            session.add(
                db_models.ProjectResource(
                    project_id=project.id,
                    entity=db_models.ProjectResourceEntity.DOCUMENT.value,
                    entity_id=None,
                    payload={"body": "same text, three times"},
                )
            )
        session.flush()
        assert (
            session.scalar(
                sqlalchemy.select(sqlalchemy.func.count()).select_from(
                    db_models.ProjectResource
                )
            )
            == 3
        )

    def test_the_same_reference_may_live_in_two_projects(
        self, session: orm.Session
    ) -> None:
        """The constraint is scoped to a project. One session may be attached to several."""
        entity_id = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
        for _ in range(2):
            project = _add_project(session)
            session.add(
                db_models.ProjectResource(
                    project_id=project.id,
                    entity=db_models.ProjectResourceEntity.PIPELINE.value,
                    entity_id=entity_id,
                )
            )
        session.flush()


class TestCascade:
    def test_deleting_a_project_removes_its_resources(
        self, fk_db_engine: sqlalchemy.Engine
    ) -> None:
        """On the foreign-key-enforcing engine, so this is about the schema: the service also
        deletes explicitly, and this says the declared cascade is real and not decoration.
        """
        with orm.Session(bind=fk_db_engine) as session, session.begin():
            project = _add_project(session)
            session.flush()
            session.add(
                db_models.ProjectResource(
                    project_id=project.id,
                    entity=db_models.ProjectResourceEntity.DOCUMENT.value,
                    payload={"body": "attached to a project about to be deleted"},
                )
            )
            session.flush()
            project_id = project.id
            session.execute(
                sqlalchemy.delete(db_models.Project).where(
                    db_models.Project.id == project_id
                )
            )

        with orm.Session(bind=fk_db_engine) as session:
            remaining = session.scalar(
                sqlalchemy.select(sqlalchemy.func.count())
                .select_from(db_models.ProjectResource)
                .where(db_models.ProjectResource.project_id == project_id)
            )
            assert remaining == 0


class TestJsonColumnsStoreSqlNull:
    """`None` in a JSON column is SQL `NULL`, never the JSON document `null`.

    The difference is invisible in Python -- both read back as `None` -- and not invisible in
    SQL: `WHERE data IS NULL` matches the first and not the second, so a table holding both
    answers that predicate with half its rows. `_json_object_column` pins it with
    `none_as_null=True` rather than leaving it to the dialect's default.
    """

    def test_an_unset_json_column_is_sql_null(self, session: orm.Session) -> None:
        project = _add_project(session)
        session.add(
            db_models.ProjectResource(
                project_id=project.id,
                entity=db_models.ProjectResourceEntity.DOCUMENT.value,
                payload={"body": "a document with no metadata"},
            )
        )
        session.flush()

        # Against the column, not the ORM attribute: `IS NULL` is the whole point, and a JSON
        # `null` would read back as `None` through the mapper either way.
        table = _table("project_resource")
        for column in (table.c.data, table.c.extra_data):
            assert (
                session.scalar(
                    sqlalchemy.select(sqlalchemy.func.count())
                    .select_from(table)
                    .where(column.is_(None))
                )
                == 1
            )

    def test_an_in_place_edit_is_noticed(self, session: orm.Session) -> None:
        """`MutableDict` is why a PATCH that mutates the dict rather than replacing it flushes.

        Naming a type opts the column out of `_TableBase.type_annotation_map`, which is where
        the mutable wrapper would otherwise have come from -- so `_json_object_column` re-applies
        it. Without that, this write is lost with no error.
        """
        project = _add_project(session)
        project.data = {"team": "research"}
        session.flush()

        project.data["team"] = "tangle"
        assert project in session.dirty

        session.flush()
        session.expire(project)
        assert project.data == {"team": "tangle"}


class TestCreationStamps:
    def test_every_table_carries_created_at_and_updated_at(self) -> None:
        """`workspace` gained `updated_at` late; this is what keeps the three consistent."""
        for table_name in _TABLE_NAMES:
            columns = _table(table_name).c
            assert "created_at" in columns, table_name
            assert "updated_at" in columns, table_name

    def test_a_new_row_carries_one_timestamp_under_two_names(
        self, session: orm.Session
    ) -> None:
        """`updated_at == created_at` exactly, until something writes to the row.

        Two separate `utc_now()` calls put microseconds between them, which makes "has this
        ever been edited?" unanswerable and orders a page of never-edited rows by a difference
        that means nothing. `_updated_at_column` reads `created_at` out of the insert's own
        parameters instead.
        """
        project = _add_project(session)
        session.flush()
        assert project.updated_at == project.created_at

    def test_an_edit_moves_only_updated_at(self, session: orm.Session) -> None:
        project = _add_project(session)
        session.flush()
        created_at = project.created_at

        project.name = "renamed"
        session.flush()

        assert project.created_at == created_at
        assert project.updated_at > created_at


class TestNoTombstones:
    def test_no_table_here_has_a_soft_delete_column(self) -> None:
        """Deletion is hard everywhere here, and that is load-bearing.

        A `deleted_at` would let `uq_project_resource_project_id_entity_entity_id` hold a slot
        for a row nobody can see, and the write path would then have to decide whether a
        colliding insert is a duplicate or a revival -- the `user_pipelines` revival bug, where a
        PUT to a soft-deleted path resurrects the row and reports an ordinary edit.
        """
        for table_name in _TABLE_NAMES:
            columns = set(_table(table_name).c.keys())
            assert not columns & {"deleted_at", "archived_at", "is_deleted"}, table_name


class TestIndexes:
    def test_the_keyset_access_paths_are_indexed(self) -> None:
        """Each listing route's ORDER BY, and each filter in front of it."""
        project_indexes = {
            index.name: [column.name for column in index.columns]
            for index in _table("project").indexes
        }
        assert project_indexes["ix_project_updated_at_id"] == [
            "updated_at",
            "id",
        ]
        assert project_indexes["ix_project_workspace_id_updated_at_id"] == [
            "workspace_id",
            "updated_at",
            "id",
        ]
        assert project_indexes["ix_project_created_by_updated_at_id"] == [
            "created_by",
            "updated_at",
            "id",
        ]

        resource_indexes = {
            index.name: [column.name for column in index.columns]
            for index in _table("project_resource").indexes
        }
        assert resource_indexes["ix_project_resource_project_id_created_at_id"] == [
            "project_id",
            "created_at",
            "id",
        ]
        assert resource_indexes["ix_project_resource_entity_entity_id"] == [
            "entity",
            "entity_id",
        ]

    def test_the_grouped_count_is_served_by_the_unique_constraint_prefix(
        self,
    ) -> None:
        """No standalone `(project_id, entity)` index: `count_resources` groups by exactly those
        two columns and they are the unique constraint's leftmost prefix, so a second copy would
        be written on every insert and read by nothing."""
        constraint_columns = [
            column.name
            for column in db_models.PROJECT_RESOURCE_ENTITY_CONSTRAINT.columns
        ]
        assert constraint_columns[:2] == ["project_id", "entity"]
        assert not any(
            [column.name for column in index.columns] == ["project_id", "entity"]
            for index in _table("project_resource").indexes
        )


class TestEntityIsNotADatabaseEnum:
    def test_entity_compiles_to_a_plain_varchar(self) -> None:
        """Adding `pipeline` or `run` later must be a code change, not a migration."""
        ddl = _mysql_ddl(_table("project_resource"))
        entity_line = next(
            part for part in ddl.splitlines() if part.strip().startswith("entity ")
        )
        assert "VARCHAR" in entity_line
        assert "ENUM" not in ddl.upper().replace("PROJECT_RESOURCE", "")

    def test_no_check_constraint_names_an_entity(self) -> None:
        """The one CHECK is about the reference/payload pair, not about which entities exist --
        enumerating them would put the next one behind a migration."""
        checks = {
            constraint.name: str(constraint.sqltext)
            for constraint in _table("project_resource").constraints
            if isinstance(constraint, sqlalchemy.CheckConstraint)
        }
        assert list(checks) == ["ck_project_resource_reference_or_payload"]
        assert not [
            (name, entity)
            for name, sqltext in checks.items()
            for entity in db_models.ProjectResourceEntity
            if entity.value in sqltext
        ]


def _add_project(session: orm.Session) -> db_models.Project:
    project = db_models.Project(workspace_id=SANDBOX, name="A project")
    session.add(project)
    session.flush()
    return project
