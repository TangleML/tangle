from typing import Any

import pytest
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import component_structures as structures
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend import filter_query_sql


def _make_task_spec(
    *,
    pipeline_name: str = "test-pipeline",
) -> structures.TaskSpec:
    return structures.TaskSpec(
        component_ref=structures.ComponentReference(
            spec=structures.ComponentSpec(
                name=pipeline_name,
                implementation=structures.ContainerImplementation(
                    container=structures.ContainerSpec(image="test-image")
                ),
            )
        )
    )


@pytest.fixture()
def session_factory() -> orm.sessionmaker:
    db_engine = database_ops.create_db_engine(database_uri="sqlite://")
    bts._TableBase.metadata.create_all(db_engine)
    return orm.sessionmaker(db_engine)


def _create_run(
    session_factory: orm.sessionmaker,
    service: api_server_sql.PipelineRunsApiService_Sql,
    **kwargs,
) -> api_server_sql.PipelineRunResponse:
    """Create a pipeline run using a fresh session (mirrors production per-request sessions)."""
    with session_factory() as session:
        return service.create(session, **kwargs)


def _get_index_names(
    *,
    engine: sqlalchemy.Engine,
    table_name: str,
) -> set[str]:
    inspector = sqlalchemy.inspect(engine)
    return {idx["name"] for idx in inspector.get_indexes(table_name)}


class TestMigrateDb:
    """Verify migrate_db creates indexes on a pre-existing DB missing them."""

    @pytest.fixture()
    def bare_engine(self) -> sqlalchemy.Engine:
        """Create tables then drop all indexes so migrate_db can recreate them."""
        db_engine = database_ops.create_db_engine(database_uri="sqlite://")
        bts._TableBase.metadata.create_all(db_engine)

        with db_engine.connect() as conn:
            for table_name in ("execution_node", "pipeline_run_annotation"):
                for idx_name in _get_index_names(
                    engine=db_engine, table_name=table_name
                ):
                    conn.execute(sqlalchemy.text(f"DROP INDEX IF EXISTS {idx_name}"))
            conn.commit()
        return db_engine

    def test_creates_execution_node_cache_key_index(
        self,
        bare_engine: sqlalchemy.Engine,
    ) -> None:
        assert bts.ExecutionNode._IX_EXECUTION_NODE_CACHE_KEY not in _get_index_names(
            engine=bare_engine,
            table_name="execution_node",
        )
        database_ops.migrate_db(db_engine=bare_engine)
        assert bts.ExecutionNode._IX_EXECUTION_NODE_CACHE_KEY in _get_index_names(
            engine=bare_engine,
            table_name="execution_node",
        )

    def test_creates_annotation_run_id_key_value_index(
        self,
        bare_engine: sqlalchemy.Engine,
    ) -> None:
        assert (
            bts.PipelineRunAnnotation._IX_ANNOTATION_RUN_ID_KEY_VALUE
            not in _get_index_names(
                engine=bare_engine, table_name="pipeline_run_annotation"
            )
        )
        database_ops.migrate_db(db_engine=bare_engine)
        assert (
            bts.PipelineRunAnnotation._IX_ANNOTATION_RUN_ID_KEY_VALUE
            in _get_index_names(
                engine=bare_engine, table_name="pipeline_run_annotation"
            )
        )

    def test_idempotent(
        self,
        bare_engine: sqlalchemy.Engine,
    ) -> None:
        database_ops.migrate_db(db_engine=bare_engine)
        indexes_after_first = {
            "execution_node": _get_index_names(
                engine=bare_engine, table_name="execution_node"
            ),
            "pipeline_run_annotation": _get_index_names(
                engine=bare_engine,
                table_name="pipeline_run_annotation",
            ),
        }
        database_ops.migrate_db(db_engine=bare_engine)
        indexes_after_second = {
            "execution_node": _get_index_names(
                engine=bare_engine, table_name="execution_node"
            ),
            "pipeline_run_annotation": _get_index_names(
                engine=bare_engine,
                table_name="pipeline_run_annotation",
            ),
        }
        assert indexes_after_first == indexes_after_second


class TestIsAnnotationKeyAlreadyBackfilled:
    def test_false_on_empty_db(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        with session_factory() as session:
            assert (
                database_ops._is_pipeline_run_annotation_key_already_backfilled(
                    session=session,
                    key=filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
                )
                is False
            )

    def test_false_with_unrelated_annotation(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(session_factory, service, root_task=_make_task_spec())
        with session_factory() as session:
            service.set_annotation(session=session, id=run.id, key="team", value="ml")
        with session_factory() as session:
            assert (
                database_ops._is_pipeline_run_annotation_key_already_backfilled(
                    session=session,
                    key=filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
                )
                is False
            )

    def test_true_when_key_exists(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="alice",
        )
        with session_factory() as session:
            assert (
                database_ops._is_pipeline_run_annotation_key_already_backfilled(
                    session=session,
                    key=filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
                )
                is True
            )

    def test_matches_exact_key(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """Only returns True for the exact key queried, not other keys."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(session_factory, service, root_task=_make_task_spec())
        with session_factory() as session:
            service.set_annotation(session=session, id=run.id, key="team", value="ml")
        with session_factory() as session:
            assert (
                database_ops._is_pipeline_run_annotation_key_already_backfilled(
                    session=session,
                    key="team",
                )
                is True
            )
            assert (
                database_ops._is_pipeline_run_annotation_key_already_backfilled(
                    session=session,
                    key="other_key",
                )
                is False
            )

    def test_true_after_backfill(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """Create a run, delete its write-path annotation, then backfill."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        engine = session_factory.kw["bind"]
        key = filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY

        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="bob",
        )
        with session_factory() as session:
            session.query(bts.PipelineRunAnnotation).filter_by(
                pipeline_run_id=run.id,
                key=key,
            ).delete()
            session.commit()

        with session_factory() as session:
            assert (
                database_ops._is_pipeline_run_annotation_key_already_backfilled(
                    session=session,
                    key=key,
                )
                is False
            )
        database_ops._backfill_pipeline_run_created_by_annotations(db_engine=engine)
        with session_factory() as session:
            assert (
                database_ops._is_pipeline_run_annotation_key_already_backfilled(
                    session=session,
                    key=key,
                )
                is True
            )


class TestCreatedByBackfill:
    def test_backfill_populates_annotation_value(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """The INSERT path produces the correct annotation value."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        engine = session_factory.kw["bind"]
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="alice",
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY

        # Remove write-path annotation so the backfill INSERT actually runs
        with session_factory() as session:
            session.query(bts.PipelineRunAnnotation).filter_by(
                pipeline_run_id=run.id,
                key=key,
            ).delete()
            session.commit()

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run.id)

        database_ops._backfill_pipeline_run_created_by_annotations(db_engine=engine)

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == "alice"

    def test_backfill_skips_empty_created_by(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """Runs with created_by='' are not backfilled (isnot(None) passes but empty string has no value)."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        engine = session_factory.kw["bind"]
        key = filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY

        # Create a run then set created_by to empty string directly in DB
        run = _create_run(session_factory, service, root_task=_make_task_spec())
        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.created_by = ""
            session.commit()

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run.id)

        database_ops._backfill_pipeline_run_created_by_annotations(db_engine=engine)

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    def test_backfill_idempotent(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        engine = session_factory.kw["bind"]

        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="alice",
        )

        database_ops._backfill_pipeline_run_created_by_annotations(db_engine=engine)
        database_ops._backfill_pipeline_run_created_by_annotations(db_engine=engine)

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert (
            annotations[filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY]
            == "alice"
        )

    def test_backfill_skips_null_created_by(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        engine = session_factory.kw["bind"]

        run = _create_run(session_factory, service, root_task=_make_task_spec())
        assert run.created_by is None

        database_ops._backfill_pipeline_run_created_by_annotations(db_engine=engine)

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert (
            filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY
            not in annotations
        )

    def test_backfill_mixed_runs_and_repeated_backfills(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """Simulates a realistic sequence: create runs, backfill, create more runs, backfill again.
        Verifies all annotations are correct and no duplicates are created."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        engine = session_factory.kw["bind"]
        key = filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY

        run_alice = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="alice",
        )
        run_no_user = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
        )

        database_ops._backfill_pipeline_run_created_by_annotations(db_engine=engine)

        run_bob = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="bob",
        )
        run_alice2 = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="alice",
        )

        database_ops._backfill_pipeline_run_created_by_annotations(db_engine=engine)
        database_ops._backfill_pipeline_run_created_by_annotations(db_engine=engine)

        with session_factory() as session:
            assert (
                service.list_annotations(session=session, id=run_alice.id)[key]
                == "alice"
            )
            assert key not in service.list_annotations(
                session=session, id=run_no_user.id
            )
            assert (
                service.list_annotations(session=session, id=run_bob.id)[key] == "bob"
            )
            assert (
                service.list_annotations(session=session, id=run_alice2.id)[key]
                == "alice"
            )

    def test_backfill_uses_single_session(
        self,
        session_factory: orm.sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Check and insert happen in the same session (single transaction)."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="alice",
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY
        with session_factory() as session:
            session.query(bts.PipelineRunAnnotation).filter_by(
                pipeline_run_id=run.id,
                key=key,
            ).delete()
            session.commit()

        engine = session_factory.kw["bind"]
        session_count = 0
        _original_session_init = orm.Session.__init__

        def _counting_init(
            self: Any,
            *args: Any,
            **kwargs: Any,
        ) -> None:
            nonlocal session_count
            session_count += 1
            _original_session_init(self, *args, **kwargs)

        monkeypatch.setattr(orm.Session, "__init__", _counting_init)

        database_ops._backfill_pipeline_run_created_by_annotations(db_engine=engine)

        assert session_count == 1
