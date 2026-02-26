"""Tests for database_ops: backfill and annotation helpers.

Pipeline Name Resolution Path
==============================

Phase 1 (bulk SQL -- extra_data path):
  pipeline_run.extra_data -> ["pipeline_name"] -> value
       |                          |                 |
       +-- None                   +-- key missing   +-- ""
       v                          v                 v
    SQL NULL (safe)            SQL NULL (safe)   filtered by != ""

Phase 2 (bulk SQL -- component_spec JSON path):
  execution_node.task_spec -> 'componentRef' -> 'spec' ->> 'name'
       |                          |                 |         |
       +-- NULL                   +-- key missing   +-- null  +-- null
       v                          v                 v         v
    SQL NULL (safe)            SQL NULL (safe)   SQL NULL   SQL NULL
"""

import pytest
import sqlalchemy
from sqlalchemy import orm
from typing import Any

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


def _set_execution_node_task_spec(
    *,
    session_factory: orm.sessionmaker,
    run_id: str,
    task_spec: structures.TaskSpec,
) -> None:
    """Replace the execution_node's task_spec JSON with the given TaskSpec.

    Use to test Phase 2 fallback paths where spec is None or name is None,
    since the service's create() requires a valid spec to run.
    """
    with session_factory() as session:
        db_run = session.get(bts.PipelineRun, run_id)
        exec_node = session.get(bts.ExecutionNode, db_run.root_execution_id)
        exec_node.task_spec = task_spec.to_json_dict()
        session.commit()


def _set_execution_node_task_spec_raw(
    *,
    session_factory: orm.sessionmaker,
    run_id: str,
    task_spec_dict: dict[str, Any] | None,
) -> None:
    """Set task_spec to an arbitrary dict (or None) bypassing Pydantic.

    Use to test JSON paths that Pydantic's TaskSpec cannot represent
    (e.g. empty dict, missing componentRef, null task_spec).
    """
    with session_factory() as session:
        db_run = session.get(bts.PipelineRun, run_id)
        exec_node = session.get(bts.ExecutionNode, db_run.root_execution_id)
        exec_node.task_spec = task_spec_dict
        session.commit()


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


def _delete_annotation(
    *,
    session_factory: orm.sessionmaker,
    run_id: str,
    key: filter_query_sql.PipelineRunAnnotationSystemKey,
) -> None:
    """Remove a write-path annotation so backfill can be tested in isolation."""
    with session_factory() as session:
        session.execute(
            sqlalchemy.delete(bts.PipelineRunAnnotation).where(
                bts.PipelineRunAnnotation.pipeline_run_id == run_id,
                bts.PipelineRunAnnotation.key == key,
            )
        )
        session.commit()


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


class TestPipelineNameBackfill:
    """Integration tests for pipeline name backfill (Phase 1 + Phase 2)."""

    # --- Orchestration tests (full backfill flow) ---

    def test_backfill_populates_name(self, session_factory):
        """P1 happy path: extra_data -> ["pipeline_name"] -> [value] OK"""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(pipeline_name="my-pipeline"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run.id)

        engine = session_factory.kw["bind"]
        database_ops._backfill_pipeline_run_name_annotations(db_engine=engine)

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == "my-pipeline"

    def test_backfill_populates_name_via_both_phases(self, session_factory):
        """Orchestrator resolves run_a via P1 (extra_data) and run_b via P2 (component_spec)."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME

        run_a = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(pipeline_name="from-extra-data"),
        )
        _delete_annotation(session_factory=session_factory, run_id=run_a.id, key=key)

        run_b = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(pipeline_name="from-component-spec"),
        )
        _delete_annotation(session_factory=session_factory, run_id=run_b.id, key=key)
        with session_factory() as session:
            db_run_b = session.get(bts.PipelineRun, run_b.id)
            db_run_b.extra_data = None
            session.commit()

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run_a.id)
            assert key not in service.list_annotations(session=session, id=run_b.id)

        engine = session_factory.kw["bind"]
        database_ops._backfill_pipeline_run_name_annotations(db_engine=engine)

        with session_factory() as session:
            assert (
                service.list_annotations(session=session, id=run_a.id)[key]
                == "from-extra-data"
            )
            assert (
                service.list_annotations(session=session, id=run_b.id)[key]
                == "from-component-spec"
            )

    def test_backfill_phase2_skips_phase1_insert_within_transaction(
        self, session_factory
    ):
        """Phase 2's anti-join sees Phase 1's insert within the same transaction.

        A run that both phases could match (has extra_data AND component_spec
        name). Phase 1 inserts the annotation; Phase 2 must skip it via the
        anti-join, not insert a duplicate. Proves execute() writes are visible
        within the shared transaction buffer.
        """
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME

        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(pipeline_name="shared-name"),
        )
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run.id)

        engine = session_factory.kw["bind"]
        database_ops._backfill_pipeline_run_name_annotations(db_engine=engine)

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == "shared-name"

        # Verify exactly one annotation row — no duplicate from Phase 2.
        with session_factory() as session:
            count = (
                session.query(bts.PipelineRunAnnotation)
                .filter(
                    bts.PipelineRunAnnotation.pipeline_run_id == run.id,
                    bts.PipelineRunAnnotation.key == key,
                )
                .count()
            )
        assert count == 1

    def test_backfill_skips_when_key_already_exists(self, session_factory):
        """Once any NAME annotation exists, subsequent backfill calls are no-ops."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        engine = session_factory.kw["bind"]
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME

        run_a = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(pipeline_name="pipeline-a"),
        )
        _delete_annotation(session_factory=session_factory, run_id=run_a.id, key=key)

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run_a.id)

        database_ops._backfill_pipeline_run_name_annotations(db_engine=engine)

        with session_factory() as session:
            assert (
                service.list_annotations(session=session, id=run_a.id)[key]
                == "pipeline-a"
            )

        run_b = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(pipeline_name="pipeline-b"),
        )
        _delete_annotation(session_factory=session_factory, run_id=run_b.id, key=key)

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run_b.id)

        # Backfill is a no-op because run_a's annotation already exists
        database_ops._backfill_pipeline_run_name_annotations(db_engine=engine)

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run_b.id)

    # --- Phase 1 tests (_backfill_pipeline_names_from_extra_data) ---

    def test_backfill_phase1_skips_none_extra_data(self, session_factory):
        """P1 null point: [extra_data=None] FAIL"""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(pipeline_name="some-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = None
            session.commit()

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run.id)

        with session_factory() as session:
            database_ops._backfill_pipeline_names_from_extra_data(session=session)
            session.commit()

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    def test_backfill_phase1_skips_missing_key(self, session_factory):
        """P1 null point: extra_data -> [key missing] FAIL"""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(pipeline_name="some-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = {}
            session.commit()

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run.id)

        with session_factory() as session:
            database_ops._backfill_pipeline_names_from_extra_data(session=session)
            session.commit()

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    def test_backfill_phase1_inserts_empty_name(self, session_factory):
        """P1 valid: extra_data -> ["pipeline_name"] -> [""]
        Passes: empty string is a valid name, annotation inserted with value=""."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(pipeline_name="some-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = {"pipeline_name": ""}
            session.commit()

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run.id)

        with session_factory() as session:
            database_ops._backfill_pipeline_names_from_extra_data(session=session)
            session.commit()

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == ""

    # --- Phase 2 tests (_backfill_pipeline_names_from_component_spec) ---
    # Ordered by JSON traversal depth (0 -> 4).
    # Path: ExecutionNode row -> task_spec -> componentRef -> spec -> name

    def test_p2_depth0_execution_node_missing(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """Path: [ExecutionNode row missing] -> task_spec -> componentRef -> spec -> name
        Fails at: INNER JOIN finds no execution_node, row excluded."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(pipeline_name="some-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = None
            exec_node = session.get(bts.ExecutionNode, db_run.root_execution_id)
            session.delete(exec_node)
            session.commit()

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run.id)

        with session_factory() as session:
            database_ops._backfill_pipeline_names_from_component_spec(session=session)
            session.commit()

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    def test_p2_depth1_task_spec_null(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """Path: task_spec=[NULL] -> componentRef -> spec -> name
        Fails at: task_spec column is NULL, JSON extraction returns NULL."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(pipeline_name="some-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = None
            session.commit()
        _set_execution_node_task_spec_raw(
            session_factory=session_factory,
            run_id=run.id,
            task_spec_dict=None,
        )

        with session_factory() as session:
            database_ops._backfill_pipeline_names_from_component_spec(session=session)
            session.commit()

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    def test_p2_depth1_task_spec_empty(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """Path: task_spec={} -> [componentRef missing] -> spec -> name
        Fails at: task_spec is empty dict, 'componentRef' key absent."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(pipeline_name="some-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = None
            session.commit()
        _set_execution_node_task_spec_raw(
            session_factory=session_factory,
            run_id=run.id,
            task_spec_dict={},
        )

        with session_factory() as session:
            database_ops._backfill_pipeline_names_from_component_spec(session=session)
            session.commit()

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    def test_p2_depth2_component_ref_missing(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """Path: task_spec -> [componentRef missing] -> spec -> name
        Fails at: 'componentRef' key absent from task_spec."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(pipeline_name="some-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = None
            session.commit()
        _set_execution_node_task_spec_raw(
            session_factory=session_factory,
            run_id=run.id,
            task_spec_dict={"other_key": "value"},
        )

        with session_factory() as session:
            database_ops._backfill_pipeline_names_from_component_spec(session=session)
            session.commit()

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    def test_p2_depth2_component_ref_null(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """Path: task_spec -> [componentRef=null] -> spec -> name
        Fails at: componentRef is null, JSON extraction returns NULL."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(pipeline_name="some-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = None
            session.commit()
        _set_execution_node_task_spec_raw(
            session_factory=session_factory,
            run_id=run.id,
            task_spec_dict={"componentRef": None},
        )

        with session_factory() as session:
            database_ops._backfill_pipeline_names_from_component_spec(session=session)
            session.commit()

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    def test_p2_depth3_spec_null(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """Path: task_spec -> componentRef -> [spec=null] -> name
        Fails at: spec is null, JSON extraction returns NULL."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(pipeline_name="some-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = None
            session.commit()
        _set_execution_node_task_spec(
            session_factory=session_factory,
            run_id=run.id,
            task_spec=structures.TaskSpec(
                component_ref=structures.ComponentReference(
                    name="placeholder",
                    spec=None,
                )
            ),
        )

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run.id)

        with session_factory() as session:
            database_ops._backfill_pipeline_names_from_component_spec(session=session)
            session.commit()

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    def test_p2_depth4_name_null(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """Path: task_spec -> componentRef -> spec -> [name=null]
        Fails at: name is null, JSON extraction returns NULL."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(pipeline_name="some-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = None
            session.commit()
        _set_execution_node_task_spec(
            session_factory=session_factory,
            run_id=run.id,
            task_spec=structures.TaskSpec(
                component_ref=structures.ComponentReference(
                    spec=structures.ComponentSpec(
                        name=None,
                    )
                )
            ),
        )

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run.id)

        with session_factory() as session:
            database_ops._backfill_pipeline_names_from_component_spec(session=session)
            session.commit()

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    def test_p2_depth4_name_empty_string(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """Path: task_spec -> componentRef -> spec -> [name=""]
        Passes: empty string is a valid name, annotation inserted with value=""."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(pipeline_name="some-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = None
            session.commit()
        _set_execution_node_task_spec_raw(
            session_factory=session_factory,
            run_id=run.id,
            task_spec_dict={
                "componentRef": {"spec": {"name": ""}},
            },
        )

        with session_factory() as session:
            database_ops._backfill_pipeline_names_from_component_spec(session=session)
            session.commit()

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == ""

    def test_p2_depth4_name_valid(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """Path: task_spec -> componentRef -> spec -> [name="fallback-name"]
        Passes: valid name extracted, annotation inserted."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(pipeline_name="fallback-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = None
            session.commit()

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run.id)

        with session_factory() as session:
            database_ops._backfill_pipeline_names_from_component_spec(session=session)
            session.commit()

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == "fallback-name"


class TestCreatedByBackfill:
    def test_backfill_populates_created_by(
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
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run.id)

        database_ops._backfill_pipeline_run_created_by_annotations(db_engine=engine)

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == "alice"

    def test_backfill_skips_null_created_by(self, session_factory):
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(session_factory, service, root_task=_make_task_spec())
        engine = session_factory.kw["bind"]

        database_ops._backfill_pipeline_run_created_by_annotations(db_engine=engine)

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert (
            filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY
            not in annotations
        )

    def test_backfill_skips_empty_created_by(self, session_factory):
        """Runs with created_by='' are not backfilled."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="",
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run.id)

        engine = session_factory.kw["bind"]
        database_ops._backfill_pipeline_run_created_by_annotations(db_engine=engine)

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    def test_backfill_skips_when_key_already_exists(self, session_factory):
        """Once any CREATED_BY annotation exists, subsequent backfill calls are no-ops."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        engine = session_factory.kw["bind"]
        key = filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY

        run_alice = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="alice",
        )
        _delete_annotation(
            session_factory=session_factory, run_id=run_alice.id, key=key
        )

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run_alice.id)

        database_ops._backfill_pipeline_run_created_by_annotations(db_engine=engine)

        with session_factory() as session:
            assert (
                service.list_annotations(session=session, id=run_alice.id)[key]
                == "alice"
            )

        run_bob = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="bob",
        )
        _delete_annotation(session_factory=session_factory, run_id=run_bob.id, key=key)

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run_bob.id)

        # Backfill is a no-op because run_alice's annotation already exists
        database_ops._backfill_pipeline_run_created_by_annotations(db_engine=engine)

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run_bob.id)

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
