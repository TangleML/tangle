"""Tests for database_ops: backfill and annotation helpers.

Pipeline Name Resolution Path
==============================

Phase 1 (bulk SQL -- extra_data path):
  pipeline_run.extra_data -> ["pipeline_name"] -> value
       |                          |                 |
       +-- None                   +-- key missing   +-- ""
       v                          v                 v
    SQL NULL (safe)            SQL NULL (safe)   filtered by != ""

Phase 2 (Python -- component_spec path):
  task_spec_dict -> TaskSpec -> component_ref -> spec -> name
       |                |         (required)      |       |
       (always dict)    +-- malformed             +--None +-- None
                        v                         v       +-- ""
                     returns None               None      v
                                                        None
"""

import pytest
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import component_structures as structures
from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend import filter_query_sql


def _make_task_spec(pipeline_name: str = "test-pipeline") -> structures.TaskSpec:
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
    session_factory,
    run_id,
    *,
    task_spec: structures.TaskSpec,
):
    """Replace the execution_node's task_spec JSON with the given TaskSpec.

    Use to test Phase 2 fallback paths where spec is None or name is None,
    since the service's create() requires a valid spec to run.
    """
    with session_factory() as session:
        db_run = session.get(bts.PipelineRun, run_id)
        exec_node = session.get(bts.ExecutionNode, db_run.root_execution_id)
        exec_node.task_spec = task_spec.to_json_dict()
        session.commit()


@pytest.fixture()
def session_factory():
    engine = sqlalchemy.create_engine(
        "sqlite://",
        poolclass=sqlalchemy.pool.StaticPool,
        connect_args={"check_same_thread": False},
    )
    bts._TableBase.metadata.create_all(engine)
    return orm.sessionmaker(engine)


def _create_run(session_factory, service, **kwargs):
    """Create a pipeline run using a fresh session (mirrors production per-request sessions)."""
    with session_factory() as session:
        return service.create(session, **kwargs)


def _delete_annotation(*, session_factory, run_id, key):
    """Remove a write-path annotation so backfill can be tested in isolation."""
    with session_factory() as session:
        session.execute(
            sqlalchemy.delete(bts.PipelineRunAnnotation).where(
                bts.PipelineRunAnnotation.pipeline_run_id == run_id,
                bts.PipelineRunAnnotation.key == key,
            )
        )
        session.commit()


class TestIsAnnotationKeyAlreadyBackfilled:
    def test_false_on_empty_db(self, session_factory):
        engine = session_factory.kw["bind"]
        assert (
            database_ops.is_annotation_key_already_backfilled(
                db_engine=engine,
                key=filter_query_sql.SystemKey.CREATED_BY,
            )
            is False
        )

    def test_false_with_unrelated_annotation(self, session_factory):
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(session_factory, service, root_task=_make_task_spec())
        with session_factory() as session:
            service.set_annotation(session=session, id=run.id, key="team", value="ml")
        engine = session_factory.kw["bind"]
        assert (
            database_ops.is_annotation_key_already_backfilled(
                db_engine=engine,
                key=filter_query_sql.SystemKey.CREATED_BY,
            )
            is False
        )

    def test_true_when_key_exists(self, session_factory):
        service = api_server_sql.PipelineRunsApiService_Sql()
        _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="alice",
        )
        engine = session_factory.kw["bind"]
        assert (
            database_ops.is_annotation_key_already_backfilled(
                db_engine=engine,
                key=filter_query_sql.SystemKey.CREATED_BY,
            )
            is True
        )

    def test_matches_exact_key(self, session_factory):
        """Only returns True for the exact key queried, not other keys."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(session_factory, service, root_task=_make_task_spec())
        with session_factory() as session:
            service.set_annotation(session=session, id=run.id, key="team", value="ml")
        engine = session_factory.kw["bind"]
        assert (
            database_ops.is_annotation_key_already_backfilled(
                db_engine=engine,
                key="team",
            )
            is True
        )
        assert (
            database_ops.is_annotation_key_already_backfilled(
                db_engine=engine,
                key="other_key",
            )
            is False
        )


class TestGetPipelineNameFromTaskSpec:
    """Unit tests for get_pipeline_name_from_task_spec (Phase 2 helper)."""

    def test_returns_name(self):
        """Happy path: task_spec_dict -> TaskSpec -> component_ref -> spec -> [name] OK"""
        task = _make_task_spec("my-pipe")
        result = database_ops.get_pipeline_name_from_task_spec(
            task_spec_dict=task.to_json_dict()
        )
        assert result == "my-pipe"

    def test_returns_none_when_spec_is_none(self):
        """Null point: task_spec_dict -> TaskSpec -> component_ref -> [spec=None] FAIL"""
        task_dict = {"component_ref": {}}
        result = database_ops.get_pipeline_name_from_task_spec(
            task_spec_dict=task_dict,
        )
        assert result is None

    def test_returns_none_when_name_is_none(self):
        """Null point: task_spec_dict -> ... -> spec -> [name=None] FAIL"""
        task_dict = {
            "component_ref": {
                "spec": {
                    "implementation": {
                        "container": {"image": "img"},
                    }
                }
            }
        }
        result = database_ops.get_pipeline_name_from_task_spec(task_spec_dict=task_dict)
        assert result is None

    def test_returns_none_when_name_is_empty(self):
        """Null point: task_spec_dict -> ... -> spec -> [name=""] FAIL"""
        task_dict = {
            "component_ref": {
                "spec": {
                    "name": "",
                    "implementation": {
                        "container": {"image": "img"},
                    },
                }
            }
        }
        result = database_ops.get_pipeline_name_from_task_spec(task_spec_dict=task_dict)
        assert result is None

    def test_returns_none_on_malformed_dict(self):
        """Null point: [task_spec_dict=malformed] -> from_json_dict() raises FAIL"""
        result = database_ops.get_pipeline_name_from_task_spec(
            task_spec_dict={"bad": "data"}
        )
        assert result is None


class TestPipelineNameBackfill:
    """Integration tests for pipeline name backfill (Phase 1 + Phase 2)."""

    # --- Orchestration tests (full backfill flow) ---

    def test_backfill_populates_name(self, session_factory):
        """P1 happy path: extra_data -> ["pipeline_name"] -> [value] OK"""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("my-pipeline"),
        )
        key = filter_query_sql.SystemKey.NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run.id)

        engine = session_factory.kw["bind"]
        database_ops.backfill_pipeline_name_annotations(db_engine=engine)

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == "my-pipeline"

    def test_backfill_populates_name_via_both_phases(self, session_factory):
        """Orchestrator resolves run_a via P1 (extra_data) and run_b via P2 (component_spec)."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.SystemKey.NAME

        run_a = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("from-extra-data"),
        )
        _delete_annotation(session_factory=session_factory, run_id=run_a.id, key=key)

        run_b = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("from-component-spec"),
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
        database_ops.backfill_pipeline_name_annotations(db_engine=engine)

        with session_factory() as session:
            assert (
                service.list_annotations(session=session, id=run_a.id)[key]
                == "from-extra-data"
            )
            assert (
                service.list_annotations(session=session, id=run_b.id)[key]
                == "from-component-spec"
            )

    def test_backfill_skips_when_key_already_exists(self, session_factory):
        """Once any NAME annotation exists, subsequent backfill calls are no-ops."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        engine = session_factory.kw["bind"]
        key = filter_query_sql.SystemKey.NAME

        run_a = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("pipeline-a"),
        )
        _delete_annotation(session_factory=session_factory, run_id=run_a.id, key=key)

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run_a.id)

        database_ops.backfill_pipeline_name_annotations(db_engine=engine)

        with session_factory() as session:
            assert (
                service.list_annotations(session=session, id=run_a.id)[key]
                == "pipeline-a"
            )

        run_b = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("pipeline-b"),
        )
        _delete_annotation(session_factory=session_factory, run_id=run_b.id, key=key)

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run_b.id)

        # Backfill is a no-op because run_a's annotation already exists
        database_ops.backfill_pipeline_name_annotations(db_engine=engine)

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run_b.id)

    # --- Phase 1 tests (_backfill_pipeline_names_from_extra_data) ---

    def test_backfill_phase1_skips_none_extra_data(self, session_factory):
        """P1 null point: [extra_data=None] FAIL"""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("some-name"),
        )
        key = filter_query_sql.SystemKey.NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = None
            session.commit()

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run.id)

        engine = session_factory.kw["bind"]
        database_ops._backfill_pipeline_names_from_extra_data(db_engine=engine)

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    def test_backfill_phase1_skips_missing_key(self, session_factory):
        """P1 null point: extra_data -> [key missing] FAIL"""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("some-name"),
        )
        key = filter_query_sql.SystemKey.NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = {}
            session.commit()

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run.id)

        engine = session_factory.kw["bind"]
        database_ops._backfill_pipeline_names_from_extra_data(db_engine=engine)

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    def test_backfill_phase1_skips_empty_name(self, session_factory):
        """P1 null point: extra_data -> ["pipeline_name"] -> [""] FAIL"""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("some-name"),
        )
        key = filter_query_sql.SystemKey.NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = {"pipeline_name": ""}
            session.commit()

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run.id)

        engine = session_factory.kw["bind"]
        database_ops._backfill_pipeline_names_from_extra_data(db_engine=engine)

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    # --- Phase 2 tests (_backfill_pipeline_names_from_component_spec) ---

    def test_backfill_phase2_fallback_from_component_spec(self, session_factory):
        """P1 null point: [extra_data=None] FAIL
        P2 happy path: task_spec -> component_ref -> spec -> [name] OK"""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("fallback-name"),
        )
        key = filter_query_sql.SystemKey.NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = None
            session.commit()

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run.id)

        engine = session_factory.kw["bind"]
        database_ops._backfill_pipeline_names_from_component_spec(db_engine=engine)

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == "fallback-name"

    def test_backfill_phase2_skips_when_spec_is_none(self, session_factory):
        """P1 null point: [extra_data=None] FAIL
        P2 null point: task_spec -> component_ref -> [spec=None] FAIL"""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("some-name"),
        )
        key = filter_query_sql.SystemKey.NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = None
            session.commit()
        _set_execution_node_task_spec(
            session_factory,
            run.id,
            task_spec=structures.TaskSpec(
                component_ref=structures.ComponentReference(
                    name="placeholder", spec=None
                )
            ),
        )

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run.id)

        engine = session_factory.kw["bind"]
        database_ops._backfill_pipeline_names_from_component_spec(db_engine=engine)

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    def test_backfill_phase2_skips_when_no_name_anywhere(self, session_factory):
        """P1 null point: [extra_data=None] FAIL
        P2 null point: task_spec -> ... -> spec -> [name=None] FAIL"""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("some-name"),
        )
        key = filter_query_sql.SystemKey.NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = None
            session.commit()
        _set_execution_node_task_spec(
            session_factory,
            run.id,
            task_spec=structures.TaskSpec(
                component_ref=structures.ComponentReference(
                    spec=structures.ComponentSpec(
                        name=None,
                        implementation=structures.ContainerImplementation(
                            container=structures.ContainerSpec(image="img")
                        ),
                    )
                )
            ),
        )

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run.id)

        engine = session_factory.kw["bind"]
        database_ops._backfill_pipeline_names_from_component_spec(db_engine=engine)

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    def test_backfill_phase2_skips_when_execution_node_missing(self, session_factory):
        """P2 null point: execution_node for root_execution_id does not exist."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("some-name"),
        )
        key = filter_query_sql.SystemKey.NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = None
            exec_node = session.get(bts.ExecutionNode, db_run.root_execution_id)
            session.delete(exec_node)
            session.commit()

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run.id)

        engine = session_factory.kw["bind"]
        database_ops._backfill_pipeline_names_from_component_spec(db_engine=engine)

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations


class TestCreatedByBackfill:
    def test_backfill_populates_created_by(self, session_factory):
        """The INSERT path produces the correct annotation value."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="alice",
        )
        key = filter_query_sql.SystemKey.CREATED_BY
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run.id)

        engine = session_factory.kw["bind"]
        database_ops.backfill_created_by_annotations(db_engine=engine)

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == "alice"

    def test_backfill_skips_null_created_by(self, session_factory):
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(session_factory, service, root_task=_make_task_spec())

        engine = session_factory.kw["bind"]
        database_ops.backfill_created_by_annotations(db_engine=engine)

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert filter_query_sql.SystemKey.CREATED_BY not in annotations

    def test_backfill_skips_empty_created_by(self, session_factory):
        """Runs with created_by='' are not backfilled."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="",
        )
        key = filter_query_sql.SystemKey.CREATED_BY
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run.id)

        engine = session_factory.kw["bind"]
        database_ops.backfill_created_by_annotations(db_engine=engine)

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    def test_backfill_skips_when_key_already_exists(self, session_factory):
        """Once any CREATED_BY annotation exists, subsequent backfill calls are no-ops."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        engine = session_factory.kw["bind"]
        key = filter_query_sql.SystemKey.CREATED_BY

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

        database_ops.backfill_created_by_annotations(db_engine=engine)

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
        database_ops.backfill_created_by_annotations(db_engine=engine)

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run_bob.id)
