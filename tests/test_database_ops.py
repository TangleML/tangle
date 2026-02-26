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


class TestPipelineNameBackfill:
    def test_backfill_populates_name(self, session_factory):
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

    def test_backfill_idempotent(self, session_factory):
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("my-pipeline"),
        )
        key = filter_query_sql.SystemKey.NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        engine = session_factory.kw["bind"]
        database_ops.backfill_pipeline_name_annotations(db_engine=engine)
        database_ops.backfill_pipeline_name_annotations(db_engine=engine)

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == "my-pipeline"

    def test_backfill_skips_empty_extra_data(self, session_factory):
        """Runs with extra_data={} (no pipeline_name key) are skipped."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("some-name"),
        )
        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = {}
            session.commit()

        key = filter_query_sql.SystemKey.NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        engine = session_factory.kw["bind"]
        database_ops.backfill_pipeline_name_annotations(db_engine=engine)

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    def test_backfill_mixed_runs_and_repeated_backfills(self, session_factory):
        service = api_server_sql.PipelineRunsApiService_Sql()
        engine = session_factory.kw["bind"]
        key = filter_query_sql.SystemKey.NAME

        run_a = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("pipeline-a"),
        )
        _delete_annotation(session_factory=session_factory, run_id=run_a.id, key=key)

        database_ops.backfill_pipeline_name_annotations(db_engine=engine)

        run_b = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("pipeline-b"),
        )
        _delete_annotation(session_factory=session_factory, run_id=run_b.id, key=key)

        database_ops.backfill_pipeline_name_annotations(db_engine=engine)
        database_ops.backfill_pipeline_name_annotations(db_engine=engine)

        with session_factory() as session:
            assert (
                service.list_annotations(session=session, id=run_a.id)[key]
                == "pipeline-a"
            )
            assert (
                service.list_annotations(session=session, id=run_b.id)[key]
                == "pipeline-b"
            )


class TestCreatedByBackfill:
    def test_backfill_populates_created_by(self, session_factory):
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

    def test_backfill_idempotent(self, session_factory):
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="alice",
        )
        key = filter_query_sql.SystemKey.CREATED_BY
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        engine = session_factory.kw["bind"]
        database_ops.backfill_created_by_annotations(db_engine=engine)
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
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="",
        )
        key = filter_query_sql.SystemKey.CREATED_BY
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        engine = session_factory.kw["bind"]
        database_ops.backfill_created_by_annotations(db_engine=engine)

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    def test_backfill_mixed_runs_and_repeated_backfills(self, session_factory):
        service = api_server_sql.PipelineRunsApiService_Sql()
        engine = session_factory.kw["bind"]
        key = filter_query_sql.SystemKey.CREATED_BY

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
        _delete_annotation(
            session_factory=session_factory, run_id=run_alice.id, key=key
        )

        database_ops.backfill_created_by_annotations(db_engine=engine)

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
        _delete_annotation(session_factory=session_factory, run_id=run_bob.id, key=key)
        _delete_annotation(
            session_factory=session_factory, run_id=run_alice2.id, key=key
        )

        database_ops.backfill_created_by_annotations(db_engine=engine)
        database_ops.backfill_created_by_annotations(db_engine=engine)

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
