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


class TestCreatedByBackfill:
    def test_backfill_idempotent(self, session_factory):
        service = api_server_sql.PipelineRunsApiService_Sql()

        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="alice",
        )

        engine = session_factory.kw["bind"]

        database_ops.backfill_created_by_annotations(db_engine=engine)
        database_ops.backfill_created_by_annotations(db_engine=engine)

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[filter_query_sql.SystemKey.CREATED_BY] == "alice"

    def test_backfill_skips_null_created_by(self, session_factory):
        service = api_server_sql.PipelineRunsApiService_Sql()

        run = _create_run(session_factory, service, root_task=_make_task_spec())
        assert run.created_by is None

        engine = session_factory.kw["bind"]
        database_ops.backfill_created_by_annotations(db_engine=engine)

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert filter_query_sql.SystemKey.CREATED_BY not in annotations

    def test_backfill_mixed_runs_and_repeated_backfills(self, session_factory):
        """Simulates a realistic sequence: create runs, backfill, create more runs, backfill again.
        Verifies all annotations are correct and no duplicates are created."""
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
