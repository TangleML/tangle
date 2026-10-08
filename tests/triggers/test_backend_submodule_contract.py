"""Guards the one upstream method the trigger sink needs but does not own.

The sink has to write a `TriggerHistory` row and the pipeline run it points at in a single
transaction, so it cannot use `PipelineRunsApiService_Sql.create` — that method opens its own
`session.begin()` and commits. It needs `_create_in_transaction`, which flushes so the caller
can read `pipeline_run.id` but leaves committing to the caller.

That method arrives with TangleML/tangle#344, and the `backend/` submodule here is still pinned
below it. These tests are therefore **red on purpose** until the pointer is bumped to the merge
commit: they are what stops Tangle landing a sink against an API the pinned submodule
does not expose. A leading underscore means upstream promises nothing, so the shape the sink
depends on — the parameters, the flush, the absence of a commit — is asserted here rather than
assumed.
"""

import inspect

import pytest
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend import api_server_sql, database_ops
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import component_structures as structures

_METHOD_NAME = "_create_in_transaction"


def _make_task_spec(*, pipeline_name: str) -> structures.TaskSpec:
    return structures.TaskSpec(
        component_ref=structures.ComponentReference(
            spec=structures.ComponentSpec(
                name=pipeline_name,
                implementation=structures.ContainerImplementation(
                    container=structures.ContainerSpec(image="test-image:latest"),
                ),
            ),
        ),
    )


def _count(*, session: orm.Session, table: type) -> int:
    return session.scalar(sqlalchemy.select(sqlalchemy.func.count()).select_from(table))


@pytest.fixture()
def session_factory() -> orm.sessionmaker:
    engine = database_ops.create_db_engine(database_uri="sqlite://")
    bts._TableBase.metadata.create_all(engine)
    return orm.sessionmaker(engine)


@pytest.fixture()
def service() -> api_server_sql.PipelineRunsApiService_Sql:
    return api_server_sql.PipelineRunsApiService_Sql()


class TestPipelineRunCreationInACallerOwnedTransaction:
    def test_the_method_exists(self) -> None:
        assert hasattr(api_server_sql.PipelineRunsApiService_Sql, _METHOD_NAME), (
            f"the pinned backend/ submodule has no {_METHOD_NAME}; "
            "bump it to the merge commit of TangleML/tangle#344"
        )

    def test_it_takes_the_arguments_the_sink_passes(self) -> None:
        signature = inspect.signature(
            getattr(api_server_sql.PipelineRunsApiService_Sql, _METHOD_NAME)
        )
        assert {"session", "root_task", "annotations", "created_by"} <= set(
            signature.parameters
        )

    def test_it_flushes_so_the_caller_can_use_the_run_id(
        self, session_factory: orm.sessionmaker, service: object
    ) -> None:
        with session_factory() as session:
            session.begin()
            pipeline_run = getattr(service, _METHOD_NAME)(
                session, root_task=_make_task_spec(pipeline_name="sink-flush")
            )
            # The sink writes this id into TriggerHistory.pipeline_run_id, so it has to be
            # readable before anything is committed.
            assert pipeline_run.id is not None
            session.rollback()

    def test_it_does_not_commit_so_the_caller_can_still_roll_back(
        self, session_factory: orm.sessionmaker, service: object
    ) -> None:
        with session_factory() as session:
            session.begin()
            getattr(service, _METHOD_NAME)(
                session,
                root_task=_make_task_spec(pipeline_name="sink-rollback"),
            )
            session.rollback()

        # An internal commit creeping back upstream would leave these rows behind, and the
        # sink would start a real pipeline for a trigger whose history row never persisted.
        with session_factory() as session:
            assert _count(session=session, table=bts.PipelineRun) == 0
            assert _count(session=session, table=bts.ExecutionNode) == 0
