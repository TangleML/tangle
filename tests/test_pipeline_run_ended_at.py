"""Tests for the system/pipeline_run.ended_at annotation kept by orchestrator_sql."""

import datetime
from typing import Callable

import pytest
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import component_structures as structures
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend import filter_query_sql
from cloud_pipelines_backend import orchestrator_sql  # noqa: F401

_ENDED_AT = filter_query_sql.PipelineRunAnnotationSystemKey.ENDED_AT
_Status = bts.ContainerExecutionStatus


def _container_component() -> structures.ComponentSpec:
    return structures.ComponentSpec(
        implementation=structures.ContainerImplementation(
            container=structures.ContainerSpec(image="python")
        ),
    )


def _graph_task(*, task_ids: list[str]) -> structures.TaskSpec:
    tasks = {
        task_id: structures.TaskSpec(
            component_ref=structures.ComponentReference(spec=_container_component())
        )
        for task_id in task_ids
    }
    return structures.TaskSpec(
        component_ref=structures.ComponentReference(
            spec=structures.ComponentSpec(
                implementation=structures.GraphImplementation(
                    graph=structures.GraphSpec(tasks=tasks)
                ),
            )
        )
    )


@pytest.fixture()
def session_factory() -> Callable[[], orm.Session]:
    db_engine = database_ops.create_db_engine_and_migrate_db(database_uri="sqlite://")
    return lambda: orm.Session(bind=db_engine)


def _create_run(
    *,
    session_factory: Callable[[], orm.Session],
    root_task: structures.TaskSpec,
) -> str:
    return (
        api_server_sql.PipelineRunsApiService_Sql()
        .create(session=session_factory(), root_task=root_task, created_by="ada")
        .id
    )


def _task_node(*, session: orm.Session, task_id: str) -> bts.ExecutionNode:
    return session.scalars(
        sqlalchemy.select(bts.ExecutionNode).where(
            bts.ExecutionNode.task_id_in_parent_execution == task_id
        )
    ).one()


def _ended_at(*, session: orm.Session, pipeline_run_id: str) -> str | None:
    return session.scalar(
        sqlalchemy.select(bts.PipelineRunAnnotation.value).where(
            bts.PipelineRunAnnotation.pipeline_run_id == pipeline_run_id,
            bts.PipelineRunAnnotation.key == _ENDED_AT,
        )
    )


def _set_statuses(
    *,
    session_factory: Callable[[], orm.Session],
    statuses: dict[str, bts.ContainerExecutionStatus],
) -> None:
    with session_factory() as session:
        for task_id, status in statuses.items():
            _task_node(session=session, task_id=task_id).container_execution_status = (
                status
            )
        session.commit()


class TestPipelineRunEndedAt:
    def test_no_annotation_while_any_execution_is_unfinished(
        self, session_factory: Callable[[], orm.Session]
    ) -> None:
        run_id = _create_run(
            session_factory=session_factory, root_task=_graph_task(task_ids=["a", "b"])
        )
        _set_statuses(
            session_factory=session_factory, statuses={"a": _Status.SUCCEEDED}
        )

        with session_factory() as session:
            assert _ended_at(session=session, pipeline_run_id=run_id) is None

    def test_annotation_written_when_the_last_execution_ends(
        self, session_factory: Callable[[], orm.Session]
    ) -> None:
        run_id = _create_run(
            session_factory=session_factory, root_task=_graph_task(task_ids=["a", "b"])
        )
        _set_statuses(
            session_factory=session_factory, statuses={"a": _Status.SUCCEEDED}
        )
        _set_statuses(session_factory=session_factory, statuses={"b": _Status.FAILED})

        with session_factory() as session:
            value = _ended_at(session=session, pipeline_run_id=run_id)
        assert value is not None
        assert datetime.datetime.fromisoformat(value).tzinfo is not None

    def test_every_ended_status_counts(
        self, session_factory: Callable[[], orm.Session]
    ) -> None:
        ended = sorted(bts.CONTAINER_STATUSES_ENDED)
        task_ids = [status.value.lower() for status in ended]
        run_id = _create_run(
            session_factory=session_factory, root_task=_graph_task(task_ids=task_ids)
        )
        _set_statuses(
            session_factory=session_factory, statuses=dict(zip(task_ids, ended))
        )

        with session_factory() as session:
            assert _ended_at(session=session, pipeline_run_id=run_id) is not None

    def test_annotation_removed_when_an_execution_leaves_ended(
        self, session_factory: Callable[[], orm.Session]
    ) -> None:
        run_id = _create_run(
            session_factory=session_factory, root_task=_graph_task(task_ids=["a"])
        )
        _set_statuses(
            session_factory=session_factory, statuses={"a": _Status.SUCCEEDED}
        )
        _set_statuses(session_factory=session_factory, statuses={"a": _Status.QUEUED})

        with session_factory() as session:
            assert _ended_at(session=session, pipeline_run_id=run_id) is None

    def test_moving_between_ended_statuses_keeps_the_first_timestamp(
        self, session_factory: Callable[[], orm.Session]
    ) -> None:
        run_id = _create_run(
            session_factory=session_factory, root_task=_graph_task(task_ids=["a"])
        )
        _set_statuses(
            session_factory=session_factory, statuses={"a": _Status.SUCCEEDED}
        )
        with session_factory() as session:
            first = _ended_at(session=session, pipeline_run_id=run_id)
        _set_statuses(session_factory=session_factory, statuses={"a": _Status.FAILED})

        with session_factory() as session:
            assert _ended_at(session=session, pipeline_run_id=run_id) == first

    def test_transition_flushed_before_commit_is_still_recorded(
        self, session_factory: Callable[[], orm.Session]
    ) -> None:
        run_id = _create_run(
            session_factory=session_factory, root_task=_graph_task(task_ids=["a"])
        )
        with session_factory() as session:
            _task_node(session=session, task_id="a").container_execution_status = (
                _Status.SUCCEEDED
            )
            session.flush()
            session.scalar(sqlalchemy.select(bts.PipelineRun.id))
            session.commit()

        with session_factory() as session:
            assert _ended_at(session=session, pipeline_run_id=run_id) is not None

    def test_rolled_back_transition_leaves_no_annotation(
        self, session_factory: Callable[[], orm.Session]
    ) -> None:
        run_id = _create_run(
            session_factory=session_factory, root_task=_graph_task(task_ids=["a"])
        )
        with session_factory() as session:
            _task_node(session=session, task_id="a").container_execution_status = (
                _Status.SUCCEEDED
            )
            session.flush()
            session.rollback()

        with session_factory() as session:
            assert _ended_at(session=session, pipeline_run_id=run_id) is None

    def test_container_root_run(
        self, session_factory: Callable[[], orm.Session]
    ) -> None:
        run_id = _create_run(
            session_factory=session_factory,
            root_task=structures.TaskSpec(
                component_ref=structures.ComponentReference(spec=_container_component())
            ),
        )
        with session_factory() as session:
            root_id = session.get(bts.PipelineRun, run_id).root_execution_id
            session.get(bts.ExecutionNode, root_id).container_execution_status = (
                _Status.SUCCEEDED
            )
            session.commit()

        with session_factory() as session:
            assert _ended_at(session=session, pipeline_run_id=run_id) is not None

    def test_other_runs_are_untouched(
        self, session_factory: Callable[[], orm.Session]
    ) -> None:
        ended_run = _create_run(
            session_factory=session_factory, root_task=_graph_task(task_ids=["a"])
        )
        running_run = _create_run(
            session_factory=session_factory, root_task=_graph_task(task_ids=["b"])
        )
        _set_statuses(
            session_factory=session_factory, statuses={"a": _Status.SUCCEEDED}
        )

        with session_factory() as session:
            assert _ended_at(session=session, pipeline_run_id=ended_run) is not None
            assert _ended_at(session=session, pipeline_run_id=running_run) is None

    def test_users_cannot_set_the_annotation(
        self, session_factory: Callable[[], orm.Session]
    ) -> None:
        run_id = _create_run(
            session_factory=session_factory, root_task=_graph_task(task_ids=["a"])
        )
        with pytest.raises(Exception):
            api_server_sql.PipelineRunsApiService_Sql().set_annotation(
                session=session_factory(),
                id=run_id,
                key=_ENDED_AT,
                value="2026-01-01T00:00:00+00:00",
                user_name="ada",
            )


def test_key_value_index_is_created_on_an_existing_database() -> None:
    db_engine = database_ops.create_db_engine(database_uri="sqlite://")
    bts._TableBase.metadata.create_all(db_engine)
    with db_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text(
                f"DROP INDEX {bts.PipelineRunAnnotation._IX_ANNOTATION_KEY_VALUE}"
            )
        )

    database_ops.migrate_db(db_engine=db_engine, do_skip_backfill=True)

    index_names = {
        index["name"]
        for index in sqlalchemy.inspect(db_engine).get_indexes(
            bts.PipelineRunAnnotation.__tablename__
        )
    }
    assert bts.PipelineRunAnnotation._IX_ANNOTATION_KEY_VALUE in index_names
