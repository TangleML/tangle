"""Tests for SQLAlchemy event listeners in orchestrator_sql and instrumentation.metrics."""

import time
import unittest.mock

import pytest
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend import (
    orchestrator_sql,
)  # noqa: F401 — registers set listener
from cloud_pipelines_backend.instrumentation import (
    metrics,
)  # noqa: F401 — registers before_commit listener


@pytest.fixture()
def session() -> orm.Session:
    db_engine = database_ops.create_db_engine(database_uri="sqlite://")
    bts._TableBase.metadata.create_all(db_engine)
    with orm.Session(db_engine) as s:
        yield s


class TestStatusHistoryListeners:
    def test_status_change_appends_history_to_extra_data(
        self, session: orm.Session
    ) -> None:
        node = bts.ExecutionNode(task_spec={})
        session.add(node)
        node.container_execution_status = bts.ContainerExecutionStatus.QUEUED
        session.commit()

        history = node.extra_data[bts.EXECUTION_NODE_EXTRA_DATA_STATUS_HISTORY_KEY]
        assert len(history) == 1
        assert history[0]["status"] == bts.ContainerExecutionStatus.QUEUED

    def test_duplicate_status_is_not_appended_to_history(
        self, session: orm.Session
    ) -> None:
        node = bts.ExecutionNode(task_spec={})
        session.add(node)
        node.container_execution_status = bts.ContainerExecutionStatus.QUEUED
        node.container_execution_status = bts.ContainerExecutionStatus.QUEUED
        session.commit()

        history = node.extra_data[bts.EXECUTION_NODE_EXTRA_DATA_STATUS_HISTORY_KEY]
        assert len(history) == 1

    def test_second_status_change_records_duration_metric(
        self, session: orm.Session
    ) -> None:
        node = bts.ExecutionNode(task_spec={})
        session.add(node)
        node.container_execution_status = bts.ContainerExecutionStatus.QUEUED
        session.commit()

        node.container_execution_status = bts.ContainerExecutionStatus.RUNNING
        with unittest.mock.patch.object(
            metrics.execution_status_transition_duration, "record"
        ) as mock_record:
            session.commit()

        mock_record.assert_called_once()
        assert mock_record.call_args.kwargs["attributes"] == {
            "execution.status.from": bts.ContainerExecutionStatus.QUEUED,
            "execution.status.to": bts.ContainerExecutionStatus.RUNNING,
        }


class TestExecutionNodeUpdatedAt:
    """Verify execution_node.updated_at is self-maintaining via
    insert_default/onupdate on the mapped_column -- no listener needed."""

    def test_creation_sets_updated_at_without_explicit_value(
        self, session: orm.Session
    ) -> None:
        node = bts.ExecutionNode(task_spec={})
        session.add(node)
        session.commit()

        assert node.updated_at is not None

    def test_status_change_bumps_updated_at(self, session: orm.Session) -> None:
        node = bts.ExecutionNode(task_spec={})
        session.add(node)
        session.commit()
        first_updated_at = node.updated_at
        assert first_updated_at is not None

        time.sleep(0.001)
        node.container_execution_status = bts.ContainerExecutionStatus.QUEUED
        session.commit()

        assert node.updated_at is not None
        assert node.updated_at > first_updated_at

    def test_extra_data_only_change_bumps_updated_at(
        self, session: orm.Session
    ) -> None:
        """A plain extra_data mutation with no status touch also bumps
        updated_at -- this is the case a status-scoped listener would miss.
        `PipelineRunsApiService_Sql.terminate()` only ever sets
        extra_data["desired_state"], never container_execution_status
        directly, so onupdate (fires on any UPDATE to the row) is what
        makes that gap get covered too."""
        node = bts.ExecutionNode(task_spec={}, extra_data={})
        session.add(node)
        session.commit()
        first_updated_at = node.updated_at
        assert first_updated_at is not None

        time.sleep(0.001)
        node.extra_data["desired_state"] = "TERMINATED"
        session.commit()

        assert node.updated_at is not None
        assert node.updated_at > first_updated_at
