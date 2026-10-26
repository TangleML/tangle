"""Tests for SQLAlchemy event listeners in orchestrator_sql and instrumentation.metrics."""

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
            "execution.run_attempt": "first",
        }


class TestRunAttempt:
    """A preempted execution reaches RUNNING more than once; only the first is a start."""

    @staticmethod
    def _history(*statuses: str) -> list[dict[str, str]]:
        return [
            {"status": s, "first_observed_at": "2024-01-01T00:00:00+00:00"}
            for s in statuses
        ]

    @pytest.mark.parametrize(
        ("statuses", "expected"),
        [
            ((), "none"),
            (("QUEUED",), "none"),
            (("QUEUED", "PENDING"), "none"),
            (("QUEUED", "PENDING", "RUNNING"), "first"),
            (("QUEUED", "PENDING", "RUNNING", "PENDING"), "none"),
            (("QUEUED", "PENDING", "RUNNING", "PENDING", "RUNNING"), "repeat"),
            (
                (
                    "QUEUED",
                    "PENDING",
                    "RUNNING",
                    "PENDING",
                    "RUNNING",
                    "PENDING",
                    "RUNNING",
                ),
                "repeat",
            ),
            (("QUEUED", "PENDING", "RUNNING", "SUCCEEDED"), "none"),
        ],
    )
    def test_run_attempt_for_history(
        self, statuses: tuple[str, ...], expected: str
    ) -> None:
        assert metrics._run_attempt(history=self._history(*statuses)) == expected

    def test_resumed_execution_is_labelled_repeat_end_to_end(
        self, session: orm.Session
    ) -> None:
        """The preemption walk: three PENDING->RUNNING samples, first then repeat twice."""
        node = bts.ExecutionNode(task_spec={})
        session.add(node)
        attempts = []
        for status in [
            bts.ContainerExecutionStatus.QUEUED,
            bts.ContainerExecutionStatus.PENDING,
            bts.ContainerExecutionStatus.RUNNING,
            bts.ContainerExecutionStatus.PENDING,
            bts.ContainerExecutionStatus.RUNNING,
            bts.ContainerExecutionStatus.PENDING,
            bts.ContainerExecutionStatus.RUNNING,
        ]:
            node.container_execution_status = status
            with unittest.mock.patch.object(
                metrics.execution_status_transition_duration, "record"
            ) as mock_record:
                session.commit()
            for call in mock_record.call_args_list:
                attrs = call.kwargs["attributes"]
                if attrs["execution.status.to"] == bts.ContainerExecutionStatus.RUNNING:
                    attempts.append(attrs["execution.run_attempt"])

        assert attempts == ["first", "repeat", "repeat"]
