from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend.api_server_sql import (
    ExecutionStatusSummary,
    PipelineRunsApiService_Sql,
)


def _initialize_db_and_get_session_factory():
    db_engine = database_ops.create_db_engine_and_migrate_db(database_uri="sqlite://")
    return lambda: orm.Session(bind=db_engine)


def _create_execution_node(session, task_spec=None, status=None, parent=None):
    """Helper to create an ExecutionNode with optional status and parent."""
    node = bts.ExecutionNode(task_spec=task_spec or {})
    if parent is not None:
        node.parent_execution = parent
    if status is not None:
        node.container_execution_status = status
    session.add(node)
    session.flush()
    return node


def _link_ancestor(session, execution_node, ancestor_node):
    """Create an ExecutionToAncestorExecutionLink."""
    link = bts.ExecutionToAncestorExecutionLink(
        ancestor_execution=ancestor_node,
        execution=execution_node,
    )
    session.add(link)
    session.flush()


def _create_pipeline_run(session, root_execution, created_by=None, annotations=None):
    """Helper to create a PipelineRun linked to a root execution node."""
    run = bts.PipelineRun(root_execution=root_execution)
    if created_by:
        run.created_by = created_by
    if annotations:
        run.annotations = annotations
    session.add(run)
    session.flush()
    return run


class TestExecutionStatusSummary:
    def test_initial_state(self):
        summary = ExecutionStatusSummary()
        assert summary.total_executions == 0
        assert summary.ended_executions == 0
        assert summary.has_ended is False

    def test_accumulate_all_ended_statuses(self):
        """Add each ended status with 2^i count for robust uniqueness."""
        summary = ExecutionStatusSummary()
        ended_statuses = sorted(bts.CONTAINER_STATUSES_ENDED, key=lambda s: s.value)
        expected_total = 0
        expected_ended = 0
        for i, status in enumerate(ended_statuses):
            count = 2**i
            summary.count_execution_status(status=status, count=count)
            expected_total += count
            expected_ended += count
            assert summary.total_executions == expected_total
            assert summary.ended_executions == expected_ended
            assert summary.has_ended is True

    def test_accumulate_all_in_progress_statuses(self):
        """Add each in-progress status with 2^i count for robust uniqueness."""
        summary = ExecutionStatusSummary()
        in_progress_statuses = sorted(
            set(bts.ContainerExecutionStatus) - bts.CONTAINER_STATUSES_ENDED,
            key=lambda s: s.value,
        )
        expected_total = 0
        for i, status in enumerate(in_progress_statuses):
            count = 2**i
            summary.count_execution_status(status=status, count=count)
            expected_total += count
            assert summary.total_executions == expected_total
            assert summary.ended_executions == 0
            assert summary.has_ended is False

    def test_accumulate_all_statuses(self):
        """Add every status with 2^i count. Summary math must be exact."""
        summary = ExecutionStatusSummary()
        all_statuses = sorted(bts.ContainerExecutionStatus, key=lambda s: s.value)
        expected_total = 0
        expected_ended = 0
        for i, status in enumerate(all_statuses):
            count = 2**i
            expected_total += count
            if status in bts.CONTAINER_STATUSES_ENDED:
                expected_ended += count
            summary.count_execution_status(status=status, count=count)
            assert summary.total_executions == expected_total
            assert summary.ended_executions == expected_ended
            assert summary.has_ended == (expected_ended == expected_total)


class TestPipelineRunServiceList:
    def test_list_empty(self):
        session_factory = _initialize_db_and_get_session_factory()
        service = PipelineRunsApiService_Sql()
        with session_factory() as session:
            result = service.list(session=session)
            assert result.pipeline_runs == []
            assert result.next_page_token is None

    def test_list_returns_pipeline_runs(self):
        session_factory = _initialize_db_and_get_session_factory()
        service = PipelineRunsApiService_Sql()
        with session_factory() as session:
            root = _create_execution_node(session)
            root_id = root.id
            _create_pipeline_run(session, root, created_by="user1")
            session.commit()

        with session_factory() as session:
            result = service.list(session=session)
            assert len(result.pipeline_runs) == 1
            assert result.pipeline_runs[0].root_execution_id == root_id
            assert result.pipeline_runs[0].created_by == "user1"
            assert result.pipeline_runs[0].execution_status_stats is None

    def test_list_with_execution_stats(self):
        session_factory = _initialize_db_and_get_session_factory()
        service = PipelineRunsApiService_Sql()
        with session_factory() as session:
            root = _create_execution_node(session)
            root_id = root.id
            child1 = _create_execution_node(
                session,
                parent=root,
                status=bts.ContainerExecutionStatus.SUCCEEDED,
            )
            child2 = _create_execution_node(
                session,
                parent=root,
                status=bts.ContainerExecutionStatus.RUNNING,
            )
            _link_ancestor(session, child1, root)
            _link_ancestor(session, child2, root)
            _create_pipeline_run(session, root)
            session.commit()

        with session_factory() as session:
            result = service.list(session=session, include_execution_stats=True)
            assert len(result.pipeline_runs) == 1
            run = result.pipeline_runs[0]
            assert run.root_execution_id == root_id
            stats = run.execution_status_stats
            assert stats is not None
            assert stats["SUCCEEDED"] == 1
            assert stats["RUNNING"] == 1
            summary = run.execution_summary
            assert summary is not None
            assert summary.total_executions == 2
            assert summary.ended_executions == 1
            assert summary.has_ended is False
