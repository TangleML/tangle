import json

import pytest
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import component_structures as structures
from cloud_pipelines_backend import errors
from cloud_pipelines_backend import filter_query_sql


class TestExecutionStatusSummary:
    def test_initial_state(self):
        summary = api_server_sql.ExecutionStatusSummary()
        assert summary.total_executions == 0
        assert summary.ended_executions == 0
        assert summary.has_ended is False

    def test_accumulate_all_ended_statuses(self):
        """Add each ended status with 2^i count for robust uniqueness."""
        summary = api_server_sql.ExecutionStatusSummary()
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
        summary = api_server_sql.ExecutionStatusSummary()
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
        summary = api_server_sql.ExecutionStatusSummary()
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


def _make_task_spec(pipeline_name: str = "test-pipeline") -> structures.TaskSpec:
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


@pytest.fixture()
def session_factory():
    engine = sqlalchemy.create_engine(
        "sqlite://",
        poolclass=sqlalchemy.pool.StaticPool,
        connect_args={"check_same_thread": False},
    )
    bts._TableBase.metadata.create_all(engine)
    return orm.sessionmaker(engine)


@pytest.fixture()
def db_session(session_factory):
    with session_factory() as session:
        yield session


@pytest.fixture()
def service():
    return api_server_sql.PipelineRunsApiService_Sql()


def _create_run(session_factory, service, **kwargs):
    """Create a pipeline run using a fresh session (mirrors production per-request sessions)."""
    with session_factory() as session:
        return service.create(session, **kwargs)


class TestPipelineRunServiceList:
    def test_list_empty(self, session_factory, service):
        with session_factory() as session:
            result = service.list(
                session=session,
            )
        assert result.pipeline_runs == []
        assert result.next_page_token is None

    def test_list_returns_pipeline_runs(self, session_factory, service):
        _create_run(session_factory, service, root_task=_make_task_spec("pipeline-a"))
        _create_run(session_factory, service, root_task=_make_task_spec("pipeline-b"))

        with session_factory() as session:
            result = service.list(
                session=session,
            )
        assert len(result.pipeline_runs) == 2

    def test_list_with_execution_stats(self, session_factory, service):
        _create_run(session_factory, service, root_task=_make_task_spec())

        with session_factory() as session:
            result = service.list(
                session=session,
                include_execution_stats=True,
            )
        assert len(result.pipeline_runs) == 1
        assert result.pipeline_runs[0].execution_status_stats is not None

    def test_list_filter_created_by(self, session_factory, service):
        _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="user1",
        )
        _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="user2",
        )

        with session_factory() as session:
            result = service.list(
                session=session,
                filter="created_by:user1",
            )
        assert len(result.pipeline_runs) == 1
        assert result.pipeline_runs[0].created_by == "user1"

    def test_list_filter_created_by_empty(self, session_factory, service):
        _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by=None,
        )
        _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="user1",
        )

        with session_factory() as session:
            result = service.list(
                session=session,
                filter="created_by:",
            )
        assert len(result.pipeline_runs) == 1
        assert result.pipeline_runs[0].created_by is None

    def test_list_pagination(self, session_factory, service):
        for i in range(12):
            _create_run(
                session_factory,
                service,
                root_task=_make_task_spec(f"pipeline-{i}"),
            )

        with session_factory() as session:
            page1 = service.list(
                session=session,
            )
        assert len(page1.pipeline_runs) == 10
        assert page1.next_page_token is not None

        with session_factory() as session:
            page2 = service.list(
                session=session,
                page_token=page1.next_page_token,
            )
        assert len(page2.pipeline_runs) == 2
        assert page2.next_page_token is None

    def test_list_filter_unsupported(self, session_factory, service):
        with session_factory() as session:
            with pytest.raises(NotImplementedError, match="Unsupported filter"):
                service.list(
                    session=session,
                    filter="unknown_key:value",
                )

    def test_list_with_pipeline_names(self, session_factory, service):
        _create_run(session_factory, service, root_task=_make_task_spec("my-pipeline"))

        with session_factory() as session:
            result = service.list(
                session=session,
                include_pipeline_names=True,
            )
        assert len(result.pipeline_runs) == 1
        assert result.pipeline_runs[0].pipeline_name == "my-pipeline"

    def test_list_filter_created_by_me(self, session_factory, service):
        _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="alice@example.com",
        )
        _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="bob@example.com",
        )

        with session_factory() as session:
            result = service.list(
                session=session,
                current_user="alice@example.com",
                filter="created_by:me",
            )
        assert len(result.pipeline_runs) == 1
        assert result.pipeline_runs[0].created_by == "alice@example.com"


class TestCreatePipelineRunResponse:
    def test_base_response(self, session_factory, service):
        run = _create_run(session_factory, service, root_task=_make_task_spec())
        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            response = service._create_pipeline_run_response(
                session=session,
                pipeline_run=db_run,
                include_pipeline_names=False,
                include_execution_stats=False,
            )
        assert response.id == run.id
        assert response.pipeline_name is None
        assert response.execution_status_stats is None

    def test_pipeline_name_from_task_spec(self, session_factory, service):
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("my-pipeline"),
        )
        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            response = service._create_pipeline_run_response(
                session=session,
                pipeline_run=db_run,
                include_pipeline_names=True,
                include_execution_stats=False,
            )
        assert response.pipeline_name == "my-pipeline"

    def test_pipeline_name_from_extra_data(self, session_factory, service):
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("spec-name"),
        )
        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = {"pipeline_name": "cached-name"}
            session.commit()
        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            response = service._create_pipeline_run_response(
                session=session,
                pipeline_run=db_run,
                include_pipeline_names=True,
                include_execution_stats=False,
            )
        assert response.pipeline_name == "cached-name"

    def test_pipeline_name_none_when_no_execution_node(self, session_factory, service):
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("some-name"),
        )
        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.root_execution_id = "nonexistent-id"
            db_run.extra_data = {}
            session.commit()
        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            response = service._create_pipeline_run_response(
                session=session,
                pipeline_run=db_run,
                include_pipeline_names=True,
                include_execution_stats=False,
            )
        assert response.pipeline_name is None

    def test_with_execution_stats(self, session_factory, service):
        run = _create_run(session_factory, service, root_task=_make_task_spec())
        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            response = service._create_pipeline_run_response(
                session=session,
                pipeline_run=db_run,
                include_pipeline_names=False,
                include_execution_stats=True,
            )
        assert response.execution_status_stats is not None


class TestPipelineRunServiceCreate:
    def test_create_returns_pipeline_run(self, session_factory, service):
        result = _create_run(
            session_factory, service, root_task=_make_task_spec("my-pipeline")
        )
        assert result.id is not None
        assert result.root_execution_id is not None
        assert result.created_at is not None

    def test_create_with_created_by(self, session_factory, service):
        result = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="user1@example.com",
        )
        assert result.created_by == "user1@example.com"

    def test_create_with_annotations(self, session_factory, service):
        annotations = {"team": "ml-ops", "project": "search"}
        result = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            annotations=annotations,
        )
        assert result.annotations == annotations

    def test_create_without_created_by(self, session_factory, service):
        result = _create_run(session_factory, service, root_task=_make_task_spec())
        assert result.created_by is None

    def test_create_writes_created_by_annotation(self, session_factory, service):
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="alice@example.com",
        )
        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[filter_query_sql.SystemKey.CREATED_BY] == "alice@example.com"

    def test_create_without_created_by_no_annotation(self, session_factory, service):
        run = _create_run(session_factory, service, root_task=_make_task_spec())
        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert filter_query_sql.SystemKey.CREATED_BY not in annotations


class TestPipelineRunAnnotationCrud:
    def test_set_annotation(self, session_factory, service):
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="user1",
        )
        with session_factory() as session:
            service.set_annotation(
                session=session,
                id=run.id,
                key="team",
                value="ml-ops",
                user_name="user1",
            )
        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations["team"] == "ml-ops"

    def test_set_annotation_overwrites(self, session_factory, service):
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="user1",
        )
        with session_factory() as session:
            service.set_annotation(
                session=session,
                id=run.id,
                key="team",
                value="old-value",
                user_name="user1",
            )
        with session_factory() as session:
            service.set_annotation(
                session=session,
                id=run.id,
                key="team",
                value="new-value",
                user_name="user1",
            )
        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations["team"] == "new-value"

    def test_delete_annotation(self, session_factory, service):
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="user1",
        )
        with session_factory() as session:
            service.set_annotation(
                session=session,
                id=run.id,
                key="team",
                value="ml-ops",
                user_name="user1",
            )
        with session_factory() as session:
            service.delete_annotation(
                session=session,
                id=run.id,
                key="team",
                user_name="user1",
            )
        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert "team" not in annotations

    def test_list_annotations_empty(self, session_factory, service):
        run = _create_run(session_factory, service, root_task=_make_task_spec())
        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations == {}

    def test_set_annotation_rejects_system_key(self, session_factory, service):
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="user1",
        )
        with session_factory() as session:
            with pytest.raises(
                errors.InvalidAnnotationKeyError, match="reserved for system use"
            ):
                service.set_annotation(
                    session=session,
                    id=run.id,
                    key="system/pipeline_run.created_by",
                    value="hacker",
                    user_name="user1",
                )

    def test_delete_annotation_rejects_system_key(self, session_factory, service):
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="user1",
        )
        with session_factory() as session:
            with pytest.raises(
                errors.InvalidAnnotationKeyError, match="reserved for system use"
            ):
                service.delete_annotation(
                    session=session,
                    id=run.id,
                    key="system/pipeline_run.created_by",
                    user_name="user1",
                )


class TestFilterQueryApiWiring:
    def test_filter_query_validates_invalid_json(self, session_factory, service):
        from pydantic import ValidationError

        invalid_json = '{"bad_key": "not_valid"}'
        with session_factory() as session:
            with pytest.raises(ValidationError):
                service.list(
                    session=session,
                    filter_query=invalid_json,
                )

    def test_mutual_exclusivity_rejected(self, session_factory, service):
        with session_factory() as session:
            with pytest.raises(
                errors.MutuallyExclusiveFilterError, match="Cannot use both"
            ):
                service.list(
                    session=session,
                    filter="created_by:alice",
                    filter_query='{"and": [{"key_exists": {"key": "team"}}]}',
                )


class TestFilterQueryIntegration:
    def _set_annotation(self, *, session_factory, service, run_id, key, value):
        with session_factory() as session:
            service.set_annotation(
                session=session,
                id=run_id,
                key=key,
                value=value,
                user_name="test-user",
            )

    def test_annotation_key_exists(self, session_factory, service):
        run1 = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="test-user",
        )
        run2 = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="test-user",
        )
        self._set_annotation(
            session_factory=session_factory,
            service=service,
            run_id=run1.id,
            key="team",
            value="ml-ops",
        )

        fq = json.dumps({"and": [{"key_exists": {"key": "team"}}]})
        with session_factory() as session:
            result = service.list(
                session=session,
                filter_query=fq,
            )
        assert len(result.pipeline_runs) == 1
        assert result.pipeline_runs[0].id == run1.id

    def test_annotation_value_equals(self, session_factory, service):
        run1 = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="test-user",
        )
        run2 = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="test-user",
        )
        self._set_annotation(
            session_factory=session_factory,
            service=service,
            run_id=run1.id,
            key="team",
            value="ml-ops",
        )
        self._set_annotation(
            session_factory=session_factory,
            service=service,
            run_id=run2.id,
            key="team",
            value="data-eng",
        )

        fq = json.dumps({"and": [{"value_equals": {"key": "team", "value": "ml-ops"}}]})
        with session_factory() as session:
            result = service.list(
                session=session,
                filter_query=fq,
            )
        assert len(result.pipeline_runs) == 1
        assert result.pipeline_runs[0].id == run1.id

    def test_annotation_value_contains(self, session_factory, service):
        run1 = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="test-user",
        )
        run2 = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="test-user",
        )
        self._set_annotation(
            session_factory=session_factory,
            service=service,
            run_id=run1.id,
            key="team",
            value="ml-ops",
        )
        self._set_annotation(
            session_factory=session_factory,
            service=service,
            run_id=run2.id,
            key="team",
            value="data-eng",
        )

        fq = json.dumps(
            {"and": [{"value_contains": {"key": "team", "value_substring": "ml"}}]}
        )
        with session_factory() as session:
            result = service.list(
                session=session,
                filter_query=fq,
            )
        assert len(result.pipeline_runs) == 1
        assert result.pipeline_runs[0].id == run1.id

    def test_annotation_value_in(self, session_factory, service):
        run1 = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="test-user",
        )
        run2 = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="test-user",
        )
        run3 = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="test-user",
        )
        self._set_annotation(
            session_factory=session_factory,
            service=service,
            run_id=run1.id,
            key="env",
            value="prod",
        )
        self._set_annotation(
            session_factory=session_factory,
            service=service,
            run_id=run2.id,
            key="env",
            value="staging",
        )
        self._set_annotation(
            session_factory=session_factory,
            service=service,
            run_id=run3.id,
            key="env",
            value="dev",
        )

        fq = json.dumps(
            {"and": [{"value_in": {"key": "env", "values": ["prod", "staging"]}}]}
        )
        with session_factory() as session:
            result = service.list(
                session=session,
                filter_query=fq,
            )
        assert len(result.pipeline_runs) == 2
        result_ids = {r.id for r in result.pipeline_runs}
        assert run1.id in result_ids
        assert run2.id in result_ids

    def test_and_multiple_annotations(self, session_factory, service):
        run1 = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="test-user",
        )
        run2 = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="test-user",
        )
        self._set_annotation(
            session_factory=session_factory,
            service=service,
            run_id=run1.id,
            key="team",
            value="ml-ops",
        )
        self._set_annotation(
            session_factory=session_factory,
            service=service,
            run_id=run1.id,
            key="env",
            value="prod",
        )
        self._set_annotation(
            session_factory=session_factory,
            service=service,
            run_id=run2.id,
            key="team",
            value="ml-ops",
        )

        fq = json.dumps(
            {
                "and": [
                    {"key_exists": {"key": "team"}},
                    {"value_equals": {"key": "env", "value": "prod"}},
                ]
            }
        )
        with session_factory() as session:
            result = service.list(
                session=session,
                filter_query=fq,
            )
        assert len(result.pipeline_runs) == 1
        assert result.pipeline_runs[0].id == run1.id

    def test_or_annotations(self, session_factory, service):
        run1 = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="test-user",
        )
        run2 = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="test-user",
        )
        run3 = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="test-user",
        )
        self._set_annotation(
            session_factory=session_factory,
            service=service,
            run_id=run1.id,
            key="team",
            value="ml-ops",
        )
        self._set_annotation(
            session_factory=session_factory,
            service=service,
            run_id=run2.id,
            key="project",
            value="search",
        )

        fq = json.dumps(
            {
                "or": [
                    {"key_exists": {"key": "team"}},
                    {"key_exists": {"key": "project"}},
                ]
            }
        )
        with session_factory() as session:
            result = service.list(
                session=session,
                filter_query=fq,
            )
        assert len(result.pipeline_runs) == 2
        result_ids = {r.id for r in result.pipeline_runs}
        assert run1.id in result_ids
        assert run2.id in result_ids

    def test_not_annotation(self, session_factory, service):
        run1 = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="test-user",
        )
        run2 = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="test-user",
        )
        self._set_annotation(
            session_factory=session_factory,
            service=service,
            run_id=run1.id,
            key="deprecated",
            value="true",
        )

        fq = json.dumps({"and": [{"not": {"key_exists": {"key": "deprecated"}}}]})
        with session_factory() as session:
            result = service.list(
                session=session,
                filter_query=fq,
            )
        assert len(result.pipeline_runs) == 1
        assert result.pipeline_runs[0].id == run2.id

    def test_no_match(self, session_factory, service):
        _create_run(session_factory, service, root_task=_make_task_spec())

        fq = json.dumps({"and": [{"key_exists": {"key": "nonexistent"}}]})
        with session_factory() as session:
            result = service.list(
                session=session,
                filter_query=fq,
            )
        assert len(result.pipeline_runs) == 0

    def test_time_range_raises_not_implemented(self, session_factory, service):
        fq = json.dumps(
            {
                "and": [
                    {
                        "time_range": {
                            "key": "system/pipeline_run.date.created_at",
                            "start_time": "2024-01-01T00:00:00Z",
                        }
                    }
                ]
            }
        )
        with session_factory() as session:
            with pytest.raises(NotImplementedError, match="TimeRangePredicate"):
                service.list(
                    session=session,
                    filter_query=fq,
                )

    def test_pagination_preserves_filter_query(self, session_factory, service):
        for _ in range(12):
            run = _create_run(
                session_factory,
                service,
                root_task=_make_task_spec(),
                created_by="test-user",
            )
            self._set_annotation(
                session_factory=session_factory,
                service=service,
                run_id=run.id,
                key="team",
                value="ml-ops",
            )

        fq = json.dumps({"and": [{"key_exists": {"key": "team"}}]})
        with session_factory() as session:
            page1 = service.list(
                session=session,
                filter_query=fq,
            )
        assert len(page1.pipeline_runs) == 10
        assert page1.next_page_token is not None

        decoded = filter_query_sql.PageToken.decode(page1.next_page_token)
        assert decoded.filter_query == fq

        with session_factory() as session:
            page2 = service.list(
                session=session,
                page_token=page1.next_page_token,
            )
        assert len(page2.pipeline_runs) == 2
        assert page2.next_page_token is None

    def test_filter_query_created_by(self, session_factory, service):
        _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="alice",
        )
        _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="bob",
        )

        fq = json.dumps(
            {
                "and": [
                    {
                        "value_equals": {
                            "key": filter_query_sql.SystemKey.CREATED_BY,
                            "value": "alice",
                        }
                    }
                ]
            }
        )
        with session_factory() as session:
            result = service.list(
                session=session,
                filter_query=fq,
            )
        assert len(result.pipeline_runs) == 1
        assert result.pipeline_runs[0].created_by == "alice"

    def test_filter_query_created_by_me(self, session_factory, service):
        _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="alice",
        )
        _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="bob",
        )

        fq = json.dumps(
            {
                "and": [
                    {
                        "value_equals": {
                            "key": filter_query_sql.SystemKey.CREATED_BY,
                            "value": "me",
                        }
                    }
                ]
            }
        )
        with session_factory() as session:
            result = service.list(
                session=session,
                filter_query=fq,
                current_user="alice",
            )
        assert len(result.pipeline_runs) == 1
        assert result.pipeline_runs[0].created_by == "alice"

    def test_filter_query_created_by_unsupported_predicate(
        self, session_factory, service
    ):
        fq = json.dumps(
            {
                "and": [
                    {
                        "value_contains": {
                            "key": filter_query_sql.SystemKey.CREATED_BY,
                            "value_substring": "al",
                        }
                    }
                ]
            }
        )
        with session_factory() as session:
            with pytest.raises(errors.InvalidAnnotationKeyError, match="not supported"):
                service.list(
                    session=session,
                    filter_query=fq,
                )
