import pytest
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import component_structures as structures
from cloud_pipelines_backend import errors


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
        assert annotations == {"team": "ml-ops"}

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
        assert annotations == {"team": "new-value"}

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
        assert annotations == {}

    def test_list_annotations_empty(self, session_factory, service):
        run = _create_run(session_factory, service, root_task=_make_task_spec())
        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations == {}


class TestResolveFilterValue:
    def test_no_page_token_no_filter(self):
        filter_value, offset = api_server_sql._resolve_filter_value(
            filter=None, page_token=None
        )
        assert filter_value is None
        assert offset == 0

    def test_no_page_token_with_filter(self):
        filter_value, offset = api_server_sql._resolve_filter_value(
            filter="created_by:alice",
            page_token=None,
        )
        assert filter_value == "created_by:alice"
        assert offset == 0

    def test_page_token_overrides_filter(self):
        token = api_server_sql._encode_page_token(
            {"offset": 20, "filter": "created_by:bob"}
        )
        filter_value, offset = api_server_sql._resolve_filter_value(
            filter="created_by:alice",
            page_token=token,
        )
        assert filter_value == "created_by:bob"
        assert offset == 20

    def test_page_token_without_filter_key(self):
        token = api_server_sql._encode_page_token({"offset": 10})
        filter_value, offset = api_server_sql._resolve_filter_value(
            filter="created_by:alice",
            page_token=token,
        )
        assert filter_value is None
        assert offset == 10

    def test_page_token_without_offset_key(self):
        token = api_server_sql._encode_page_token({"filter": "created_by:bob"})
        filter_value, offset = api_server_sql._resolve_filter_value(
            filter=None,
            page_token=token,
        )
        assert filter_value == "created_by:bob"
        assert offset == 0


class TestBuildFilterWhereClauses:
    def test_no_filter(self):
        clauses, next_filter = api_server_sql._build_filter_where_clauses(
            filter_value=None,
            current_user=None,
        )
        assert clauses == []
        assert next_filter is None

    def test_created_by_literal(self):
        clauses, next_filter = api_server_sql._build_filter_where_clauses(
            filter_value="created_by:alice",
            current_user=None,
        )
        assert len(clauses) == 1
        assert next_filter == "created_by:alice"

    def test_created_by_me_resolves(self):
        clauses, next_filter = api_server_sql._build_filter_where_clauses(
            filter_value="created_by:me",
            current_user="alice@example.com",
        )
        assert len(clauses) == 1
        assert next_filter == "created_by:alice@example.com"

    def test_created_by_me_no_current_user(self):
        clauses, next_filter = api_server_sql._build_filter_where_clauses(
            filter_value="created_by:me",
            current_user=None,
        )
        assert len(clauses) == 1
        assert next_filter == "created_by:"

    def test_created_by_empty_value(self):
        clauses, next_filter = api_server_sql._build_filter_where_clauses(
            filter_value="created_by:",
            current_user=None,
        )
        assert len(clauses) == 1
        assert next_filter == "created_by:"

    def test_unsupported_key_raises(self):
        with pytest.raises(NotImplementedError, match="Unsupported filter"):
            api_server_sql._build_filter_where_clauses(
                filter_value="unknown_key:value",
                current_user=None,
            )

    def test_text_search_raises(self):
        with pytest.raises(NotImplementedError, match="Text search"):
            api_server_sql._build_filter_where_clauses(
                filter_value="some_text_without_colon",
                current_user=None,
            )


class TestFilterQueryApiWiring:
    def test_filter_query_returns_not_implemented(self, session_factory, service):
        valid_json = '{"and": [{"key_exists": {"key": "team"}}]}'
        with session_factory() as session:
            with pytest.raises(NotImplementedError, match="not yet implemented"):
                service.list(
                    session=session,
                    filter_query=valid_json,
                )

    def test_filter_query_validates_before_501(self, session_factory, service):
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
