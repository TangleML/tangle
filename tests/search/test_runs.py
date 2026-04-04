import datetime
import json

import pytest
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import component_structures as structures
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend import errors
from cloud_pipelines_backend.search import filter_query_sql
from cloud_pipelines_backend.search import runs as search_runs


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
    engine = database_ops.create_db_engine(database_uri="sqlite://")
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

    def test_list_filter_created_by_empty_raises(self, session_factory, service):
        with session_factory() as session:
            with pytest.raises(errors.ApiValidationError, match="non-empty value"):
                service.list(
                    session=session,
                    filter="created_by:",
                )

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
            response = search_runs._create_pipeline_run_response(
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
            response = search_runs._create_pipeline_run_response(
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
            response = search_runs._create_pipeline_run_response(
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
            response = search_runs._create_pipeline_run_response(
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
            response = search_runs._create_pipeline_run_response(
                session=session,
                pipeline_run=db_run,
                include_pipeline_names=False,
                include_execution_stats=True,
            )
        assert response.execution_status_stats is not None


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
            with pytest.raises(errors.ApiValidationError, match="Cannot use both"):
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

    def _create_run_at(self, *, session_factory, service, created_at, **kwargs):
        """Create a run and override its created_at timestamp."""
        run = _create_run(
            session_factory, service, root_task=_make_task_spec(), **kwargs
        )
        with session_factory() as session:
            session.execute(
                sqlalchemy.update(bts.PipelineRun)
                .where(bts.PipelineRun.id == run.id)
                .values(created_at=created_at)
            )
            session.commit()
        return run

    def _utc(self, *, year, month, day, hour=0, minute=0, second=0):
        return datetime.datetime(
            year,
            month,
            day,
            hour,
            minute,
            second,
            tzinfo=datetime.timezone.utc,
        )

    def test_list_filter_query_time_range(self, session_factory, service):
        self._create_run_at(
            session_factory=session_factory,
            service=service,
            created_at=self._utc(year=2024, month=1, day=1),
        )
        feb = self._create_run_at(
            session_factory=session_factory,
            service=service,
            created_at=self._utc(year=2024, month=2, day=1),
        )
        self._create_run_at(
            session_factory=session_factory,
            service=service,
            created_at=self._utc(year=2024, month=3, day=1),
        )

        fq = json.dumps(
            {
                "and": [
                    {
                        "time_range": {
                            "key": "system/pipeline_run.date.created_at",
                            "start_time": "2024-01-15T00:00:00Z",
                            "end_time": "2024-02-15T00:00:00Z",
                        }
                    }
                ]
            }
        )
        with session_factory() as session:
            result = service.list(session=session, filter_query=fq)
        assert len(result.pipeline_runs) == 1
        assert result.pipeline_runs[0].id == feb.id

    def test_list_filter_query_time_range_start_boundary(
        self, session_factory, service
    ):
        run = self._create_run_at(
            session_factory=session_factory,
            service=service,
            created_at=self._utc(year=2024, month=1, day=1),
        )
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
            result = service.list(session=session, filter_query=fq)
        assert len(result.pipeline_runs) == 1
        assert result.pipeline_runs[0].id == run.id

    def test_list_filter_query_time_range_end_boundary(self, session_factory, service):
        self._create_run_at(
            session_factory=session_factory,
            service=service,
            created_at=self._utc(year=2024, month=2, day=1),
        )

        fq = json.dumps(
            {
                "and": [
                    {
                        "time_range": {
                            "key": "system/pipeline_run.date.created_at",
                            "start_time": "2024-01-01T00:00:00Z",
                            "end_time": "2024-02-01T00:00:00Z",
                        }
                    }
                ]
            }
        )
        with session_factory() as session:
            result = service.list(session=session, filter_query=fq)
        assert len(result.pipeline_runs) == 0

    def test_list_filter_query_time_range_start_only(self, session_factory, service):
        self._create_run_at(
            session_factory=session_factory,
            service=service,
            created_at=self._utc(year=2024, month=1, day=1),
        )
        mar = self._create_run_at(
            session_factory=session_factory,
            service=service,
            created_at=self._utc(year=2024, month=3, day=1),
        )

        fq = json.dumps(
            {
                "and": [
                    {
                        "time_range": {
                            "key": "system/pipeline_run.date.created_at",
                            "start_time": "2024-02-01T00:00:00Z",
                        }
                    }
                ]
            }
        )
        with session_factory() as session:
            result = service.list(session=session, filter_query=fq)
        assert len(result.pipeline_runs) == 1
        assert result.pipeline_runs[0].id == mar.id

    def test_list_filter_query_time_range_end_only(self, session_factory, service):
        jan = self._create_run_at(
            session_factory=session_factory,
            service=service,
            created_at=self._utc(year=2024, month=1, day=1),
        )
        self._create_run_at(
            session_factory=session_factory,
            service=service,
            created_at=self._utc(year=2024, month=3, day=1),
        )

        fq = json.dumps(
            {
                "and": [
                    {
                        "time_range": {
                            "key": "system/pipeline_run.date.created_at",
                            "end_time": "2024-02-01T00:00:00Z",
                        }
                    }
                ]
            }
        )
        with session_factory() as session:
            result = service.list(session=session, filter_query=fq)
        assert len(result.pipeline_runs) == 1
        assert result.pipeline_runs[0].id == jan.id

    def test_list_filter_query_time_range_not(self, session_factory, service):
        jan = self._create_run_at(
            session_factory=session_factory,
            service=service,
            created_at=self._utc(year=2024, month=1, day=1),
        )
        feb = self._create_run_at(
            session_factory=session_factory,
            service=service,
            created_at=self._utc(year=2024, month=2, day=1),
        )
        mar = self._create_run_at(
            session_factory=session_factory,
            service=service,
            created_at=self._utc(year=2024, month=3, day=1),
        )

        fq = json.dumps(
            {
                "and": [
                    {
                        "not": {
                            "time_range": {
                                "key": "system/pipeline_run.date.created_at",
                                "start_time": "2024-01-15T00:00:00Z",
                                "end_time": "2024-02-15T00:00:00Z",
                            }
                        }
                    }
                ]
            }
        )
        with session_factory() as session:
            result = service.list(session=session, filter_query=fq)
        result_ids = {r.id for r in result.pipeline_runs}
        assert feb.id not in result_ids
        assert jan.id in result_ids
        assert mar.id in result_ids

    def test_list_filter_query_time_range_after_annotation(
        self, session_factory, service
    ):
        self._create_run_at(
            session_factory=session_factory,
            service=service,
            created_at=self._utc(year=2024, month=1, day=1),
            created_by="alice",
        )
        feb = self._create_run_at(
            session_factory=session_factory,
            service=service,
            created_at=self._utc(year=2024, month=2, day=1),
            created_by="alice",
        )
        self._create_run_at(
            session_factory=session_factory,
            service=service,
            created_at=self._utc(year=2024, month=3, day=1),
            created_by="bob",
        )

        fq = json.dumps(
            {
                "and": [
                    {
                        "value_equals": {
                            "key": "system/pipeline_run.created_by",
                            "value": "alice",
                        }
                    },
                    {
                        "time_range": {
                            "key": "system/pipeline_run.date.created_at",
                            "start_time": "2024-01-15T00:00:00Z",
                            "end_time": "2024-03-01T00:00:00Z",
                        }
                    },
                ]
            }
        )
        with session_factory() as session:
            result = service.list(session=session, filter_query=fq)
        assert len(result.pipeline_runs) == 1
        assert result.pipeline_runs[0].id == feb.id

    def test_list_filter_query_time_range_with_annotation(
        self, session_factory, service
    ):
        jan = self._create_run_at(
            session_factory=session_factory,
            service=service,
            created_at=self._utc(year=2024, month=1, day=1),
            created_by="test-user",
        )
        self._set_annotation(
            session_factory=session_factory,
            service=service,
            run_id=jan.id,
            key="team",
            value="ml-ops",
        )
        self._create_run_at(
            session_factory=session_factory,
            service=service,
            created_at=self._utc(year=2024, month=2, day=1),
            created_by="test-user",
        )
        mar = self._create_run_at(
            session_factory=session_factory,
            service=service,
            created_at=self._utc(year=2024, month=3, day=1),
            created_by="test-user",
        )
        self._set_annotation(
            session_factory=session_factory,
            service=service,
            run_id=mar.id,
            key="team",
            value="ml-ops",
        )

        fq = json.dumps(
            {
                "and": [
                    {"value_equals": {"key": "team", "value": "ml-ops"}},
                    {
                        "time_range": {
                            "key": "system/pipeline_run.date.created_at",
                            "start_time": "2024-02-01T00:00:00Z",
                        }
                    },
                ]
            }
        )
        with session_factory() as session:
            result = service.list(session=session, filter_query=fq)
        assert len(result.pipeline_runs) == 1
        assert result.pipeline_runs[0].id == mar.id

    def test_list_filter_query_time_range_before_annotation(
        self, session_factory, service
    ):
        jan = self._create_run_at(
            session_factory=session_factory,
            service=service,
            created_at=self._utc(year=2024, month=1, day=1),
            created_by="test-user",
        )
        self._set_annotation(
            session_factory=session_factory,
            service=service,
            run_id=jan.id,
            key="team",
            value="ml-ops",
        )
        self._create_run_at(
            session_factory=session_factory,
            service=service,
            created_at=self._utc(year=2024, month=2, day=1),
            created_by="test-user",
        )
        mar = self._create_run_at(
            session_factory=session_factory,
            service=service,
            created_at=self._utc(year=2024, month=3, day=1),
            created_by="test-user",
        )
        self._set_annotation(
            session_factory=session_factory,
            service=service,
            run_id=mar.id,
            key="team",
            value="ml-ops",
        )

        fq = json.dumps(
            {
                "and": [
                    {
                        "time_range": {
                            "key": "system/pipeline_run.date.created_at",
                            "start_time": "2024-02-01T00:00:00Z",
                        }
                    },
                    {"value_equals": {"key": "team", "value": "ml-ops"}},
                ]
            }
        )
        with session_factory() as session:
            result = service.list(session=session, filter_query=fq)
        assert len(result.pipeline_runs) == 1
        assert result.pipeline_runs[0].id == mar.id

    def test_list_filter_query_time_range_offset_timezone(
        self, session_factory, service
    ):
        self._create_run_at(
            session_factory=session_factory,
            service=service,
            created_at=self._utc(year=2024, month=1, day=1, hour=2, minute=0),
        )
        run_b = self._create_run_at(
            session_factory=session_factory,
            service=service,
            created_at=self._utc(year=2024, month=1, day=1, hour=2, minute=30),
        )
        run_c = self._create_run_at(
            session_factory=session_factory,
            service=service,
            created_at=self._utc(year=2024, month=1, day=1, hour=6, minute=0),
        )

        fq = json.dumps(
            {
                "and": [
                    {
                        "time_range": {
                            "key": "system/pipeline_run.date.created_at",
                            "start_time": "2024-01-01T08:00:00+05:30",
                        }
                    }
                ]
            }
        )
        with session_factory() as session:
            result = service.list(session=session, filter_query=fq)
        assert len(result.pipeline_runs) == 2
        returned_ids = {r.id for r in result.pipeline_runs}
        assert returned_ids == {run_b.id, run_c.id}

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

        decoded = filter_query_sql._decode_page_token(page_token=page1.next_page_token)
        assert decoded["filter_query"] == fq

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
                            "key": filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
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
                            "key": filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
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
                            "key": filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
                            "value_substring": "al",
                        }
                    }
                ]
            }
        )
        with session_factory() as session:
            with pytest.raises(errors.ApiValidationError, match="not supported"):
                service.list(
                    session=session,
                    filter_query=fq,
                )


class TestGetPipelineNameFromTaskSpec:
    """Unit tests for get_pipeline_name_from_task_spec."""

    def test_returns_name(self):
        """Happy path: task_spec_dict -> TaskSpec -> component_ref -> spec -> name"""
        task = _make_task_spec(pipeline_name="my-pipe")
        from cloud_pipelines_backend.search import runs

        result = runs.get_pipeline_name_from_task_spec(
            task_spec_dict=task.to_json_dict()
        )
        assert result == "my-pipe"

    def test_returns_none_when_spec_is_none(self):
        """task_spec_dict -> TaskSpec -> component_ref -> [spec=None]"""
        from cloud_pipelines_backend.search import runs

        result = runs.get_pipeline_name_from_task_spec(
            task_spec_dict={"component_ref": {}},
        )
        assert result is None

    def test_returns_none_when_name_is_none(self):
        """task_spec_dict -> ... -> spec -> [name=None]"""
        from cloud_pipelines_backend.search import runs

        result = runs.get_pipeline_name_from_task_spec(
            task_spec_dict={
                "component_ref": {
                    "spec": {
                        "implementation": {
                            "container": {"image": "img"},
                        }
                    }
                }
            },
        )
        assert result is None

    def test_returns_none_when_name_is_empty(self):
        """task_spec_dict -> ... -> spec -> [name=""]"""
        from cloud_pipelines_backend.search import runs

        result = runs.get_pipeline_name_from_task_spec(
            task_spec_dict={
                "component_ref": {
                    "spec": {
                        "name": "",
                        "implementation": {
                            "container": {"image": "img"},
                        },
                    }
                }
            },
        )
        assert result is None

    def test_returns_none_on_malformed_dict(self):
        """[task_spec_dict=malformed] -> from_json_dict() raises"""
        from cloud_pipelines_backend.search import runs

        result = runs.get_pipeline_name_from_task_spec(task_spec_dict={"bad": "data"})
        assert result is None


class TestCalculateExecutionStatusStats:
    """Unit tests for search_runs._calculate_execution_status_stats."""

    _MINIMAL_TASK_SPEC: dict = {
        "componentRef": {
            "spec": {
                "name": "stub",
                "implementation": {
                    "container": {"image": "stub:latest"},
                },
            }
        }
    }

    def _make_execution(
        self,
        session: orm.Session,
        *,
        root: bts.ExecutionNode,
        status: bts.ContainerExecutionStatus | None = None,
    ) -> bts.ExecutionNode:
        node = bts.ExecutionNode(task_spec=self._MINIMAL_TASK_SPEC)
        node.container_execution_status = status
        session.add(node)
        session.flush()
        link = bts.ExecutionToAncestorExecutionLink(
            ancestor_execution=root,
            execution=node,
        )
        session.add(link)
        session.flush()
        return node

    def test_empty(self, session_factory):
        root = bts.ExecutionNode(task_spec=self._MINIMAL_TASK_SPEC)
        with session_factory() as session:
            session.add(root)
            session.flush()
            result = search_runs._calculate_execution_status_stats(
                session=session, root_execution_id=root.id
            )
        assert result == {}

    def test_single_status_group(self, session_factory):
        root = bts.ExecutionNode(task_spec=self._MINIMAL_TASK_SPEC)
        with session_factory() as session:
            session.add(root)
            session.flush()
            self._make_execution(
                session,
                root=root,
                status=bts.ContainerExecutionStatus.SUCCEEDED,
            )
            self._make_execution(
                session,
                root=root,
                status=bts.ContainerExecutionStatus.SUCCEEDED,
            )
            result = search_runs._calculate_execution_status_stats(
                session=session, root_execution_id=root.id
            )
        assert result == {bts.ContainerExecutionStatus.SUCCEEDED: 2}

    def test_multiple_status_groups(self, session_factory):
        root = bts.ExecutionNode(task_spec=self._MINIMAL_TASK_SPEC)
        with session_factory() as session:
            session.add(root)
            session.flush()
            self._make_execution(
                session,
                root=root,
                status=bts.ContainerExecutionStatus.SUCCEEDED,
            )
            self._make_execution(
                session,
                root=root,
                status=bts.ContainerExecutionStatus.SUCCEEDED,
            )
            self._make_execution(
                session,
                root=root,
                status=bts.ContainerExecutionStatus.FAILED,
            )
            result = search_runs._calculate_execution_status_stats(
                session=session, root_execution_id=root.id
            )
        assert result == {
            bts.ContainerExecutionStatus.SUCCEEDED: 2,
            bts.ContainerExecutionStatus.FAILED: 1,
        }

    def test_null_statuses_excluded(self, session_factory):
        root = bts.ExecutionNode(task_spec=self._MINIMAL_TASK_SPEC)
        with session_factory() as session:
            session.add(root)
            session.flush()
            self._make_execution(
                session,
                root=root,
                status=bts.ContainerExecutionStatus.RUNNING,
            )
            self._make_execution(session, root=root, status=None)
            result = search_runs._calculate_execution_status_stats(
                session=session, root_execution_id=root.id
            )
        assert result == {bts.ContainerExecutionStatus.RUNNING: 1}
