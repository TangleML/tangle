import datetime
import json
from collections.abc import Callable

import pytest
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import component_structures as structures
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend import errors
from cloud_pipelines_backend import filter_query_sql


class TestExecutionStatusSummary:
    def test_all_ended_statuses(self) -> None:
        stats = {
            status: 2**i
            for i, status in enumerate(
                sorted(bts.CONTAINER_STATUSES_ENDED, key=lambda s: s.value)
            )
        }
        total = sum(stats.values())
        summary = api_server_sql.ExecutionStatusSummary(
            total_executions=total,
            ended_executions=total,
            has_ended=True,
        )
        assert summary.total_executions == total
        assert summary.ended_executions == total
        assert summary.has_ended is True

    def test_all_in_progress_statuses(self) -> None:
        in_progress = sorted(
            set(bts.ContainerExecutionStatus) - bts.CONTAINER_STATUSES_ENDED,
            key=lambda s: s.value,
        )
        stats = {status: 2**i for i, status in enumerate(in_progress)}
        total = sum(stats.values())
        summary = api_server_sql.ExecutionStatusSummary(
            total_executions=total,
            ended_executions=0,
            has_ended=False,
        )
        assert summary.total_executions == total
        assert summary.ended_executions == 0
        assert summary.has_ended is False

    def test_mixed_statuses(self) -> None:
        all_statuses = sorted(bts.ContainerExecutionStatus, key=lambda s: s.value)
        stats = {status: 2**i for i, status in enumerate(all_statuses)}
        total = sum(stats.values())
        ended = sum(c for s, c in stats.items() if s in bts.CONTAINER_STATUSES_ENDED)
        summary = api_server_sql.ExecutionStatusSummary(
            total_executions=total,
            ended_executions=ended,
            has_ended=(ended == total),
        )
        assert summary.total_executions == total
        assert summary.ended_executions == ended
        assert summary.has_ended is False


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
        assert "~" in page1.next_page_token

        with session_factory() as session:
            page2 = service.list(
                session=session,
                page_token=page1.next_page_token,
            )
        assert len(page2.pipeline_runs) == 2
        assert page2.next_page_token is None

    def test_list_cursor_pagination_order(self, session_factory, service):
        for i in range(5):
            _create_run(
                session_factory,
                service,
                root_task=_make_task_spec(f"pipeline-{i}"),
            )

        with session_factory() as session:
            result = service.list(session=session)

        dates = [r.created_at for r in result.pipeline_runs]
        assert dates == sorted(dates, reverse=True)

    def test_list_cursor_pagination_no_overlap(self, session_factory, service):
        for i in range(12):
            _create_run(
                session_factory,
                service,
                root_task=_make_task_spec(f"pipeline-{i}"),
            )

        with session_factory() as session:
            page1 = service.list(session=session)
        with session_factory() as session:
            page2 = service.list(session=session, page_token=page1.next_page_token)
        page1_ids = {r.id for r in page1.pipeline_runs}
        page2_ids = {r.id for r in page2.pipeline_runs}
        assert page1_ids.isdisjoint(page2_ids)

    def test_list_cursor_pagination_stable_under_inserts(
        self, session_factory, service
    ):
        for i in range(12):
            _create_run(
                session_factory,
                service,
                root_task=_make_task_spec(f"pipeline-{i}"),
            )

        with session_factory() as session:
            page1 = service.list(session=session)
        page1_ids = {r.id for r in page1.pipeline_runs}

        _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("pipeline-new"),
        )

        with session_factory() as session:
            page2 = service.list(session=session, page_token=page1.next_page_token)
        page2_ids = {r.id for r in page2.pipeline_runs}
        assert page1_ids.isdisjoint(page2_ids)
        assert len(page2.pipeline_runs) == 2

    def test_list_invalid_page_token_raises(self, session_factory, service):
        """page_token without ~ raises ApiValidationError (422)."""
        with session_factory() as session:
            with pytest.raises(
                errors.ApiValidationError, match="Unrecognized page_token"
            ):
                service.list(session=session, page_token="not-a-cursor")

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

    def test_create_mirrors_name_and_created_by(self, session_factory, service):
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("my-pipeline"),
            created_by="alice",
        )
        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert (
            annotations[filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME]
            == "my-pipeline"
        )
        assert (
            annotations[filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY]
            == "alice"
        )

    def test_create_mirrors_name_only(self, session_factory, service):
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("solo-pipeline"),
        )
        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert (
            annotations[filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME]
            == "solo-pipeline"
        )
        assert (
            annotations[filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY]
            == ""
        )

    def test_create_mirrors_created_by_only(self, session_factory, service):
        task_spec = _make_task_spec("placeholder")
        task_spec.component_ref.spec.name = None
        run = _create_run(
            session_factory, service, root_task=task_spec, created_by="alice"
        )
        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert (
            annotations[filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY]
            == "alice"
        )
        assert (
            annotations[filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME]
            == ""
        )

    def test_create_mirrors_empty_values_as_empty_string(
        self, session_factory, service
    ):
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(""),
            created_by="",
        )
        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert (
            annotations[filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME]
            == ""
        )
        assert (
            annotations[filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY]
            == ""
        )

    def test_create_mirrors_absent_values_as_empty_string(
        self, session_factory, service
    ):
        task_spec = _make_task_spec("placeholder")
        task_spec.component_ref.spec.name = None
        run = _create_run(session_factory, service, root_task=task_spec)
        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert (
            annotations[filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME]
            == ""
        )
        assert (
            annotations[filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY]
            == ""
        )


class TestCreateMirrorsUserAnnotations:
    def test_create_mirrors_user_annotations(
        self,
        session_factory: orm.sessionmaker,
        service: api_server_sql.PipelineRunsApiService_Sql,
    ) -> None:
        annotations = {"team": "ml-ops", "project": "search"}
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("my-pipeline"),
            annotations=annotations,
        )
        with session_factory() as session:
            mirrored = service.list_annotations(session=session, id=run.id)
        assert mirrored["team"] == "ml-ops"
        assert mirrored["project"] == "search"

    def test_create_mirrors_user_annotations_empty_dict(
        self,
        session_factory: orm.sessionmaker,
        service: api_server_sql.PipelineRunsApiService_Sql,
    ) -> None:
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("my-pipeline"),
            annotations={},
        )
        with session_factory() as session:
            mirrored = service.list_annotations(session=session, id=run.id)
        assert set(mirrored.keys()) == {
            filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
            filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME,
        }

    def test_create_mirrors_user_annotations_none(
        self,
        session_factory: orm.sessionmaker,
        service: api_server_sql.PipelineRunsApiService_Sql,
    ) -> None:
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("my-pipeline"),
            annotations=None,
        )
        with session_factory() as session:
            mirrored = service.list_annotations(session=session, id=run.id)
        assert set(mirrored.keys()) == {
            filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
            filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME,
        }

    def test_create_skips_system_prefix_in_user_annotations(
        self,
        session_factory: orm.sessionmaker,
        service: api_server_sql.PipelineRunsApiService_Sql,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        annotations = {"system/foo": "bar", "valid": "ok"}
        with caplog.at_level("WARNING"):
            run = _create_run(
                session_factory,
                service,
                root_task=_make_task_spec("my-pipeline"),
                annotations=annotations,
            )
        with session_factory() as session:
            mirrored = service.list_annotations(session=session, id=run.id)
        assert "valid" in mirrored
        assert mirrored["valid"] == "ok"
        assert "system/foo" not in mirrored
        assert any("system/foo" in r.message for r in caplog.records)

    def test_create_mirrors_user_annotations_none_value_as_empty_string(
        self,
        session_factory: orm.sessionmaker,
        service: api_server_sql.PipelineRunsApiService_Sql,
    ) -> None:
        annotations = {"tag": None}
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("my-pipeline"),
            annotations=annotations,
        )
        with session_factory() as session:
            mirrored = service.list_annotations(session=session, id=run.id)
        assert mirrored["tag"] == ""

    def test_create_user_annotations_coexist_with_system(
        self,
        session_factory: orm.sessionmaker,
        service: api_server_sql.PipelineRunsApiService_Sql,
    ) -> None:
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("my-pipeline"),
            created_by="alice",
            annotations={"team": "a"},
        )
        with session_factory() as session:
            mirrored = service.list_annotations(session=session, id=run.id)
        assert mirrored["team"] == "a"
        assert (
            mirrored[filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY]
            == "alice"
        )
        assert (
            mirrored[filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME]
            == "my-pipeline"
        )


class TestPipelineRunAnnotationCrud:
    def test_system_annotations_coexist_with_user_annotations(
        self, session_factory, service
    ):
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec("my-pipeline"),
            created_by="alice",
        )
        with session_factory() as session:
            service.set_annotation(
                session=session,
                id=run.id,
                key="team",
                value="ml-ops",
                user_name="alice",
            )
        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations["team"] == "ml-ops"
        assert (
            annotations[filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME]
            == "my-pipeline"
        )
        assert (
            annotations[filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY]
            == "alice"
        )

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

    def test_list_annotations_only_system(self, session_factory, service):
        run = _create_run(session_factory, service, root_task=_make_task_spec())
        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations == {
            filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME: "test-pipeline",
            filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY: "",
        }

    def test_set_annotation_rejects_system_key(self, session_factory, service):
        run = _create_run(
            session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="user1",
        )
        with session_factory() as session:
            with pytest.raises(
                errors.ApiValidationError, match="reserved for system use"
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
                errors.ApiValidationError, match="reserved for system use"
            ):
                service.delete_annotation(
                    session=session,
                    id=run.id,
                    key="system/pipeline_run.created_by",
                    user_name="user1",
                )


class TestTruncateForAnnotation:
    """Unit tests for _truncate_for_annotation() helper."""

    def test_exact_255_unchanged(self) -> None:
        value = "a" * bts._STR_MAX_LENGTH
        result = api_server_sql._truncate_for_annotation(
            value=value,
            field_name=filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME,
            pipeline_run_id="run-1",
        )
        assert result == value

    def test_256_truncated_and_logs_warning(self, caplog) -> None:
        value = "b" * 256
        field = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        with caplog.at_level("WARNING"):
            result = api_server_sql._truncate_for_annotation(
                value=value,
                field_name=field,
                pipeline_run_id="run-xyz",
            )
        assert result == "b" * bts._STR_MAX_LENGTH
        assert len(caplog.records) == 1
        msg = caplog.records[0].message
        assert "run-xyz" in msg
        assert str(field) in msg


class TestAnnotationValueOverflow:
    """Reproduction tests using mysql_varchar_limit_session_factory (SQLite TRIGGER
    enforcement). These tests prove that >255 char values are rejected,
    mimicking MySQL's DataError 1406.

    Covers all write paths into pipeline_run_annotation:
    - set_annotation(): long key, long value
    - create() via _mirror_system_annotations(): long pipeline_name, long created_by
    """

    def test_set_annotation_long_value_truncated(
        self,
        mysql_varchar_limit_session_factory: orm.sessionmaker,
        service: api_server_sql.PipelineRunsApiService_Sql,
    ) -> None:
        """set_annotation() with a 300-char value is truncated to 255
        via _mirror_single_annotation()."""
        run = _create_run(
            mysql_varchar_limit_session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="user1",
        )
        with mysql_varchar_limit_session_factory() as session:
            service.set_annotation(
                session=session,
                id=run.id,
                key="team",
                value="v" * 300,
                user_name="user1",
            )
        with mysql_varchar_limit_session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations["team"] == "v" * bts._STR_MAX_LENGTH

    def test_set_annotation_long_key_raises_on_overflow(
        self,
        mysql_varchar_limit_session_factory: orm.sessionmaker,
        service: api_server_sql.PipelineRunsApiService_Sql,
    ) -> None:
        """set_annotation() with a 300-char key overflows the
        VARCHAR(255) key column and triggers IntegrityError."""
        run = _create_run(
            mysql_varchar_limit_session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="user1",
        )
        with mysql_varchar_limit_session_factory() as session:
            with pytest.raises(
                sqlalchemy.exc.IntegrityError, match="Data too long.*key"
            ):
                service.set_annotation(
                    session=session,
                    id=run.id,
                    key="k" * 300,
                    value="short",
                    user_name="user1",
                )

    def test_create_run_long_pipeline_name_truncated(
        self,
        mysql_varchar_limit_session_factory: orm.sessionmaker,
        service: api_server_sql.PipelineRunsApiService_Sql,
    ) -> None:
        """create() with a 300-char pipeline name is truncated to 255
        in _mirror_system_annotations()."""
        run = _create_run(
            mysql_varchar_limit_session_factory,
            service,
            root_task=_make_task_spec("p" * 300),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        with mysql_varchar_limit_session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == "p" * bts._STR_MAX_LENGTH

    def test_create_run_long_created_by_truncated(
        self,
        mysql_varchar_limit_session_factory: orm.sessionmaker,
        service: api_server_sql.PipelineRunsApiService_Sql,
    ) -> None:
        """create() with a 300-char created_by is truncated to 255
        in _mirror_system_annotations()."""
        run = _create_run(
            mysql_varchar_limit_session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="u" * 300,
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY
        with mysql_varchar_limit_session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == "u" * bts._STR_MAX_LENGTH

    def test_create_truncates_long_user_annotation_value(
        self,
        mysql_varchar_limit_session_factory: orm.sessionmaker,
        service: api_server_sql.PipelineRunsApiService_Sql,
    ) -> None:
        """create() with a 300-char user annotation value is truncated to 255
        via _mirror_pipeline_run_annotations()."""
        run = _create_run(
            mysql_varchar_limit_session_factory,
            service,
            root_task=_make_task_spec(),
            annotations={"long_val": "x" * 300},
        )
        with mysql_varchar_limit_session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations["long_val"] == "x" * bts._STR_MAX_LENGTH


class TestSetAnnotationBehavior:
    def test_set_annotation_none_value_stored_as_empty_string(
        self,
        session_factory: orm.sessionmaker,
        service: api_server_sql.PipelineRunsApiService_Sql,
    ) -> None:
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
                key="tag",
                value=None,
                user_name="user1",
            )
        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations["tag"] == ""


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

    def test_pagination_with_filter_query(self, session_factory, service):
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
        assert "~" in page1.next_page_token

        with session_factory() as session:
            page2 = service.list(
                session=session,
                page_token=page1.next_page_token,
                filter_query=fq,
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
    """Unit tests for _get_pipeline_name_from_task_spec."""

    def test_returns_name(self):
        """Happy path: task_spec_dict -> TaskSpec -> component_ref -> spec -> name"""
        task = _make_task_spec(pipeline_name="my-pipe")
        result = api_server_sql._get_pipeline_name_from_task_spec(
            task_spec_dict=task.to_json_dict()
        )
        assert result == "my-pipe"

    def test_returns_none_when_spec_is_none(self):
        """task_spec_dict -> TaskSpec -> component_ref -> [spec=None]"""
        result = api_server_sql._get_pipeline_name_from_task_spec(
            task_spec_dict={"component_ref": {}},
        )
        assert result is None

    def test_returns_none_when_name_is_none(self):
        """task_spec_dict -> ... -> spec -> [name=None]"""
        result = api_server_sql._get_pipeline_name_from_task_spec(
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
        result = api_server_sql._get_pipeline_name_from_task_spec(
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
        result = api_server_sql._get_pipeline_name_from_task_spec(
            task_spec_dict={"bad": "data"}
        )
        assert result is None


def _initialize_db_and_get_session_factory() -> Callable[[], orm.Session]:
    db_engine = database_ops.create_db_engine_and_migrate_db(database_uri="sqlite://")
    return lambda: orm.Session(bind=db_engine)


def _create_execution_node(
    *,
    session: orm.Session,
    task_spec: dict | None = None,
    status: bts.ContainerExecutionStatus | None = None,
    parent: bts.ExecutionNode | None = None,
) -> bts.ExecutionNode:
    """Helper to create an ExecutionNode with optional status and parent."""
    node = bts.ExecutionNode(task_spec=task_spec or {})
    if parent is not None:
        node.parent_execution = parent
    if status is not None:
        node.container_execution_status = status
    session.add(node)
    session.flush()
    return node


def _link_ancestor(
    *,
    session: orm.Session,
    execution_node: bts.ExecutionNode,
    ancestor_node: bts.ExecutionNode,
) -> None:
    """Create an ExecutionToAncestorExecutionLink."""
    link = bts.ExecutionToAncestorExecutionLink(
        ancestor_execution=ancestor_node,
        execution=execution_node,
    )
    session.add(link)
    session.flush()


def _create_pipeline_run(
    *,
    session: orm.Session,
    root_execution: bts.ExecutionNode,
) -> bts.PipelineRun:
    """Helper to create a PipelineRun linked to a root execution node."""
    run = bts.PipelineRun(root_execution=root_execution)
    session.add(run)
    session.flush()
    return run


class TestPipelineRunServiceList:
    def test_list_empty(self) -> None:
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()
        with session_factory() as session:
            result = service.list(session=session)
            assert result.pipeline_runs == []
            assert result.next_page_token is None

    def test_list_returns_pipeline_runs(self) -> None:
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()
        with session_factory() as session:
            root = _create_execution_node(session=session)
            root_id = root.id
            _create_pipeline_run(session=session, root_execution=root)
            session.commit()

        with session_factory() as session:
            result = service.list(session=session)
            assert len(result.pipeline_runs) == 1
            assert result.pipeline_runs[0].root_execution_id == root_id
            assert result.pipeline_runs[0].created_by is None
            assert result.pipeline_runs[0].execution_status_stats is None

    def test_list_with_execution_stats(self) -> None:
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()
        with session_factory() as session:
            root = _create_execution_node(session=session)
            root_id = root.id
            child1 = _create_execution_node(
                session=session,
                parent=root,
                status=bts.ContainerExecutionStatus.SUCCEEDED,
            )
            child2 = _create_execution_node(
                session=session,
                parent=root,
                status=bts.ContainerExecutionStatus.RUNNING,
            )
            _link_ancestor(session=session, execution_node=child1, ancestor_node=root)
            _link_ancestor(session=session, execution_node=child2, ancestor_node=root)
            _create_pipeline_run(session=session, root_execution=root)
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


class TestPipelineRunServiceGet:
    def test_get_not_found(self):
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()
        with session_factory() as session:
            with pytest.raises(errors.ItemNotFoundError):
                service.get(session=session, id="nonexistent-id")

    def test_get_returns_pipeline_run(self):
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()
        with session_factory() as session:
            root = _create_execution_node(session=session)
            root_id = root.id
            run = _create_pipeline_run(session=session, root_execution=root)
            run_id = run.id
            session.commit()

        with session_factory() as session:
            result = service.get(session=session, id=run_id)
            assert result.id == run_id
            assert result.root_execution_id == root_id
            assert result.execution_status_stats is None
            assert result.execution_summary is None

    def test_get_with_execution_stats(self):
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()
        with session_factory() as session:
            root = _create_execution_node(session=session)
            root_id = root.id
            child1 = _create_execution_node(
                session=session,
                parent=root,
                status=bts.ContainerExecutionStatus.SUCCEEDED,
            )
            child2 = _create_execution_node(
                session=session,
                parent=root,
                status=bts.ContainerExecutionStatus.RUNNING,
            )
            _link_ancestor(session=session, execution_node=child1, ancestor_node=root)
            _link_ancestor(session=session, execution_node=child2, ancestor_node=root)
            run = _create_pipeline_run(session=session, root_execution=root)
            run_id = run.id
            session.commit()

        with session_factory() as session:
            result = service.get(
                session=session, id=run_id, include_execution_stats=True
            )
            assert result.id == run_id
            assert result.root_execution_id == root_id
            stats = result.execution_status_stats
            assert stats is not None
            assert stats["SUCCEEDED"] == 1
            assert stats["RUNNING"] == 1
            summary = result.execution_summary
            assert summary is not None
            assert summary.total_executions == 2
            assert summary.ended_executions == 1
            assert summary.has_ended is False
