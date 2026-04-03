import pytest
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import component_structures as structures
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend import errors
from cloud_pipelines_backend.annotation import utils as annotation_utils
from cloud_pipelines_backend.search import filter_query_sql


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
def service():
    return api_server_sql.PipelineRunsApiService_Sql()


def _create_run(session_factory, service, **kwargs):
    """Create a pipeline run using a fresh session (mirrors production per-request sessions)."""
    with session_factory() as session:
        return service.create(session, **kwargs)


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
    """Unit tests for truncate_for_annotation() helper."""

    def test_exact_255_unchanged(self) -> None:
        value = "a" * bts._STR_MAX_LENGTH
        result = annotation_utils._truncate_for_annotation(
            value=value,
            field_name=filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME,
            pipeline_run_id="run-1",
        )
        assert result == value

    def test_256_truncated_and_logs_warning(self, caplog) -> None:
        value = "b" * 256
        field = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        with caplog.at_level("WARNING"):
            result = annotation_utils._truncate_for_annotation(
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
    - create() via mirror_system_annotations(): long pipeline_name, long created_by
    """

    # TODO: set_annotation() currently has no truncation guard for the
    # VARCHAR(255) limit on annotation key/value columns. These tests
    # document the failure. Fix deferred to a separate PR to avoid
    # convoluting the backfill + mirror_system_annotations fix.

    def test_set_annotation_long_value_raises_on_overflow(
        self,
        mysql_varchar_limit_session_factory: orm.sessionmaker,
        service: api_server_sql.PipelineRunsApiService_Sql,
    ) -> None:
        """set_annotation() with a 300-char value overflows the
        VARCHAR(255) column and triggers IntegrityError."""
        run = _create_run(
            mysql_varchar_limit_session_factory,
            service,
            root_task=_make_task_spec(),
            created_by="user1",
        )
        with mysql_varchar_limit_session_factory() as session:
            with pytest.raises(
                sqlalchemy.exc.IntegrityError, match="Data too long.*value"
            ):
                service.set_annotation(
                    session=session,
                    id=run.id,
                    key="team",
                    value="v" * 300,
                    user_name="user1",
                )

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
        in mirror_system_annotations()."""
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
        in mirror_system_annotations()."""
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
