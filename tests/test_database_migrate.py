"""Tests for database_migrate: backfill and annotation helpers.

Pipeline Name Resolution Path
==============================

Source 1 (bulk SQL -- extra_data path):
  pipeline_run.extra_data -> ["pipeline_name"] -> value
       |                          |                 |
       +-- None                   +-- key missing   +-- ""
       v                          v                 v
    SQL NULL (safe)            SQL NULL (safe)   valid (inserted)

Source 2 (bulk SQL -- component_spec JSON path):
  execution_node.task_spec -> 'componentRef' -> 'spec' ->> 'name'
       |                          |                 |         |
       +-- NULL                   +-- key missing   +-- null  +-- null
       v                          v                 v         v
    SQL NULL (safe)            SQL NULL (safe)   SQL NULL   SQL NULL
"""

import logging
from unittest import mock

import pytest
import sqlalchemy
from sqlalchemy import orm
from typing import Any

from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import component_structures as structures
from cloud_pipelines_backend import database_migrate
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend import filter_query_sql


def _make_task_spec(
    *,
    pipeline_name: str = "test-pipeline",
) -> structures.TaskSpec:
    return structures.TaskSpec(
        component_ref=structures.ComponentReference(
            spec=structures.ComponentSpec(
                name=pipeline_name,
                implementation=structures.ContainerImplementation(
                    container=structures.ContainerSpec(image="test-image")
                ),
            )
        )
    )


def _set_execution_node_task_spec(
    *,
    session_factory: orm.sessionmaker,
    run_id: str,
    task_spec: structures.TaskSpec,
) -> None:
    """Replace the execution_node's task_spec JSON with the given TaskSpec.

    Use to test fallback paths where spec is None or name is None,
    since the service's create() requires a valid spec to run.
    """
    with session_factory() as session:
        db_run = session.get(bts.PipelineRun, run_id)
        exec_node = session.get(bts.ExecutionNode, db_run.root_execution_id)
        exec_node.task_spec = task_spec.to_json_dict()
        session.commit()


def _set_execution_node_task_spec_raw(
    *,
    session_factory: orm.sessionmaker,
    run_id: str,
    task_spec_dict: dict[str, Any] | None,
) -> None:
    """Set task_spec to an arbitrary dict (or None) bypassing Pydantic.

    Use to test JSON paths that Pydantic's TaskSpec cannot represent
    (e.g. empty dict, missing componentRef, null task_spec).
    """
    with session_factory() as session:
        db_run = session.get(bts.PipelineRun, run_id)
        exec_node = session.get(bts.ExecutionNode, db_run.root_execution_id)
        exec_node.task_spec = task_spec_dict
        session.commit()


@pytest.fixture()
def session_factory() -> orm.sessionmaker:
    db_engine = database_ops.create_db_engine(database_uri="sqlite://")
    bts._TableBase.metadata.create_all(db_engine)
    return orm.sessionmaker(db_engine)


def _create_run(
    *,
    session_factory: orm.sessionmaker,
    service: api_server_sql.PipelineRunsApiService_Sql,
    **kwargs: Any,
) -> api_server_sql.PipelineRunResponse:
    """Create a pipeline run using a fresh session (mirrors production per-request sessions)."""
    with session_factory() as session:
        return service.create(session, **kwargs)


def _delete_annotation(
    *,
    session_factory: orm.sessionmaker,
    run_id: str,
    key: filter_query_sql.PipelineRunAnnotationSystemKey,
) -> None:
    """Remove a write-path annotation so backfill can be tested in isolation."""
    with session_factory() as session:
        session.execute(
            sqlalchemy.delete(bts.PipelineRunAnnotation).where(
                bts.PipelineRunAnnotation.pipeline_run_id == run_id,
                bts.PipelineRunAnnotation.key == key,
            )
        )
        session.commit()


def _delete_all_annotations_for_key(
    *,
    session_factory: orm.sessionmaker,
    key: filter_query_sql.PipelineRunAnnotationSystemKey,
) -> None:
    """Remove all annotations with the given key from the DB."""
    with session_factory() as session:
        session.execute(
            sqlalchemy.delete(bts.PipelineRunAnnotation).where(
                bts.PipelineRunAnnotation.key == key,
            )
        )
        session.commit()


def _count_annotations(
    *,
    session_factory: orm.sessionmaker,
    key: filter_query_sql.PipelineRunAnnotationSystemKey,
) -> int:
    """Count annotation rows with the given key."""
    with session_factory() as session:
        return (
            session.query(bts.PipelineRunAnnotation)
            .filter(bts.PipelineRunAnnotation.key == key)
            .count()
        )


class TestIsAnnotationKeyAlreadyBackfilled:
    def test_false_on_empty_db(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        with session_factory() as session:
            assert (
                database_migrate._is_pipeline_run_annotation_key_already_backfilled(
                    session=session,
                    key=filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
                )
                is False
            )

    def test_false_with_unrelated_annotation(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(),
        )
        _delete_all_annotations_for_key(
            session_factory=session_factory,
            key=filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
        )
        assert (
            _count_annotations(
                session_factory=session_factory,
                key=filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
            )
            == 0
        )
        with session_factory() as session:
            service.set_annotation(session=session, id=run.id, key="team", value="ml")
        with session_factory() as session:
            assert (
                database_migrate._is_pipeline_run_annotation_key_already_backfilled(
                    session=session,
                    key=filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
                )
                is False
            )

    def test_true_when_key_exists(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        with session_factory() as session:
            assert (
                database_migrate._is_pipeline_run_annotation_key_already_backfilled(
                    session=session,
                    key=filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
                )
                is False
            )
        _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(),
            created_by="alice",
        )
        with session_factory() as session:
            assert (
                database_migrate._is_pipeline_run_annotation_key_already_backfilled(
                    session=session,
                    key=filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
                )
                is True
            )

    def test_matches_exact_key(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """Only returns True for the exact key queried, not other keys."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(),
        )
        with session_factory() as session:
            assert (
                database_migrate._is_pipeline_run_annotation_key_already_backfilled(
                    session=session,
                    key="team",
                )
                is False
            )
        with session_factory() as session:
            service.set_annotation(session=session, id=run.id, key="team", value="ml")
        with session_factory() as session:
            assert (
                database_migrate._is_pipeline_run_annotation_key_already_backfilled(
                    session=session,
                    key="team",
                )
                is True
            )
            assert (
                database_migrate._is_pipeline_run_annotation_key_already_backfilled(
                    session=session,
                    key="other_key",
                )
                is False
            )


class TestPipelineNameBackfill:
    """Integration tests for pipeline name backfill functions."""

    # --- Basic functionality ---

    def test_backfill_populates_name_from_extra_data(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="my-pipeline"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)
        assert _count_annotations(session_factory=session_factory, key=key) == 0

        with session_factory() as session:
            database_migrate.backfill_pipeline_names_from_extra_data(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == "my-pipeline"

    def test_backfill_populates_name_from_component_spec(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="fallback-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)
        assert _count_annotations(session_factory=session_factory, key=key) == 0

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = None
            session.commit()

        with session_factory() as session:
            database_migrate.backfill_pipeline_names_from_component_spec(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == "fallback-name"

    def test_backfill_via_run_all(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """run_all_annotation_backfills resolves run_a via extra_data and run_b via component_spec."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME

        run_a = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="from-extra-data"),
        )

        run_b = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="from-component-spec"),
        )
        with session_factory() as session:
            db_run_b = session.get(bts.PipelineRun, run_b.id)
            db_run_b.extra_data = None
            session.commit()

        _delete_all_annotations_for_key(session_factory=session_factory, key=key)
        assert _count_annotations(session_factory=session_factory, key=key) == 0

        with session_factory() as session:
            database_migrate.run_all_annotation_backfills(
                session=session,
                do_skip_already_backfilled=True,
            )

        with session_factory() as session:
            assert (
                service.list_annotations(session=session, id=run_a.id)[key]
                == "from-extra-data"
            )
            assert (
                service.list_annotations(session=session, id=run_b.id)[key]
                == "from-component-spec"
            )

    def test_anti_join_prevents_duplicate(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """Component_spec's anti-join sees extra_data's insert within the same transaction."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME

        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="shared-name"),
        )
        _delete_all_annotations_for_key(session_factory=session_factory, key=key)
        assert _count_annotations(session_factory=session_factory, key=key) == 0

        with session_factory() as session:
            database_migrate.run_all_annotation_backfills(
                session=session,
                do_skip_already_backfilled=True,
            )

        with session_factory() as session:
            count = (
                session.query(bts.PipelineRunAnnotation)
                .filter(
                    bts.PipelineRunAnnotation.pipeline_run_id == run.id,
                    bts.PipelineRunAnnotation.key == key,
                )
                .count()
            )
        assert count == 1

    def test_backfill_skips_when_key_already_exists(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """Once any NAME annotation exists, run_all_annotation_backfills skips the name backfill."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME

        run_a = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="pipeline-a"),
        )

        run_b = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="pipeline-b"),
        )
        _delete_annotation(session_factory=session_factory, run_id=run_b.id, key=key)

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run_b.id)

        with session_factory() as session:
            database_migrate.run_all_annotation_backfills(
                session=session,
                do_skip_already_backfilled=True,
            )

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run_b.id)

    def test_force_backfill_fills_missing_name(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """do_skip_already_backfilled=False bypasses skip guard and fills missing rows."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME

        run_a = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="pipeline-a"),
        )

        run_b = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="pipeline-b"),
        )
        _delete_annotation(session_factory=session_factory, run_id=run_b.id, key=key)

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run_b.id)

        with session_factory() as session:
            database_migrate.run_all_annotation_backfills(
                session=session,
                do_skip_already_backfilled=False,
            )

        with session_factory() as session:
            assert (
                service.list_annotations(session=session, id=run_b.id)[key]
                == "pipeline-b"
            )

    # --- Phase 1 tests (backfill_pipeline_names_from_extra_data) ---

    def test_backfill_extra_data_skips_none_extra_data(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="some-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)
        assert _count_annotations(session_factory=session_factory, key=key) == 0

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = None
            session.commit()

        with session_factory() as session:
            database_migrate.backfill_pipeline_names_from_extra_data(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    def test_backfill_extra_data_skips_missing_key(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="some-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)
        assert _count_annotations(session_factory=session_factory, key=key) == 0

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = {}
            session.commit()

        with session_factory() as session:
            database_migrate.backfill_pipeline_names_from_extra_data(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    def test_backfill_extra_data_inserts_empty_name(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="some-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)
        assert _count_annotations(session_factory=session_factory, key=key) == 0

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = {"pipeline_name": ""}
            session.commit()

        with session_factory() as session:
            database_migrate.backfill_pipeline_names_from_extra_data(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == ""

    def test_backfill_extra_data_skips_null_value(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="some-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)
        assert _count_annotations(session_factory=session_factory, key=key) == 0

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = {"pipeline_name": None}
            session.commit()

        with session_factory() as session:
            database_migrate.backfill_pipeline_names_from_extra_data(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    # --- Overflow truncation tests ---

    def test_backfill_extra_data_long_name_truncated(
        self,
        mysql_varchar_limit_session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory=mysql_varchar_limit_session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="some-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(
            session_factory=mysql_varchar_limit_session_factory, run_id=run.id, key=key
        )
        assert (
            _count_annotations(
                session_factory=mysql_varchar_limit_session_factory, key=key
            )
            == 0
        )

        long_name = "x" * 300
        with mysql_varchar_limit_session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = {"pipeline_name": long_name}
            session.commit()

        with mysql_varchar_limit_session_factory() as session:
            database_migrate.backfill_pipeline_names_from_extra_data(
                session=session,
                auto_commit=True,
            )

        with mysql_varchar_limit_session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == "x" * bts._STR_MAX_LENGTH

    def test_backfill_extra_data_exact_255_preserved(
        self,
        mysql_varchar_limit_session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory=mysql_varchar_limit_session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="some-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(
            session_factory=mysql_varchar_limit_session_factory, run_id=run.id, key=key
        )
        assert (
            _count_annotations(
                session_factory=mysql_varchar_limit_session_factory, key=key
            )
            == 0
        )

        exact_name = "x" * bts._STR_MAX_LENGTH
        with mysql_varchar_limit_session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = {"pipeline_name": exact_name}
            session.commit()

        with mysql_varchar_limit_session_factory() as session:
            database_migrate.backfill_pipeline_names_from_extra_data(
                session=session,
                auto_commit=True,
            )

        with mysql_varchar_limit_session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == exact_name

    def test_backfill_component_spec_long_name_truncated(
        self,
        mysql_varchar_limit_session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory=mysql_varchar_limit_session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="some-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(
            session_factory=mysql_varchar_limit_session_factory, run_id=run.id, key=key
        )
        assert (
            _count_annotations(
                session_factory=mysql_varchar_limit_session_factory, key=key
            )
            == 0
        )

        long_name = "y" * 300
        with mysql_varchar_limit_session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = None
            session.commit()
        _set_execution_node_task_spec_raw(
            session_factory=mysql_varchar_limit_session_factory,
            run_id=run.id,
            task_spec_dict={
                "componentRef": {"spec": {"name": long_name}},
            },
        )

        with mysql_varchar_limit_session_factory() as session:
            database_migrate.backfill_pipeline_names_from_component_spec(
                session=session,
                auto_commit=True,
            )

        with mysql_varchar_limit_session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == "y" * bts._STR_MAX_LENGTH

    def test_backfill_component_spec_exact_255_preserved(
        self,
        mysql_varchar_limit_session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory=mysql_varchar_limit_session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="some-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(
            session_factory=mysql_varchar_limit_session_factory, run_id=run.id, key=key
        )
        assert (
            _count_annotations(
                session_factory=mysql_varchar_limit_session_factory, key=key
            )
            == 0
        )

        exact_name = "y" * bts._STR_MAX_LENGTH
        with mysql_varchar_limit_session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = None
            session.commit()
        _set_execution_node_task_spec_raw(
            session_factory=mysql_varchar_limit_session_factory,
            run_id=run.id,
            task_spec_dict={
                "componentRef": {"spec": {"name": exact_name}},
            },
        )

        with mysql_varchar_limit_session_factory() as session:
            database_migrate.backfill_pipeline_names_from_component_spec(
                session=session,
                auto_commit=True,
            )

        with mysql_varchar_limit_session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == exact_name

    # --- Component spec depth tests ---

    def test_component_spec_depth0_execution_node_missing(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="some-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)
        assert _count_annotations(session_factory=session_factory, key=key) == 0

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = None
            exec_node = session.get(bts.ExecutionNode, db_run.root_execution_id)
            session.delete(exec_node)
            session.commit()

        with session_factory() as session:
            database_migrate.backfill_pipeline_names_from_component_spec(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    def test_component_spec_depth1_task_spec_null(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="some-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)
        assert _count_annotations(session_factory=session_factory, key=key) == 0

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = None
            session.commit()
        _set_execution_node_task_spec_raw(
            session_factory=session_factory,
            run_id=run.id,
            task_spec_dict=None,
        )

        with session_factory() as session:
            database_migrate.backfill_pipeline_names_from_component_spec(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    def test_component_spec_depth1_task_spec_empty(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="some-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)
        assert _count_annotations(session_factory=session_factory, key=key) == 0

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = None
            session.commit()
        _set_execution_node_task_spec_raw(
            session_factory=session_factory,
            run_id=run.id,
            task_spec_dict={},
        )

        with session_factory() as session:
            database_migrate.backfill_pipeline_names_from_component_spec(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    def test_component_spec_depth2_component_ref_missing(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="some-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)
        assert _count_annotations(session_factory=session_factory, key=key) == 0

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = None
            session.commit()
        _set_execution_node_task_spec_raw(
            session_factory=session_factory,
            run_id=run.id,
            task_spec_dict={"other_key": "value"},
        )

        with session_factory() as session:
            database_migrate.backfill_pipeline_names_from_component_spec(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    def test_component_spec_depth2_component_ref_null(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="some-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)
        assert _count_annotations(session_factory=session_factory, key=key) == 0

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = None
            session.commit()
        _set_execution_node_task_spec_raw(
            session_factory=session_factory,
            run_id=run.id,
            task_spec_dict={"componentRef": None},
        )

        with session_factory() as session:
            database_migrate.backfill_pipeline_names_from_component_spec(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    def test_component_spec_depth3_spec_null(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="some-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)
        assert _count_annotations(session_factory=session_factory, key=key) == 0

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = None
            session.commit()
        _set_execution_node_task_spec(
            session_factory=session_factory,
            run_id=run.id,
            task_spec=structures.TaskSpec(
                component_ref=structures.ComponentReference(
                    name="placeholder",
                    spec=None,
                )
            ),
        )

        with session_factory() as session:
            database_migrate.backfill_pipeline_names_from_component_spec(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    def test_component_spec_depth4_name_null(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="some-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)
        assert _count_annotations(session_factory=session_factory, key=key) == 0

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = None
            session.commit()
        _set_execution_node_task_spec(
            session_factory=session_factory,
            run_id=run.id,
            task_spec=structures.TaskSpec(
                component_ref=structures.ComponentReference(
                    spec=structures.ComponentSpec(name=None)
                )
            ),
        )

        with session_factory() as session:
            database_migrate.backfill_pipeline_names_from_component_spec(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert key not in annotations

    def test_component_spec_depth4_name_empty_string(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="some-name"),
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)
        assert _count_annotations(session_factory=session_factory, key=key) == 0

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = None
            session.commit()
        _set_execution_node_task_spec_raw(
            session_factory=session_factory,
            run_id=run.id,
            task_spec_dict={"componentRef": {"spec": {"name": ""}}},
        )

        with session_factory() as session:
            database_migrate.backfill_pipeline_names_from_component_spec(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == ""


class TestCreatedByBackfill:
    def test_backfill_populates_created_by(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(),
            created_by="alice",
        )
        key = filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)
        assert _count_annotations(session_factory=session_factory, key=key) == 0

        with session_factory() as session:
            database_migrate.backfill_created_by_annotations(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == "alice"

    def test_backfill_skips_when_key_already_exists(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """Once any CREATED_BY annotation exists, run_all skips the backfill."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY

        _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(),
            created_by="alice",
        )

        run_bob = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(),
            created_by="bob",
        )
        _delete_annotation(session_factory=session_factory, run_id=run_bob.id, key=key)

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run_bob.id)

        with session_factory() as session:
            database_migrate.run_all_annotation_backfills(
                session=session,
                do_skip_already_backfilled=True,
            )

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run_bob.id)

    def test_force_backfill_fills_missing_created_by(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """do_skip_already_backfilled=False bypasses skip guard and fills missing rows."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY

        _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(),
            created_by="alice",
        )

        run_bob = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(),
            created_by="bob",
        )
        _delete_annotation(session_factory=session_factory, run_id=run_bob.id, key=key)

        with session_factory() as session:
            assert key not in service.list_annotations(session=session, id=run_bob.id)

        with session_factory() as session:
            database_migrate.run_all_annotation_backfills(
                session=session,
                do_skip_already_backfilled=False,
            )

        with session_factory() as session:
            assert (
                service.list_annotations(session=session, id=run_bob.id)[key] == "bob"
            )


class TestBackfillIdempotency:
    """Verify each backfill function is safe to call multiple times."""

    def test_created_by_idempotent(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY
        for name in ["alice", "bob", "carol"]:
            run = _create_run(
                session_factory=session_factory,
                service=service,
                root_task=_make_task_spec(),
                created_by=name,
            )
            _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)
        assert _count_annotations(session_factory=session_factory, key=key) == 0

        with session_factory() as session:
            count1 = database_migrate.backfill_created_by_annotations(
                session=session,
                auto_commit=True,
            )
        assert count1 == 3
        assert _count_annotations(session_factory=session_factory, key=key) == 3

        with session_factory() as session:
            count2 = database_migrate.backfill_created_by_annotations(
                session=session,
                auto_commit=True,
            )
        assert count2 == 0
        assert _count_annotations(session_factory=session_factory, key=key) == 3

    def test_names_extra_data_idempotent(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        for _ in range(3):
            run = _create_run(
                session_factory=session_factory,
                service=service,
                root_task=_make_task_spec(pipeline_name="pipe"),
            )
            _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)
            _set_execution_node_task_spec_raw(
                session_factory=session_factory,
                run_id=run.id,
                task_spec_dict=None,
            )
        assert _count_annotations(session_factory=session_factory, key=key) == 0

        with session_factory() as session:
            count1 = database_migrate.backfill_pipeline_names_from_extra_data(
                session=session,
                auto_commit=True,
            )
        assert count1 == 3
        assert _count_annotations(session_factory=session_factory, key=key) == 3

        with session_factory() as session:
            count2 = database_migrate.backfill_pipeline_names_from_extra_data(
                session=session,
                auto_commit=True,
            )
        assert count2 == 0
        assert _count_annotations(session_factory=session_factory, key=key) == 3

    def test_names_component_spec_idempotent(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        for _ in range(3):
            run = _create_run(
                session_factory=session_factory,
                service=service,
                root_task=_make_task_spec(pipeline_name="pipe"),
            )
            _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)
            with session_factory() as session:
                db_run = session.get(bts.PipelineRun, run.id)
                db_run.extra_data = None
                session.commit()
        assert _count_annotations(session_factory=session_factory, key=key) == 0

        with session_factory() as session:
            count1 = database_migrate.backfill_pipeline_names_from_component_spec(
                session=session,
                auto_commit=True,
            )
        assert count1 == 3
        assert _count_annotations(session_factory=session_factory, key=key) == 3

        with session_factory() as session:
            count2 = database_migrate.backfill_pipeline_names_from_component_spec(
                session=session,
                auto_commit=True,
            )
        assert count2 == 0
        assert _count_annotations(session_factory=session_factory, key=key) == 3


class TestBackfillOrderIndependence:
    """Verify pipeline name backfills produce the same result regardless of call order."""

    def _setup_two_runs(
        self,
        *,
        session_factory: orm.sessionmaker,
    ) -> tuple[str, str]:
        """Create run_a (extra_data=NULL, comp_spec="comp-a") and
        run_b (extra_data="extra-b", comp_spec=NULL task_spec)."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME

        run_a = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="comp-a"),
        )
        _delete_annotation(session_factory=session_factory, run_id=run_a.id, key=key)
        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run_a.id)
            db_run.extra_data = None
            session.commit()

        run_b = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="extra-b"),
        )
        _delete_annotation(session_factory=session_factory, run_id=run_b.id, key=key)
        _set_execution_node_task_spec_raw(
            session_factory=session_factory,
            run_id=run_b.id,
            task_spec_dict=None,
        )

        assert _count_annotations(session_factory=session_factory, key=key) == 0
        return run_a.id, run_b.id

    def test_extra_data_first_then_component_spec(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        run_a_id, run_b_id = self._setup_two_runs(session_factory=session_factory)
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME

        with session_factory() as session:
            database_migrate.backfill_pipeline_names_from_extra_data(
                session=session,
                auto_commit=True,
            )
        with session_factory() as session:
            database_migrate.backfill_pipeline_names_from_component_spec(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            assert (
                service.list_annotations(session=session, id=run_a_id)[key] == "comp-a"
            )
            assert (
                service.list_annotations(session=session, id=run_b_id)[key] == "extra-b"
            )

    def test_component_spec_first_then_extra_data(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        run_a_id, run_b_id = self._setup_two_runs(session_factory=session_factory)
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME

        with session_factory() as session:
            database_migrate.backfill_pipeline_names_from_component_spec(
                session=session,
                auto_commit=True,
            )
        with session_factory() as session:
            database_migrate.backfill_pipeline_names_from_extra_data(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            assert (
                service.list_annotations(session=session, id=run_a_id)[key] == "comp-a"
            )
            assert (
                service.list_annotations(session=session, id=run_b_id)[key] == "extra-b"
            )

    def test_order_with_both_sources_extra_data_first(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """When a run has both sources, whichever runs first wins."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME

        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="cs-name"),
        )
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)
        assert _count_annotations(session_factory=session_factory, key=key) == 0
        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = {"pipeline_name": "ed-name"}
            session.commit()

        with session_factory() as session:
            database_migrate.backfill_pipeline_names_from_extra_data(
                session=session,
                auto_commit=True,
            )
        with session_factory() as session:
            database_migrate.backfill_pipeline_names_from_component_spec(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            assert (
                service.list_annotations(session=session, id=run.id)[key] == "ed-name"
            )

    def test_order_with_both_sources_component_spec_first(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """When a run has both sources, whichever runs first wins."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME

        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="cs-name"),
        )
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)
        assert _count_annotations(session_factory=session_factory, key=key) == 0
        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run.id)
            db_run.extra_data = {"pipeline_name": "ed-name"}
            session.commit()

        with session_factory() as session:
            database_migrate.backfill_pipeline_names_from_component_spec(
                session=session,
                auto_commit=True,
            )
        with session_factory() as session:
            database_migrate.backfill_pipeline_names_from_extra_data(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            assert (
                service.list_annotations(session=session, id=run.id)[key] == "cs-name"
            )


class TestBackfillDataParity:
    """Verify backfill creates annotations for NULL/empty source values."""

    def test_created_by_null_gets_empty_annotation(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY
        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(),
        )
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)
        assert _count_annotations(session_factory=session_factory, key=key) == 0

        with session_factory() as session:
            database_migrate.backfill_created_by_annotations(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == ""

    def test_created_by_empty_gets_empty_annotation(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY
        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(),
            created_by="",
        )
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)
        assert _count_annotations(session_factory=session_factory, key=key) == 0

        with session_factory() as session:
            database_migrate.backfill_created_by_annotations(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == ""

    def test_all_backfills_data_parity(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """After backfill, every run has a created_by annotation (data parity)."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY

        for created_by in ["alice", None, "", "bob"]:
            kwargs = {"root_task": _make_task_spec()}
            if created_by is not None:
                kwargs["created_by"] = created_by
            run = _create_run(
                session_factory=session_factory, service=service, **kwargs
            )
            _delete_annotation(
                session_factory=session_factory,
                run_id=run.id,
                key=key,
            )
        assert _count_annotations(session_factory=session_factory, key=key) == 0

        with session_factory() as session:
            database_migrate.backfill_created_by_annotations(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            total_runs = session.query(bts.PipelineRun).count()
            annotation_count = _count_annotations(
                session_factory=session_factory,
                key=key,
            )
        assert annotation_count == total_runs


class TestBackfillErrorHandling:
    """Verify error behavior: functions raise, orchestrator catches."""

    def test_run_all_backfills_logs_exception_on_error(
        self,
        session_factory: orm.sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY
        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(),
            created_by="alice",
        )
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)
        assert _count_annotations(session_factory=session_factory, key=key) == 0

        monkeypatch.setattr(
            database_migrate,
            "backfill_created_by_annotations",
            mock.Mock(side_effect=RuntimeError("boom")),
        )
        with caplog.at_level(
            logging.ERROR, logger="cloud_pipelines_backend.database_migrate"
        ):
            with session_factory() as session:
                database_migrate.run_all_annotation_backfills(
                    session=session,
                    do_skip_already_backfilled=True,
                )
        assert "Annotation backfill failed" in caplog.text
        assert _count_annotations(session_factory=session_factory, key=key) == 0

    def test_backfill_function_propagates_error(
        self,
        session_factory: orm.sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Individual backfill functions DO raise (no internal try-catch)."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY
        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(),
            created_by="alice",
        )
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)
        assert _count_annotations(session_factory=session_factory, key=key) == 0

        original_execute = orm.Session.execute

        def _raising_execute(
            self_session: orm.Session,
            statement: Any,
            *args: Any,
            **kwargs: Any,
        ) -> Any:
            stmt_str = str(statement)
            if "INSERT" in stmt_str and "pipeline_run_annotation" in stmt_str:
                raise RuntimeError("simulated execute error")
            return original_execute(self_session, statement, *args, **kwargs)

        monkeypatch.setattr(orm.Session, "execute", _raising_execute)

        with session_factory() as session:
            with pytest.raises(RuntimeError, match="simulated execute error"):
                database_migrate.backfill_created_by_annotations(
                    session=session,
                    auto_commit=True,
                )

        assert _count_annotations(session_factory=session_factory, key=key) == 0

    def test_backfill_error_does_not_commit(
        self,
        session_factory: orm.sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """If execute raises, commit is never called and no rows are inserted."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY
        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(),
            created_by="alice",
        )
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)
        assert _count_annotations(session_factory=session_factory, key=key) == 0

        commit_called = False
        original_execute = orm.Session.execute
        original_commit = orm.Session.commit

        def _raising_execute(
            self_session: orm.Session,
            statement: Any,
            *args: Any,
            **kwargs: Any,
        ) -> Any:
            stmt_str = str(statement)
            if "INSERT" in stmt_str and "pipeline_run_annotation" in stmt_str:
                raise RuntimeError("simulated execute error")
            return original_execute(self_session, statement, *args, **kwargs)

        def _tracking_commit(
            self_session: orm.Session,
        ) -> None:
            nonlocal commit_called
            commit_called = True
            return original_commit(self_session)

        monkeypatch.setattr(orm.Session, "execute", _raising_execute)
        monkeypatch.setattr(orm.Session, "commit", _tracking_commit)

        with session_factory() as session:
            with pytest.raises(RuntimeError, match="simulated execute error"):
                database_migrate.backfill_created_by_annotations(
                    session=session,
                    auto_commit=True,
                )

        assert not commit_called
        assert _count_annotations(session_factory=session_factory, key=key) == 0
