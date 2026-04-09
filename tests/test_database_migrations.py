"""Tests for database_migrations: backfill and annotation helpers.

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
from cloud_pipelines_backend import database_migrations
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


class TestCountMissingAnnotationsForKey:
    def test_zero_on_empty_db(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """Empty DB has 0 runs and 0 annotations — nothing to backfill."""
        with session_factory() as session:
            assert (
                database_migrations._count_missing_annotations_for_key(
                    session=session,
                    key=filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
                )
                == 0
            )

    def test_nonzero_with_unrelated_annotation(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """Run has an unrelated annotation but is missing the queried key."""
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
                database_migrations._count_missing_annotations_for_key(
                    session=session,
                    key=filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
                )
                == 1
            )

    def test_zero_when_all_runs_have_key(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """All runs have the annotation — nothing to backfill."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(),
            created_by="alice",
        )
        with session_factory() as session:
            assert (
                database_migrations._count_missing_annotations_for_key(
                    session=session,
                    key=filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
                )
                == 0
            )

    def test_matches_exact_key(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """Only returns 0 for the exact key queried, not other keys."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(),
        )
        with session_factory() as session:
            assert (
                database_migrations._count_missing_annotations_for_key(
                    session=session,
                    key="team",
                )
                == 1
            )
        with session_factory() as session:
            service.set_annotation(session=session, id=run.id, key="team", value="ml")
        with session_factory() as session:
            assert (
                database_migrations._count_missing_annotations_for_key(
                    session=session,
                    key="team",
                )
                == 0
            )
            assert (
                database_migrations._count_missing_annotations_for_key(
                    session=session,
                    key="other_key",
                )
                == 1
            )

    def test_nonzero_when_partially_backfilled(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """Some runs have the annotation, some don't — 1 missing."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY
        _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(),
            created_by="alice",
        )
        run_b = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(),
            created_by="bob",
        )
        _delete_annotation(session_factory=session_factory, run_id=run_b.id, key=key)
        with session_factory() as session:
            assert (
                database_migrations._count_missing_annotations_for_key(
                    session=session,
                    key=key,
                )
                == 1
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
            database_migrations.backfill_pipeline_names_from_extra_data(
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
            database_migrations.backfill_pipeline_names_from_component_spec_and_parity(
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
            result = database_migrations.run_all_annotation_backfills(
                session=session,
            )

        assert result.error is None
        assert result.created_by is not None
        assert result.names_extra_data is not None
        assert result.names_component_spec is not None
        assert (
            result.names_extra_data.status == database_migrations.BackfillStatus.SUCCESS
        )
        assert (
            result.names_component_spec.status
            == database_migrations.BackfillStatus.SUCCESS
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
            result = database_migrations.run_all_annotation_backfills(
                session=session,
            )

        assert result.error is None
        assert result.names_extra_data is not None
        assert (
            result.names_extra_data.status == database_migrations.BackfillStatus.SUCCESS
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

    def test_backfill_detects_partial_and_fills_missing(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """Partial backfill is detected: run_b missing annotation gets filled."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME

        _create_run(
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
            result = database_migrations.run_all_annotation_backfills(
                session=session,
            )

        assert result.error is None
        assert result.names_extra_data is not None
        assert (
            result.names_extra_data.status == database_migrations.BackfillStatus.SUCCESS
        )
        assert result.names_extra_data.rows_inserted >= 1

        with session_factory() as session:
            assert (
                service.list_annotations(session=session, id=run_b.id)[key]
                == "pipeline-b"
            )
            assert (
                database_migrations._count_missing_annotations_for_key(
                    session=session,
                    key=key,
                )
                == 0
            )

    def test_skips_when_all_runs_have_annotation(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """All runs have the annotation — skip guard prevents re-run."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME

        _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="pipeline-a"),
        )
        _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="pipeline-b"),
        )
        assert _count_annotations(session_factory=session_factory, key=key) == 2

        with session_factory() as session:
            result = database_migrations.run_all_annotation_backfills(
                session=session,
            )

        assert result.error is None
        assert result.names_extra_data is not None
        assert (
            result.names_extra_data.status == database_migrations.BackfillStatus.SKIPPED
        )
        assert result.names_component_spec is not None
        assert (
            result.names_component_spec.status
            == database_migrations.BackfillStatus.SKIPPED
        )
        assert _count_annotations(session_factory=session_factory, key=key) == 2

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
            database_migrations.backfill_pipeline_names_from_extra_data(
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
            database_migrations.backfill_pipeline_names_from_extra_data(
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
            database_migrations.backfill_pipeline_names_from_extra_data(
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
            database_migrations.backfill_pipeline_names_from_extra_data(
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
            database_migrations.backfill_pipeline_names_from_extra_data(
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
            database_migrations.backfill_pipeline_names_from_extra_data(
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
            database_migrations.backfill_pipeline_names_from_component_spec_and_parity(
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
            database_migrations.backfill_pipeline_names_from_component_spec_and_parity(
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
            database_migrations.backfill_pipeline_names_from_component_spec_and_parity(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == ""

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
            database_migrations.backfill_pipeline_names_from_component_spec_and_parity(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == ""

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
            database_migrations.backfill_pipeline_names_from_component_spec_and_parity(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == ""

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
            database_migrations.backfill_pipeline_names_from_component_spec_and_parity(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == ""

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
            database_migrations.backfill_pipeline_names_from_component_spec_and_parity(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == ""

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
            database_migrations.backfill_pipeline_names_from_component_spec_and_parity(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == ""

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
            database_migrations.backfill_pipeline_names_from_component_spec_and_parity(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == ""

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
            database_migrations.backfill_pipeline_names_from_component_spec_and_parity(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == ""


class TestComponentSpecParityCatchall:
    """Tests for Phase B of backfill_pipeline_names_from_component_spec_and_parity:
    every pipeline_run gets a name annotation (empty string if no name found)."""

    def test_parity_fills_all_runs(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """Two runs: one with valid name, one with null spec.
        After backfill, both have annotations — the second with empty string."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME

        run_named = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="real-pipeline"),
        )
        run_no_name = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="placeholder"),
        )

        _delete_annotation(
            session_factory=session_factory, run_id=run_named.id, key=key
        )
        _delete_annotation(
            session_factory=session_factory, run_id=run_no_name.id, key=key
        )
        assert _count_annotations(session_factory=session_factory, key=key) == 0

        with session_factory() as session:
            db_run = session.get(bts.PipelineRun, run_no_name.id)
            db_run.extra_data = None
            session.commit()
        _set_execution_node_task_spec(
            session_factory=session_factory,
            run_id=run_no_name.id,
            task_spec=structures.TaskSpec(
                component_ref=structures.ComponentReference(
                    spec=structures.ComponentSpec(name=None)
                )
            ),
        )

        with session_factory() as session:
            total = database_migrations.backfill_pipeline_names_from_component_spec_and_parity(
                session=session,
                auto_commit=True,
            )

        assert total == 2

        with session_factory() as session:
            ann_named = service.list_annotations(session=session, id=run_named.id)
            ann_no_name = service.list_annotations(session=session, id=run_no_name.id)
        assert ann_named[key] == "real-pipeline"
        assert ann_no_name[key] == ""

    def test_parity_idempotent(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """Running the function twice inserts 0 rows the second time."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME

        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="my-pipeline"),
        )
        _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        with session_factory() as session:
            first = database_migrations.backfill_pipeline_names_from_component_spec_and_parity(
                session=session,
                auto_commit=True,
            )
        assert first > 0

        with session_factory() as session:
            second = database_migrations.backfill_pipeline_names_from_component_spec_and_parity(
                session=session,
                auto_commit=True,
            )
        assert second == 0

    def test_parity_count_equals_pipeline_runs(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """After backfill, the number of name annotations equals the number of pipeline runs."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME

        for i in range(3):
            run = _create_run(
                session_factory=session_factory,
                service=service,
                root_task=_make_task_spec(pipeline_name=f"pipe-{i}"),
            )
            _delete_annotation(session_factory=session_factory, run_id=run.id, key=key)

        assert _count_annotations(session_factory=session_factory, key=key) == 0

        with session_factory() as session:
            database_migrations.backfill_pipeline_names_from_component_spec_and_parity(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            run_count = session.query(
                sqlalchemy.func.count(bts.PipelineRun.id)
            ).scalar()
            ann_count = _count_annotations(session_factory=session_factory, key=key)
        assert ann_count == run_count


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
            database_migrations.backfill_created_by_annotations(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
        assert annotations[key] == "alice"

    def test_backfill_detects_partial_and_fills_missing(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """Partial backfill is detected: run_bob missing annotation gets filled."""
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
            database_migrations.run_all_annotation_backfills(
                session=session,
            )

        with session_factory() as session:
            assert (
                service.list_annotations(session=session, id=run_bob.id)[key] == "bob"
            )
            assert (
                database_migrations._count_missing_annotations_for_key(
                    session=session,
                    key=key,
                )
                == 0
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
            count1 = database_migrations.backfill_created_by_annotations(
                session=session,
                auto_commit=True,
            )
        assert count1 == 3
        assert _count_annotations(session_factory=session_factory, key=key) == 3

        with session_factory() as session:
            count2 = database_migrations.backfill_created_by_annotations(
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
            count1 = database_migrations.backfill_pipeline_names_from_extra_data(
                session=session,
                auto_commit=True,
            )
        assert count1 == 3
        assert _count_annotations(session_factory=session_factory, key=key) == 3

        with session_factory() as session:
            count2 = database_migrations.backfill_pipeline_names_from_extra_data(
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
            count1 = database_migrations.backfill_pipeline_names_from_component_spec_and_parity(
                session=session,
                auto_commit=True,
            )
        assert count1 == 3
        assert _count_annotations(session_factory=session_factory, key=key) == 3

        with session_factory() as session:
            count2 = database_migrations.backfill_pipeline_names_from_component_spec_and_parity(
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
            database_migrations.backfill_pipeline_names_from_extra_data(
                session=session,
                auto_commit=True,
            )
        with session_factory() as session:
            database_migrations.backfill_pipeline_names_from_component_spec_and_parity(
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
        """When component_spec_and_parity runs first, Phase B fills run_b
        with empty string. The subsequent extra_data backfill cannot override
        because the anti-join sees an existing annotation."""
        run_a_id, run_b_id = self._setup_two_runs(session_factory=session_factory)
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME

        with session_factory() as session:
            database_migrations.backfill_pipeline_names_from_component_spec_and_parity(
                session=session,
                auto_commit=True,
            )
        with session_factory() as session:
            database_migrations.backfill_pipeline_names_from_extra_data(
                session=session,
                auto_commit=True,
            )

        with session_factory() as session:
            assert (
                service.list_annotations(session=session, id=run_a_id)[key] == "comp-a"
            )
            assert service.list_annotations(session=session, id=run_b_id)[key] == ""

    def test_order_with_both_sources_extra_data_first(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """extra_data first: fills ed-run, then spec_and_parity fills cs-run."""
        run_a_id, run_b_id = self._setup_two_runs(session_factory=session_factory)
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME

        with session_factory() as session:
            database_migrations.backfill_pipeline_names_from_extra_data(
                session=session,
                auto_commit=True,
            )
        with session_factory() as session:
            ann_a = service.list_annotations(session=session, id=run_a_id)
            ann_b = service.list_annotations(session=session, id=run_b_id)
        assert key not in ann_a
        assert ann_b[key] == "extra-b"

        with session_factory() as session:
            database_migrations.backfill_pipeline_names_from_component_spec_and_parity(
                session=session,
                auto_commit=True,
            )
        with session_factory() as session:
            ann_a = service.list_annotations(session=session, id=run_a_id)
            ann_b = service.list_annotations(session=session, id=run_b_id)
        assert ann_a[key] == "comp-a"
        assert ann_b[key] == "extra-b"

    def test_order_with_both_sources_component_spec_first(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """spec_and_parity first: fills cs-run + parity for ed-run,
        then extra_data can't override (anti-join skips)."""
        run_a_id, run_b_id = self._setup_two_runs(session_factory=session_factory)
        service = api_server_sql.PipelineRunsApiService_Sql()
        key = filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME

        with session_factory() as session:
            database_migrations.backfill_pipeline_names_from_component_spec_and_parity(
                session=session,
                auto_commit=True,
            )
        with session_factory() as session:
            ann_a = service.list_annotations(session=session, id=run_a_id)
            ann_b = service.list_annotations(session=session, id=run_b_id)
        assert ann_a[key] == "comp-a"
        assert ann_b[key] == ""

        with session_factory() as session:
            database_migrations.backfill_pipeline_names_from_extra_data(
                session=session,
                auto_commit=True,
            )
        with session_factory() as session:
            ann_a = service.list_annotations(session=session, id=run_a_id)
            ann_b = service.list_annotations(session=session, id=run_b_id)
        assert ann_a[key] == "comp-a"
        assert ann_b[key] == ""


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
            database_migrations.backfill_created_by_annotations(
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
            database_migrations.backfill_created_by_annotations(
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
            database_migrations.backfill_created_by_annotations(
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
            database_migrations,
            "backfill_created_by_annotations",
            mock.Mock(side_effect=RuntimeError("boom")),
        )
        with caplog.at_level(
            logging.ERROR, logger="cloud_pipelines_backend.database_migrations"
        ):
            with session_factory() as session:
                result = database_migrations.run_all_annotation_backfills(
                    session=session,
                )
        assert "Annotation backfill failed" in caplog.text
        assert result.error is not None
        assert "boom" in result.error
        assert result.created_by is None
        assert result.names_extra_data is None
        assert result.names_component_spec is None
        assert _count_annotations(session_factory=session_factory, key=key) == 0
        d = result.to_dict()
        assert "error" in d
        assert "created_by" not in d
        assert "names_extra_data" not in d
        assert "names_component_spec" not in d

    def test_run_all_backfills_to_dict_on_success(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """to_dict returns step dicts with status/rows_inserted on success."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        created_by_key = filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY
        pipeline_name_key = (
            filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        )
        _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="p1"),
            created_by="alice",
        )
        _delete_all_annotations_for_key(
            session_factory=session_factory,
            key=pipeline_name_key,
        )
        _delete_all_annotations_for_key(
            session_factory=session_factory,
            key=created_by_key,
        )
        assert (
            _count_annotations(session_factory=session_factory, key=pipeline_name_key)
            == 0
        )
        assert (
            _count_annotations(session_factory=session_factory, key=created_by_key) == 0
        )

        with session_factory() as session:
            result = database_migrations.run_all_annotation_backfills(
                session=session,
            )

        d = result.to_dict()
        assert "error" not in d
        assert d["names_extra_data"]["status"] == "success"
        assert d["names_extra_data"]["rows_inserted"] >= 1
        assert d["names_component_spec"]["status"] == "success"
        assert d["created_by"]["status"] == "success"
        assert d["created_by"]["rows_inserted"] >= 1

    def test_run_all_backfills_to_dict_on_skipped(
        self,
        session_factory: orm.sessionmaker,
    ) -> None:
        """to_dict returns 'skipped' when all annotations already exist."""
        service = api_server_sql.PipelineRunsApiService_Sql()
        _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="p1"),
            created_by="alice",
        )

        with session_factory() as session:
            result = database_migrations.run_all_annotation_backfills(
                session=session,
            )

        d = result.to_dict()
        assert "error" not in d
        assert d["created_by"]["status"] == "skipped"
        assert d["created_by"]["rows_inserted"] == 0
        assert d["names_extra_data"]["status"] == "skipped"
        assert d["names_component_spec"]["status"] == "skipped"

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
                database_migrations.backfill_created_by_annotations(
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
                database_migrations.backfill_created_by_annotations(
                    session=session,
                    auto_commit=True,
                )

        assert not commit_called
        assert _count_annotations(session_factory=session_factory, key=key) == 0


class TestMigrateDbBackfillFlag:
    """Tests for migrate_db do_skip_backfill flag."""

    def test_migrate_db_runs_backfills(self) -> None:
        """migrate_db with do_skip_backfill=False backfills all annotation keys."""
        db_engine = database_ops.create_db_engine(database_uri="sqlite://")
        bts._TableBase.metadata.create_all(db_engine)
        session_factory = orm.sessionmaker(db_engine)

        service = api_server_sql.PipelineRunsApiService_Sql()
        run = _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="my-pipeline"),
            created_by="alice",
        )

        created_by_key = filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY
        pipeline_name_key = (
            filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        )
        _delete_all_annotations_for_key(
            session_factory=session_factory, key=created_by_key
        )
        _delete_all_annotations_for_key(
            session_factory=session_factory, key=pipeline_name_key
        )
        assert (
            _count_annotations(session_factory=session_factory, key=created_by_key) == 0
        )
        assert (
            _count_annotations(session_factory=session_factory, key=pipeline_name_key)
            == 0
        )

        database_ops.migrate_db(db_engine=db_engine, do_skip_backfill=False)

        assert (
            _count_annotations(session_factory=session_factory, key=created_by_key) == 1
        )
        assert (
            _count_annotations(session_factory=session_factory, key=pipeline_name_key)
            == 1
        )
        with session_factory() as session:
            annotations = service.list_annotations(session=session, id=run.id)
            assert annotations[created_by_key] == "alice"
            assert annotations[pipeline_name_key] == "my-pipeline"

    def test_migrate_db_skips_backfills_when_flag_set(self) -> None:
        """do_skip_backfill=True prevents annotation backfills from running."""
        db_engine = database_ops.create_db_engine(database_uri="sqlite://")
        bts._TableBase.metadata.create_all(db_engine)
        session_factory = orm.sessionmaker(db_engine)

        service = api_server_sql.PipelineRunsApiService_Sql()
        _create_run(
            session_factory=session_factory,
            service=service,
            root_task=_make_task_spec(pipeline_name="my-pipeline"),
            created_by="alice",
        )

        created_by_key = filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY
        pipeline_name_key = (
            filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
        )
        _delete_all_annotations_for_key(
            session_factory=session_factory, key=created_by_key
        )
        _delete_all_annotations_for_key(
            session_factory=session_factory, key=pipeline_name_key
        )
        assert (
            _count_annotations(session_factory=session_factory, key=created_by_key) == 0
        )
        assert (
            _count_annotations(session_factory=session_factory, key=pipeline_name_key)
            == 0
        )

        database_ops.migrate_db(db_engine=db_engine, do_skip_backfill=True)

        assert (
            _count_annotations(session_factory=session_factory, key=created_by_key) == 0
        )
        assert (
            _count_annotations(session_factory=session_factory, key=pipeline_name_key)
            == 0
        )


# ---------------------------------------------------------------------------
# secret_value column migration
# ---------------------------------------------------------------------------


def _create_secret_table_varchar(
    *,
    db_engine: sqlalchemy.Engine,
) -> None:
    """Create the secret table with VARCHAR(255) for secret_value."""
    with db_engine.connect() as conn:
        conn.execute(
            sqlalchemy.text(
                "CREATE TABLE secret ("
                "  user_id VARCHAR(255) NOT NULL,"
                "  secret_name VARCHAR(255) NOT NULL,"
                "  secret_value VARCHAR(255) NOT NULL,"
                "  created_at DATETIME NOT NULL,"
                "  updated_at DATETIME NOT NULL,"
                "  expires_at DATETIME,"
                "  PRIMARY KEY (user_id, secret_name)"
                ")"
            )
        )
        conn.execute(
            sqlalchemy.text(
                "INSERT INTO secret"
                " (user_id, secret_name, secret_value, created_at, updated_at)"
                " VALUES ('u1', 'key1', 'my-secret-value',"
                " '2025-01-01', '2025-01-01')"
            )
        )
        conn.commit()


def _get_secret_value_column_type(
    *,
    db_engine: sqlalchemy.Engine,
) -> sqlalchemy.types.TypeEngine:
    """Return the SQLAlchemy type of secret.secret_value from the DB."""
    inspector = sqlalchemy.inspect(db_engine)
    columns = {c["name"]: c for c in inspector.get_columns("secret")}
    return columns["secret_value"]["type"]


def test_migrate_secret_value_column(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Migration widens secret_value from VARCHAR to TEXT and preserves data."""
    db_engine = database_ops.create_db_engine(database_uri="sqlite://")
    _create_secret_table_varchar(db_engine=db_engine)

    col_type_before = _get_secret_value_column_type(db_engine=db_engine)
    assert isinstance(col_type_before, sqlalchemy.types.VARCHAR)

    with caplog.at_level(logging.INFO):
        database_migrations.migrate_secret_value_column(db_engine=db_engine)

    col_type_after = _get_secret_value_column_type(db_engine=db_engine)
    assert isinstance(col_type_after, sqlalchemy.types.TEXT)

    with db_engine.connect() as conn:
        row = conn.execute(
            sqlalchemy.text(
                "SELECT secret_value FROM secret"
                " WHERE user_id = 'u1' AND secret_name = 'key1'"
            )
        ).fetchone()
    assert row is not None
    assert row[0] == "my-secret-value"
    assert "migrate column to TEXT: complete" in caplog.messages


def test_migrate_secret_value_column_idempotent(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Second call is a no-op when secret_value is already TEXT."""
    db_engine = database_ops.create_db_engine(database_uri="sqlite://")
    _create_secret_table_varchar(db_engine=db_engine)

    with caplog.at_level(logging.INFO):
        database_migrations.migrate_secret_value_column(db_engine=db_engine)
        database_migrations.migrate_secret_value_column(db_engine=db_engine)

    col_type_after = _get_secret_value_column_type(db_engine=db_engine)
    assert isinstance(col_type_after, sqlalchemy.types.TEXT)

    our_msgs = [
        m
        for m in caplog.messages
        if m.startswith("migrate column to TEXT:")
    ]
    assert our_msgs[-2] == "migrate column to TEXT: complete"
    assert our_msgs[-1] == "migrate column to TEXT: skipped (already TEXT)"
