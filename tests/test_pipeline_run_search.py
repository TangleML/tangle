"""Tests for the pipeline run list() annotation filtering functionality.

These tests document the JSON string format expected by the API and verify
the WHERE clause generation for annotation filters.
"""

import json

from sqlalchemy import orm
import pytest

from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import database_ops


def _initialize_db_and_get_session_factory():
    """Initialize an in-memory SQLite database and return a session factory."""
    db_engine = database_ops.create_db_engine_and_migrate_db(database_uri="sqlite://")
    return lambda: orm.Session(bind=db_engine)


class TestPipelineRunListAnnotationFilter:
    """Tests for PipelineRunsApiService_Sql.list() annotation filtering.

    Each test shows the JSON string input format and the expected SQL WHERE clause.
    """

    def test_list_with_no_annotation_filter(self):
        """Test list with annotation_filter=None returns all pipeline runs."""
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()

        with session_factory() as session:
            response = service.list(
                session=session,
                annotation_filter=None,
                debug_where_clause=True,
            )

            assert response.debug_where_clause == "(no where clauses)"
            assert response.pipeline_runs == []

    def test_list_with_key_contains_filter_json(self):
        """Test annotation filtering with JSON string: key CONTAINS operator.

        JSON Input:
            {"annotation_filters": [{"key": {"operator": "contains", "text": "env"}}]}

        Expected WHERE clause:
            EXISTS (... WHERE key LIKE '%env%')
        """
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()

        with session_factory() as session:
            # JSON string input (as received from API)
            json_input = json.dumps(
                {
                    "annotation_filters": [
                        {"key": {"operator": "contains", "text": "env"}}
                    ]
                }
            )

            response = service.list(
                session=session,
                annotation_filter=json_input,
                debug_where_clause=True,
            )

            expected = (
                "EXISTS (SELECT pipeline_run_annotation.pipeline_run_id, "
                'pipeline_run_annotation."key", pipeline_run_annotation.value \n'
                "FROM pipeline_run_annotation, pipeline_run \n"
                "WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id "
                "AND (pipeline_run_annotation.\"key\" LIKE '%' || 'env' || '%'))"
            )
            assert response.debug_where_clause == expected

    def test_list_with_key_equals_filter_json(self):
        """Test annotation filtering with JSON string: key EQUALS operator.

        JSON Input:
            {"annotation_filters": [{"key": {"operator": "equals", "text": "environment"}}]}

        Expected WHERE clause:
            EXISTS (... WHERE key = 'environment')
        """
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()

        with session_factory() as session:
            json_input = json.dumps(
                {
                    "annotation_filters": [
                        {"key": {"operator": "equals", "text": "environment"}}
                    ]
                }
            )

            response = service.list(
                session=session,
                annotation_filter=json_input,
                debug_where_clause=True,
            )

            expected = (
                "EXISTS (SELECT pipeline_run_annotation.pipeline_run_id, "
                'pipeline_run_annotation."key", pipeline_run_annotation.value \n'
                "FROM pipeline_run_annotation, pipeline_run \n"
                "WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id "
                "AND pipeline_run_annotation.\"key\" = 'environment')"
            )
            assert response.debug_where_clause == expected

    def test_list_with_key_equals_negate_filter_json(self):
        """Test annotation filtering with JSON string: key EQUALS with negate=true.

        JSON Input:
            {"annotation_filters": [{"key": {"operator": "equals", "text": "environment", "negate": true}}]}

        Expected WHERE clause:
            EXISTS (... WHERE key != 'environment')
        """
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()

        with session_factory() as session:
            json_input = json.dumps(
                {
                    "annotation_filters": [
                        {
                            "key": {
                                "operator": "equals",
                                "text": "environment",
                                "negate": True,
                            }
                        }
                    ]
                }
            )

            response = service.list(
                session=session,
                annotation_filter=json_input,
                debug_where_clause=True,
            )

            expected = (
                "EXISTS (SELECT pipeline_run_annotation.pipeline_run_id, "
                'pipeline_run_annotation."key", pipeline_run_annotation.value \n'
                "FROM pipeline_run_annotation, pipeline_run \n"
                "WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id "
                "AND pipeline_run_annotation.\"key\" != 'environment')"
            )
            assert response.debug_where_clause == expected

    def test_list_with_key_in_set_filter_json(self):
        """Test annotation filtering with JSON string: key IN_SET operator.

        JSON Input:
            {"annotation_filters": [{"key": {"operator": "in_set", "texts": ["environment", "team"]}}]}

        Expected WHERE clause:
            EXISTS (... WHERE key IN ('environment', 'team'))
        """
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()

        with session_factory() as session:
            json_input = json.dumps(
                {
                    "annotation_filters": [
                        {
                            "key": {
                                "operator": "in_set",
                                "texts": ["environment", "team"],
                            }
                        }
                    ]
                }
            )

            response = service.list(
                session=session,
                annotation_filter=json_input,
                debug_where_clause=True,
            )

            expected = (
                "EXISTS (SELECT pipeline_run_annotation.pipeline_run_id, "
                'pipeline_run_annotation."key", pipeline_run_annotation.value \n'
                "FROM pipeline_run_annotation, pipeline_run \n"
                "WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id "
                "AND pipeline_run_annotation.\"key\" IN ('environment', 'team'))"
            )
            assert response.debug_where_clause == expected

    def test_list_with_key_and_value_filter_json(self):
        """Test annotation filtering with JSON string: key AND value on same row.

        JSON Input:
            {
              "annotation_filters": [{
                "key": {"operator": "equals", "text": "environment"},
                "value": {"operator": "equals", "text": "production"}
              }]
            }

        Expected WHERE clause:
            EXISTS (... WHERE key = 'environment' AND value = 'production')
        """
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()

        with session_factory() as session:
            json_input = json.dumps(
                {
                    "annotation_filters": [
                        {
                            "key": {"operator": "equals", "text": "environment"},
                            "value": {"operator": "equals", "text": "production"},
                        }
                    ]
                }
            )

            response = service.list(
                session=session,
                annotation_filter=json_input,
                debug_where_clause=True,
            )

            expected = (
                "EXISTS (SELECT pipeline_run_annotation.pipeline_run_id, "
                'pipeline_run_annotation."key", pipeline_run_annotation.value \n'
                "FROM pipeline_run_annotation, pipeline_run \n"
                "WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id "
                "AND pipeline_run_annotation.\"key\" = 'environment' "
                "AND pipeline_run_annotation.value = 'production')"
            )
            assert response.debug_where_clause == expected

    def test_list_with_multiple_filters_or_json(self):
        """Test annotation filtering with JSON string: multiple filters with OR (default).

        JSON Input:
            {
              "annotation_filters": [
                {"key": {"operator": "equals", "text": "environment"},
                 "value": {"operator": "equals", "text": "production"}},
                {"key": {"operator": "equals", "text": "team"},
                 "value": {"operator": "equals", "text": "backend"}}
              ]
            }

        Expected WHERE clause:
            (EXISTS ... env=prod) OR (EXISTS ... team=backend)
        """
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()

        with session_factory() as session:
            json_input = json.dumps(
                {
                    "annotation_filters": [
                        {
                            "key": {"operator": "equals", "text": "environment"},
                            "value": {"operator": "equals", "text": "production"},
                        },
                        {
                            "key": {"operator": "equals", "text": "team"},
                            "value": {"operator": "equals", "text": "backend"},
                        },
                    ]
                }
            )

            response = service.list(
                session=session,
                annotation_filter=json_input,
                debug_where_clause=True,
            )

            expected = (
                "(EXISTS (SELECT pipeline_run_annotation.pipeline_run_id, "
                'pipeline_run_annotation."key", pipeline_run_annotation.value \n'
                "FROM pipeline_run_annotation, pipeline_run \n"
                "WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id "
                "AND pipeline_run_annotation.\"key\" = 'environment' "
                "AND pipeline_run_annotation.value = 'production')) "
                "OR "
                "(EXISTS (SELECT pipeline_run_annotation.pipeline_run_id, "
                'pipeline_run_annotation."key", pipeline_run_annotation.value \n'
                "FROM pipeline_run_annotation, pipeline_run \n"
                "WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id "
                "AND pipeline_run_annotation.\"key\" = 'team' "
                "AND pipeline_run_annotation.value = 'backend'))"
            )
            assert response.debug_where_clause == expected

    def test_list_with_multiple_filters_and_json(self):
        """Test annotation filtering with JSON string: multiple filters with AND.

        JSON Input:
            {
              "operator": "and",
              "annotation_filters": [
                {"key": {"operator": "contains", "text": "env"},
                 "value": {"operator": "in_set", "texts": ["prod", "staging"]}},
                {"key": {"operator": "equals", "text": "team"},
                 "value": {"operator": "contains", "text": "test", "negate": true}}
              ]
            }

        Expected WHERE clause:
            (EXISTS ... env LIKE %env% AND value IN (...))
            AND
            (EXISTS ... team AND value NOT LIKE %test%)
        """
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()

        with session_factory() as session:
            json_input = json.dumps(
                {
                    "operator": "and",
                    "annotation_filters": [
                        {
                            "key": {"operator": "contains", "text": "env"},
                            "value": {
                                "operator": "in_set",
                                "texts": ["prod", "staging"],
                            },
                        },
                        {
                            "key": {"operator": "equals", "text": "team"},
                            "value": {
                                "operator": "contains",
                                "text": "test",
                                "negate": True,
                            },
                        },
                    ],
                }
            )

            response = service.list(
                session=session,
                annotation_filter=json_input,
                debug_where_clause=True,
            )

            expected = (
                "(EXISTS (SELECT pipeline_run_annotation.pipeline_run_id, "
                'pipeline_run_annotation."key", pipeline_run_annotation.value \n'
                "FROM pipeline_run_annotation, pipeline_run \n"
                "WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id "
                "AND (pipeline_run_annotation.\"key\" LIKE '%' || 'env' || '%') "
                "AND pipeline_run_annotation.value IN ('prod', 'staging'))) "
                "AND "
                "(EXISTS (SELECT pipeline_run_annotation.pipeline_run_id, "
                'pipeline_run_annotation."key", pipeline_run_annotation.value \n'
                "FROM pipeline_run_annotation, pipeline_run \n"
                "WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id "
                "AND pipeline_run_annotation.\"key\" = 'team' "
                "AND (pipeline_run_annotation.value NOT LIKE '%' || 'test' || '%')))"
            )
            assert response.debug_where_clause == expected

    def test_list_with_user_filter_and_annotation_filter(self):
        """Test WHERE clause ordering: user filter BEFORE annotation filter.

        This test verifies that simple column filters (indexed) come before
        EXISTS subqueries (annotation filters) for performance optimization.

        Filter input: created_by:alice
        Annotation filter: {"annotation_filters": [{"key": {"operator": "equals", "text": "env"}}]}

        Expected WHERE clause order:
            1. pipeline_run.created_by = 'alice'  (simple, indexed)
            2. EXISTS (...)                        (subquery)
        """
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()

        with session_factory() as session:
            json_input = json.dumps(
                {"annotation_filters": [{"key": {"operator": "equals", "text": "env"}}]}
            )

            response = service.list(
                session=session,
                filter="created_by:alice",
                annotation_filter=json_input,
                debug_where_clause=True,
            )

            # Verify the WHERE clause contains both filters
            # User filter should come BEFORE annotation filter
            assert "created_by = 'alice'" in response.debug_where_clause
            assert "EXISTS" in response.debug_where_clause

            # Verify ordering: created_by appears before EXISTS
            created_by_pos = response.debug_where_clause.find("created_by")
            exists_pos = response.debug_where_clause.find("EXISTS")
            assert (
                created_by_pos < exists_pos
            ), "User filter should appear before annotation filter in WHERE clause"

    def test_list_with_invalid_json_raises_error(self):
        """Test that invalid JSON in annotation_filter raises ApiServiceError."""
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()

        with session_factory() as session:
            with pytest.raises(api_server_sql.ApiServiceError) as exc_info:
                service.list(
                    session=session,
                    annotation_filter="not valid json",
                    debug_where_clause=True,
                )

            # Pydantic's error message includes "Invalid JSON"
            assert "Invalid annotation_filter" in str(exc_info.value)
            assert "Invalid JSON" in str(exc_info.value)

    def test_list_with_missing_annotation_filters_key_raises_error(self):
        """Test that JSON without 'annotation_filters' key raises ApiServiceError."""
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()

        with session_factory() as session:
            with pytest.raises(api_server_sql.ApiServiceError) as exc_info:
                service.list(
                    session=session,
                    annotation_filter='{"operator": "and"}',
                    debug_where_clause=True,
                )

            # Pydantic's error message indicates 'annotation_filters' field is required
            assert "Invalid annotation_filter" in str(exc_info.value)
            assert "annotation_filters" in str(exc_info.value)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
