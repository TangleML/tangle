"""Tests for the pipeline run search functionality."""

from sqlalchemy import orm
import pytest

from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import database_ops


def _initialize_db_and_get_session_factory():
    """Initialize an in-memory SQLite database and return a session factory."""
    db_engine = database_ops.create_db_engine_and_migrate_db(database_uri="sqlite://")
    return lambda: orm.Session(bind=db_engine)


class TestPipelineRunSearch:
    """Tests for PipelineRunsApiService_Sql.search()"""

    def test_search_with_no_filters(self):
        """Test search with filters=None returns all pipeline runs."""
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()

        with session_factory() as session:
            response = service.search(
                session=session,
                filters=None,
                debug_where_clause=True,
            )

            assert response.debug_where_clause == "(no where clauses)"
            assert response.pipeline_runs == []

    def test_search_with_key_exists_filter(self):
        """Test search with KeyFilter EXISTS operator."""
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()

        with session_factory() as session:
            filters = api_server_sql.SearchFilters(
                annotation_filters=api_server_sql.FilterGroup(
                    operator=api_server_sql.GroupOperator.AND,
                    filters=[
                        api_server_sql.KeyFilter(
                            operator=api_server_sql.KeyFilterOperator.EXISTS
                        )
                    ],
                )
            )

            response = service.search(
                session=session,
                filters=filters,
                debug_where_clause=True,
            )

            expected = (
                "EXISTS (SELECT pipeline_run_annotation.pipeline_run_id, "
                'pipeline_run_annotation."key", pipeline_run_annotation.value \n'
                "FROM pipeline_run_annotation, pipeline_run \n"
                "WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id "
                'AND pipeline_run_annotation."key" IS NOT NULL)'
            )
            assert response.debug_where_clause == expected

    def test_search_with_key_exists_no_group_operator_filter(self):
        """Test search with KeyFilter EXISTS operator without specifying group operator.

        When operator is not specified in FilterGroup, it should default to AND logic.
        """
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()

        with session_factory() as session:
            # Note: operator is not specified, should default to AND
            filters = api_server_sql.SearchFilters(
                annotation_filters=api_server_sql.FilterGroup(
                    filters=[
                        api_server_sql.KeyFilter(
                            operator=api_server_sql.KeyFilterOperator.EXISTS
                        )
                    ],
                )
            )

            response = service.search(
                session=session,
                filters=filters,
                debug_where_clause=True,
            )

            # Should produce the same result as test_search_with_key_exists_filter
            expected = (
                "EXISTS (SELECT pipeline_run_annotation.pipeline_run_id, "
                'pipeline_run_annotation."key", pipeline_run_annotation.value \n'
                "FROM pipeline_run_annotation, pipeline_run \n"
                "WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id "
                'AND pipeline_run_annotation."key" IS NOT NULL)'
            )
            assert response.debug_where_clause == expected

    def test_search_with_key_contains_filter(self):
        """Test search with KeyFilter CONTAINS operator."""
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()

        with session_factory() as session:
            filters = api_server_sql.SearchFilters(
                annotation_filters=api_server_sql.FilterGroup(
                    operator=api_server_sql.GroupOperator.AND,
                    filters=[
                        api_server_sql.KeyFilter(
                            operator=api_server_sql.KeyFilterOperator.CONTAINS,
                            key="env",
                        )
                    ],
                )
            )

            response = service.search(
                session=session,
                filters=filters,
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

    def test_search_with_key_equals_filter(self):
        """Test search with KeyFilter EQUALS operator."""
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()

        with session_factory() as session:
            filters = api_server_sql.SearchFilters(
                annotation_filters=api_server_sql.FilterGroup(
                    operator=api_server_sql.GroupOperator.AND,
                    filters=[
                        api_server_sql.KeyFilter(
                            operator=api_server_sql.KeyFilterOperator.EQUALS,
                            key="environment",
                        )
                    ],
                )
            )

            response = service.search(
                session=session,
                filters=filters,
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

    def test_search_with_key_equals_negate_filter(self):
        """Test search with KeyFilter EQUALS operator with negate=True."""
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()

        with session_factory() as session:
            filters = api_server_sql.SearchFilters(
                annotation_filters=api_server_sql.FilterGroup(
                    operator=api_server_sql.GroupOperator.AND,
                    filters=[
                        api_server_sql.KeyFilter(
                            operator=api_server_sql.KeyFilterOperator.EQUALS,
                            key="environment",
                            negate=True,
                        )
                    ],
                )
            )

            response = service.search(
                session=session,
                filters=filters,
                debug_where_clause=True,
            )

            expected = (
                "NOT (EXISTS (SELECT pipeline_run_annotation.pipeline_run_id, "
                'pipeline_run_annotation."key", pipeline_run_annotation.value \n'
                "FROM pipeline_run_annotation, pipeline_run \n"
                "WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id "
                "AND pipeline_run_annotation.\"key\" = 'environment'))"
            )
            assert response.debug_where_clause == expected

    def test_search_with_key_in_set_filter(self):
        """Test search with KeyFilter IN_SET operator."""
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()

        with session_factory() as session:
            filters = api_server_sql.SearchFilters(
                annotation_filters=api_server_sql.FilterGroup(
                    operator=api_server_sql.GroupOperator.AND,
                    filters=[
                        api_server_sql.KeyFilter(
                            operator=api_server_sql.KeyFilterOperator.IN_SET,
                            keys=["environment", "team"],
                        )
                    ],
                )
            )

            response = service.search(
                session=session,
                filters=filters,
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

    def test_search_with_value_contains_filter(self):
        """Test search with ValueFilter CONTAINS operator.

        Searches across ALL annotation values for substring match.
        """
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()

        with session_factory() as session:
            filters = api_server_sql.SearchFilters(
                annotation_filters=api_server_sql.FilterGroup(
                    operator=api_server_sql.GroupOperator.AND,
                    filters=[
                        api_server_sql.ValueFilter(
                            operator=api_server_sql.ValueFilterOperator.CONTAINS,
                            value="prod",
                        )
                    ],
                )
            )

            response = service.search(
                session=session,
                filters=filters,
                debug_where_clause=True,
            )

            expected = (
                "EXISTS (SELECT pipeline_run_annotation.pipeline_run_id, "
                'pipeline_run_annotation."key", pipeline_run_annotation.value \n'
                "FROM pipeline_run_annotation, pipeline_run \n"
                "WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id "
                "AND (pipeline_run_annotation.value LIKE '%' || 'prod' || '%'))"
            )
            assert response.debug_where_clause == expected

    def test_search_with_value_equals_filter(self):
        """Test search with ValueFilter EQUALS operator.

        Searches across ALL annotation values for exact match.
        """
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()

        with session_factory() as session:
            filters = api_server_sql.SearchFilters(
                annotation_filters=api_server_sql.FilterGroup(
                    operator=api_server_sql.GroupOperator.AND,
                    filters=[
                        api_server_sql.ValueFilter(
                            operator=api_server_sql.ValueFilterOperator.EQUALS,
                            value="production",
                        )
                    ],
                )
            )

            response = service.search(
                session=session,
                filters=filters,
                debug_where_clause=True,
            )

            expected = (
                "EXISTS (SELECT pipeline_run_annotation.pipeline_run_id, "
                'pipeline_run_annotation."key", pipeline_run_annotation.value \n'
                "FROM pipeline_run_annotation, pipeline_run \n"
                "WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id "
                "AND pipeline_run_annotation.value = 'production')"
            )
            assert response.debug_where_clause == expected

    def test_search_with_value_equals_negate_filter(self):
        """Test search with ValueFilter EQUALS operator with negate=True.

        Searches across ALL annotation values for exact match, then negates.
        """
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()

        with session_factory() as session:
            filters = api_server_sql.SearchFilters(
                annotation_filters=api_server_sql.FilterGroup(
                    operator=api_server_sql.GroupOperator.AND,
                    filters=[
                        api_server_sql.ValueFilter(
                            operator=api_server_sql.ValueFilterOperator.EQUALS,
                            value="production",
                            negate=True,
                        )
                    ],
                )
            )

            response = service.search(
                session=session,
                filters=filters,
                debug_where_clause=True,
            )

            expected = (
                "NOT (EXISTS (SELECT pipeline_run_annotation.pipeline_run_id, "
                'pipeline_run_annotation."key", pipeline_run_annotation.value \n'
                "FROM pipeline_run_annotation, pipeline_run \n"
                "WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id "
                "AND pipeline_run_annotation.value = 'production'))"
            )
            assert response.debug_where_clause == expected

    def test_search_with_value_in_set_filter(self):
        """Test search with ValueFilter IN_SET operator.

        Searches across ALL annotation values for set membership.
        """
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()

        with session_factory() as session:
            filters = api_server_sql.SearchFilters(
                annotation_filters=api_server_sql.FilterGroup(
                    operator=api_server_sql.GroupOperator.AND,
                    filters=[
                        api_server_sql.ValueFilter(
                            operator=api_server_sql.ValueFilterOperator.IN_SET,
                            values=["backend", "frontend"],
                        )
                    ],
                )
            )

            response = service.search(
                session=session,
                filters=filters,
                debug_where_clause=True,
            )

            expected = (
                "EXISTS (SELECT pipeline_run_annotation.pipeline_run_id, "
                'pipeline_run_annotation."key", pipeline_run_annotation.value \n'
                "FROM pipeline_run_annotation, pipeline_run \n"
                "WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id "
                "AND pipeline_run_annotation.value IN ('backend', 'frontend'))"
            )
            assert response.debug_where_clause == expected

    def test_search_with_complex_nested_filters(self):
        """Test search with complex nested filter groups.

        Structure:
            Root Group (OR):
            ├── Group 1 (OR):
            │   ├── KeyFilter(CONTAINS, key="env")
            │   └── ValueFilter(EQUALS, value="admin", negate=True)
            └── Group 2 (AND):
                ├── KeyFilter(EXISTS, key="status", negate=True)
                └── ValueFilter(IN_SET, values=["high", "critical"])
        """
        session_factory = _initialize_db_and_get_session_factory()
        service = api_server_sql.PipelineRunsApiService_Sql()

        with session_factory() as session:
            filters = api_server_sql.SearchFilters(
                annotation_filters=api_server_sql.FilterGroup(
                    operator=api_server_sql.GroupOperator.OR,
                    filters=[
                        # Group 1 (OR)
                        api_server_sql.FilterGroup(
                            operator=api_server_sql.GroupOperator.OR,
                            filters=[
                                api_server_sql.KeyFilter(
                                    operator=api_server_sql.KeyFilterOperator.CONTAINS,
                                    key="env",
                                ),
                                api_server_sql.ValueFilter(
                                    operator=api_server_sql.ValueFilterOperator.EQUALS,
                                    value="admin",
                                    negate=True,
                                ),
                            ],
                        ),
                        # Group 2 (AND)
                        api_server_sql.FilterGroup(
                            operator=api_server_sql.GroupOperator.AND,
                            filters=[
                                api_server_sql.KeyFilter(
                                    operator=api_server_sql.KeyFilterOperator.EXISTS,
                                    key="status",
                                    negate=True,
                                ),
                                api_server_sql.ValueFilter(
                                    operator=api_server_sql.ValueFilterOperator.IN_SET,
                                    values=["high", "critical"],
                                ),
                            ],
                        ),
                    ],
                )
            )

            response = service.search(
                session=session,
                filters=filters,
                debug_where_clause=True,
            )

            # Expected SQL structure:
            #
            # Root Group (OR):
            # ├── Group 1 (OR):
            # │   ├── KeyFilter(CONTAINS, key="env")
            # │   └── ValueFilter(EQUALS, value="admin", negate=True)
            # └── Group 2 (AND):
            #     ├── KeyFilter(EXISTS, key="status", negate=True)
            #     └── ValueFilter(IN_SET, values=["high", "critical"])
            #
            expected = (
                # ===== Root Group (OR) =====
                # |
                # |-- Group 1 (OR) ------------------------------------------
                # |   |
                # |   |-- Filter 1: KeyFilter(CONTAINS, key="env")
                "(EXISTS (SELECT pipeline_run_annotation.pipeline_run_id, "
                'pipeline_run_annotation."key", pipeline_run_annotation.value \n'
                "FROM pipeline_run_annotation, pipeline_run \n"
                "WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id "
                "AND (pipeline_run_annotation.\"key\" LIKE '%' || 'env' || '%'))) "
                # |   |
                # |   OR
                "OR "
                # |   |
                # |   |-- Filter 2: ValueFilter(EQUALS, value="admin", negate=True)
                "NOT (EXISTS (SELECT pipeline_run_annotation.pipeline_run_id, "
                'pipeline_run_annotation."key", pipeline_run_annotation.value \n'
                "FROM pipeline_run_annotation, pipeline_run \n"
                "WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id "
                "AND pipeline_run_annotation.value = 'admin')) "
                # |
                # OR (Root level)
                "OR "
                # |
                # |-- Group 2 (AND) ------------------------------------------
                # |   |
                # |   |-- Filter 1: KeyFilter(EXISTS, key="status", negate=True)
                "NOT (EXISTS (SELECT pipeline_run_annotation.pipeline_run_id, "
                'pipeline_run_annotation."key", pipeline_run_annotation.value \n'
                "FROM pipeline_run_annotation, pipeline_run \n"
                "WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id "
                'AND pipeline_run_annotation."key" IS NOT NULL)) '
                # |   |
                # |   AND
                "AND "
                # |   |
                # |   |-- Filter 2: ValueFilter(IN_SET, values=["high", "critical"])
                "(EXISTS (SELECT pipeline_run_annotation.pipeline_run_id, "
                'pipeline_run_annotation."key", pipeline_run_annotation.value \n'
                "FROM pipeline_run_annotation, pipeline_run \n"
                "WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id "
                "AND pipeline_run_annotation.value IN ('high', 'critical')))"
                # ===== End Root Group =====
            )
            assert response.debug_where_clause == expected


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
