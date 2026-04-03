import json

import pydantic
import pytest
import sqlalchemy as sql
from sqlalchemy.dialects import sqlite as sqlite_dialect

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import errors
from cloud_pipelines_backend.search import filter_query_models
from cloud_pipelines_backend.search import filter_query_sql


def _compile(clause: sql.ColumnElement) -> str:
    """Compile a SQLAlchemy clause to a normalized string for assertion."""
    raw = str(
        clause.compile(
            dialect=sqlite_dialect.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    return " ".join(raw.split())


class TestLeafPredicates:
    def test_key_exists(self):
        fq = filter_query_models.FilterQuery.model_validate(
            {"and": [{"key_exists": {"key": "team"}}]},
        )
        clause = filter_query_sql.filter_query_to_where_clause(filter_query=fq)
        assert _compile(clause) == (
            "EXISTS (SELECT pipeline_run_annotation.pipeline_run_id"
            " FROM pipeline_run_annotation, pipeline_run"
            " WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id"
            " AND pipeline_run_annotation.\"key\" = 'team')"
        )

    def test_value_equals(self):
        fq = filter_query_models.FilterQuery.model_validate(
            {"and": [{"value_equals": {"key": "team", "value": "ml-ops"}}]},
        )
        clause = filter_query_sql.filter_query_to_where_clause(filter_query=fq)
        assert _compile(clause) == (
            "EXISTS (SELECT pipeline_run_annotation.pipeline_run_id"
            " FROM pipeline_run_annotation, pipeline_run"
            " WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id"
            " AND pipeline_run_annotation.\"key\" = 'team'"
            " AND pipeline_run_annotation.value = 'ml-ops')"
        )

    def test_value_contains(self):
        fq = filter_query_models.FilterQuery.model_validate(
            {"and": [{"value_contains": {"key": "team", "value_substring": "ml"}}]},
        )
        clause = filter_query_sql.filter_query_to_where_clause(filter_query=fq)
        assert _compile(clause) == (
            "EXISTS (SELECT pipeline_run_annotation.pipeline_run_id"
            " FROM pipeline_run_annotation, pipeline_run"
            " WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id"
            " AND pipeline_run_annotation.\"key\" = 'team'"
            " AND (pipeline_run_annotation.value LIKE '%' || 'ml' || '%'))"
        )

    def test_value_in(self):
        fq = filter_query_models.FilterQuery.model_validate(
            {"and": [{"value_in": {"key": "env", "values": ["prod", "staging"]}}]},
        )
        clause = filter_query_sql.filter_query_to_where_clause(filter_query=fq)
        assert _compile(clause) == (
            "EXISTS (SELECT pipeline_run_annotation.pipeline_run_id"
            " FROM pipeline_run_annotation, pipeline_run"
            " WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id"
            " AND pipeline_run_annotation.\"key\" = 'env'"
            " AND pipeline_run_annotation.value IN ('prod', 'staging'))"
        )


class TestLogicalPredicates:
    def test_and_predicates(self):
        fq = filter_query_models.FilterQuery.model_validate(
            {
                "and": [
                    {"key_exists": {"key": "team"}},
                    {"value_equals": {"key": "env", "value": "prod"}},
                ]
            },
        )
        clause = filter_query_sql.filter_query_to_where_clause(filter_query=fq)
        assert _compile(clause) == (
            "(EXISTS (SELECT pipeline_run_annotation.pipeline_run_id"
            " FROM pipeline_run_annotation, pipeline_run"
            " WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id"
            " AND pipeline_run_annotation.\"key\" = 'team'))"
            " AND"
            " (EXISTS (SELECT pipeline_run_annotation.pipeline_run_id"
            " FROM pipeline_run_annotation, pipeline_run"
            " WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id"
            " AND pipeline_run_annotation.\"key\" = 'env'"
            " AND pipeline_run_annotation.value = 'prod'))"
        )

    def test_or_predicates(self):
        fq = filter_query_models.FilterQuery.model_validate(
            {
                "or": [
                    {"key_exists": {"key": "team"}},
                    {"key_exists": {"key": "project"}},
                ]
            },
        )
        clause = filter_query_sql.filter_query_to_where_clause(filter_query=fq)
        assert _compile(clause) == (
            "(EXISTS (SELECT pipeline_run_annotation.pipeline_run_id"
            " FROM pipeline_run_annotation, pipeline_run"
            " WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id"
            " AND pipeline_run_annotation.\"key\" = 'team'))"
            " OR"
            " (EXISTS (SELECT pipeline_run_annotation.pipeline_run_id"
            " FROM pipeline_run_annotation, pipeline_run"
            " WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id"
            " AND pipeline_run_annotation.\"key\" = 'project'))"
        )

    def test_not_predicate(self):
        fq = filter_query_models.FilterQuery.model_validate(
            {"and": [{"not": {"key_exists": {"key": "deprecated"}}}]},
        )
        clause = filter_query_sql.filter_query_to_where_clause(filter_query=fq)
        assert _compile(clause) == (
            "NOT (EXISTS (SELECT pipeline_run_annotation.pipeline_run_id"
            " FROM pipeline_run_annotation, pipeline_run"
            " WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id"
            " AND pipeline_run_annotation.\"key\" = 'deprecated'))"
        )

    def test_nested_and_or(self):
        fq = filter_query_models.FilterQuery.model_validate(
            {
                "and": [
                    {
                        "or": [
                            {"key_exists": {"key": "team"}},
                            {"key_exists": {"key": "project"}},
                        ]
                    },
                    {"value_equals": {"key": "env", "value": "prod"}},
                ]
            },
        )
        clause = filter_query_sql.filter_query_to_where_clause(filter_query=fq)
        assert _compile(clause) == (
            "((EXISTS (SELECT pipeline_run_annotation.pipeline_run_id"
            " FROM pipeline_run_annotation, pipeline_run"
            " WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id"
            " AND pipeline_run_annotation.\"key\" = 'team'))"
            " OR"
            " (EXISTS (SELECT pipeline_run_annotation.pipeline_run_id"
            " FROM pipeline_run_annotation, pipeline_run"
            " WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id"
            " AND pipeline_run_annotation.\"key\" = 'project')))"
            " AND"
            " (EXISTS (SELECT pipeline_run_annotation.pipeline_run_id"
            " FROM pipeline_run_annotation, pipeline_run"
            " WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id"
            " AND pipeline_run_annotation.\"key\" = 'env'"
            " AND pipeline_run_annotation.value = 'prod'))"
        )


class TestTimeRangePredicate:
    _KEY = filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_AT

    _EXISTS_VALUE_EQUALS_TEAM = (
        "(EXISTS (SELECT pipeline_run_annotation.pipeline_run_id"
        " FROM pipeline_run_annotation, pipeline_run"
        " WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id"
        " AND pipeline_run_annotation.\"key\" = 'team'"
        " AND pipeline_run_annotation.value = 'ml-ops'))"
    )
    _EXISTS_KEY_EXISTS_ENV = (
        "(EXISTS (SELECT pipeline_run_annotation.pipeline_run_id"
        " FROM pipeline_run_annotation, pipeline_run"
        " WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id"
        " AND pipeline_run_annotation.\"key\" = 'env'))"
    )
    _EXISTS_KEY_EXISTS_DEPRECATED = (
        "(EXISTS (SELECT pipeline_run_annotation.pipeline_run_id"
        " FROM pipeline_run_annotation, pipeline_run"
        " WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id"
        " AND pipeline_run_annotation.\"key\" = 'deprecated'))"
    )

    def test_time_range_with_start_and_end(self):
        fq = filter_query_models.FilterQuery.model_validate(
            {
                "and": [
                    {
                        "time_range": {
                            "key": self._KEY,
                            "start_time": "2024-01-15T10:30:00Z",
                            "end_time": "2024-02-15T23:59:59.999Z",
                        }
                    }
                ]
            },
        )
        compiled = _compile(
            filter_query_sql.filter_query_to_where_clause(filter_query=fq)
        )
        assert compiled == (
            "pipeline_run.created_at >= '2024-01-15 10:30:00.000000'"
            " AND pipeline_run.created_at < '2024-02-15 23:59:59.999000'"
        )

    def test_time_range_start_only(self):
        fq = filter_query_models.FilterQuery.model_validate(
            {
                "and": [
                    {
                        "time_range": {
                            "key": self._KEY,
                            "start_time": "2024-06-01T00:00:00+00:00",
                        }
                    }
                ]
            },
        )
        compiled = _compile(
            filter_query_sql.filter_query_to_where_clause(filter_query=fq)
        )
        assert compiled == "pipeline_run.created_at >= '2024-06-01 00:00:00.000000'"

    def test_time_range_end_only(self):
        fq = filter_query_models.FilterQuery.model_validate(
            {
                "and": [
                    {
                        "time_range": {
                            "key": self._KEY,
                            "end_time": "2024-06-01T00:00:00+00:00",
                        }
                    }
                ]
            },
        )
        compiled = _compile(
            filter_query_sql.filter_query_to_where_clause(filter_query=fq)
        )
        assert compiled == "pipeline_run.created_at < '2024-06-01 00:00:00.000000'"

    def test_time_range_with_milliseconds(self):
        fq = filter_query_models.FilterQuery.model_validate(
            {
                "and": [
                    {
                        "time_range": {
                            "key": self._KEY,
                            "start_time": "2024-01-01T12:00:00.500Z",
                            "end_time": "2024-01-02T12:00:00.999Z",
                        }
                    }
                ]
            },
        )
        compiled = _compile(
            filter_query_sql.filter_query_to_where_clause(filter_query=fq)
        )
        assert compiled == (
            "pipeline_run.created_at >= '2024-01-01 12:00:00.500000'"
            " AND pipeline_run.created_at < '2024-01-02 12:00:00.999000'"
        )

    def test_time_range_with_offset_timezone(self):
        fq = filter_query_models.FilterQuery.model_validate(
            {
                "and": [
                    {
                        "time_range": {
                            "key": self._KEY,
                            "start_time": "2024-01-01T08:00:00+05:30",
                        }
                    }
                ]
            },
        )
        compiled = _compile(
            filter_query_sql.filter_query_to_where_clause(filter_query=fq)
        )
        assert compiled == ("pipeline_run.created_at >= '2024-01-01 02:30:00.000000'")

    def test_time_range_not(self):
        fq = filter_query_models.FilterQuery.model_validate(
            {
                "and": [
                    {
                        "not": {
                            "time_range": {
                                "key": self._KEY,
                                "start_time": "2024-01-15T00:00:00Z",
                                "end_time": "2024-02-15T00:00:00Z",
                            }
                        }
                    }
                ]
            },
        )
        compiled = _compile(
            filter_query_sql.filter_query_to_where_clause(filter_query=fq)
        )
        assert compiled == (
            "NOT (pipeline_run.created_at >= '2024-01-15 00:00:00.000000'"
            " AND pipeline_run.created_at < '2024-02-15 00:00:00.000000')"
        )

    def test_time_range_combined_with_annotation(self):
        fq = filter_query_models.FilterQuery.model_validate(
            {
                "and": [
                    {
                        "time_range": {
                            "key": self._KEY,
                            "start_time": "2024-01-01T00:00:00Z",
                            "end_time": "2024-02-01T00:00:00Z",
                        }
                    },
                    {"value_equals": {"key": "team", "value": "ml-ops"}},
                ]
            },
        )
        compiled = _compile(
            filter_query_sql.filter_query_to_where_clause(filter_query=fq)
        )
        assert compiled == (
            "pipeline_run.created_at >= '2024-01-01 00:00:00.000000'"
            " AND pipeline_run.created_at < '2024-02-01 00:00:00.000000'"
            f" AND {self._EXISTS_VALUE_EQUALS_TEAM}"
        )

    def test_time_range_not_first_in_list(self):
        fq = filter_query_models.FilterQuery.model_validate(
            {
                "and": [
                    {"value_equals": {"key": "team", "value": "ml-ops"}},
                    {
                        "time_range": {
                            "key": self._KEY,
                            "start_time": "2024-01-01T00:00:00Z",
                            "end_time": "2024-02-01T00:00:00Z",
                        }
                    },
                ]
            },
        )
        compiled = _compile(
            filter_query_sql.filter_query_to_where_clause(filter_query=fq)
        )
        assert compiled == (
            f"{self._EXISTS_VALUE_EQUALS_TEAM}"
            " AND pipeline_run.created_at >= '2024-01-01 00:00:00.000000'"
            " AND pipeline_run.created_at < '2024-02-01 00:00:00.000000'"
        )

    def test_time_range_last_in_list(self):
        fq = filter_query_models.FilterQuery.model_validate(
            {
                "and": [
                    {"key_exists": {"key": "env"}},
                    {"value_equals": {"key": "team", "value": "ml-ops"}},
                    {
                        "time_range": {
                            "key": self._KEY,
                            "start_time": "2024-01-01T00:00:00Z",
                            "end_time": "2024-06-01T00:00:00Z",
                        }
                    },
                ]
            },
        )
        compiled = _compile(
            filter_query_sql.filter_query_to_where_clause(filter_query=fq)
        )
        assert compiled == (
            f"{self._EXISTS_KEY_EXISTS_ENV}"
            f" AND {self._EXISTS_VALUE_EQUALS_TEAM}"
            " AND pipeline_run.created_at >= '2024-01-01 00:00:00.000000'"
            " AND pipeline_run.created_at < '2024-06-01 00:00:00.000000'"
        )

    def test_time_range_nested_in_or(self):
        fq = filter_query_models.FilterQuery.model_validate(
            {
                "or": [
                    {
                        "time_range": {
                            "key": self._KEY,
                            "start_time": "2024-01-01T00:00:00Z",
                            "end_time": "2024-02-01T00:00:00Z",
                        }
                    },
                    {"value_equals": {"key": "team", "value": "ml-ops"}},
                ]
            },
        )
        compiled = _compile(
            filter_query_sql.filter_query_to_where_clause(filter_query=fq)
        )
        assert compiled == (
            "pipeline_run.created_at >= '2024-01-01 00:00:00.000000'"
            " AND pipeline_run.created_at < '2024-02-01 00:00:00.000000'"
            f" OR {self._EXISTS_VALUE_EQUALS_TEAM}"
        )

    def test_time_range_deeply_nested(self):
        fq = filter_query_models.FilterQuery.model_validate(
            {
                "and": [
                    {
                        "or": [
                            {
                                "and": [
                                    {
                                        "time_range": {
                                            "key": self._KEY,
                                            "start_time": "2024-01-01T00:00:00Z",
                                            "end_time": "2024-02-01T00:00:00Z",
                                        }
                                    },
                                    {
                                        "value_equals": {
                                            "key": "team",
                                            "value": "ml-ops",
                                        }
                                    },
                                ]
                            },
                            {"key_exists": {"key": "deprecated"}},
                        ]
                    }
                ]
            },
        )
        compiled = _compile(
            filter_query_sql.filter_query_to_where_clause(filter_query=fq)
        )
        assert compiled == (
            "pipeline_run.created_at >= '2024-01-01 00:00:00.000000'"
            " AND pipeline_run.created_at < '2024-02-01 00:00:00.000000'"
            f" AND {self._EXISTS_VALUE_EQUALS_TEAM}"
            f" OR {self._EXISTS_KEY_EXISTS_DEPRECATED}"
        )

    def test_multiple_time_ranges_in_and(self):
        fq = filter_query_models.FilterQuery.model_validate(
            {
                "and": [
                    {
                        "time_range": {
                            "key": self._KEY,
                            "start_time": "2024-01-01T00:00:00Z",
                            "end_time": "2024-06-01T00:00:00Z",
                        }
                    },
                    {
                        "time_range": {
                            "key": self._KEY,
                            "start_time": "2024-03-01T00:00:00Z",
                            "end_time": "2024-04-01T00:00:00Z",
                        }
                    },
                ]
            },
        )
        compiled = _compile(
            filter_query_sql.filter_query_to_where_clause(filter_query=fq)
        )
        assert compiled == (
            "pipeline_run.created_at >= '2024-01-01 00:00:00.000000'"
            " AND pipeline_run.created_at < '2024-06-01 00:00:00.000000'"
            " AND pipeline_run.created_at >= '2024-03-01 00:00:00.000000'"
            " AND pipeline_run.created_at < '2024-04-01 00:00:00.000000'"
        )

    def test_multiple_time_ranges_in_or(self):
        fq = filter_query_models.FilterQuery.model_validate(
            {
                "or": [
                    {
                        "time_range": {
                            "key": self._KEY,
                            "start_time": "2024-01-01T00:00:00Z",
                            "end_time": "2024-02-01T00:00:00Z",
                        }
                    },
                    {
                        "time_range": {
                            "key": self._KEY,
                            "start_time": "2024-03-01T00:00:00Z",
                            "end_time": "2024-04-01T00:00:00Z",
                        }
                    },
                ]
            },
        )
        compiled = _compile(
            filter_query_sql.filter_query_to_where_clause(filter_query=fq)
        )
        assert compiled == (
            "pipeline_run.created_at >= '2024-01-01 00:00:00.000000'"
            " AND pipeline_run.created_at < '2024-02-01 00:00:00.000000'"
            " OR pipeline_run.created_at >= '2024-03-01 00:00:00.000000'"
            " AND pipeline_run.created_at < '2024-04-01 00:00:00.000000'"
        )

    def test_time_range_invalid_key_rejected(self):
        predicate = filter_query_models.TimeRangePredicate(
            time_range=filter_query_models.TimeRange(
                key="custom/my_date",
                start_time="2024-01-01T00:00:00Z",
            )
        )
        with pytest.raises(errors.ApiValidationError, match="only supports key"):
            filter_query_sql._predicate_to_clause(predicate=predicate)

    def test_time_range_system_key_registered(self):
        supported = filter_query_sql.SYSTEM_KEY_SUPPORTED_PREDICATES
        assert filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_AT in supported
        assert (
            filter_query_models.TimeRangePredicate
            in supported[filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_AT]
        )

    def test_time_range_naive_datetime_rejected(self):
        with pytest.raises(pydantic.ValidationError):
            filter_query_models.FilterQuery.model_validate(
                {
                    "and": [
                        {
                            "time_range": {
                                "key": self._KEY,
                                "start_time": "2024-01-01T00:00:00",
                            }
                        }
                    ]
                },
            )


class TestPageToken:
    def test_decode_none(self):
        token = filter_query_sql._decode_page_token(page_token=None)
        assert token == {}

    def test_encode_decode_roundtrip(self):
        encoded = filter_query_sql._encode_page_token(
            page_token_dict={
                "offset": 20,
                "filter": "created_by:alice",
                "filter_query": '{"and": [{"key_exists": {"key": "team"}}]}',
            }
        )
        decoded = filter_query_sql._decode_page_token(page_token=encoded)
        assert decoded["offset"] == 20
        assert decoded["filter"] == "created_by:alice"
        assert decoded["filter_query"] == '{"and": [{"key_exists": {"key": "team"}}]}'

    def test_decode_with_filter_query(self):
        fq_json = '{"or": [{"value_equals": {"key": "env", "value": "prod"}}]}'
        encoded = filter_query_sql._encode_page_token(
            page_token_dict={
                "offset": 10,
                "filter_query": fq_json,
            }
        )
        decoded = filter_query_sql._decode_page_token(page_token=encoded)
        assert decoded["filter_query"] == fq_json
        assert decoded.get("filter") is None
        assert decoded["offset"] == 10

    def test_decode_empty_string(self):
        token = filter_query_sql._decode_page_token(page_token="")
        assert token == {}


class TestConvertLegacyFilterToFilterQuery:
    def test_created_by_literal(self):
        result = filter_query_sql._convert_legacy_filter_to_filter_query(
            filter_value="created_by:alice",
        )
        parsed = json.loads(result)
        assert parsed == {
            "and": [
                {
                    "value_equals": {
                        "key": "system/pipeline_run.created_by",
                        "value": "alice",
                    }
                }
            ]
        }

    def test_created_by_me_not_resolved(self):
        result = filter_query_sql._convert_legacy_filter_to_filter_query(
            filter_value="created_by:me",
        )
        parsed = json.loads(result)
        assert parsed["and"][0]["value_equals"]["value"] == "me"

    def test_created_by_empty_raises(self):
        with pytest.raises(errors.ApiValidationError, match="non-empty value"):
            filter_query_sql._convert_legacy_filter_to_filter_query(
                filter_value="created_by:",
            )

    def test_unsupported_key_raises(self):
        with pytest.raises(NotImplementedError, match="Unsupported filter"):
            filter_query_sql._convert_legacy_filter_to_filter_query(
                filter_value="unknown_key:value",
            )

    def test_text_search_raises(self):
        with pytest.raises(NotImplementedError, match="Text search"):
            filter_query_sql._convert_legacy_filter_to_filter_query(
                filter_value="some_text_without_colon",
            )


class TestBuildListFilters:
    def test_no_filters(self):
        clauses, offset, next_token = filter_query_sql.build_list_filters(
            filter_value=None,
            filter_query_value=None,
            page_token_value=None,
            current_user=None,
            page_size=10,
        )
        assert clauses == []
        assert offset == 0
        decoded = filter_query_sql._decode_page_token(page_token=next_token)
        assert decoded["offset"] == 10
        assert decoded.get("filter") is None
        assert decoded.get("filter_query") is None

    def test_mutual_exclusivity_raises(self):
        with pytest.raises(errors.ApiValidationError, match="Cannot use both"):
            filter_query_sql.build_list_filters(
                filter_value="created_by:alice",
                filter_query_value='{"and": [{"key_exists": {"key": "team"}}]}',
                page_token_value=None,
                current_user=None,
                page_size=10,
            )

    def test_legacy_filter_produces_annotation_clause(self):
        clauses, offset, next_token = filter_query_sql.build_list_filters(
            filter_value="created_by:alice",
            filter_query_value=None,
            page_token_value=None,
            current_user=None,
            page_size=10,
        )
        assert len(clauses) == 1
        assert _compile(clauses[0]) == (
            "EXISTS (SELECT pipeline_run_annotation.pipeline_run_id"
            " FROM pipeline_run_annotation, pipeline_run"
            " WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id"
            " AND pipeline_run_annotation.\"key\" = 'system/pipeline_run.created_by'"
            " AND pipeline_run_annotation.value = 'alice')"
        )
        assert offset == 0
        decoded = filter_query_sql._decode_page_token(page_token=next_token)
        assert "filter" not in decoded
        assert "filter_query" in decoded

    def test_filter_query_produces_clauses(self):
        fq = '{"and": [{"key_exists": {"key": "team"}}]}'
        clauses, _offset, next_token = filter_query_sql.build_list_filters(
            filter_value=None,
            filter_query_value=fq,
            page_token_value=None,
            current_user=None,
            page_size=10,
        )
        assert len(clauses) == 1
        assert _compile(clauses[0]) == (
            "EXISTS (SELECT pipeline_run_annotation.pipeline_run_id"
            " FROM pipeline_run_annotation, pipeline_run"
            " WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id"
            " AND pipeline_run_annotation.\"key\" = 'team')"
        )
        decoded = filter_query_sql._decode_page_token(page_token=next_token)
        assert decoded["filter_query"] == fq

    def test_page_token_with_legacy_filter_converts(self):
        encoded_token = filter_query_sql._encode_page_token(
            page_token_dict={
                "offset": 20,
                "filter": "created_by:alice",
            }
        )
        clauses, offset, next_token = filter_query_sql.build_list_filters(
            filter_value=None,
            filter_query_value=None,
            page_token_value=encoded_token,
            current_user=None,
            page_size=10,
        )
        assert offset == 20
        assert len(clauses) == 1
        assert _compile(clauses[0]) == (
            "EXISTS (SELECT pipeline_run_annotation.pipeline_run_id"
            " FROM pipeline_run_annotation, pipeline_run"
            " WHERE pipeline_run_annotation.pipeline_run_id = pipeline_run.id"
            " AND pipeline_run_annotation.\"key\" = 'system/pipeline_run.created_by'"
            " AND pipeline_run_annotation.value = 'alice')"
        )
        decoded = filter_query_sql._decode_page_token(page_token=next_token)
        assert decoded["offset"] == 30
        assert "filter" not in decoded
        assert "filter_query" in decoded

    def test_page_token_restores_filter_query(self):
        fq = '{"and": [{"key_exists": {"key": "env"}}]}'
        encoded_token = filter_query_sql._encode_page_token(
            page_token_dict={
                "offset": 10,
                "filter_query": fq,
            }
        )
        clauses, offset, next_token = filter_query_sql.build_list_filters(
            filter_value=None,
            filter_query_value=None,
            page_token_value=encoded_token,
            current_user=None,
            page_size=5,
        )
        assert offset == 10
        assert len(clauses) == 1
        decoded = filter_query_sql._decode_page_token(page_token=next_token)
        assert decoded["offset"] == 15
        assert decoded["filter_query"] == fq

    def test_page_size_reflected_in_next_token(self):
        _, _, next_token = filter_query_sql.build_list_filters(
            filter_value=None,
            filter_query_value=None,
            page_token_value=None,
            current_user=None,
            page_size=25,
        )
        decoded = filter_query_sql._decode_page_token(page_token=next_token)
        assert decoded["offset"] == 25

    def test_created_by_me_resolved_in_next_token(self):
        clauses, _offset, next_token = filter_query_sql.build_list_filters(
            filter_value="created_by:me",
            filter_query_value=None,
            page_token_value=None,
            current_user="bob@example.com",
            page_size=10,
        )
        assert len(clauses) == 1
        decoded = filter_query_sql._decode_page_token(page_token=next_token)
        assert "filter" not in decoded
        assert "filter_query" in decoded
        parsed_fq = json.loads(decoded["filter_query"])
        assert parsed_fq["and"][0]["value_equals"]["value"] == "me"


class TestPipelineRunAnnotationSystemKeyValidation:
    def test_check_predicate_allowed_skips_logical_operator(self):
        pred = filter_query_models.AndPredicate(
            **{
                "and": [
                    filter_query_models.KeyExistsPredicate(
                        key_exists=filter_query_models.KeyExists(key="x")
                    )
                ]
            }
        )
        assert filter_query_sql._check_predicate_allowed(predicate=pred) is None

    def test_check_predicate_allowed_supported(self):
        pred = filter_query_models.ValueEqualsPredicate(
            value_equals=filter_query_models.ValueEquals(
                key=filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
                value="alice",
            )
        )
        assert filter_query_sql._check_predicate_allowed(predicate=pred) is None

    def test_check_predicate_allowed_unsupported(self):
        pred = filter_query_models.ValueContainsPredicate(
            value_contains=filter_query_models.ValueContains(
                key=filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
                value_substring="al",
            )
        )
        with pytest.raises(errors.ApiValidationError, match="not supported"):
            filter_query_sql._check_predicate_allowed(predicate=pred)

    def test_check_predicate_allowed_non_system_key(self):
        pred = filter_query_models.ValueContainsPredicate(
            value_contains=filter_query_models.ValueContains(
                key="team", value_substring="ml"
            )
        )
        assert filter_query_sql._check_predicate_allowed(predicate=pred) is None

    def test_check_predicate_allowed_key_exists_supported(self):
        pred = filter_query_models.KeyExistsPredicate(
            key_exists=filter_query_models.KeyExists(
                key=filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY
            )
        )
        assert filter_query_sql._check_predicate_allowed(predicate=pred) is None

    def test_check_predicate_allowed_value_in_supported(self):
        pred = filter_query_models.ValueInPredicate(
            value_in=filter_query_models.ValueIn(
                key=filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
                values=["alice"],
            )
        )
        assert filter_query_sql._check_predicate_allowed(predicate=pred) is None

    def test_check_predicate_allowed_skips_time_range(self):
        pred = filter_query_models.TimeRangePredicate(
            time_range=filter_query_models.TimeRange(
                key="created_at", start_time="2024-01-01T00:00:00Z"
            )
        )
        assert filter_query_sql._check_predicate_allowed(predicate=pred) is None

    def test_check_predicate_allowed_skips_or_predicate(self):
        pred = filter_query_models.OrPredicate(
            **{
                "or": [
                    filter_query_models.KeyExistsPredicate(
                        key_exists=filter_query_models.KeyExists(key="x")
                    )
                ]
            }
        )
        assert filter_query_sql._check_predicate_allowed(predicate=pred) is None

    def test_check_predicate_allowed_skips_not_predicate(self):
        pred = filter_query_models.NotPredicate(
            **{
                "not": filter_query_models.KeyExistsPredicate(
                    key_exists=filter_query_models.KeyExists(key="x")
                )
            },
        )
        assert filter_query_sql._check_predicate_allowed(predicate=pred) is None

    def test_resolve_system_key_value_me(self):
        result = filter_query_sql._resolve_system_key_value(
            key=filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
            value="me",
            current_user="alice@example.com",
        )
        assert result == "alice@example.com"

    def test_resolve_system_key_value_me_no_user(self):
        result = filter_query_sql._resolve_system_key_value(
            key=filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
            value="me",
            current_user=None,
        )
        assert result == ""

    def test_resolve_system_key_value_passthrough(self):
        result = filter_query_sql._resolve_system_key_value(
            key="team",
            value="me",
            current_user="alice",
        )
        assert result == "me"

    def test_maybe_resolve_system_values(self):
        pred = filter_query_models.ValueEqualsPredicate(
            value_equals=filter_query_models.ValueEquals(
                key=filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
                value="me",
            )
        )
        resolved = filter_query_sql._maybe_resolve_system_values(
            predicate=pred,
            current_user="bob@example.com",
        )
        assert resolved.value_equals.value == "bob@example.com"

    def test_validate_and_resolve_predicate(self):
        pred = filter_query_models.ValueEqualsPredicate(
            value_equals=filter_query_models.ValueEquals(
                key=filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
                value="me",
            )
        )
        resolved = filter_query_sql._validate_and_resolve_predicate(
            predicate=pred,
            current_user="charlie@example.com",
        )
        assert resolved.value_equals.value == "charlie@example.com"

    def test_check_predicate_allowed_name_key_exists(self):
        pred = filter_query_models.KeyExistsPredicate(
            key_exists=filter_query_models.KeyExists(
                key=filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME
            )
        )
        filter_query_sql._check_predicate_allowed(predicate=pred)

    def test_check_predicate_allowed_name_value_equals(self):
        pred = filter_query_models.ValueEqualsPredicate(
            value_equals=filter_query_models.ValueEquals(
                key=filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME,
                value="my-pipeline",
            )
        )
        filter_query_sql._check_predicate_allowed(predicate=pred)

    def test_check_predicate_allowed_name_value_contains(self):
        pred = filter_query_models.ValueContainsPredicate(
            value_contains=filter_query_models.ValueContains(
                key=filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME,
                value_substring="nightly",
            )
        )
        filter_query_sql._check_predicate_allowed(predicate=pred)

    def test_check_predicate_allowed_name_value_in(self):
        pred = filter_query_models.ValueInPredicate(
            value_in=filter_query_models.ValueIn(
                key=filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME,
                values=["a", "b"],
            )
        )
        filter_query_sql._check_predicate_allowed(predicate=pred)

    def test_check_predicate_allowed_key_exists_on_created_at_rejected(self):
        pred = filter_query_models.KeyExistsPredicate(
            key_exists=filter_query_models.KeyExists(
                key=filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_AT
            )
        )
        with pytest.raises(errors.ApiValidationError, match="not supported"):
            filter_query_sql._check_predicate_allowed(predicate=pred)

    def test_check_predicate_allowed_value_equals_on_created_at_rejected(self):
        pred = filter_query_models.ValueEqualsPredicate(
            value_equals=filter_query_models.ValueEquals(
                key=filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_AT,
                value="2024-01-01",
            )
        )
        with pytest.raises(errors.ApiValidationError, match="not supported"):
            filter_query_sql._check_predicate_allowed(predicate=pred)

    def test_check_predicate_allowed_time_range_on_created_by_rejected(self):
        pred = filter_query_models.TimeRangePredicate(
            time_range=filter_query_models.TimeRange(
                key=filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
                start_time="2024-01-01T00:00:00Z",
            )
        )
        with pytest.raises(errors.ApiValidationError, match="not supported"):
            filter_query_sql._check_predicate_allowed(predicate=pred)

    def test_check_predicate_allowed_time_range_on_name_rejected(self):
        pred = filter_query_models.TimeRangePredicate(
            time_range=filter_query_models.TimeRange(
                key=filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME,
                start_time="2024-01-01T00:00:00Z",
            )
        )
        with pytest.raises(errors.ApiValidationError, match="not supported"):
            filter_query_sql._check_predicate_allowed(predicate=pred)
