import json

import pytest
import sqlalchemy as sql
from sqlalchemy.dialects import sqlite as sqlite_dialect

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import errors
from cloud_pipelines_backend import filter_query_models
from cloud_pipelines_backend import filter_query_sql


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


class TestUnsupportedPredicate:
    def test_time_range_raises_not_implemented(self):
        predicate = filter_query_models.TimeRangePredicate(
            time_range=filter_query_models.TimeRange(
                key="system/pipeline_run.date.created_at",
                start_time="2024-01-01T00:00:00Z",
            )
        )
        with pytest.raises(NotImplementedError, match="TimeRangePredicate"):
            filter_query_sql._predicate_to_clause(predicate=predicate)


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
