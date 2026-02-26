import json

import pytest
import sqlalchemy as sql
from sqlalchemy.dialects import sqlite as sqlite_dialect

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import errors
from cloud_pipelines_backend import filter_query_models
from cloud_pipelines_backend import filter_query_sql


def _compile(clause: sql.ColumnElement) -> str:
    """Compile a SQLAlchemy clause to a string for assertion."""
    return str(
        clause.compile(
            dialect=sqlite_dialect.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )


class TestLeafPredicates:
    def test_key_exists(self):
        fq = filter_query_models.FilterQuery.model_validate(
            {"and": [{"key_exists": {"key": "team"}}]},
        )
        clause = filter_query_sql.filter_query_to_where_clause(filter_query=fq)
        compiled = _compile(clause)
        assert "pipeline_run_annotation" in compiled
        assert "key" in compiled
        assert "EXISTS" in compiled.upper()

    def test_value_equals(self):
        fq = filter_query_models.FilterQuery.model_validate(
            {"and": [{"value_equals": {"key": "team", "value": "ml-ops"}}]},
        )
        clause = filter_query_sql.filter_query_to_where_clause(filter_query=fq)
        compiled = _compile(clause)
        assert "ml-ops" in compiled
        assert "EXISTS" in compiled.upper()

    def test_value_contains(self):
        fq = filter_query_models.FilterQuery.model_validate(
            {"and": [{"value_contains": {"key": "team", "value_substring": "ml"}}]},
        )
        clause = filter_query_sql.filter_query_to_where_clause(filter_query=fq)
        compiled = _compile(clause)
        assert "ml" in compiled
        assert "LIKE" in compiled.upper()

    def test_value_in(self):
        fq = filter_query_models.FilterQuery.model_validate(
            {"and": [{"value_in": {"key": "env", "values": ["prod", "staging"]}}]},
        )
        clause = filter_query_sql.filter_query_to_where_clause(filter_query=fq)
        compiled = _compile(clause)
        assert "prod" in compiled
        assert "staging" in compiled
        assert "IN" in compiled.upper()


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
        compiled = _compile(clause)
        assert "AND" in compiled.upper()
        assert compiled.upper().count("EXISTS") == 2

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
        compiled = _compile(clause)
        assert "OR" in compiled.upper()
        assert compiled.upper().count("EXISTS") == 2

    def test_not_predicate(self):
        fq = filter_query_models.FilterQuery.model_validate(
            {"and": [{"not": {"key_exists": {"key": "deprecated"}}}]},
        )
        clause = filter_query_sql.filter_query_to_where_clause(filter_query=fq)
        compiled = _compile(clause)
        assert "NOT" in compiled.upper()
        assert "EXISTS" in compiled.upper()

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
        compiled = _compile(clause)
        assert compiled.upper().count("EXISTS") == 3


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
        token = filter_query_sql.PageToken.decode(None)
        assert token.offset == 0
        assert token.filter is None
        assert token.filter_query is None

    def test_encode_decode_roundtrip(self):
        original = filter_query_sql.PageToken(
            offset=20,
            filter="created_by:alice",
            filter_query='{"and": [{"key_exists": {"key": "team"}}]}',
        )
        encoded = original.encode()
        decoded = filter_query_sql.PageToken.decode(encoded)
        assert decoded.offset == 20
        assert decoded.filter == "created_by:alice"
        assert decoded.filter_query == '{"and": [{"key_exists": {"key": "team"}}]}'

    def test_decode_with_filter_query(self):
        fq_json = '{"or": [{"value_equals": {"key": "env", "value": "prod"}}]}'
        original = filter_query_sql.PageToken(offset=10, filter_query=fq_json)
        decoded = filter_query_sql.PageToken.decode(original.encode())
        assert decoded.filter_query == fq_json
        assert decoded.filter is None
        assert decoded.offset == 10

    def test_decode_empty_string(self):
        token = filter_query_sql.PageToken.decode("")
        assert token.offset == 0
        assert token.filter is None
        assert token.filter_query is None


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
        assert next_token.offset == 10
        assert next_token.filter is None
        assert next_token.filter_query is None

    def test_mutual_exclusivity_raises(self):
        with pytest.raises(
            errors.MutuallyExclusiveFilterError, match="Cannot use both"
        ):
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
        compiled = _compile(clauses[0])
        assert "EXISTS" in compiled.upper()
        assert "pipeline_run_annotation" in compiled
        assert offset == 0
        assert next_token.filter is None
        assert next_token.filter_query is not None

    def test_filter_query_produces_clauses(self):
        fq = '{"and": [{"key_exists": {"key": "team"}}]}'
        clauses, offset, next_token = filter_query_sql.build_list_filters(
            filter_value=None,
            filter_query_value=fq,
            page_token_value=None,
            current_user=None,
            page_size=10,
        )
        assert len(clauses) == 1
        compiled = _compile(clauses[0])
        assert "EXISTS" in compiled.upper()
        assert next_token.filter_query == fq

    def test_page_token_with_legacy_filter_converts(self):
        token = filter_query_sql.PageToken(
            offset=20,
            filter="created_by:alice",
        )
        clauses, offset, next_token = filter_query_sql.build_list_filters(
            filter_value=None,
            filter_query_value=None,
            page_token_value=token.encode(),
            current_user=None,
            page_size=10,
        )
        assert offset == 20
        assert len(clauses) == 1
        compiled = _compile(clauses[0])
        assert "EXISTS" in compiled.upper()
        assert next_token.offset == 30
        assert next_token.filter is None
        assert next_token.filter_query is not None

    def test_page_token_restores_filter_query(self):
        fq = '{"and": [{"key_exists": {"key": "env"}}]}'
        token = filter_query_sql.PageToken(offset=10, filter_query=fq)
        clauses, offset, next_token = filter_query_sql.build_list_filters(
            filter_value=None,
            filter_query_value=None,
            page_token_value=token.encode(),
            current_user=None,
            page_size=5,
        )
        assert offset == 10
        assert len(clauses) == 1
        assert next_token.offset == 15
        assert next_token.filter_query == fq

    def test_page_size_reflected_in_next_token(self):
        _, _, next_token = filter_query_sql.build_list_filters(
            filter_value=None,
            filter_query_value=None,
            page_token_value=None,
            current_user=None,
            page_size=25,
        )
        assert next_token.offset == 25

    def test_created_by_me_resolved_in_next_token(self):
        clauses, offset, next_token = filter_query_sql.build_list_filters(
            filter_value="created_by:me",
            filter_query_value=None,
            page_token_value=None,
            current_user="bob@example.com",
            page_size=10,
        )
        assert len(clauses) == 1
        assert next_token.filter is None
        assert next_token.filter_query is not None
        parsed_fq = json.loads(next_token.filter_query)
        assert parsed_fq["and"][0]["value_equals"]["value"] == "me"


class TestSystemKeyValidation:
    def test_get_predicate_key_value_equals(self):
        pred = filter_query_models.ValueEqualsPredicate(
            value_equals=filter_query_models.ValueEquals(key="team", value="ml")
        )
        assert filter_query_sql._get_predicate_key(predicate=pred) == "team"

    def test_get_predicate_key_logical_operator(self):
        pred = filter_query_models.AndPredicate(
            **{
                "and": [
                    filter_query_models.KeyExistsPredicate(
                        key_exists=filter_query_models.KeyExists(key="x")
                    )
                ]
            }
        )
        assert filter_query_sql._get_predicate_key(predicate=pred) is None

    def test_check_predicate_allowed_supported(self):
        pred = filter_query_models.ValueEqualsPredicate(
            value_equals=filter_query_models.ValueEquals(
                key=filter_query_sql.SystemKey.CREATED_BY, value="alice"
            )
        )
        filter_query_sql._check_predicate_allowed(predicate=pred)

    def test_check_predicate_allowed_unsupported(self):
        pred = filter_query_models.ValueContainsPredicate(
            value_contains=filter_query_models.ValueContains(
                key=filter_query_sql.SystemKey.CREATED_BY, value_substring="al"
            )
        )
        with pytest.raises(errors.InvalidAnnotationKeyError, match="not supported"):
            filter_query_sql._check_predicate_allowed(predicate=pred)

    def test_check_predicate_allowed_non_system_key(self):
        pred = filter_query_models.ValueContainsPredicate(
            value_contains=filter_query_models.ValueContains(
                key="team", value_substring="ml"
            )
        )
        filter_query_sql._check_predicate_allowed(predicate=pred)

    def test_resolve_system_key_value_me(self):
        result = filter_query_sql._resolve_system_key_value(
            key=filter_query_sql.SystemKey.CREATED_BY,
            value="me",
            current_user="alice@example.com",
        )
        assert result == "alice@example.com"

    def test_resolve_system_key_value_me_no_user(self):
        result = filter_query_sql._resolve_system_key_value(
            key=filter_query_sql.SystemKey.CREATED_BY,
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
                key=filter_query_sql.SystemKey.CREATED_BY, value="me"
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
                key=filter_query_sql.SystemKey.CREATED_BY, value="me"
            )
        )
        resolved = filter_query_sql._validate_and_resolve_predicate(
            predicate=pred,
            current_user="charlie@example.com",
        )
        assert resolved.value_equals.value == "charlie@example.com"
