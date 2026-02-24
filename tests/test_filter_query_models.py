import pydantic
import pytest

from cloud_pipelines_backend import filter_query_models


class TestFilterQuery:
    def test_full_example_from_design_doc(self):
        json_str = """
        {
          "and": [
            {"key_exists": {"key": "team"}},
            {"value_equals": {"key": "env", "value": "prod"}},
            {"not": {"key_exists": {"key": "deprecated"}}},
            {"or": [{"value_contains": {"key": "name", "value_substring": "nightly"}}]}
          ]
        }
        """
        result = filter_query_models.FilterQuery.model_validate_json(json_str)
        assert len(result.and_) == 4
        assert isinstance(result.and_[0], filter_query_models.KeyExistsPredicate)
        assert isinstance(result.and_[1], filter_query_models.ValueEqualsPredicate)
        assert isinstance(result.and_[2], filter_query_models.NotPredicate)
        assert isinstance(result.and_[3], filter_query_models.OrPredicate)


class TestLeafPredicates:
    def test_key_exists(self):
        json_str = '{"key_exists": {"key": "team"}}'
        result = filter_query_models.KeyExistsPredicate.model_validate_json(json_str)
        assert result.key_exists == filter_query_models.KeyExists(key="team")

    def test_value_equals(self):
        json_str = '{"value_equals": {"key": "env", "value": "prod"}}'
        result = filter_query_models.ValueEqualsPredicate.model_validate_json(json_str)
        assert result.value_equals == filter_query_models.ValueEquals(
            key="env", value="prod"
        )

    def test_value_contains(self):
        json_str = '{"value_contains": {"key": "name", "value_substring": "nightly"}}'
        result = filter_query_models.ValueContainsPredicate.model_validate_json(
            json_str
        )
        assert result.value_contains == filter_query_models.ValueContains(
            key="name", value_substring="nightly"
        )

    def test_value_in(self):
        json_str = '{"value_in": {"key": "env", "values": ["prod", "staging"]}}'
        result = filter_query_models.ValueInPredicate.model_validate_json(json_str)
        assert result.value_in == filter_query_models.ValueIn(
            key="env", values=["prod", "staging"]
        )

    def test_time_range_with_both_times(self):
        json_str = '{"time_range": {"key": "system/pipeline_run.date.created_at", "start_time": "2024-01-01T00:00:00Z", "end_time": "2024-12-31T23:59:59Z"}}'
        result = filter_query_models.TimeRangePredicate.model_validate_json(json_str)
        assert result.time_range.key == "system/pipeline_run.date.created_at"
        assert result.time_range.start_time is not None
        assert result.time_range.end_time is not None

    def test_time_range_without_end_time(self):
        json_str = '{"time_range": {"key": "system/pipeline_run.date.created_at", "start_time": "2024-01-01T00:00:00Z"}}'
        result = filter_query_models.TimeRangePredicate.model_validate_json(json_str)
        assert result.time_range.key == "system/pipeline_run.date.created_at"
        assert result.time_range.start_time is not None
        assert result.time_range.end_time is None

    def test_time_range_rejects_naive_datetime(self):
        json_str = '{"time_range": {"key": "k", "start_time": "2024-01-01T00:00:00"}}'
        with pytest.raises(pydantic.ValidationError, match="timezone"):
            filter_query_models.TimeRangePredicate.model_validate_json(json_str)


class TestEmptyStringRejections:
    def test_key_exists_empty_key(self):
        json_str = '{"key_exists": {"key": ""}}'
        with pytest.raises(pydantic.ValidationError):
            filter_query_models.KeyExistsPredicate.model_validate_json(json_str)

    def test_value_equals_empty_key(self):
        json_str = '{"value_equals": {"key": "", "value": "prod"}}'
        with pytest.raises(pydantic.ValidationError):
            filter_query_models.ValueEqualsPredicate.model_validate_json(json_str)

    def test_value_equals_empty_value(self):
        json_str = '{"value_equals": {"key": "env", "value": ""}}'
        with pytest.raises(pydantic.ValidationError):
            filter_query_models.ValueEqualsPredicate.model_validate_json(json_str)

    def test_value_contains_empty_key(self):
        json_str = '{"value_contains": {"key": "", "value_substring": "nightly"}}'
        with pytest.raises(pydantic.ValidationError):
            filter_query_models.ValueContainsPredicate.model_validate_json(json_str)

    def test_value_contains_empty_substring(self):
        json_str = '{"value_contains": {"key": "name", "value_substring": ""}}'
        with pytest.raises(pydantic.ValidationError):
            filter_query_models.ValueContainsPredicate.model_validate_json(json_str)

    def test_value_in_empty_key(self):
        json_str = '{"value_in": {"key": "", "values": ["prod"]}}'
        with pytest.raises(pydantic.ValidationError):
            filter_query_models.ValueInPredicate.model_validate_json(json_str)

    def test_value_in_empty_values_list(self):
        json_str = '{"value_in": {"key": "env", "values": []}}'
        with pytest.raises(pydantic.ValidationError):
            filter_query_models.ValueInPredicate.model_validate_json(json_str)

    def test_value_in_empty_string_in_list(self):
        json_str = '{"value_in": {"key": "env", "values": ["prod", ""]}}'
        with pytest.raises(pydantic.ValidationError):
            filter_query_models.ValueInPredicate.model_validate_json(json_str)

    def test_time_range_empty_key(self):
        json_str = '{"time_range": {"key": "", "start_time": "2024-01-01T00:00:00Z"}}'
        with pytest.raises(pydantic.ValidationError):
            filter_query_models.TimeRangePredicate.model_validate_json(json_str)

    def test_time_range_empty_start_time(self):
        json_str = '{"time_range": {"key": "k", "start_time": ""}}'
        with pytest.raises(pydantic.ValidationError):
            filter_query_models.TimeRangePredicate.model_validate_json(json_str)


class TestLogicalOperators:
    def test_not_predicate(self):
        json_str = '{"not": {"key_exists": {"key": "deprecated"}}}'
        result = filter_query_models.NotPredicate.model_validate_json(json_str)
        assert isinstance(result.not_, filter_query_models.KeyExistsPredicate)

    def test_and_predicate(self):
        json_str = '{"and": [{"key_exists": {"key": "team"}}, {"value_equals": {"key": "env", "value": "prod"}}]}'
        result = filter_query_models.AndPredicate.model_validate_json(json_str)
        assert len(result.and_) == 2

    def test_or_predicate(self):
        json_str = (
            '{"or": [{"key_exists": {"key": "a"}}, {"key_exists": {"key": "b"}}]}'
        )
        result = filter_query_models.OrPredicate.model_validate_json(json_str)
        assert len(result.or_) == 2

    def test_nested_and_or(self):
        json_str = """
        {
            "and": [
                {"or": [
                    {"key_exists": {"key": "a"}},
                    {"key_exists": {"key": "b"}},
                    {"key_exists": {"key": "c"}}
                ]},
                {"value_equals": {"key": "d", "value": "e"}}
            ]
        }
        """
        result = filter_query_models.AndPredicate.model_validate_json(json_str)
        assert len(result.and_) == 2
        assert isinstance(result.and_[0], filter_query_models.OrPredicate)
        assert len(result.and_[0].or_) == 3

    def test_deeply_nested(self):
        json_str = """
        {
            "and": [
                {"or": [
                    {"and": [
                        {"key_exists": {"key": "deep"}}
                    ]}
                ]}
            ]
        }
        """
        result = filter_query_models.AndPredicate.model_validate_json(json_str)
        assert len(result.and_) == 1
        inner_or = result.and_[0]
        assert isinstance(inner_or, filter_query_models.OrPredicate)
        assert len(inner_or.or_) == 1
        inner_and = inner_or.or_[0]
        assert isinstance(inner_and, filter_query_models.AndPredicate)
        assert len(inner_and.and_) == 1


class TestValidationRejections:
    def test_two_keys_in_one_predicate_rejected(self):
        json_str = '{"key_exists": {"key": "team"}, "value_equals": {"key": "env", "value": "prod"}}'
        with pytest.raises(pydantic.ValidationError):
            filter_query_models.FilterQuery.model_validate_json(json_str)

    def test_root_must_be_and_or_or(self):
        json_str = '{"key_exists": {"key": "team"}}'
        with pytest.raises(
            pydantic.ValidationError, match="Extra inputs are not permitted"
        ):
            filter_query_models.FilterQuery.model_validate_json(json_str)

    def test_missing_required_field(self):
        json_str = '{"value_equals": {"key": "env"}}'
        with pytest.raises(pydantic.ValidationError):
            filter_query_models.ValueEqualsPredicate.model_validate_json(json_str)

    def test_root_both_and_or_rejected(self):
        json_str = '{"and": [{"key_exists": {"key": "a"}}], "or": [{"key_exists": {"key": "b"}}]}'
        with pytest.raises(pydantic.ValidationError, match="exactly one"):
            filter_query_models.FilterQuery.model_validate_json(json_str)

    def test_empty_object_rejected(self):
        json_str = "{}"
        with pytest.raises(pydantic.ValidationError, match="exactly one"):
            filter_query_models.FilterQuery.model_validate_json(json_str)

    def test_empty_and_list_rejected(self):
        json_str = '{"and": []}'
        with pytest.raises(pydantic.ValidationError, match="too_short"):
            filter_query_models.FilterQuery.model_validate_json(json_str)

    def test_empty_or_list_rejected(self):
        json_str = '{"or": []}'
        with pytest.raises(pydantic.ValidationError, match="too_short"):
            filter_query_models.FilterQuery.model_validate_json(json_str)

    def test_nested_empty_and_rejected(self):
        json_str = '{"and": [{"and": []}]}'
        with pytest.raises(pydantic.ValidationError):
            filter_query_models.FilterQuery.model_validate_json(json_str)
