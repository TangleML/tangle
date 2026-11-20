"""Readers for the annotations that say where a task runs."""

import json

import pytest

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.instrumentation import execution_annotations as ea


def _execution(**annotations: str) -> bts.ExecutionNode:
    return bts.ExecutionNode(task_spec={"annotations": annotations})


class TestCloudProvider:
    def test_absent_annotation_reads_none(self) -> None:
        assert ea.cloud_provider(execution=_execution()) is None

    def test_value_is_returned_as_written(self) -> None:
        execution = _execution(**{ea.CLOUD_PROVIDER_KEY: "gke-lt3"})
        assert ea.cloud_provider(execution=execution) == "gke-lt3"

    def test_absent_task_spec_is_not_an_error(self) -> None:
        assert ea.cloud_provider(execution=bts.ExecutionNode(task_spec=None)) is None


class TestTarget:
    @pytest.fixture(autouse=True)
    def _targets(self) -> None:
        ea.register_targets(
            targets={
                "gke-lt3": ea.Target(provider="google", cluster="gke-lt3"),
                "lt3": ea.Target(provider="google", cluster="gke-lt3"),
                "byo": ea.Target(
                    provider="byo",
                    cluster="",
                    cluster_from_annotation="example.test/byo/cluster",
                ),
            }
        )

    def test_no_annotation_resolves_to_none(self) -> None:
        assert ea.target(execution=_execution()) is None

    def test_unknown_value_resolves_to_none(self) -> None:
        execution = _execution(**{ea.CLOUD_PROVIDER_KEY: "not-in-the-config"})
        assert ea.target(execution=execution) is None

    def test_an_alias_resolves_to_the_same_target_as_its_key(self) -> None:
        by_key = _execution(**{ea.CLOUD_PROVIDER_KEY: "gke-lt3"})
        by_alias = _execution(**{ea.CLOUD_PROVIDER_KEY: "lt3"})
        assert ea.target(execution=by_key) == ea.target(execution=by_alias)

    def test_provider_naming_its_own_cluster_reads_the_registered_key(self) -> None:
        execution = _execution(
            **{
                ea.CLOUD_PROVIDER_KEY: "byo",
                "example.test/byo/cluster": "rack-7",
            }
        )
        assert ea.target(execution=execution) == ea.Target(
            provider="byo",
            cluster="rack-7",
            cluster_from_annotation="example.test/byo/cluster",
        )

    def test_provider_naming_no_cluster_reads_none_not_empty(self) -> None:
        execution = _execution(**{ea.CLOUD_PROVIDER_KEY: "byo"})
        assert ea.target(execution=execution).cluster == "none"

    def test_unhashable_provider_reads_none_rather_than_raising(self) -> None:
        execution = _execution(**{ea.CLOUD_PROVIDER_KEY: ["byo"]})
        assert ea.cloud_provider(execution=execution) is None
        assert ea.target(execution=execution) is None


class TestAccelerators:
    def test_absent_annotation_reads_none(self) -> None:
        assert ea.accelerators(execution=_execution()) is None

    def test_empty_string_reads_none(self) -> None:
        execution = _execution(**{ea.RESOURCES_ACCELERATORS_KEY: ""})
        assert ea.accelerators(execution=execution) is None

    def test_empty_object_reads_none(self) -> None:
        execution = _execution(**{ea.RESOURCES_ACCELERATORS_KEY: "{}"})
        assert ea.accelerators(execution=execution) is None

    @pytest.mark.parametrize("raw", ['{"nvidia-h100":"8"}', '{"nvidia-h100": "8"}'])
    def test_whitespace_does_not_make_two_requests(self, raw: str) -> None:
        execution = _execution(**{ea.RESOURCES_ACCELERATORS_KEY: raw})
        assert ea.accelerators(execution=execution) == ("nvidia-h100", 8)

    def test_integer_quantity_is_accepted(self) -> None:
        execution = _execution(
            **{ea.RESOURCES_ACCELERATORS_KEY: json.dumps({"nvidia-h100": 4})}
        )
        assert ea.accelerators(execution=execution) == ("nvidia-h100", 4)

    def test_two_models_is_an_error(self) -> None:
        execution = _execution(
            **{ea.RESOURCES_ACCELERATORS_KEY: '{"nvidia-h100":"8","tpu":"1"}'}
        )
        with pytest.raises(ValueError, match="Multiple accelerator types"):
            ea.accelerators(execution=execution)

    def test_malformed_json_is_an_error(self) -> None:
        execution = _execution(**{ea.RESOURCES_ACCELERATORS_KEY: "{not json"})
        with pytest.raises(ValueError):
            ea.accelerators(execution=execution)

    def test_annotation_sent_as_an_object_is_a_value_error(self) -> None:
        execution = _execution(**{ea.RESOURCES_ACCELERATORS_KEY: {"nvidia-l4": "8"}})
        with pytest.raises(ValueError, match="not a string"):
            ea.accelerators(execution=execution)

    @pytest.mark.parametrize("raw", ['{"nvidia-l4": null}', '{"nvidia-l4": ["8"]}'])
    def test_unusable_count_is_a_value_error(self, raw: str) -> None:
        execution = _execution(**{ea.RESOURCES_ACCELERATORS_KEY: raw})
        with pytest.raises(ValueError, match="not a number"):
            ea.accelerators(execution=execution)

    def test_no_input_raises_type_error(self) -> None:
        """A caller catching ValueError is enough; nothing here escapes as TypeError."""
        for raw in [{"nvidia-l4": "8"}, '{"nvidia-l4": null}', '{"nvidia-l4": ["8"]}']:
            execution = _execution(**{ea.RESOURCES_ACCELERATORS_KEY: raw})
            with pytest.raises(ValueError):
                ea.accelerators(execution=execution)
