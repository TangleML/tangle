"""Readers for the annotations that say where a task runs."""

import json
import re

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


class TestAsLabel:
    """Two spellings of one machine have to be one series, and a paste must not be one."""

    @pytest.mark.parametrize(
        "value,expected",
        [
            ("b300", "b300"),
            ("B300 cluster", "b300-cluster"),
            ("NVIDIA-B300-SXM6-PC", "nvidia-b300-sxm6-pc"),
            ("  b300  ", "b300"),
            ("b300__cluster", "b300-cluster"),
            ("--b300--", "b300"),
            ("", "none"),
            ("   ", "none"),
            ("---", "none"),
            ("!!!", "none"),
            ("x" * 80, "x" * 40),
            ("a" * 39 + "-b", "a" * 39),
        ],
        ids=[
            "already-fine",
            "spaces-and-case",
            "an-accelerator-model",
            "padded",
            "underscores-collapse",
            "dashes-trimmed",
            "empty",
            "whitespace-only",
            "dashes-only",
            "punctuation-only",
            "overlong",
            "truncation-does-not-leave-a-dash",
        ],
    )
    def test_a_client_value_becomes_label_shaped(
        self, value: str, expected: str
    ) -> None:
        assert ea.as_label(value=value) == expected

    def test_every_result_is_label_shaped(self) -> None:
        """Whatever goes in, what comes out matches the shape a label may take."""
        inputs = [
            "b300",
            "B300 cluster",
            "x" * 200,
            "!!!",
            "ünïcodé-b300",
            "a/b:c=d",
            "\n\ttabbed\n",
        ]
        for value in inputs:
            result = ea.as_label(value=value)
            assert re.fullmatch(r"[a-z0-9]+(-[a-z0-9]+)*", result), (value, result)
            assert len(result) <= 40


class TestClusterNamedByTheTask:
    """Davies takes its cluster from the task: reshaped, never checked against a list."""

    KEY = "ml.shopify.io/davies/cluster"

    def _target(self, named: object) -> object:
        ea.register_targets(
            targets={
                "davies": ea.Target(
                    provider="davies",
                    cluster="davies",
                    cluster_from_annotation=self.KEY,
                )
            }
        )
        annotations = {ea.CLOUD_PROVIDER_KEY: "davies"}
        if named is not None:
            annotations[self.KEY] = named
        execution = bts.ExecutionNode(task_spec={"annotations": annotations})
        try:
            return ea.target(execution=execution).cluster
        finally:
            ea.register_targets(targets={})

    @pytest.mark.parametrize(
        "named,expected",
        [
            ("b300", "b300"),
            ("B300 Cluster", "b300-cluster"),
            ("a-cluster-added-yesterday", "a-cluster-added-yesterday"),
            ("", "none"),
            ("   ", "none"),
            (None, "none"),
            (["b300"], "none"),
            ({"a": 1}, "none"),
        ],
        ids=[
            "as-written",
            "the-same-name-spelled-loudly",
            "a-name-no-list-ever-held",
            "empty",
            "whitespace",
            "absent",
            "a-list",
            "an-object",
        ],
    )
    def test_the_name_is_reshaped_into_a_label(
        self, named: object, expected: str
    ) -> None:
        assert self._target(named) == expected


class TestWhereATaskActuallyRan:
    """The annotation says where a task was aimed; routing can move it afterwards.

    A percentage redirect sends a share of runs to a successor cluster without
    rewriting the annotation, so a label built from the annotation alone names the
    wrong cluster, provider and namespace for exactly the tasks a migration moved.
    """

    FIELD = "tangle_launcher_key"

    def _register(self, *, field: str | None) -> None:
        ea.register_targets(
            targets={
                "lt3": ea.Target(provider="google", cluster="lt3", namespace="old"),
                "eo9": ea.Target(provider="google", cluster="eo9", namespace="new"),
            },
            launcher_key_field=field,
        )

    def teardown_method(self) -> None:
        ea.register_targets(targets={})

    def test_the_handle_beats_the_annotation(self) -> None:
        self._register(field=self.FIELD)
        execution = _execution(**{ea.CLOUD_PROVIDER_KEY: "lt3"})
        resolved = ea.target(execution=execution, launcher_data={self.FIELD: "eo9"})
        assert (resolved.cluster, resolved.namespace) == ("eo9", "new")

    def test_without_a_handle_the_annotation_still_answers(self) -> None:
        """Before the launch there is no handle, and the aim is all there is."""
        self._register(field=self.FIELD)
        execution = _execution(**{ea.CLOUD_PROVIDER_KEY: "lt3"})
        assert ea.target(execution=execution).cluster == "lt3"

    @pytest.mark.parametrize(
        "launcher_data",
        [
            None,
            {},
            {"tangle_launcher_key": ""},
            {"tangle_launcher_key": None},
            {"tangle_launcher_key": ["eo9"]},
            {"other_field": "eo9"},
            "not a dict",
            ["not a dict"],
        ],
        ids=[
            "absent",
            "empty",
            "empty-string",
            "null",
            "a-list",
            "a-different-field",
            "a-string",
            "a-list-not-a-dict",
        ],
    )
    def test_a_handle_that_does_not_say_falls_back(self, launcher_data: object) -> None:
        self._register(field=self.FIELD)
        execution = _execution(**{ea.CLOUD_PROVIDER_KEY: "lt3"})
        assert ea.target(execution=execution, launcher_data=launcher_data).cluster == (
            "lt3"
        )

    def test_a_deployment_recording_no_field_ignores_the_handle(self) -> None:
        """Registering no field name means the handle is not consulted at all."""
        self._register(field=None)
        execution = _execution(**{ea.CLOUD_PROVIDER_KEY: "lt3"})
        resolved = ea.target(execution=execution, launcher_data={self.FIELD: "eo9"})
        assert resolved.cluster == "lt3"

    def test_a_handle_naming_a_target_nobody_registers_reads_as_unrouted(self) -> None:
        self._register(field=self.FIELD)
        execution = _execution(**{ea.CLOUD_PROVIDER_KEY: "lt3"})
        assert (
            ea.target(execution=execution, launcher_data={self.FIELD: "retired"})
            is None
        )
