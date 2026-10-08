"""Unit tests for the intent contract the producer gates on.

`emissions/intents.py` exists so the producer can ask "does this status fire you?" without
knowing any kind's field names. What is pinned here is that the single-status implementation
compares the way the old inline `!=` did, and that a subclass may add a required field.
"""

import dataclasses

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.emissions import intents as emission_intents

Status = bts.ContainerExecutionStatus


@dataclasses.dataclass(frozen=True, kw_only=True)
class _RequiredFieldIntent(emission_intents.SingleStatusIntent):
    """A subclass adding a required field after the base's defaulted one.

    This is the shape the base exists to permit, so it is declared at module scope: a
    dataclass hierarchy normally rejects "non-default argument follows default argument", and
    `kw_only=True` is what makes it legal.
    """

    event_key: str


class TestSingleStatusIntent:
    def test_it_defaults_to_succeeded(self) -> None:
        assert emission_intents.SingleStatusIntent().on_status is Status.SUCCEEDED

    def test_it_fires_on_exactly_the_declared_status(self) -> None:
        intent = emission_intents.SingleStatusIntent(on_status=Status.FAILED)
        assert intent.matches(node_status=Status.FAILED)

    def test_it_does_not_fire_on_any_other_status(self) -> None:
        intent = emission_intents.SingleStatusIntent(on_status=Status.FAILED)
        # Including the other terminal ones: a single-status intent is exact, not a set.
        assert not intent.matches(node_status=Status.SUCCEEDED)
        assert not intent.matches(node_status=Status.CANCELLED)
        assert not intent.matches(node_status=Status.RUNNING)


class TestSubclassingWithARequiredField:
    def test_kw_only_lets_a_required_field_follow_the_defaulted_one(
        self,
    ) -> None:
        """The blocker `kw_only=True` removes, pinned so a later edit cannot reintroduce it."""
        intent = _RequiredFieldIntent(event_key="orders-ready")
        assert intent.event_key == "orders-ready"
        assert intent.on_status is Status.SUCCEEDED

    def test_the_inherited_rule_still_applies(self) -> None:
        intent = _RequiredFieldIntent(
            event_key="orders-ready", on_status=Status.CANCELLED
        )
        assert intent.matches(node_status=Status.CANCELLED)
        assert not intent.matches(node_status=Status.SUCCEEDED)
