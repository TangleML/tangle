"""The request models are the only thing standing between a posted blob and a run-time error.

Every case here is a shape the evaluator would raise `ValueError` on, asserted to be rejected
while it is still a request.
"""

from typing import Any

import pydantic
import pytest

from cloud_pipelines_backend.triggers import api_routes, db_models, evaluation
from cloud_pipelines_backend.user_pipelines import db_models as user_pipeline_db_models

# Every create request names a pipeline. Nothing here touches a database, and the model does
# not check that the pipeline exists — that is the foreign key's job — so a literal id is
# enough to satisfy the required field and keep each test about the thing it is testing.
_PIPELINE_ID = "pipeline-under-test"


def _create_request(**fields: Any) -> api_routes.SubscriptionCreateRequest:
    """A create request with the target already filled in, unless a test names its own."""
    fields.setdefault("pipeline_task_spec_from_user_pipeline_id", _PIPELINE_ID)
    return api_routes.SubscriptionCreateRequest(**fields)


def _nested(*, depth: int) -> dict[str, Any]:
    """A chain of single-child `all` branches `depth` levels tall, a leaf at the bottom."""
    condition: dict[str, Any] = {"event": "a"}
    for _ in range(depth - 1):
        condition = {"op": "all", "children": [condition]}
    return condition


class TestConditionGrammar:
    def test_leaf_round_trips(self) -> None:
        request = _create_request(name="nightly", condition={"event": "orders-ready"})
        assert api_routes.definition_from(
            name=request.name,
            condition=request.condition,
            templates={},
        ) == {
            "name": "nightly",
            "condition": {"event": "orders-ready"},
        }

    def test_nested_branches_round_trip(self) -> None:
        condition = {
            "op": "all",
            "children": [
                {"op": "any", "children": [{"event": "us"}, {"event": "eu"}]},
                {"event": "refunds", "expire_seconds": 86400},
            ],
        }
        request = _create_request(name="n", condition=condition)
        # Stored verbatim: an omitted expire_seconds is not written back as an explicit null.
        assert (
            api_routes.definition_from(
                name=request.name, condition=request.condition, templates={}
            )["condition"]
            == condition
        )

    @pytest.mark.parametrize(
        "condition",
        [
            {"op": "some", "children": [{"event": "a"}]},  # unknown operator
            {"op": "all", "children": {"event": "a"}},  # children not a list
            {"op": "all", "children": []},  # empty: all([]) is vacuously true
            {"all": [{"event": "a"}]},  # operator as the key
            {"event": 7},  # event not a string
            {"event": ""},  # empty event name
            "orders-ready",  # a bare string
            7,
            None,
        ],
    )
    def test_malformed_node_is_rejected(self, condition: object) -> None:
        with pytest.raises(pydantic.ValidationError):
            _create_request(name="n", condition=condition)


class TestExpireSeconds:
    @pytest.mark.parametrize("expire_seconds", [0, -60, 1.5, "60", True])
    def test_rejected_exactly_as_the_evaluator_would(
        self, expire_seconds: object
    ) -> None:
        """Strict, so pydantic cannot coerce a value the evaluator will later refuse.

        The blob is stored as posted, so a coerced `True` would reach `_expire_seconds` intact
        and raise there — a 500 for what is a bad request.
        """
        node = {"event": "a", "expire_seconds": expire_seconds}
        with pytest.raises(ValueError):
            evaluation.event_expiries(condition=node)
        with pytest.raises(pydantic.ValidationError):
            _create_request(name="n", condition=node)

    def test_omitted_and_explicit_null_both_mean_never_expires(self) -> None:
        for condition in (
            {"event": "a"},
            {"event": "a", "expire_seconds": None},
        ):
            request = _create_request(name="n", condition=condition)
            assert evaluation.event_expiries(
                condition=api_routes.definition_from(
                    name=request.name,
                    condition=request.condition,
                    templates={},
                )["condition"]
            ) == {"a": None}


class TestUnknownKeys:
    @pytest.mark.parametrize(
        "payload",
        [
            {
                "name": "n",
                "condition": {"event": "a"},
                "created_by": "someone-else",
            },
            {"name": "n", "condition": {"event": "a"}, "cycle": 7},
            {"name": "n", "condition": {"event": "a", "colour": "red"}},
            {
                "name": "n",
                "condition": {
                    "op": "all",
                    "children": [{"event": "a"}],
                    "x": 1,
                },
            },
        ],
    )
    def test_extra_key_is_rejected(self, payload: dict[str, object]) -> None:
        with pytest.raises(pydantic.ValidationError):
            _create_request(**payload)


class TestNotValidated:
    def test_unknown_event_name_is_accepted(self) -> None:
        """No producer registry: a subscription may precede the thing that emits its events."""
        _create_request(name="n", condition={"event": "nothing-emits-this-yet"})


class TestConditionDepth:
    """Depth used to be uncapped here, deliberately; `_MAX_CONDITION_DEPTH` reverses that.

    Not a style rule — an uncapped tree commits and then breaks the shared listing for every
    caller. `TestConditionDepth` in test_trigger_api_routes is where that is demonstrated; here
    it is only the arithmetic of the cap.
    """

    def test_the_depth_the_old_test_called_acceptable_is_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError, match="levels deep"):
            _create_request(name="n", condition=_nested(depth=50))

    def test_nesting_up_to_the_cap_is_accepted(self) -> None:
        """The cap is generous: nothing anyone writes by hand comes close to it."""
        _create_request(
            name="n", condition=_nested(depth=api_routes._MAX_CONDITION_DEPTH)
        )

    def test_one_level_past_the_cap_is_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError, match="levels deep"):
            _create_request(
                name="n",
                condition=_nested(depth=api_routes._MAX_CONDITION_DEPTH + 1),
            )

    def test_the_cap_counts_the_deepest_branch_not_the_first(self) -> None:
        """A shallow first child must not hide a deep sibling from the walk."""
        condition = {
            "op": "all",
            "children": [
                {"event": "shallow"},
                _nested(depth=api_routes._MAX_CONDITION_DEPTH),
            ],
        }
        with pytest.raises(pydantic.ValidationError, match="levels deep"):
            _create_request(name="n", condition=condition)

    def test_an_update_is_capped_too(self) -> None:
        """Both write paths call the same door; a PATCH must not be the way around it."""
        with pytest.raises(pydantic.ValidationError, match="levels deep"):
            api_routes.SubscriptionUpdateRequest(
                condition=_nested(depth=api_routes._MAX_CONDITION_DEPTH + 1)
            )


class TestUpdateRequest:
    def test_every_field_is_optional(self) -> None:
        request = api_routes.SubscriptionUpdateRequest()
        assert request.name is None
        assert request.condition is None
        assert request.enabled is None

    def test_metadata_only_edit_needs_no_condition(self) -> None:
        assert api_routes.SubscriptionUpdateRequest(name="renamed").condition is None

    def test_extra_key_is_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            api_routes.SubscriptionUpdateRequest(created_by="someone-else")


class TestNameIsStripped:
    """A padded name is the same name; the lookup key matches byte-exact, so it must not survive.

    `TestNameWhitespace` in test_trigger_api_routes is where the consequence is shown — here it
    is only the constraint, on both models that can write a name.
    """

    def test_create_strips_the_ends(self) -> None:
        assert (
            _create_request(name="  nightly  ", condition={"event": "a"}).name
            == "nightly"
        )

    def test_create_rejects_a_name_that_is_only_whitespace(self) -> None:
        """`min_length` is applied after the strip, so this is a 422 and not a blank name."""
        with pytest.raises(pydantic.ValidationError):
            _create_request(name="   ", condition={"event": "a"})

    def test_create_leaves_the_middle_alone(self) -> None:
        assert _create_request(name=" a b ", condition={"event": "a"}).name == "a b"

    def test_update_strips_the_ends(self) -> None:
        assert (
            api_routes.SubscriptionUpdateRequest(name="  renamed  ").name == "renamed"
        )

    def test_update_rejects_a_name_that_is_only_whitespace(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            api_routes.SubscriptionUpdateRequest(name=" ")


class TestEventNameFitsTheColumn:
    """The cap on a posted event name is what keeps a write inside `event_name`'s column.

    `trigger_event_state.event_name` is half a composite PK and an indexed column, so it cannot
    be widened to TEXT; and a readiness event name reaches the system as an annotation value,
    which the emissions producer bounds three orders of magnitude higher (64KB). The only writer
    of the column is `event_state.sync`, reading names out of a definition blob these models
    built — so this cap, and nothing downstream of it, is the guarantee. Asserted rather than
    assumed, because the two ends derive from the same constant by convention, not by reference.
    """

    def test_the_cap_is_the_column_width(self) -> None:
        column = db_models.TriggerEventState.__table__.c.event_name
        assert column.type.length == api_routes._MAX_NAME_LENGTH

    def test_an_event_name_at_the_cap_is_accepted(self) -> None:
        at_cap = "e" * api_routes._MAX_NAME_LENGTH
        request = _create_request(name="n", condition={"event": at_cap})
        definition = api_routes.definition_from(
            name=request.name,
            condition=request.condition,
            templates={},
        )
        # Through the blob and back out of the evaluator: the name `sync` would write is the
        # one that was validated, at full length and untruncated.
        assert evaluation.event_names(condition=definition["condition"]) == {at_cap}

    @pytest.mark.parametrize(
        "condition",
        [
            {"event": "e" * 256},
            {"op": "all", "children": [{"event": "ok"}, {"event": "e" * 256}]},
        ],
        ids=["leaf", "nested-under-a-valid-sibling"],
    )
    def test_an_event_name_over_the_cap_is_rejected(self, condition: object) -> None:
        """Including deep in the tree — one over-long leaf fails the whole request."""
        with pytest.raises(pydantic.ValidationError):
            _create_request(name="n", condition=condition)

    def test_an_edit_cannot_widen_a_name_either(self) -> None:
        """The update model replaces the condition wholesale, so it needs the same cap."""
        with pytest.raises(pydantic.ValidationError):
            api_routes.SubscriptionUpdateRequest(condition={"event": "e" * 256})

    def test_the_subscription_name_is_capped_to_its_own_column(self) -> None:
        column = db_models.TriggerSubscription.__table__.c.name
        assert column.type.length == api_routes._MAX_NAME_LENGTH
        _create_request(
            name="n" * api_routes._MAX_NAME_LENGTH, condition={"event": "a"}
        )
        with pytest.raises(pydantic.ValidationError):
            _create_request(name="n" * 256, condition={"event": "a"})


class TestEventNameIsAnRfc1123Label:
    """`event_name` is half of `trigger_event_state`'s composite PK, so the collation decides.

    Under MySQL's `utf8mb4_0900_ai_ci` "Orders-Ready" and "orders-ready" are the same key, and
    so are "caf\u00e9" and "cafe"; under the SQLite the tests run on they are four. Two engines
    disagreeing about how many rows a definition writes is the bug. `_EVENT_NAME_PATTERN`
    removes it at the door by allowing one legal spelling of each name, rather than by teaching
    the writer to fold — the same RFC 1123 label `quota.api_routes` already settled on.
    """

    @pytest.mark.parametrize(
        "event",
        ["a", "1", "ok", "orders-ready", "a-b-c", "1-2", "e" * 255],
        ids=lambda e: e[:20],
    )
    def test_a_conforming_name_is_accepted(self, event: str) -> None:
        request = _create_request(name="n", condition={"event": event})
        definition = api_routes.definition_from(
            name=request.name,
            condition=request.condition,
            templates={},
        )
        assert evaluation.event_names(condition=definition["condition"]) == {event}

    @pytest.mark.parametrize(
        ("event", "why"),
        [
            ("orders_ready", "underscore"),
            ("Orders-Ready", "uppercase, folds onto the lowercase spelling"),
            ("caf\u00e9", "accent, folds onto 'cafe'"),
            ("\U0001f389", "outside the alphabet entirely"),
            ("-a", "leading hyphen"),
            ("a-", "trailing hyphen"),
            (" a", "leading space; the model does not strip an event name"),
            ("a ", "trailing space"),
            ("a b", "inner space"),
            ("orders.ready", "dot"),
            (
                "e2e-live-20260903_220945-s1-a",
                "the shape the e2e runners used to build",
            ),
        ],
        ids=lambda v: v if isinstance(v, str) and " " not in v else "",
    )
    def test_a_non_conforming_name_is_rejected(self, event: str, why: str) -> None:
        with pytest.raises(pydantic.ValidationError):
            _create_request(name="n", condition={"event": event})

    def test_the_whole_request_fails_for_one_bad_leaf(self) -> None:
        """A valid sibling does not rescue it — the tree is validated leaf by leaf."""
        with pytest.raises(pydantic.ValidationError):
            _create_request(
                name="n",
                condition={
                    "op": "all",
                    "children": [{"event": "ok"}, {"event": "Orders_Ready"}],
                },
            )

    def test_an_edit_cannot_reintroduce_one(self) -> None:
        """The update model replaces the condition wholesale, so it needs the same rule."""
        api_routes.SubscriptionUpdateRequest(condition={"event": "orders-ready"})
        with pytest.raises(pydantic.ValidationError):
            api_routes.SubscriptionUpdateRequest(condition={"event": "orders_ready"})

    def test_the_two_spellings_that_used_to_collide_cannot_both_exist(
        self,
    ) -> None:
        """The pair from the module docstring: only one of them is now expressible."""
        request = _create_request(name="n", condition={"event": "orders-ready"})
        definition = api_routes.definition_from(
            name=request.name,
            condition=request.condition,
            templates={},
        )
        assert evaluation.event_names(condition=definition["condition"]) == {
            "orders-ready"
        }
        with pytest.raises(pydantic.ValidationError):
            _create_request(name="n", condition={"event": "Orders-Ready"})


class TestGrammarMatchesTheEvaluator:
    """`Literal` cannot be built from the evaluator's constants, so assert they agree."""

    def test_keys_match(self) -> None:
        assert set(api_routes.EventCondition.model_fields) == {
            evaluation.EVENT_KEY,
            evaluation.EXPIRE_SECONDS_KEY,
        }
        assert set(api_routes.BranchCondition.model_fields) == {
            evaluation.OP_KEY,
            evaluation.CHILDREN_KEY,
        }

    def test_operators_match(self) -> None:
        accepted = {
            op
            for op in (evaluation.ALL_OP, evaluation.ANY_OP)
            if api_routes.BranchCondition(op=op, children=[{"event": "a"}]).op == op
        }
        assert accepted == {evaluation.ALL_OP, evaluation.ANY_OP}


class TestRejectionNamesTheField:
    """A 422 body is built from `ValidationError.errors()`, so the `loc` is what the caller reads.

    Asserting the location, not just that something was raised, is what makes the message
    actionable: "condition.1.expire_seconds" points at the offending leaf rather than at the
    whole tree.
    """

    @pytest.mark.parametrize(
        ("payload", "expected_loc"),
        [
            ({"condition": {"event": "a"}}, ("name",)),
            ({"name": "", "condition": {"event": "a"}}, ("name",)),
            (
                {"name": "n", "condition": {"event": "a"}, "created_by": "x"},
                ("created_by",),
            ),
        ],
    )
    def test_error_location(
        self, payload: dict[str, object], expected_loc: tuple[str, ...]
    ) -> None:
        with pytest.raises(pydantic.ValidationError) as caught:
            _create_request(**payload)
        assert any(error["loc"] == expected_loc for error in caught.value.errors())

    def test_the_offending_leaf_is_named_not_the_whole_tree(self) -> None:
        condition = {
            "op": "all",
            "children": [
                {"event": "fine"},
                {"event": "bad", "expire_seconds": 0},
            ],
        }
        with pytest.raises(pydantic.ValidationError) as caught:
            _create_request(name="n", condition=condition)
        locations = [error["loc"] for error in caught.value.errors()]
        # The failing child's index appears, so the caller can find it in the payload they sent.
        assert any("children" in loc and 1 in loc for loc in locations), locations

    def test_a_non_object_condition_is_rejected_by_field(self) -> None:
        with pytest.raises(pydantic.ValidationError) as caught:
            _create_request(name="n", condition=["not", "an", "object"])
        assert all(error["loc"][0] == "condition" for error in caught.value.errors())


class TestColumnAndBlobAgree:
    def test_name_column_and_blob_copy_come_from_one_value(self) -> None:
        """`name` is both a column and a blob key; listing reads the column, so they must match.

        The route sets them from this single validated value, which is what stops the two from
        ever drifting.
        """
        request = _create_request(
            name="nightly-report", condition={"event": "orders-ready"}
        )
        definition = api_routes.definition_from(
            name=request.name,
            condition=request.condition,
            templates={},
        )
        assert definition["name"] == request.name

    def test_the_stored_condition_is_walkable_by_the_evaluator(self) -> None:
        """The round-trip that matters: what validation accepts, evaluation can read back."""
        request = _create_request(
            name="n",
            condition={
                "op": "any",
                "children": [
                    {"event": "us"},
                    {"event": "eu", "expire_seconds": 60},
                ],
            },
        )
        definition = api_routes.definition_from(
            name=request.name,
            condition=request.condition,
            templates={},
        )
        assert evaluation.event_expiries(condition=definition["condition"]) == {
            "us": None,
            "eu": 60,
        }
        assert (
            evaluation.satisfied(condition=definition["condition"], emitted={"us"})
            is True
        )


class TestTheTargetIdIsBoundedByItsColumn:
    """The field was capped at the generic string maximum, the column at 36.

    Anything in between passed validation and then met MySQL strict mode at the insert, which
    is a 500 for what the boundary owed the caller as a 422.
    """

    def test_an_id_of_exactly_the_column_width_is_accepted(self) -> None:
        request = _create_request(
            name="n",
            condition={"event": "a"},
            pipeline_task_spec_from_user_pipeline_id="x"
            * user_pipeline_db_models.PIPELINE_ID_LENGTH,
        )
        assert (
            len(request.pipeline_task_spec_from_user_pipeline_id)
            == user_pipeline_db_models.PIPELINE_ID_LENGTH
        )

    def test_one_character_past_the_column_width_is_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            _create_request(
                name="n",
                condition={"event": "a"},
                pipeline_task_spec_from_user_pipeline_id="x"
                * (user_pipeline_db_models.PIPELINE_ID_LENGTH + 1),
            )

    def test_the_update_request_is_bounded_the_same_way(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            api_routes.SubscriptionUpdateRequest(
                pipeline_task_spec_from_user_pipeline_id="x"
                * (user_pipeline_db_models.PIPELINE_ID_LENGTH + 1)
            )


class TestPinEdit:
    """`null` and omitted are different requests, and only `model_fields_set` can say so."""

    def test_an_untouched_pin_is_not_addressed(self) -> None:
        assert api_routes.SubscriptionUpdateRequest(name="n").pin_edit() == (
            False,
            None,
        )

    def test_an_explicit_null_is_addressed(self) -> None:
        request = api_routes.SubscriptionUpdateRequest.model_validate(
            {
                "name": "n",
                "pipeline_task_spec_from_user_pipeline_version_key": None,
            }
        )
        assert request.pin_edit() == (True, None)

    def test_a_version_is_addressed_and_carried(self) -> None:
        version = "a" * user_pipeline_db_models.DIGEST_LENGTH
        request = api_routes.SubscriptionUpdateRequest(
            pipeline_task_spec_from_user_pipeline_version_key=version
        )
        assert request.pin_edit() == (True, version)

    def test_a_version_of_the_wrong_length_is_rejected(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            api_routes.SubscriptionUpdateRequest(
                pipeline_task_spec_from_user_pipeline_version_key="short"
            )
