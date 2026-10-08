"""Unit tests for triggers.evaluation — walking a condition against the emitted events.

Named test_trigger_evaluation so the module basename stays unique across the suite: the repo
has no __init__.py in tests and pytest runs in the default prepend import mode, which keys
modules by basename.
"""

import itertools
from typing import Any

import pytest

from cloud_pipelines_backend.triggers import evaluation


def _leaf(event: str, **extra: Any) -> dict[str, Any]:
    return {"event": event, **extra}


def _all(*children: Any) -> dict[str, Any]:
    return {"op": "all", "children": list(children)}


def _any(*children: Any) -> dict[str, Any]:
    return {"op": "any", "children": list(children)}


# all(any(US, EU), refunds, any(fx1, fx2)) — the shape the design is written against: an
# outer conjunction with two independent choices inside it.
_NESTED = _all(
    _any(_leaf("orders-us-ready"), _leaf("orders-eu-ready")),
    _leaf("refunds-ready", expire_seconds=86400),
    _any(_leaf("fx-primary-ready"), _leaf("fx-backup-ready")),
)

# One set of leaves under two nestings, so nothing but the operators can explain a
# disagreement between them. Group R is three regions, group F two fx sources, and each group
# carries one expiry. The names are constants rather than literals because the shapes being
# built from the *same* five leaves is the premise of the tests, not an incidental detail.
_US, _EU, _APAC = "orders-us-ready", "orders-eu-ready", "orders-apac-ready"
_FX1, _FX2 = "fx-primary-ready", "fx-backup-ready"
_R_EXPIRY, _F_EXPIRY = 3600, 86400

# all(any(US, EU, APAC), all(FX1, FX2)) — one region will do, but both fx sources are required.
_OUTER_ALL = _all(
    _any(_leaf(_US), _leaf(_EU), _leaf(_APAC, expire_seconds=_R_EXPIRY)),
    _all(_leaf(_FX1), _leaf(_FX2, expire_seconds=_F_EXPIRY)),
)

# any(any(FX1, FX2), all(US, EU, APAC)) — the groups swapped over: R is the strict side now
# and F the loose one. Fresh leaf dicts, so a test mutating one shape cannot reach the other.
_OUTER_ANY = _any(
    _any(_leaf(_FX1), _leaf(_FX2, expire_seconds=_F_EXPIRY)),
    _all(_leaf(_US), _leaf(_EU), _leaf(_APAC, expire_seconds=_R_EXPIRY)),
)


class TestSatisfied:
    @pytest.mark.parametrize(
        ("emitted", "expected"),
        [
            ({"orders-eu-ready", "refunds-ready", "fx-backup-ready"}, True),
            ({"orders-us-ready", "refunds-ready", "fx-primary-ready"}, True),
            # Both sides of a choice emitted: still satisfied, no double counting.
            (
                {
                    "orders-us-ready",
                    "orders-eu-ready",
                    "refunds-ready",
                    "fx-primary-ready",
                },
                True,
            ),
            # One conjunct missing is enough to hold the whole condition back.
            ({"orders-eu-ready", "refunds-ready"}, False),
            ({"refunds-ready", "fx-backup-ready"}, False),
            (set(), False),
        ],
    )
    def test_nesting(self, emitted: set[str], expected: bool) -> None:
        assert evaluation.satisfied(condition=_NESTED, emitted=emitted) is expected

    def test_a_bare_leaf_is_a_condition(self) -> None:
        assert evaluation.satisfied(condition=_leaf("solo"), emitted={"solo"}) is True
        assert evaluation.satisfied(condition=_leaf("solo"), emitted=set()) is False

    def test_nesting_is_not_capped(self) -> None:
        # Nothing is compiled, so depth costs a stack frame and nothing else.
        condition: dict[str, Any] = _leaf("deep")
        for _ in range(50):
            condition = _all(_any(condition))
        assert evaluation.satisfied(condition=condition, emitted={"deep"}) is True


class TestMatch:
    def test_it_names_where_the_match_happened(self) -> None:
        found = evaluation.evaluate(
            condition=_NESTED,
            emitted={"orders-eu-ready", "refunds-ready", "fx-backup-ready"},
        )
        assert found is not None
        # Path segments are operator[index] against the authored JSON, so the string can be
        # read straight off the payload a person posted.
        assert found.branch == "all[0].any[1]"
        assert found.events == (
            "orders-eu-ready",
            "refunds-ready",
            "fx-backup-ready",
        )

    def test_an_any_reports_only_the_child_it_chose(self) -> None:
        found = evaluation.evaluate(
            condition=_any(_leaf("first"), _leaf("second")),
            emitted={"first", "second"},
        )
        assert found is not None
        assert (found.branch, found.events) == ("any[0]", ("first",))

    def test_an_unsatisfied_condition_has_no_match(self) -> None:
        assert (
            evaluation.evaluate(condition=_all(_leaf("a"), _leaf("b")), emitted={"a"})
            is None
        )


class TestSwappedNesting:
    """The same leaves under all(any, all) and any(any, all): only the operators differ."""

    @pytest.mark.parametrize(
        ("emitted", "outer_all", "outer_any"),
        [
            (set(), False, False),
            ({_US}, False, False),
            # Two of the three regions: the strict side of _OUTER_ANY is still short.
            ({_US, _EU}, False, False),
            ({_US, _EU, _APAC}, False, True),
            # One fx satisfies any(F), but all(F) wants both.
            ({_FX1}, False, True),
            ({_FX1, _FX2}, False, True),
            ({_EU, _FX1, _FX2}, True, True),
            # Every region and one fx: each shape is decided by its own strict side.
            ({_US, _EU, _APAC, _FX1}, False, True),
        ],
    )
    def test_the_operators_decide(
        self, emitted: set[str], outer_all: bool, outer_any: bool
    ) -> None:
        assert evaluation.satisfied(condition=_OUTER_ALL, emitted=emitted) is outer_all
        assert evaluation.satisfied(condition=_OUTER_ANY, emitted=emitted) is outer_any

    def test_both_shapes_name_the_same_events_and_expiries(self) -> None:
        # The premise of the table above: neither the leaf names nor the expiries differ
        # between the shapes, so every disagreement in it is the nesting.
        expiries = {
            _US: None,
            _EU: None,
            _APAC: _R_EXPIRY,
            _FX1: None,
            _FX2: _F_EXPIRY,
        }
        assert evaluation.event_expiries(condition=_OUTER_ALL) == expiries
        assert evaluation.event_expiries(condition=_OUTER_ANY) == expiries
        assert evaluation.event_names(condition=_OUTER_ALL) == evaluation.event_names(
            condition=_OUTER_ANY
        )

    def test_one_emitted_set_reports_two_different_branches(self) -> None:
        emitted = {_EU, _FX1, _FX2}
        found_all = evaluation.evaluate(condition=_OUTER_ALL, emitted=emitted)
        found_any = evaluation.evaluate(condition=_OUTER_ANY, emitted=emitted)
        assert found_all is not None
        assert found_any is not None
        # The outer all needs every conjunct, so its evidence is the region and both fx.
        assert (found_all.branch, found_all.events) == (
            "all[0].any[1]",
            (_EU, _FX1, _FX2),
        )
        # The outer any is satisfied by its first child alone and never reads the regions.
        assert (found_any.branch, found_any.events) == (
            "any[0].any[0]",
            (_FX1,),
        )

    def test_the_outer_any_can_be_satisfied_by_its_strict_child(self) -> None:
        found = evaluation.evaluate(condition=_OUTER_ANY, emitted={_US, _EU, _APAC})
        assert found is not None
        # Second child of the any, and every leaf of the all inside it.
        assert (found.branch, found.events) == (
            "any[1].all[0]",
            (_US, _EU, _APAC),
        )

    def test_the_all_shape_implies_the_any_shape(self) -> None:
        # Not a coincidence of the cases above: group F is the strict side of _OUTER_ALL and
        # the loose side of _OUTER_ANY, so satisfying all(F) always satisfies any(F). No
        # emitted set can hold for _OUTER_ALL alone, which is why the table has no such row.
        names = (_US, _EU, _APAC, _FX1, _FX2)
        for size in range(len(names) + 1):
            for subset in itertools.combinations(names, size):
                emitted = set(subset)
                if evaluation.satisfied(condition=_OUTER_ALL, emitted=emitted):
                    assert evaluation.satisfied(
                        condition=_OUTER_ANY, emitted=emitted
                    ), emitted


class TestEventNames:
    def test_every_leaf_at_any_depth(self) -> None:
        assert evaluation.event_names(condition=_NESTED) == {
            "orders-us-ready",
            "orders-eu-ready",
            "refunds-ready",
            "fx-primary-ready",
            "fx-backup-ready",
        }

    def test_a_repeated_event_is_named_once(self) -> None:
        # One event is one row, however many times the condition mentions it.
        assert evaluation.event_names(
            condition=_all(_leaf("a"), _any(_leaf("a"), _leaf("b")))
        ) == {"a", "b"}


class TestOutstanding:
    """What the condition still needs, which is not every event it names and lacks.

    The distinction only shows up under `any`: a branch that is already satisfied wants nothing
    more, so the events it did not take are not outstanding. A flat set difference over
    `event_names` cannot see that, and asking a caller to emit one of them is asking for an
    event that would change nothing.
    """

    _A_OR_B_AND_C = _all(_any(_leaf("a"), _leaf("b")), _leaf("c"))

    @pytest.mark.parametrize(
        ("condition", "emitted", "expected"),
        [
            # The case that motivated this: the `any` is settled by `a`, so only `c` is left.
            # A set difference over `event_names` would also name `b`.
            (_A_OR_B_AND_C, {"a"}, {"c"}),
            # Nothing emitted: no branch is settled, so every option is still outstanding and
            # this agrees with the flat difference.
            (_A_OR_B_AND_C, set(), {"a", "b", "c"}),
            # Satisfied conditions are waiting for nothing at all.
            (_A_OR_B_AND_C, {"a", "c"}, set()),
            (_A_OR_B_AND_C, {"b", "c"}, set()),
            # Both sides of the choice emitted: still nothing outstanding, no double counting.
            (_A_OR_B_AND_C, {"a", "b", "c"}, set()),
            # The `any` settled but the conjunct not yet, and the mirror of it.
            (_A_OR_B_AND_C, {"b"}, {"c"}),
            (_A_OR_B_AND_C, {"c"}, {"a", "b"}),
            # `all` has no shortcut, so there it matches the flat difference exactly.
            (_all(_leaf("a"), _leaf("b")), {"a"}, {"b"}),
            (_all(_leaf("a"), _leaf("b")), set(), {"a", "b"}),
            (_all(_leaf("a"), _leaf("b")), {"a", "b"}, set()),
            # A satisfied top-level `any` needs nothing, though it names an event it lacks.
            (_any(_leaf("a"), _leaf("b")), {"b"}, set()),
            # An unsatisfied `any` offers every option: any one of them would do.
            (_any(_leaf("a"), _leaf("b")), set(), {"a", "b"}),
            # A bare leaf is a condition.
            (_leaf("a"), set(), {"a"}),
            (_leaf("a"), {"a"}, set()),
            # An event emitted that the condition never mentions changes nothing.
            (_A_OR_B_AND_C, {"unrelated"}, {"a", "b", "c"}),
        ],
    )
    def test_what_is_still_needed(
        self, condition: dict[str, Any], emitted: set[str], expected: set[str]
    ) -> None:
        assert evaluation.outstanding(condition=condition, emitted=emitted) == expected

    def test_a_satisfied_condition_is_waiting_for_nothing(self) -> None:
        # The invariant that ties this to `satisfied`: the two cannot disagree about whether
        # there is anything left to wait for. Checked over every subset of the leaves, so a
        # shape where one says "done" and the other still lists an event would be caught.
        leaves = [_US, _EU, "refunds-ready", _FX1, _FX2]
        for size in range(len(leaves) + 1):
            for combination in itertools.combinations(leaves, size):
                emitted = set(combination)
                still_needed = evaluation.outstanding(
                    condition=_NESTED, emitted=emitted
                )
                holds = evaluation.satisfied(condition=_NESTED, emitted=emitted)
                assert (still_needed == set()) is holds, (
                    emitted,
                    still_needed,
                    holds,
                )

    def test_it_never_asks_for_more_than_the_condition_names(self) -> None:
        # Outstanding is always a subset of the flat difference: this only ever removes events
        # from the answer, so no caller is asked for something new.
        names = evaluation.event_names(condition=_OUTER_ALL)
        for size in range(len(names) + 1):
            for combination in itertools.combinations(sorted(names), size):
                emitted = set(combination)
                assert evaluation.outstanding(
                    condition=_OUTER_ALL, emitted=emitted
                ) <= (names - emitted)

    def test_emitting_something_outstanding_makes_progress(self) -> None:
        # The set is not merely accurate, it is actionable: emitting any event it names must
        # shrink it. A report a caller cannot act on would be worse than none.
        emitted = {_US}
        while still_needed := evaluation.outstanding(
            condition=_OUTER_ALL, emitted=emitted
        ):
            before = still_needed
            emitted = emitted | {sorted(still_needed)[0]}
            assert (
                evaluation.outstanding(condition=_OUTER_ALL, emitted=emitted) < before
            )
        assert evaluation.satisfied(condition=_OUTER_ALL, emitted=emitted) is True

    def test_a_repeated_event_settles_every_branch_naming_it(self) -> None:
        # One event, two places. Emitting it satisfies the `any` and the conjunct at once.
        condition = _all(_leaf("a"), _any(_leaf("a"), _leaf("b")))
        assert evaluation.outstanding(condition=condition, emitted={"a"}) == set()
        assert evaluation.outstanding(condition=condition, emitted={"b"}) == {"a"}

    def test_nesting_is_not_capped(self) -> None:
        condition: dict[str, Any] = _leaf("deep")
        for _ in range(50):
            condition = _all(_any(condition))
        assert evaluation.outstanding(condition=condition, emitted=set()) == {"deep"}
        assert evaluation.outstanding(condition=condition, emitted={"deep"}) == set()

    def test_a_malformed_condition_is_rejected_the_same_way(self) -> None:
        with pytest.raises(ValueError, match="unknown condition node"):
            evaluation.outstanding(
                condition={"op": "some", "children": []}, emitted=set()
            )


class TestEventExpiries:
    def test_the_expiry_authored_on_each_leaf(self) -> None:
        assert evaluation.event_expiries(condition=_NESTED) == {
            "orders-us-ready": None,
            "orders-eu-ready": None,
            "refunds-ready": 86400,
            "fx-primary-ready": None,
            "fx-backup-ready": None,
        }

    def test_a_repeated_event_may_agree(self) -> None:
        condition = _all(
            _leaf("a", expire_seconds=60),
            _any(_leaf("a", expire_seconds=60), _leaf("b")),
        )
        assert evaluation.event_expiries(condition=condition) == {
            "a": 60,
            "b": None,
        }

    @pytest.mark.parametrize(
        "node",
        [
            _all(_leaf("a", expire_seconds=60), _leaf("a", expire_seconds=90)),
            _all(_leaf("a"), _leaf("a", expire_seconds=60)),
        ],
    )
    def test_a_repeated_event_may_not_disagree(self, node: dict[str, Any]) -> None:
        # There is one row per event, so there is no honest way to pick a winner.
        with pytest.raises(ValueError, match="conflicting expire_seconds"):
            evaluation.event_expiries(condition=node)

    @pytest.mark.parametrize("expire_seconds", [0, -5, 1.5, "60", True])
    def test_an_expiry_must_be_a_positive_whole_number_of_seconds(
        self, expire_seconds: Any
    ) -> None:
        # True is an int in Python, which would otherwise be accepted as one second.
        with pytest.raises(ValueError, match="positive number of seconds"):
            evaluation.event_expiries(
                condition=_all(_leaf("a", expire_seconds=expire_seconds))
            )


class TestMalformedConditions:
    @pytest.mark.parametrize(
        "node",
        [
            42,
            None,
            # A bare string was the leaf shape of an earlier draft; leaves are objects now.
            "orders-ready",
            ["orders-ready"],
            {},
            {"op": "all"},
            {"op": "all", "children": "orders-ready"},
            {"op": "not", "children": [{"event": "a"}]},
            {"children": [{"event": "a"}]},
            {"event": 7},
        ],
    )
    def test_both_readers_raise_and_name_the_node(self, node: Any) -> None:
        # Silence would be worse than an error in either direction: an empty event set would
        # make the sync delete every event row, and a False would strand a emitted
        # subscription. Both name the offending node so it is traceable to its branch.
        with pytest.raises(ValueError, match="unknown condition node"):
            evaluation.satisfied(condition=node, emitted={"orders-ready"})
        with pytest.raises(ValueError, match="unknown condition node"):
            evaluation.event_names(condition=node)


class TestMatchedEvents:
    def test_it_carries_the_branch_the_events_and_the_definition(self) -> None:
        definition = {"name": "eu-close", "condition": _NESTED}
        found = evaluation.evaluate(
            condition=_NESTED,
            emitted={"orders-eu-ready", "refunds-ready", "fx-backup-ready"},
        )
        assert found is not None

        payload = evaluation.matched_events(found=found, definition=definition)

        assert payload["branch"] == "all[0].any[1]"
        assert payload["branch_events"] == [
            "orders-eu-ready",
            "refunds-ready",
            "fx-backup-ready",
        ]
        assert payload["definition"] == definition

    def test_the_definition_is_snapshotted_not_shared(self) -> None:
        # A history row has to stay true after the subscription is edited, so the snapshot
        # cannot alias the definition it came from — a shallow copy would leave the nested
        # condition shared.
        definition: dict[str, Any] = {
            "name": "eu-close",
            "condition": _all(_leaf("a")),
        }
        found = evaluation.evaluate(condition=definition["condition"], emitted={"a"})
        assert found is not None
        payload = evaluation.matched_events(found=found, definition=definition)

        definition["name"] = "renamed"
        definition["condition"]["children"].append(_leaf("b"))

        assert payload["definition"]["name"] == "eu-close"
        assert payload["definition"]["condition"] == _all(_leaf("a"))
