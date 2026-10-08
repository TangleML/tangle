"""Evaluating a trigger condition against the events that have been emitted.

A condition is the nested `op`/`children` tree the API received, read straight out of
`TriggerSubscription.definition["condition"]`. Nothing is compiled: the tree is walked as
authored, against a set of event names already in memory, so recursion depth is the nesting
depth of the posted JSON.

A node is either a branch or a leaf, and the leaf may carry that event's expiry:

    {"op": "all", "children": [                                     branch
        {"op": "any", "children": [                                 branch
            {"event": "orders-us-ready"},                           leaf
            {"event": "orders-eu-ready"}]},                         leaf
        {"event": "refunds-ready", "expire_seconds": 86400}]}       leaf with an expiry
"""

import copy
import dataclasses
from collections.abc import Container, Mapping
from typing import Any, Final

OP_KEY: Final[str] = "op"
CHILDREN_KEY: Final[str] = "children"
EVENT_KEY: Final[str] = "event"
EXPIRE_SECONDS_KEY: Final[str] = "expire_seconds"
ALL_OP: Final[str] = "all"
ANY_OP: Final[str] = "any"


@dataclasses.dataclass(frozen=True)
class Match:
    """Why a condition held: which events satisfied it, and where they sit in the tree.

    Attributes:
        branch: path to the first satisfying event, e.g. `all[0].any[1]`. Each segment is the
            operator that owns the edge and the child's index, so the path reads against the
            authored JSON. One `any` deeper in the tree makes several choices; this names the
            first, and `events` carries them all.

            `""` means there was no branch to name, which is the ordinary single-event
            subscription: a bare leaf, `{"event": "a"}`, has no operator above it. An empty
            `all` would record `""` too, being vacuously satisfied — `events` tells them
            apart, one name for the leaf against nothing at all for the vacuous branch, and
            the API rejects an empty child list on write so it should not reach here.

            A sentinel such as `"leaf"` was rejected deliberately: `matched_events` is read
            back long after the subscription is gone, and a sentinel would give that column
            two shapes — a path, or a magic word every reader has to know. `""` is the
            absence of a path, and the snapshotted `definition` beside it says which shape
            the condition was.
        events: the satisfying events in tree order — an `any` contributes only the child it
            chose, so this is what the condition actually needed, not every event it mentions.
            Positional, one entry per matched leaf, so entry *i* is the leaf that `branch`'s
            walk reached; an event named on two matched leaves therefore appears twice.
            `all(a, any(a, b))` satisfied by `a` alone gives `("a", "a")`.
    """

    branch: str
    events: tuple[str, ...]


def satisfied(*, condition: object, emitted: Container[str]) -> bool:
    """Whether `condition` holds, given the events emitted and not yet expired.

    Examples:
        condition = {"op": "any", "children": [{"event": "us-ready"}, {"event": "eu-ready"}]}

            emitted = {"eu-ready"}             -> True    one child is enough for `any`
            emitted = {"orders-ready"}         -> False

        condition = {"op": "all", "children": [{"event": "us-ready"}, {"event": "eu-ready"}]}

            emitted = {"eu-ready"}             -> False   `all` wants both
            emitted = {"us-ready", "eu-ready"} -> True
    """
    return evaluate(condition=condition, emitted=emitted) is not None


def evaluate(*, condition: object, emitted: Container[str]) -> Match | None:
    """The events that satisfy `condition` and where they sit, or None when it does not hold.

    Examples:
        condition = {"op": "all", "children": [
                        {"op": "any", "children": [{"event": "us-ready"}, {"event": "eu-ready"}]},
                        {"event": "refunds-ready"}]}

            emitted = {"eu-ready", "refunds-ready"}
                -> Match(branch="all[0].any[1]", events=("eu-ready", "refunds-ready"))
                                ^^^^^^ child 0 of the `all`, then child 1 of that `any`:
                                       the first leaf that satisfied anything

            emitted = {"eu-ready", "us-ready"}
                -> None      the `all` still wants "refunds-ready"

        An `any` reports only the child it took, so a condition that mentions five events can
        match with two.

        A bare leaf has no operator above it, so there is no branch to name:

            condition = {"event": "refunds-ready"},  emitted = {"refunds-ready"}
                -> Match(branch="", events=("refunds-ready",))
    """
    found = _matching_leaves(node=condition, emitted=emitted, path="")
    if found is None:
        return None
    return Match(
        branch=found[0][0] if found else "",
        events=tuple(name for _, name in found),
    )


def event_names(*, condition: object) -> set[str]:
    """Every event the condition mentions — the set `trigger_event_state` must hold.

    Examples:
        {"op": "all", "children": [
            {"event": "orders-ready", "expire_seconds": 3600},
            {"op": "any", "children": [{"event": "fx-ready"}, {"event": "orders-ready"}]}]}

            -> {"orders-ready", "fx-ready"}

        Expiries are ignored here and an event named twice is one name: this answers "which
        rows should exist", and one event is one row.

        Ignored is not unchecked. The walk still reads every expiry, so both callers in
        `service` discard the set and call this for the raise alone — a malformed condition
        is refused before `create_subscription` inserts a row or `update_subscription` syncs
        event state, and the write never half-happens.

        Refused:
            {"event": "a", "expire_seconds": 0}       -> as would -60, "60", 1.5, True
            {"op": "some", "children": [...]}         -> unknown operator
            {"op": "all", "children": {"event": "a"}} -> children must be an array
            {"event": 7} / "a" / 7 / None             -> not a leaf, so rejected as a branch

        Accepted:
            {"op": "any", "children": []}             -> set(), the API rejects an empty child
                                                         list before it reaches here
            {"event": "a", "expire_seconds": None}    -> {"a"}, never expires

    Raises:
        ValueError: naming the offending node, for any malformed node or expiry anywhere in
            the tree. The request models are what turn it into a 422; one escaping a route
            is a 500.
    """
    return {name for name, _expire_seconds in _leaves(node=condition)}


def outstanding(*, condition: object, emitted: Container[str]) -> set[str]:
    """Only what the condition still needs — not merely the events it names and lacks.

    `event_names` answers "which rows should exist"; this answers "what are we waiting for".
    The two differ exactly where a branch is already satisfied, because a satisfied `any` needs
    nothing further from its remaining children.

    Examples:
        {"op": "all", "children": [
            {"op": "any", "children": [{"event": "us-ready"}, {"event": "eu-ready"}]},
            {"event": "refunds-ready"}]}

            emitted = {"us-ready"}   -> {"refunds-ready"}   the `any` is settled; `eu-ready`
                                                             would change nothing
            emitted = {}             -> {"us-ready", "eu-ready", "refunds-ready"}
            emitted = {"us-ready", "refunds-ready"}
                                     -> set(), the condition holds

        An unsatisfied `any` reports every option it still has, since any one of them would do.
        A satisfied condition reports nothing at all, which is the property `missing` needs: a
        subscription about to fire is not waiting on anything.
    """
    name = _leaf(node=condition)
    if name is not None:
        return set() if name in emitted else {name}
    operator, children = _branch(node=condition)
    if operator == ANY_OP and any(
        satisfied(condition=child, emitted=emitted) for child in children
    ):
        return set()
    return {
        event
        for child in children
        for event in outstanding(condition=child, emitted=emitted)
    }


def event_expiries(*, condition: object) -> dict[str, int | None]:
    """Each event the condition mentions, mapped to the expiry authored on its leaf.

    Examples:
        {"op": "all", "children": [
            {"event": "orders-ready", "expire_seconds": 86400},
            {"event": "fx-ready"}]}

            -> {"orders-ready": 86400, "fx-ready": None}       None: this arrival never goes
                                                               stale

        Naming one event twice is fine while the leaves agree — `86400` twice, or no expiry on
        either — and raises when they do not:

            {"op": "any", "children": [{"event": "a", "expire_seconds": 86400},
                                       {"event": "a", "expire_seconds": 60}]}
            -> ValueError

    Raises:
        ValueError: for a malformed node, a non-positive expiry, or two leaves that name one
            event with conflicting expiries.
    """
    expiries: dict[str, int | None] = {}
    for name, expire_seconds in _leaves(node=condition):
        if name in expiries and expiries[name] != expire_seconds:
            raise ValueError(
                f"conflicting expire_seconds for event {name!r}: {expiries[name]!r} and {expire_seconds!r}"
            )
        expiries[name] = expire_seconds
    return expiries


def matched_events(*, found: Match, definition: Mapping[str, Any]) -> dict[str, Any]:
    """The history row's evidence: where the condition matched, and what it was at the time.

    The whole definition is snapshotted, not the condition alone, so a history row still says
    what was started after the subscription has been edited out from under it.

    `branch_events` is positional: one entry per matched leaf, in tree order, so it lines up
    with `branch`. `TriggerHistory.triggered_by` on the same row is keyed by event name and so
    deduped, and the two are meant to disagree on cardinality — read `branch_events` as "which
    leaves matched" and `triggered_by` as "which emission supplied each event". Deduping
    `branch_events` to match would cost it the alignment with `branch` that makes the path
    readable.

    `branch` is `""` when the condition is a bare leaf and there is no branch to name; see
    `Match` for why that is not a sentinel.

    Examples:
        found      = Match(branch="all[0].any[1]", events=("eu-ready", "refunds-ready"))
        definition = {"name": "nightly-fx", "condition": {"op": "all", "children": [...]}}

            -> {"branch": "all[0].any[1]",
                "branch_events": ["eu-ready", "refunds-ready"],
                "definition": {"name": "nightly-fx", "condition": {...}}}

        found      = Match(branch="", events=("orders-ready",))          a bare leaf
        definition = {"name": "nightly", "condition": {"event": "orders-ready"}}

            -> {"branch": "", "branch_events": ["orders-ready"], "definition": {...}}

        found      = Match(branch="all[0]", events=("a", "a"))           all(a, any(a, b))
                                                                         satisfied by "a"
            -> {"branch": "all[0]", "branch_events": ["a", "a"], ...}
               against triggered_by {"a": "em-a"} on the same row: two matched leaves, one
               emission
    """
    return {
        "branch": found.branch,
        "branch_events": list(found.events),
        # Deep-copied: the snapshot must not follow later edits to the definition it came
        # from, and dict() alone would leave the nested condition shared.
        "definition": {key: copy.deepcopy(value) for key, value in definition.items()},
    }


def _matching_leaves(
    *, node: object, emitted: Container[str], path: str
) -> list[tuple[str, str]] | None:
    """The (path, event) pairs that satisfy `node`, or None when it does not hold.

    `all` needs every child, so it accumulates; `any` takes the first child that holds and
    stops, which is what makes the recorded branch the choice actually made.

    Examples:
        node    = {"op": "all", "children": [{"event": "a"},
                                             {"op": "any", "children": [{"event": "b"},
                                                                        {"event": "c"}]}]}
        emitted = {"a", "c"}

            -> [("all[0]", "a"), ("all[1].any[1]", "c")]      "b" is absent from both, having
                                                               lost the `any` to "c"
    """
    name = _leaf(node=node)
    if name is not None:
        return [(path, name)] if name in emitted else None
    operator, children = _branch(node=node)
    found: list[tuple[str, str]] = []
    for index, child in enumerate(children):
        segment = f"{operator}[{index}]"
        leaves = _matching_leaves(
            node=child,
            emitted=emitted,
            path=f"{path}.{segment}" if path else segment,
        )
        if operator == ANY_OP:
            if leaves is not None:
                return leaves
        elif leaves is None:
            return None
        else:
            found.extend(leaves)
    # An empty `all` is vacuously satisfied and would trigger on the first emission; the API
    # rejects an empty child list on write rather than the evaluator special-casing it.
    return None if operator == ANY_OP else found


def _leaves(*, node: object) -> list[tuple[str, int | None]]:
    """Every leaf under `node` as (event, expire_seconds), in the order authored.

    The one walk both public readers share, so the events a condition mentions and the
    expiries it authors cannot come to disagree.

    Examples:
        {"op": "any", "children": [{"event": "a", "expire_seconds": 60}, {"event": "b"}]}

            -> [("a", 60), ("b", None)]      duplicates are kept, for the caller to reconcile
    """
    name = _leaf(node=node)
    if name is not None:
        return [(name, _expire_seconds(node=node))]
    _operator, children = _branch(node=node)
    return [leaf for child in children for leaf in _leaves(node=child)]


def _leaf(*, node: object) -> str | None:
    """The event a leaf names, or None when `node` is not a leaf.

    Examples:
        {"event": "orders-ready"}                      -> "orders-ready"
        {"event": "orders-ready", "expire_seconds": 1} -> "orders-ready"
        {"op": "all", "children": [...]}               -> None, a branch
        {"event": 7} / "orders-ready" / None           -> None, and the caller's `_branch`
                                                          then rejects it by name
    """
    if isinstance(node, Mapping):
        name = node.get(EVENT_KEY)
        if isinstance(name, str):
            return name
    return None


def _expire_seconds(*, node: Mapping[str, Any]) -> int | None:
    """A leaf's expiry in seconds, or None when it never expires.

    Examples:
        {"event": "a", "expire_seconds": 86400} -> 86400
        {"event": "a"}                          -> None
        {"event": "a", "expire_seconds": None}  -> None, spelled out rather than omitted
        {"event": "a", "expire_seconds": 0}     -> ValueError, as would -60, "60", 1.5, True

    Raises:
        ValueError: for anything that is not a positive whole number of seconds. `bool` is an
            `int` in Python, so True would otherwise pass as one second.
    """
    expire_seconds = node.get(EXPIRE_SECONDS_KEY)
    if expire_seconds is None:
        return None
    if (
        isinstance(expire_seconds, bool)
        or not isinstance(expire_seconds, int)
        or expire_seconds <= 0
    ):
        raise ValueError(
            f"expire_seconds must be a positive number of seconds: {node!r}"
        )
    return expire_seconds


def _branch(*, node: object) -> tuple[str, list[Any]]:
    """The operator and children of a branch node, rejecting anything else.

    Examples:
        {"op": "all", "children": [{"event": "a"}]} -> ("all", [{"event": "a"}])
        {"op": "any", "children": []}               -> ("any", []), the API rejects an empty
                                                        child list before it reaches here
        {"op": "some", "children": [...]}           -> ValueError, unknown operator
        {"op": "all", "children": {"event": "a"}}   -> ValueError, children must be an array
        {"all": [{"event": "a"}]} / "a" / 7 / None  -> ValueError

    Raises:
        ValueError: naming the offending node, so a malformed condition is traceable to the
            branch it sits in rather than to the whole tree.
    """
    if isinstance(node, Mapping):
        operator = node.get(OP_KEY)
        children = node.get(CHILDREN_KEY)
        # A JSON array; a bare string would otherwise iterate character by character.
        if operator in (ALL_OP, ANY_OP) and isinstance(children, list):
            return str(operator), children
    raise ValueError(f"unknown condition node: {node!r}")
