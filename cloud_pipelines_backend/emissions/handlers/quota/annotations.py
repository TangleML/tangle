"""The quota vocabulary: the group-membership key, the intent, its parser and its serializer.

Two vocabularies share this module, and the file is written in two clearly separated halves
because they behave differently:

- **Half 1, orchestration.** One key, `tangleml.com/orchestration/quota-group`, holding a bare
  group name. Open-ended: the value is created at runtime and cannot be enumerated in advance.
  Read *synchronously* by the admission gate (`quota/interceptor.py`) in the orchestrator
  process, long before any emission exists.
- **Half 2, emissions.** A closed enum of keys whose value is always the fixed token "true",
  shaped exactly like `handlers/readiness/annotations.py`. Read by the producer at commit and
  by the handler at dispatch.

They live together because they describe one feature and the group-name key is the input to
both. The cost is named honestly: the orchestrator now reads its admission input out of a file
under `emissions/handlers/`, which is a locality wart -- the key is an orchestration concept
and its namespace still says so. The trade is one definition of the key instead of two that
can drift.

**The rule that keeps that safe: this module must not import `quota/`.** It owns the
group-name key outright rather than delegating to it. `quota/` has no `__init__.py`, so
`quota.interceptor` importing this module does not execute `quota.promotion`, and the sink
importing `quota.promotion` does not execute `quota.interceptor`. The two arrows never meet.
If this file ever imports `quota.something`, they start to.

"""

import dataclasses
import enum
from typing import Any, Final, Mapping

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.dispatching.handlers import base as handler_base
from cloud_pipelines_backend.emissions import annotations as emission_annotations
from cloud_pipelines_backend.emissions import intents as emission_intents
from cloud_pipelines_backend.utils.annotations import ANNOTATION_ROOT

# ── half 1: the orchestration vocabulary ─────────────────────────────────────────────────
# Path-based namespace, "/" the only separator. The namespace names the subsystem making the
# decision, never the annotated object -- so orchestration, not quota. Its nearest sibling is
# `tangleml.com/orchestration/conditional_execution/is_enabled`, which likewise decides
# whether a node runs at all.
PREFIX: Final[str] = f"{ANNOTATION_ROOT}orchestration/"

# The one key that puts a node in a quota group, and the whole of the user surface. Fixed key,
# open-ended value: unlike an emissions sink, which declares itself by key from a closed enum
# with the fixed value "true", the group name here is created at runtime.
QUOTA_GROUP_KEY: Final[str] = f"{PREFIX}quota-group"


def parse_quota_group(
    *,
    annotations: Mapping[str, Any],
) -> str | None:
    """Read the quota group a node declared.

    Pure: no DB, no logging, no id inference. Whether the named group exists is not a question
    this module can answer -- that is a `quota_group` row lookup, and it belongs to the gate.

    A value that is present but blank is treated as absent rather than as a group named "",
    so a node cannot be gated on a group whose name nothing can ever create.

    The value is a bare group name. Weights and multi-group membership are deliberately
    unrepresentable: a bare string can widen to a string-or-object union later without
    invalidating an annotation already written, whereas multi-group is a different design.

    Args:
        annotations: The node's TaskSpec annotations, or one event's annotation rows. May be
            empty.

    Returns:
        The group name, stripped of surrounding whitespace, or None when the node declared no
        group or declared a blank one.
    """
    value = annotations.get(QUOTA_GROUP_KEY)
    if value is None:
        return None
    return str(value).strip() or None


def parse_task_spec_quota_group(
    *,
    task_spec: Any,
) -> str | None:
    """Read the quota group off a node's task_spec, defending against a malformed one.

    `task_spec` is a plain JSON dict on the ORM model, so nothing guarantees its shape at
    runtime. A task_spec that is missing, not a dict, or carries a non-dict `annotations`
    yields None -- the same answer as a node that declared no group, which is the safe one:
    the node launches ungated rather than the orchestrator raising on the launch path.

    Args:
        task_spec: The node's task_spec, normally a dict. May be None or malformed.

    Returns:
        The declared group name, or None.
    """
    if not isinstance(task_spec, dict):
        return None
    annotations = task_spec.get("annotations")
    if not isinstance(annotations, dict):
        return None
    return parse_quota_group(annotations=annotations)


# ── half 2: the emissions vocabulary ─────────────────────────────────────────────────────
# Not annotation keys themselves. The kind prefix covers every quota emission key; the sink
# prefix narrows it to the sink keys, which is what the dispatcher routes on.
KIND_PREFIX: Final[str] = f"{emission_annotations.PREFIX}quota/"
SINK_PREFIX: Final[str] = f"{KIND_PREFIX}sink/"


class QuotaSinkAnnotation(str, enum.Enum):
    """The sinks a quota emission can be delivered to.

    Unlike readiness, this is **not** a user-facing choice. A node never writes one of these
    keys: quota has exactly one sink, so there is nothing to express and a second required
    annotation could only ever be set to "true" or be a bug. The member exists as the router's
    dispatch key -- the dict key in `QuotaHandler(sinks={...})` -- and is written to the row by
    `to_annotation_pairs` so the fan-out has something to route on.
    """

    # un-parks the oldest waiting nodes in the group the finished node belonged to
    PROMOTE_WAITING_NODES = f"{SINK_PREFIX}promote-waiting-nodes"


class QuotaParseCode(str, enum.Enum):
    """Why a quota intent was not produced. Returned, never logged here, so the caller can
    attach the node or emission_event id."""

    # the node is in no quota group, so there is no slot to free -> no intent
    NO_QUOTA_GROUP = "no_quota_group"


@dataclasses.dataclass(frozen=True, kw_only=True)
class QuotaIntent(emission_intents.EmissionIntent):
    """A node whose completion may free a slot in a quota group.

    Subclasses `EmissionIntent` directly rather than `SingleStatusIntent`, because quota
    declares no status: see `matches`.
    """

    # The group the node declared. **The trigger, not the lookup key.** It answers the
    # producer's only question -- "is this node in a group at all, so is there an event to
    # write?" -- and is then carried for debuggability. The sink never reads it: it resolves
    # the group through the claim row keyed on execution_node_id, so a stale name in an old
    # node's spec cannot misroute a promotion.
    quota_group: str
    # Fixed, not parsed. Membership is the opt-in, so every quota intent declares the one sink.
    sinks: tuple[QuotaSinkAnnotation, ...] = (
        QuotaSinkAnnotation.PROMOTE_WAITING_NODES,
    )

    def matches(
        self,
        *,
        node_status: bts.ContainerExecutionStatus,
    ) -> bool:
        """Fire on any status that means the node ended.

        A freed slot is a node that ended, however it ended -- succeeded, failed, cancelled or
        skipped all release the slot the node was holding. That is a fact about the system, not
        a preference, so it is hardcoded here rather than read off an annotation.

        Reuses upstream's `CONTAINER_STATUSES_ENDED` (`backend_types_sql.py:36`) rather than
        minting a terminal-status set of its own. Readiness already keeps a private
        `_TERMINAL_STATUSES`; a third copy is where drift starts.

        Args:
            node_status: The status the node just changed to.

        Returns:
            True when the node has ended.
        """
        return node_status in bts.CONTAINER_STATUSES_ENDED


def parse_quota(
    *,
    annotations: Mapping[str, Any],
) -> handler_base.ParseResult[QuotaIntent]:
    """Parse quota annotations into an intent, from a node or from an event's rows.

    There is exactly one thing to read and one way to fail, which is why this parser is so
    much shorter than readiness's: the group key is the entire opt-in, so a node that declared
    it gets an intent and a node that did not gets none. No sink key is inspected -- quota has
    one sink and no choice to express.

    A missing group is not an error. It is the overwhelming common case (most nodes are in no
    quota group), so the issue it reports is informational and marked `dropped=True` only in
    the sense that no intent was built.

    Args:
        annotations: A node's TaskSpec annotations, or one emission event's annotation rows.
            TaskSpec values are arbitrary; stored values are strings.

    Returns:
        A ParseResult holding the intent, or None with a NO_QUOTA_GROUP issue when the node is
        in no group.
    """
    quota_group = parse_quota_group(annotations=annotations)
    if quota_group is None:
        return handler_base.ParseResult(
            intent=None,
            issues=[
                handler_base.ParseIssue(
                    code=QuotaParseCode.NO_QUOTA_GROUP,
                    message="node declares no quota group; nothing to promote",
                    dropped=True,
                )
            ],
        )
    return handler_base.ParseResult(intent=QuotaIntent(quota_group=quota_group))


def to_annotation_pairs(
    *,
    intent: QuotaIntent,
) -> list[tuple[str, str]]:
    """Flatten a quota intent to the annotation rows the producer writes.

    Two kinds of pair, for two different readers:

    - the group-name key, so `parse_quota` can rebuild the intent from the row and a human
      reading the row can see which group the node was in;
    - one sink key per declared sink, so the handler's fan-out has something to route on.

    No on-status pair, because a `QuotaIntent` has no `on_status` to serialize. The round trip
    that readiness needs simply does not exist here -- the firing rule is in code.

    Args:
        intent: The quota intent to flatten.

    Returns:
        (key, value) pairs, one per annotation to store.
    """
    return [
        (QUOTA_GROUP_KEY, intent.quota_group),
        *(
            (sink.value, emission_annotations.SINK_DECLARED_VALUE)
            for sink in intent.sinks
        ),
    ]
