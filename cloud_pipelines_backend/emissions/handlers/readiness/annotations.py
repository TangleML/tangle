"""The readiness vocabulary: annotation keys, the intent, its parser and its serializer.

The readiness handler owns everything here — the keys a user puts on a node, the typed intent
that travels from the producer through to the handler, and the validation that builds it.
Parsing is pure: it returns what it found as issues instead of logging, so the caller can attach
the node or emission_event id to the log line.

One parser serves both directions, because `to_annotation_pairs` stores the same keys the node
declared: `parse_readiness` reads a node's TaskSpec on the write side and an event's annotation
rows on the read side.
"""

import dataclasses
import enum
from typing import Any, Final

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.dispatching.handlers import base as handler_base
from cloud_pipelines_backend.emissions import annotations as emission_annotations
from cloud_pipelines_backend.emissions import db_models
from cloud_pipelines_backend.emissions import intents as emission_intents

# The statuses that count as a node having ended. Readiness checks against it to flag an
# on-status that fires before the node finishes, when no output artifact can exist yet.
_TERMINAL_STATUSES: Final[frozenset[bts.ContainerExecutionStatus]] = frozenset(
    bts.CONTAINER_STATUSES_ENDED
)


# Not annotation keys themselves. The kind prefix covers every readiness key and is what
# parse_missing_event_key matches; the sink prefix narrows it to the sink keys, which is what the
# unknown-key sweep reads. What a node declares and what a row stores is always a member value of
# one of the enums below.
KIND_PREFIX: Final[str] = f"{emission_annotations.PREFIX}readiness/"
SINK_PREFIX: Final[str] = f"{KIND_PREFIX}sink/"


# The keys a user sets on a node's TaskSpec to signal readiness. Opt in by setting EVENT.
class ReadinessAnnotation(str, enum.Enum):
    # event_key; required to opt in
    EVENT = f"{KIND_PREFIX}event"
    # optional, default SUCCEEDED
    ON_STATUS = f"{KIND_PREFIX}on-status"


# Declare a sink by setting its key to SINK_DECLARED_VALUE. One member per implemented readiness
# sink, each member's value a complete annotation key, so a key a node can write is a key some
# implementation can serve. `(str, enum.Enum)` so members act as plain strings for dict lookups
# and DB writes.
class ReadinessSinkAnnotation(str, enum.Enum):
    # announces the signal so downstream work can start a pipeline run
    START_PIPELINE_RUN = f"{SINK_PREFIX}start-pipeline-run"


# Why a readiness intent was dropped or kept in a degraded form. Returned, never logged here,
# so the caller can attach the node or emission_event id.
class ReadinessParseCode(str, enum.Enum):
    # opted in but the event key is blank -> intent dropped
    BLANK_EVENT_KEY = "blank_event_key"
    # other readiness keys set with no event key at all -> intent dropped
    NO_EVENT_KEY = "no_event_key"
    # named a sink this build has no implementation for, alongside one it does (kept)
    UNKNOWN_SINK = "unknown_sink"
    # the on-status names no known container status -> intent dropped
    UNKNOWN_ON_STATUS = "unknown_on_status"
    # fires before the node ends, so it carries no outputs (intent kept)
    NON_TERMINAL_ON_STATUS = "non_terminal_on_status"
    # the node ended, but not as a success, so it carries no outputs (intent kept)
    NON_SUCCEEDED_ON_STATUS = "non_succeeded_on_status"


# A validated readiness opt-in. It says "this named thing is ready" so downstream work can
# react to it.
@dataclasses.dataclass(frozen=True, kw_only=True)
class ReadinessIntent(emission_intents.SingleStatusIntent):
    # The name downstream triggers match on (from the readiness event annotation).
    event_key: str
    # Where the signal is announced. Never empty: a node that names nowhere is defaulted to
    # START_PIPELINE_RUN, so "announce this, I do not care where" is writable. Held in
    # ReadinessSinkAnnotation declaration order rather than the order the node wrote them, so
    # the stored rows and the delivery rows keyed on them are stable.
    sinks: tuple[ReadinessSinkAnnotation, ...]
    # `on_status` is inherited from SingleStatusIntent: readiness fires on exactly the
    # status the node declared, defaulting to SUCCEEDED. Output artifacts only exist for
    # SUCCEEDED, which is why the parser flags any other choice.


def _validate(
    *,
    raw_event_key: Any,
    raw_on_status: Any,
    sinks: tuple[ReadinessSinkAnnotation, ...],
    unknown_sink_keys: tuple[str, ...],
    sink_issues: list[handler_base.ParseIssue],
) -> handler_base.ParseResult[ReadinessIntent]:
    """Validate one node's or row's readiness values into a readiness intent.

    The event key is required, so a blank one drops the whole intent; an unrecognized status
    is unusable and drops it too. The sink set never drops anything: a node that declared none
    was defaulted to START_PIPELINE_RUN upstream in parse_sinks. A valid intent that fires on a status carrying no output artifacts is kept and
    flagged instead of dropped, as is one that named a sink nothing can serve alongside one it
    can.

    Args:
        raw_event_key: The raw readiness event key. Already known to be present.
        raw_on_status: The raw on-status value, or None when it was not set.
        sinks: The resolved sinks, in declaration order; never empty.
        unknown_sink_keys: Declared sink keys matching no implemented sink.
        sink_issues: What the sink sweep found, in readiness's own codes.

    Returns:
        A ParseResult holding the intent, or None with a dropping issue when the values cannot
        make one. A returned intent may still carry an informational issue.
    """
    # Step 1: the event key is required. Present-but-blank is a mistake, so drop the whole
    # intent and report why.
    event_key = emission_annotations.clean_str(value=raw_event_key)
    if event_key is None:
        return handler_base.ParseResult(
            intent=None,
            issues=[
                handler_base.ParseIssue(
                    code=ReadinessParseCode.BLANK_EVENT_KEY,
                    message="readiness event key is present but blank; dropping the readiness intent",
                    dropped=True,
                )
            ],
        )

    # Step 2: resolve the on-status. An absent value falls back to SUCCEEDED; an unrecognized
    # value is unusable, so drop the intent and report it.
    raw_status = emission_annotations.clean_str(value=raw_on_status)
    on_status = emission_annotations.parse_on_status(raw=raw_status)
    if on_status is None:
        return handler_base.ParseResult(
            intent=None,
            issues=[
                handler_base.ParseIssue(
                    code=ReadinessParseCode.UNKNOWN_ON_STATUS,
                    message=f"readiness on-status {raw_status!r} is not a ContainerExecutionStatus; dropping the intent",
                    dropped=True,
                )
            ],
        )

    # Step 3: the intent is valid and will be kept. Flag (but do not drop) an on-status that
    # has no output artifacts, since only a succeeded node produces outputs, and carry along
    # any sink key that reached no implementation.
    issues: list[handler_base.ParseIssue] = [*sink_issues]
    if on_status not in _TERMINAL_STATUSES:
        # Fires mid-run, before the node has ended, so there are definitely no outputs yet.
        issues.append(
            handler_base.ParseIssue(
                code=ReadinessParseCode.NON_TERMINAL_ON_STATUS,
                message=(
                    f"readiness on-status {on_status.value} is not terminal; it fires on that "
                    "transition, but output artifacts are only present for SUCCEEDED"
                ),
                dropped=False,
            )
        )
    elif on_status is not bts.ContainerExecutionStatus.SUCCEEDED:
        # The node ended, but as a failure/cancel/skip rather than a success, so again no
        # outputs.
        issues.append(
            handler_base.ParseIssue(
                code=ReadinessParseCode.NON_SUCCEEDED_ON_STATUS,
                message=(
                    f"readiness on-status {on_status.value} is terminal but not SUCCEEDED; "
                    "output artifacts are only produced for SUCCEEDED"
                ),
                dropped=False,
            )
        )

    # Step 4: build the kept intent, carrying along any informational issue from step 3.
    return handler_base.ParseResult(
        intent=ReadinessIntent(event_key=event_key, sinks=sinks, on_status=on_status),
        issues=issues,
        unknown_sink_keys=unknown_sink_keys,
    )


def parse_readiness(
    *,
    annotations: dict[str, Any],
) -> handler_base.ParseResult[ReadinessIntent]:
    """Parse and validate readiness annotations, from a node or from an event's rows.

    Both sides carry the same keys, so both get the same validation. Re-validating on the read
    side still matters: a hand-written or stale row must not reach the handler as a malformed
    intent. A node or row that names a readiness key without the event key is reported rather
    than read as opting out.

    Args:
        annotations: A node's TaskSpec annotations, or one emission event's annotation rows.
            TaskSpec values are arbitrary; stored values are strings.

    Returns:
        A ParseResult whose intent is None when readiness was not opted into or was dropped as
        invalid, and otherwise the validated intent plus any informational issues.
    """
    # Readiness is opt-in, so a node that named no readiness key at all is not signalling
    # readiness, which is no error and produces no issue. A node that named one and left out the
    # event key is reported instead.
    if annotations.get(ReadinessAnnotation.EVENT) is None:
        return handler_base.ParseResult(
            intent=None,
            issues=emission_annotations.parse_missing_event_key(
                annotations=annotations,
                kind_prefix=KIND_PREFIX,
                kind=db_models.EmissionType.READINESS,
                event_key=ReadinessAnnotation.EVENT.value,
                no_event_key_code=ReadinessParseCode.NO_EVENT_KEY,
            ),
        )

    sinks, unknown_sink_keys, sink_issues = emission_annotations.parse_sinks(
        annotations=annotations,
        prefix=SINK_PREFIX,
        sink_annotation=ReadinessSinkAnnotation,
        kind=db_models.EmissionType.READINESS,
        default_sink=ReadinessSinkAnnotation.START_PIPELINE_RUN,
        unknown_sink_code=ReadinessParseCode.UNKNOWN_SINK,
    )
    return _validate(
        raw_event_key=annotations.get(ReadinessAnnotation.EVENT),
        raw_on_status=annotations.get(ReadinessAnnotation.ON_STATUS),
        sinks=sinks,
        unknown_sink_keys=unknown_sink_keys,
        sink_issues=sink_issues,
    )


def to_annotation_pairs(
    *,
    intent: ReadinessIntent,
) -> list[tuple[str, str]]:
    """Flatten a readiness intent to the annotation rows the producer writes.

    The keys are the same ReadinessAnnotation and ReadinessSinkAnnotation members the node
    declared, so parsing the rows back runs the same code path as parsing the node. The sink
    rows are a verbatim mirror of the node's, one per declared sink, carrying the canonical
    token rather than whatever spelling the node used.

    Args:
        intent: The validated readiness intent.

    Returns:
        (key, value) pairs, one per annotation to store.
    """
    return [
        (ReadinessAnnotation.EVENT.value, intent.event_key),
        (ReadinessAnnotation.ON_STATUS.value, intent.on_status.value),
        *(
            (sink.value, emission_annotations.SINK_DECLARED_VALUE)
            for sink in intent.sinks
        ),
    ]
