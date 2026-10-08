"""The emission producer: turns a node's status change into durable emission rows.

The rows are written in the same transaction that finalizes the node, so they commit or
roll back atomically with it.
"""

import dataclasses
import logging
import weakref
from collections.abc import Callable, Sequence
from typing import Any, Final

import sqlalchemy as sql
from sqlalchemy import inspect as sa_inspect
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.dispatching.handlers import base as handler_base
from cloud_pipelines_backend.emissions import db_models, intents as emission_intents
from cloud_pipelines_backend.emissions.handlers.quota import (
    annotations as quota_annotations,
)
from cloud_pipelines_backend.emissions.handlers.readiness import (
    annotations as readiness_annotations,
)
from cloud_pipelines_backend.emissions.observability import producer_observer

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True, kw_only=True)
class EmissionKindRegistration:
    """An annotation codec supplied by a built-in kind or an application extension."""

    emission_type: db_models.EmissionType
    parser: Callable[..., handler_base.ParseResult[emission_intents.EmissionIntent]]
    serializer: Callable[..., list[tuple[str, str]]]
    ignored_parse_codes: frozenset[object] = frozenset()


def builtin_kinds() -> tuple[EmissionKindRegistration, ...]:
    """The generic readiness and quota codecs, in their established delivery order."""
    return (
        EmissionKindRegistration(
            emission_type=db_models.EmissionType.READINESS,
            parser=readiness_annotations.parse_readiness,
            serializer=readiness_annotations.to_annotation_pairs,
        ),
        EmissionKindRegistration(
            emission_type=db_models.EmissionType.QUOTA,
            parser=quota_annotations.parse_quota,
            serializer=quota_annotations.to_annotation_pairs,
            ignored_parse_codes=frozenset(
                {quota_annotations.QuotaParseCode.NO_QUOTA_GROUP}
            ),
        ),
    )


def configure_kinds(*, registrations: Sequence[EmissionKindRegistration]) -> None:
    """Replace the process's codecs before installing listeners or processing transitions.

    Applications may supply their own kinds and order alongside `builtin_kinds()`.
    Registration changes no persisted annotation keys, type names or delivery data.
    """
    kinds = tuple(registrations)
    if len({kind.emission_type for kind in kinds}) != len(kinds):
        raise ValueError("duplicate emission kind registration")
    global _KINDS, _SERIALIZERS, _OPT_OUT_PARSE_CODES
    _KINDS = kinds
    _SERIALIZERS = {kind.emission_type: kind.serializer for kind in kinds}
    _OPT_OUT_PARSE_CODES = frozenset(
        code for kind in kinds for code in kind.ignored_parse_codes
    )


configure_kinds(registrations=builtin_kinds())

# Status transitions recorded at the moment of assignment, drained at commit.
#
# The transition cannot be read back off the ORM at commit time: attribute history is a
# diff against the committed state, so any flush empties it — including the flush that
# begin_nested() issues for this producer's own savepoint. Reading history would let the
# first node's emission hide every other node's transition in the same commit.
#
# Keyed by InstanceState, not by the node: ExecutionNode is a MappedAsDataclass, so Python
# generates __eq__ and sets __hash__ to None, and an unhashable object cannot be a dict
# key. sa_inspect(node) returns that node's InstanceState — stable for the life of the
# instance, hashable by identity, and what SQLAlchemy's own weak collections key on.
#
# WeakKeyDictionary: the node holds its state strongly and the identity map releases the
# state once the node is collected, so an entry lives exactly as long as the change it
# describes and needs no cleanup pass.
#
# Written by `record_status_assignment` and drained by `maybe_emit_node_status_change`; both
# listeners are registered at the bottom of this module by `install_listeners`.
_PENDING_TRANSITIONS: weakref.WeakKeyDictionary[
    orm.InstanceState, bts.ContainerExecutionStatus
] = weakref.WeakKeyDictionary()

# One commit can transition an entire downstream subgraph: a skip cascade recurses over
# every descendant without committing, so the id list has no natural bound while a bind
# parameter list does. 1000 sits far below every engine's ceiling (SQLite allows 32766
# here; MySQL is bounded by max_allowed_packet) and far above the realistic commit size,
# so in practice the loop below runs once. At this size the parameter payload is ~22 KB
# for 20-character ids, where a chunk near the ceiling would be ~368 KB for no benefit.
_RUN_ID_LOOKUP_CHUNK: Final[int] = 1000


def _pending_transition_count() -> int:
    """How many recorded transitions are waiting to be drained.

    The only safe read of the ledger from another thread. `len()` on a WeakKeyDictionary does
    not iterate it, so the metrics thread cannot trip over an entry the emitting thread is
    adding, removing, or having collected underneath it. Anything that walks the mapping —
    listing nodes, ageing entries — is not safe here and is deliberately absent.

    Exists only to be observed through, which is why it is bound into `telemetry` below rather
    than offered as part of this module's own surface.

    Returns:
        The ledger's current size.
    """
    return len(_PENDING_TRANSITIONS)


# Everything this module reports rather than emits, behind one name. `telemetry` is the whole
# of the producer's observability: grep it and you have found every line that measures rather
# than emits, and nothing else in this file has to mention a counter, a span or a gauge.
#
# The ledger read is bound here, at the one place that owns the ledger, so the gauge can be
# opted into later — `telemetry.observe_pending_transitions()` — without the caller needing to
# know what it reads or how.
telemetry: Final[producer_observer.ProducerTelemetry] = (
    producer_observer.ProducerTelemetry(pending_transitions=_pending_transition_count)
)


def _resolve_pipeline_run_ids(
    *,
    session: orm.Session,
    execution_node_ids: Sequence[str],
    chunk_size: int = _RUN_ID_LOOKUP_CHUNK,
) -> dict[str, str]:
    """Map each node id to the id of the PipelineRun that owns it, two queries per chunk.

    ExecutionNode has no pipeline_run_id FK. A node that IS a run's root resolves
    directly; the rest resolve through the ancestor link. Nodes owned by no run are
    absent from the result, which the caller reads as None.

    Args:
        session: the active session.
        execution_node_ids: the nodes to resolve. Duplicates are collapsed, and order is
            kept so the generated parameter lists stay deterministic.
        chunk_size: ids per query pair.

    Returns:
        A mapping of node id to owning run id, holding only the nodes a run owns.
    """
    unique_ids = list(dict.fromkeys(execution_node_ids))
    resolved: dict[str, str] = {}
    for start_at in range(0, len(unique_ids), chunk_size):
        batch = unique_ids[start_at : start_at + chunk_size]
        # A run points straight at its root execution, so a node that IS a run's root
        # resolves without touching the ancestor link.
        roots = {
            root_id: run_id
            for root_id, run_id in session.execute(
                sql.select(bts.PipelineRun.root_execution_id, bts.PipelineRun.id).where(
                    bts.PipelineRun.root_execution_id.in_(batch)
                )
            )
        }
        resolved.update(roots)
        descendants = [node_id for node_id in batch if node_id not in roots]
        if not descendants:
            continue
        # The rest are descendants: follow the ancestor link back to the run whose root
        # is the node's ancestor.
        resolved.update(
            {
                execution_id: run_id
                for execution_id, run_id in session.execute(
                    sql.select(
                        bts.ExecutionToAncestorExecutionLink.execution_id,
                        bts.PipelineRun.id,
                    )
                    .join(
                        bts.PipelineRun,
                        bts.ExecutionToAncestorExecutionLink.ancestor_execution_id
                        == bts.PipelineRun.root_execution_id,
                    )
                    .where(
                        bts.ExecutionToAncestorExecutionLink.execution_id.in_(
                            descendants
                        )
                    )
                )
            }
        )
    return resolved


def _drop_rows_with_oversized_values(
    *,
    rows: list[tuple[db_models.EmissionType, list[tuple[str, str]]]],
    execution_node_id: str,
) -> list[tuple[db_models.EmissionType, list[tuple[str, str]]]]:
    """Drop the rows whose annotation value exceeds MAX_ANNOTATION_VALUE_BYTES.

    An emission is all-or-nothing: one over-long value drops its whole (emission_type,
    annotation_pairs) entry, so no emission_event row and no annotation rows. Its sink rows go
    with it, which is what makes the drop total — nothing downstream ever names a delivery for
    this emission, so no delivery is attempted and no outcome row exists. Filtering here rather
    than letting MySQL raise DataError inside the insert loop is what keeps the loss to that one
    emission — the node's others are still written.

    Args:
        rows: the (emission_type, annotation_pairs) entries built for this status change.
        execution_node_id: the node the rows belong to, for the log line.

    Returns:
        The entries whose every value fits, in the order given.
    """
    kept: list[tuple[db_models.EmissionType, list[tuple[str, str]]]] = []
    for emission_type, annotation_pairs in rows:
        # Encoded length, since the column bounds bytes: one character costs up to 4 of them.
        oversized = [
            (key, len(value.encode("utf-8")))
            for key, value in annotation_pairs
            if len(value.encode("utf-8")) > db_models.MAX_ANNOTATION_VALUE_BYTES
        ]
        if not oversized:
            kept.append((emission_type, annotation_pairs))
            continue
        offenders = ", ".join(f"{key}={byte_length}B" for key, byte_length in oversized)
        logger.warning(
            f"Emission value too long node={execution_node_id} "
            f"type={emission_type.value} "
            f"max={db_models.MAX_ANNOTATION_VALUE_BYTES}B {offenders} (dropped)"
        )
    return kept


def _build_rows_for_matching_intents(
    *,
    intents: list[tuple[db_models.EmissionType, Any | None]],
    node_status: bts.ContainerExecutionStatus,
) -> list[tuple[db_models.EmissionType, list[tuple[str, str]]]]:
    """Serialize each configured intent whose predicate matches this transition."""
    out: list[tuple[db_models.EmissionType, list[tuple[str, str]]]] = []
    for emission_type, intent in intents:
        # The intent owns its own firing rule, so the producer never learns any kind's
        # field names: readiness and metadata compare one declared status, quota answers
        # "did the node end?". See emissions/intents.py.
        if intent is None or not intent.matches(node_status=node_status):
            continue
        # Each intent is flattened by the handler that owns its keys, so the rows come back
        # keyed exactly as the node declared them.
        out.append((emission_type, _SERIALIZERS[emission_type](intent=intent)))
    return out


def _handle_node_status_change(
    *,
    session: orm.Session,
    execution_node: bts.ExecutionNode,
    node_status: bts.ContainerExecutionStatus,
    pipeline_run_id: str | None,
) -> None:
    """Write emission_event rows for a node that just changed status.

    Parses the node's annotations and writes one emission_event row (plus its annotation
    rows) per surviving intent. A row is created only when all of the following hold:

    - the node carries an emission annotation that parses into an intent — a node that opts in
      and names no sink key is defaulted to its handler's sink rather than dropped;
    - that intent fires for `node_status`, the status the node just changed to: readiness and
      metadata compare one declared status, notification matches any of several, and quota
      answers "did the node end?";
    - none of the intent's values exceeds db_models.MAX_ANNOTATION_VALUE_BYTES — one that does
      drops that whole emission, leaving the node's other emissions to be written;
    - no row with the same natural key (execution_node, emission_type,
      container_execution_status) already exists — a duplicate is skipped rather than inserted.
      A notification node declaring several statuses therefore writes one row per status it
      reaches, not one row ever.

    Each row is inserted inside its own SAVEPOINT (`session.begin_nested()`). The SAVEPOINT
    scopes the insert so that if it violates the unique dedupe index, only that nested
    insert is rolled back: the node's own status UPDATE and every other emission row in this
    transaction stay intact, and the outer commit still succeeds. Without it, a single
    duplicate insert would poison the whole transaction and take the node's status change
    down with it. Writes nothing when no intent matches, and never raises into the caller's
    commit.

    `begin_nested()` flushes the whole session before it opens the SAVEPOINT, which is what
    keeps the node's UPDATE outside the savepoint and therefore safe from its rollback. That
    flush is also session-wide, so nothing in this call chain may read a node's attribute
    history afterwards: the caller records transitions before any emission work for exactly
    that reason.

    Args:
        session: the node's own transaction; rows commit atomically with the node.
        execution_node: the node whose status changed.
        node_status: the status it changed to (any status, not only terminal ones).
        pipeline_run_id: the id of the run that owns the node, or None when no run does.
            Resolved for the whole commit at once by the caller, so this function issues
            no query of its own.

    Returns:
        None. Side effect only: zero or more emission_event rows (plus their
        emission_event_annotation rows) are added to the session.
    """
    # Built before the try so every row can record how long the producer took to prepare it,
    # measured from the moment this node's emission work began.
    observer = telemetry.row_observer()
    try:
        # Step 1: read the node's annotations and parse them into intents. task_spec is a
        # plain dict on the ORM model; guard defensively in case it is missing or malformed.
        task_spec = getattr(execution_node, "task_spec", None)
        node_annotations = (
            task_spec.get("annotations") if isinstance(task_spec, dict) else None
        ) or {}
        # Each kind of emission parses the node with its own handler's parser, the same one
        # that rebuilds the intent from the row later, so the two directions cannot drift.
        parsed = [
            (kind.emission_type, kind.parser(annotations=node_annotations))
            for kind in _KINDS
        ]

        # Step 2: log any parse issues here, where the node id is known (the emission_event
        # row does not exist yet, and the parsers themselves are pure and never log). All
        # issues for the node go in one warning, one issue per line, so a single node maps to
        # a single log entry. The consumer logs each handler's own parse issues later, once it
        # has the row id.
        # Only the opt-out codes are dropped, not whole kinds: a mistyped signal or an
        # unusable status list writes no row, so the consumer never sees it and this is the
        # one place it can be named.
        issues = [
            issue
            for _, result in parsed
            for issue in result.issues
            if issue.code not in _OPT_OUT_PARSE_CODES
        ]
        if issues:
            issue_summary = "\n".join(
                f"  [{issue.code}] dropped={issue.dropped}: {issue.message}"
                for issue in issues
            )
            logger.warning(
                f"Emission parse issues node={execution_node.id}:\n{issue_summary}"
            )

        # Step 3: build the rows for the intents that match this status change. An empty
        # list means nothing to emit, so return before touching the session at all — most
        # status changes have no annotation subscribing to them.
        rows = _build_rows_for_matching_intents(
            intents=[(kind, result.intent) for kind, result in parsed],
            node_status=node_status,
        )
        if not rows:
            return

        # Step 4: drop any emission carrying a value the column cannot hold. A node whose
        # every emission is oversized therefore opens no savepoint and writes no row.
        rows = _drop_rows_with_oversized_values(
            rows=rows, execution_node_id=execution_node.id
        )
        if not rows:
            return

        # Step 5: insert each row under its own SAVEPOINT. Dedupe is enforced by the unique
        # index (ix_emission_event_node_id_type_status): a duplicate INSERT is
        # rejected with an IntegrityError, and begin_nested rolls back to the savepoint
        # only, leaving the node's UPDATE (and the outer commit) intact — so a rejected
        # duplicate never poisons the node's transaction.
        for emission_type, annotation_pairs in rows:
            try:
                # begin_nested opens the SAVEPOINT wrapping this single row's insert.
                with session.begin_nested():
                    event = db_models.EmissionEvent(
                        execution_node_id=execution_node.id,
                        container_execution_id=execution_node.container_execution_id,
                        pipeline_run_id=pipeline_run_id,
                        container_execution_status=node_status.value,
                        # The column holds the EmissionType `.value` string, not the member.
                        emission_type=emission_type.value,
                        extra_data=observer.timings_for_row(),
                    )
                    session.add(event)
                    # Flush inside the savepoint to assign event.id for the annotation FK.
                    session.flush()
                    for key, value in annotation_pairs:
                        session.add(
                            db_models.EmissionEventAnnotation(
                                emission_event_id=event.id,
                                key=key,
                                value=value,
                            )
                        )
                # Reported after the savepoint closed, so a row rejected as a duplicate below
                # does not show up as an emission this producer wrote.
                observer.row_written(emission_type=emission_type.value)
            except sql.exc.IntegrityError:
                # Lost the dedupe race — the row already exists. The node write is
                # unaffected because the savepoint rolled back only this emission.
                logger.warning(
                    f"Emission dedupe race node={execution_node.id} "
                    f"type={emission_type.value} (skipped)"
                )
    except Exception:
        # Emission is a side effect; it must never break the node's commit. Swallow any
        # error and log it with as much node identity as is available.
        logger.exception(
            f"Emission failed for node={getattr(execution_node, 'id', '?')} (swallowed)"
        )


def maybe_emit_node_status_change(
    *,
    session: orm.Session,
) -> None:
    """Write emission rows for every node whose status changed in this transaction.

    Wire this as a `before_commit` listener on the sessionmaker, so the rows join the same
    transaction that finalizes the node and a SUCCEEDED node already has its outputs.

    Why the transition is read from a recorded value rather than from the ORM::

        node.container_execution_status = SUCCEEDED
              │
              ├─ "set" event ──► _PENDING_TRANSITIONS[state] = SUCCEEDED   a plain dict
              │                                                           entry; no flush
              │                                                           can touch it
        (any flush may happen here: a query, a savepoint, another listener)
              │
              ▼
        before_commit  ──►  for each node in session.new + session.dirty:
                              recorded = _PENDING_TRANSITIONS.pop(sa_inspect(node))
                              if recorded != node.container_execution_status:
                                  skip          # a rollback reverted the assignment
                              SAVEPOINT ─ INSERT rows ─ RELEASE

    Attribute history cannot serve here. History is a diff against the committed state, so
    any flush empties it, and this producer's own savepoint flushes the session: reading
    history would mean writing one node's rows hides every other node's transition in the
    same commit.

    The map is keyed by `sa_inspect(node)` because ExecutionNode is a MappedAsDataclass and
    therefore unhashable; its InstanceState is hashable, stable, and dies with the node.

    Three properties keep a recorded transition honest, and none needs a rollback listener:

    - `pop` drains it, so one assignment emits at most once.
    - The value check rejects a record whose assignment was rolled back, since the rollback
      restored the attribute and the two no longer agree.
    - The map is a WeakKeyDictionary and the session holds a strong reference to any object
      with pending changes, so an entry lives exactly as long as the change it describes.

    Args:
        session: the session about to commit. Rows added here commit atomically with the
            node status changes that triggered them.

    Returns:
        None. Side effect only: emission rows are added to the committing session.
    """
    # session.new as well as session.dirty: a node inserted and given a status in the same
    # transaction has transitioned too.
    transitions: list[bts.ExecutionNode] = []
    for obj in list(session.new) + list(session.dirty):
        if not isinstance(obj, bts.ExecutionNode):
            continue
        recorded_status = _PENDING_TRANSITIONS.pop(sa_inspect(obj), None)
        if recorded_status is None:
            # The common, healthy case: the node is dirty for some other column.
            continue
        # A rollback restores the attribute and leaves the record behind, so a record that
        # no longer agrees with the attribute describes an assignment that was reverted.
        if recorded_status != obj.container_execution_status:
            telemetry.transition_rejected_stale()
            continue
        transitions.append(obj)

    # SQLAlchemy fires before_commit again as each savepoint is released, so this runs
    # several times per commit; every run after the first finds the transitions already
    # drained. Returning early keeps those re-entries free of work.
    if not transitions:
        return

    telemetry.transitions_drained(count=len(transitions))
    run_ids = _resolve_pipeline_run_ids(
        session=session, execution_node_ids=[node.id for node in transitions]
    )
    for execution_node in transitions:
        # One span per node, covering everything that node's status change emits — the ids
        # of the rows themselves do not exist until the inserts below assign them.
        with telemetry.node_span(execution_node_id=execution_node.id):
            _handle_node_status_change(
                session=session,
                execution_node=execution_node,
                # The check above proved the recorded transition and the attribute agree,
                # so the attribute is the status to emit.
                node_status=execution_node.container_execution_status,
                pipeline_run_id=run_ids.get(execution_node.id),
            )


def install_listeners(*, session_factory: orm.sessionmaker) -> None:
    """Register every listener the producer needs, for one session factory.

    Both registrations live here so there is one place to read them and one call to make:
    wiring only half of this is silent, because a session that commits without recorded
    transitions emits nothing and reports no error.

    Safe to call for every session factory. The `set` registration is on the mapped
    attribute and therefore process-wide; repeating it is a no-op.

    Args:
        session_factory: the sessionmaker whose commits should emit.

    Returns:
        None. Side effect only: `record_status_assignment` fires on assignment, and
        `maybe_emit_node_status_change` runs in each of that factory's commits.
    """
    # Both registrations are unconditional: `event.listen` ignores a repeat registration of
    # the same function for the same target, so calling this once per session factory adds
    # the attribute listener once and that factory's commit listener once. Guarding with
    # `event.contains` was tried and is worse than useless — it reports a brand-new
    # sessionmaker as already registered, because the registry keys targets by identity and
    # CPython reuses the address of a collected one, which leaves that factory committing
    # silently without emitting.
    sql.event.listen(
        bts.ExecutionNode.container_execution_status,
        "set",
        record_status_assignment,
    )
    sql.event.listen(session_factory, "before_commit", _on_before_commit)


def record_status_assignment(
    execution: bts.ExecutionNode,
    value: bts.ContainerExecutionStatus | None,
    _old_value: object,
    _initiator: object,
) -> None:
    """Record a status assignment for the commit that will carry it.

    Registered by `install_listeners` on the `set` event, so it fires the instant the
    attribute is assigned — long before anything reaches the database, which is what makes
    the record immune to the flushes that follow. SQLAlchemy calls it positionally, which is
    why this signature has no keyword-only marker.

    Args:
        execution: the node being assigned to.
        value: the status it is being set to; None is not a transition.
        _old_value: the previous value, unused — a repeat assignment is still a
            transition, and the drain compares against the attribute anyway.
        _initiator: SQLAlchemy's event token, unused.

    Returns:
        None. Side effect only: the transition is recorded for the drain at commit.
    """
    if value is not None:
        _PENDING_TRANSITIONS[sa_inspect(execution)] = value
        telemetry.transition_recorded()


def _on_before_commit(session: orm.Session) -> None:
    """Drain the recorded transitions into this commit, never raising into it.

    Registered by `install_listeners` on `before_commit`, so SQLAlchemy calls it
    positionally. Emission is a side effect: a failure here must not take the node's status
    change down with it, so everything is caught and logged.

    Args:
        session: the session about to commit.

    Returns:
        None. Side effect only: emission rows are added to the committing session.
    """
    try:
        maybe_emit_node_status_change(session=session)
    except Exception:
        logger.exception("Emission producer before_commit hook failed (swallowed)")
