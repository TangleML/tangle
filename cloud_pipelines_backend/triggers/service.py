"""Trigger writes: sync a subscription's events, then trigger it if the condition holds.

Everything here runs inside the caller's transaction and commits nothing, with one exception
named here because the rule is worth nothing if the exception is a surprise:
`record_event_and_maybe_start_runs` owns its transactions and commits several of them, each
one able to start a pipeline run. Every other function in this module leaves the transaction
to its caller -- a route wraps the call in `session.begin()`, so a raise anywhere leaves
neither a half-synced event set nor a run started against a condition that was never stored.

The fan-out cannot be given that guarantee: one transaction for the whole batch would throw
away one subscription's committed arrival because a later subscription failed. Its docstring
says what it commits and when; `_subscription_transaction` is where the commits happen.
"""

import contextlib
import dataclasses
import datetime
import enum
import functools
import logging
import time
from collections.abc import Iterator, Mapping
from typing import Any, Final

import pydantic
import sqlalchemy as sql
from sqlalchemy import exc as sql_exc
from sqlalchemy import orm

from cloud_pipelines_backend.templating.arguments import (
    annotations as template_annotations,
)
from cloud_pipelines_backend.templating.arguments import rendering, sources
from cloud_pipelines_backend.templating.arguments.observability import render_observer
from cloud_pipelines_backend.triggers import db_models, evaluation, event_state
from cloud_pipelines_backend.triggers.observability import metrics as trigger_metrics
from cloud_pipelines_backend.user_pipelines import errors as user_pipeline_errors
from cloud_pipelines_backend.user_pipelines import services as user_pipeline_services
from cloud_pipelines_backend.utils import db as db_utils
from cloud_pipelines_backend.utils import pipeline_templates

logger = logging.getLogger(__name__)


class TriggerReason(str, enum.Enum):
    """Why a call did or did not start a run, answered once per subscription.

    One enum rather than loose constants so the set is closed: `RUN_NOT_STARTED_REASONS` below
    divides it, and a member belonging to neither half is now something a reader sees here
    rather than something discovered when a subscription that never ran is reported a success.

    A `(str, enum.Enum)` mixin, the spelling used everywhere else here, because the OSS backend
    this ships against still supports Python 3.10 and `enum.StrEnum` is 3.11+.

    `__str__` is restored to the string's own because these values are a wire format -- persisted
    verbatim in an emission's outcome detail, asserted by name in the end-to-end results, and
    interpolated into log lines. Python 3.11 changed a mixin enum's `__str__` and `__format__`
    to render `TriggerReason.NO_SUBSCRIPTION`, which would write a second spelling into records
    that already hold `no_subscription`. The one-line override keeps `f"{reason}"` honest on
    every version.

    Not the same vocabulary as the reason a whole *delivery* reports -- `fan_out_incomplete`,
    `runs_not_started`, in the readiness sink. Those answer for one emission through one sink,
    and these nest inside them: one delivery reason, and one of these per subscription under it.
    """

    __str__ = str.__str__

    # The ordinary answer: the condition is still short of an event.
    AWAITING_EVENTS = "awaiting_events"
    # A run for this cycle exists — started by an arriving emission a moment earlier.
    CYCLE_ALREADY_TRIGGERED = "cycle_already_triggered"
    # The condition held, but the pipeline it points at is gone — soft-deleted, or a pinned
    # version that no longer exists. Permanent, not retryable: nothing changes until someone
    # repoints the subscription at a live pipeline, which re-evaluates and can still start the
    # run.
    USER_PIPELINE_DELETED = "user_pipeline_deleted"
    # The pipeline row is alive, but what is stored on it no longer builds into a runnable task
    # spec -- a spec written under an older schema, a row written straight into the database, a
    # version predating the CRUD routes. Permanent for the same reason as the above: every
    # retry reads the same JSON back. Recovery is the pipeline's owner re-saving it against the
    # current schema, or the subscription being repointed at one that does build.
    TARGET_UNBUILDABLE = "target_unbuildable"
    # The backstop, and the only member here that names no specific cause: the run build raised
    # something we did not anticipate. Treated as permanent because we cannot show it is not,
    # and read as a bug until someone proves otherwise -- the log carries the exception type
    # and its traceback, and the counter carries the reason.
    RUN_START_FAILED = "run_start_failed"
    SUBSCRIPTION_DISABLED = "subscription_disabled"
    NO_SUBSCRIPTION = "no_subscription"
    ARRIVAL_ALREADY_RECORDED = "arrival_already_recorded"
    # The other half of the redelivery answer, and the honest one: this emission was seen
    # before AND the delivery that saw it started a run. Distinct from the member above because
    # the two describe opposite facts about the same refusal -- `arrival_already_recorded`
    # means the arrival was banked and nothing fired, this means a run is out there carrying
    # this emission's id. Reported with `triggered=True` and the run's id, so the outcome
    # ledger stops denying a run that exists.
    RUN_ALREADY_STARTED = "run_already_started"
    # A *different*, older emission arrived after the event state had moved on, and was dropped
    # by the monotonic guard in `event_state.fill`. Not the same operational event as
    # `arrival_already_recorded`, which is this emission coming round twice: this one is a
    # signal that lost a race and will never be acted on, so a run someone expected from it is
    # not coming. Both are ordinary, neither is a failure -- but only one of them is worth
    # asking "why is that producer running late?" about.
    ARRIVAL_SUPERSEDED = "arrival_superseded"


# The reasons that mean "the arrival is committed and no run exists". Every one of them is
# permanent: the emission settles and is never redelivered, so nothing recovers on its own.
#
# Named as a set rather than spelled out as three comparisons because two separate classifiers
# split `failed` out of `outcomes` -- the fan-out below and the sink's retry merge -- and a
# reason added to one but not the other reports SUCCESS for a subscription that never ran.
RUN_NOT_STARTED_REASONS: Final[frozenset[TriggerReason]] = frozenset(
    {
        TriggerReason.USER_PIPELINE_DELETED,
        TriggerReason.TARGET_UNBUILDABLE,
        TriggerReason.RUN_START_FAILED,
    }
)

# How much of an exception's own message is carried back to the caller in `TriggerResult.error`.
# A pydantic ValidationError renders every failing field, which for a large task spec is
# kilobytes -- and this string is persisted verbatim in the emission's outcome detail. The full
# text is in the log; this is the part that has to fit in a record someone reads later.
_ERROR_DETAIL_LIMIT: Final[int] = 500

# How many of a subscription's past cycles `_run_started_by` reads, looking for the trigger a
# redelivered emission caused.
#
# The scan is bounded because the lookup has no index to seek on: `triggered_by` is JSON, and
# JSON containment is spelled three different ways across the engines this runs on. What it
# does have is the fence's UNIQUE (subscription_id, cycle), whose leading column makes
# "this subscription's rows, newest cycle first" an indexed prefix seek -- so the cost is this
# many rows read in cycle order, not a table scan.
#
# The depth is a redelivery window, not a guess at history size. A redelivery arrives once the
# original delivery's claim expires, so for the trigger to have fallen out of range this
# subscription must have fired this many more times in between. Falling out of range is not
# wrong, only less informative: the answer degrades to the `arrival_already_recorded` it would
# have given anyway.
_NUM_PAST_CYCLES_TO_SCAN: Final[int] = 20


def _error_detail(error: Exception) -> str:
    """The exception's class and message, capped at something a stored detail can hold.

    The class name is included because the message alone rarely says what was raised, and
    `run_start_failed` is diagnosed almost entirely from the type.
    """
    detail = f"{type(error).__name__}: {error}"
    if len(detail) <= _ERROR_DETAIL_LIMIT:
        return detail
    return detail[: _ERROR_DETAIL_LIMIT - 3] + "..."


class NotAuthorized(Exception):
    """The caller may not change this subscription.

    A domain error rather than an `HTTPException`, so the rule lives with the writes it guards
    and holds for callers that are not requests — a backfill or a console script gets the same
    refusal. The route translates it to a 403.
    """


class NameTaken(Exception):
    """This caller already has a subscription under that name.

    Same reasoning as `NotAuthorized`: a domain error, so the rule holds for callers that are
    not requests. The route translates it to a 409.

    Raised in place of the raw `IntegrityError` from the unique constraint on
    (created_by, name). Uncaught, that error is a 500 and a page — the wrong answer for a
    request that is well-formed and simply clashes with a row the caller already owns.
    """


def _is_name_collision(*, error: sql.exc.IntegrityError) -> bool:
    """Whether this integrity failure is the natural key and not some other constraint.

    Checked rather than assumed, so a future constraint on this table does not get silently
    reported to the caller as a duplicate name.

    Both needles are read off the constraint object rather than spelled out, so renaming it or
    changing its columns cannot leave this check quietly matching nothing. The two dialects
    word the failure differently: MySQL quotes the key name, SQLite lists the qualified
    columns, so both forms are tried.
    """
    constraint = db_models.TRIGGER_SUBSCRIPTION_USER_NAME_CONSTRAINT
    message = str(error.orig) if error.orig is not None else str(error)
    if constraint.name in message:
        return True
    return all(
        f"{column.table.name}.{column.name}" in message for column in constraint.columns
    )


def _flush_or_name_taken(*, session: orm.Session, name: str) -> None:
    """Flush the pending write, turning the natural key's rejection into `NameTaken`.

    The flush is what makes the collision surface here rather than at the route's commit. That
    matters: caught here, every caller of this service gets the domain error, and the route is
    left with one `except` instead of having to unpick an `IntegrityError` after the fact.

    Rolls back before raising. The request is being abandoned with a 409, and a session left
    holding a failed flush cannot be used for anything else — including the read the route
    would otherwise attempt on the way out.
    """
    try:
        session.flush()
    except sql.exc.IntegrityError as error:
        session.rollback()
        if not _is_name_collision(error=error):
            raise
        # Both halves of the key are named on purpose. A bare "name already exists" reads as
        # globally taken, and the caller's next move is `nightly-2` — when `nightly` is in
        # fact free for everyone but them, which is the mapping the natural key exists to
        # spare them. Safe to be this specific: the constraint is scoped to `created_by`, so
        # a 409 from it is always about a row this caller already owns.
        raise NameTaken(
            f"You already have a subscription named '{name}'. Subscription names must be "
            f"unique per creator; another user may still use this name."
        ) from error


@dataclasses.dataclass(frozen=True)
class Caller:
    """Who is asking, reduced to the two facts authorization needs.

    Deliberately not `UserDetails`: the service layer has no HTTP types in it, and a test can
    name a caller without building a request.

    Attributes:
        name: the authenticated user, compared against `created_by`.
        is_admin: whether they hold the application-wide admin permission.
    """

    name: str
    is_admin: bool = False


def ensure_may_write(
    *,
    subscription: db_models.TriggerSubscription,
    caller: Caller,
    action: str,
) -> None:
    """Raise unless `caller` may change `subscription`.

    Admins first, then the creator. Admins exist for the one failure creator-only cannot
    survive: when the creator leaves, their subscriptions would otherwise be frozen —
    un-editable and un-deletable — while still starting a run every cycle. Membership is a
    deploy-time constant, so this is not a privilege a caller can grant itself.

    Args:
        subscription: the row being changed.
        caller: who is asking.
        action: the verb for the message, e.g. "Update".

    Raises:
        NotAuthorized: naming the creator and the caller, so the message is actionable.
    """
    if caller.is_admin:
        return
    if caller.name != subscription.created_by:
        raise NotAuthorized(
            f"{action} denied: subscription '{subscription.id}' was created by"
            f" {subscription.created_by}, not {caller.name}"
        )


@dataclasses.dataclass(frozen=True)
class TriggerResult:
    """What a write did about the condition, ready for a route to serialize.

    Attributes:
        triggered: whether this *emission* started the cycle. False is a normal outcome, not an
            error. Almost always this call's own doing -- the one exception is a redelivery,
            where the run was started by an earlier delivery of the same emission and this call
            reports it rather than repeating it (`TriggerReason.RUN_ALREADY_STARTED`).
        reason: why it did not trigger, or None when it did -- with the same exception, which
            names why this call did not start the run it is reporting.
        cycle: the cycle triggered, or None when nothing was. On a redelivery it is the cycle
            of the earlier trigger, not the subscription's current one: the two differ by every
            fire since, and the current one names a cycle that has not happened.
        matched_events: the evidence written to the history row — where the condition matched,
            which events matched it, and the definition as it stood.
        missing: the events the condition is still waiting for, or **None when this call did
            not evaluate the condition at all**. The empty tuple is an answer -- "evaluated,
            nothing outstanding" -- so a path that never looked must not borrow it: a rename
            reporting `()` reads as a satisfied condition, which is how the API came to
            contradict the detail route on the same row in the same second.
        pipeline_run_id: the run this call started, or None when it did not start one.
        error: what went wrong, when `reason` names a failure rather than a normal outcome.
    """

    triggered: bool
    reason: TriggerReason | None = None
    cycle: int | None = None
    matched_events: dict[str, Any] | None = None
    missing: tuple[str, ...] | None = None
    pipeline_run_id: str | None = None
    error: str | None = None


def _validate_pipeline_is_live_owned_and_resolve_version_key(
    *,
    session: orm.Session,
    pipeline_id: str,
    pipeline_version: str | None,
    caller: Caller,
) -> tuple[str, str | None]:
    """Check the pipeline a subscription is being pointed at, and translate any pin.

    Two questions that look like one. The first is about the *pipeline* — does it exist, is it
    alive, is it the caller's — and it has to be asked even when nothing is being pinned,
    because a subscription parked on a soft-deleted pipeline is accepted by the foreign key and
    reattaches itself to whoever recreates that file path. The second is about the *version*,
    and only the resolver can answer it: the column is half of a composite foreign key that
    keys on `version_key`, which is not the published version under DISABLED versioning.

    Ordering is the point. A stranger's pipeline id fails the first check with a 404, so the
    resolver's two 422s -- which distinguish "no such content" from "keeps no history" -- are
    never an existence oracle over someone else's pipelines.

    Args:
        session: the caller's transaction. Nothing is written.
        pipeline_id: the target as it will be *after* this request, not as it was stored.
        pipeline_version: the version to pin, or None to track the current one.
        caller: who is asking. An admin skips the ownership filter, matching
            `ensure_may_write` -- they may already edit any subscription, so refusing them a
            target would be an inconsistency rather than a protection.

    Returns:
        The pipeline id to store, and the `version_key` to store (None for "track current").

        The id is returned rather than left to the caller because this function is the only
        thing that has normalized it: the lookup canonicalizes `pipeline_id` before querying,
        so an alternative UUID spelling -- `uuid4().hex` prints one, 32 characters with no
        dashes -- finds the pipeline here and then violates
        fk_trigger_subscription_user_pipeline_id at the flush. Handing back the row's own id
        makes the value that passed the check and the value that gets stored the same string.

    Raises:
        user_pipeline_errors.PipelineNotFoundError: no live pipeline of theirs has that id.
        user_pipeline_errors.PipelineError: the pin names nothing pinnable.
    """
    pipeline = user_pipeline_services.get_live_owned_pipeline(
        session=session,
        pipeline_id=pipeline_id,
        user_id=None if caller.is_admin else caller.name,
    )
    if pipeline_version is None:
        return pipeline.id, None
    return pipeline.id, user_pipeline_services.resolve_pinnable_version_key(
        session=session, pipeline=pipeline, content_digest=pipeline_version
    )


def create_subscription(
    *,
    session: orm.Session,
    definition: dict[str, Any],
    pipeline_task_spec_from_user_pipeline_id: str,
    pipeline_task_spec_from_user_pipeline_version_key: str | None = None,
    caller: Caller,
) -> db_models.TriggerSubscription:
    """Persist a new subscription and open its event states, empty.

    No authorization check: any authenticated caller may create one, and `created_by` is stamped
    from `caller` rather than read from the payload, so there is nothing here to spoof.

    A new subscription cannot trigger on creation and is deliberately not evaluated. Every event
    state starts unfilled, and the grammar has no node that holds with zero events — a branch
    needs at least one child and a leaf needs its event — so there is no condition that is
    satisfied the moment it is stored.

    Args:
        session: the caller's transaction. Nothing is committed here.
        definition: the validated payload, stored verbatim.
        pipeline_task_spec_from_user_pipeline_id: the pipeline a satisfied condition starts.
            Required, not defaulted: the column is NOT NULL, and a subscription with nothing
            to start is a row that can only fail later, at trigger time.
        pipeline_task_spec_from_user_pipeline_version_key: the version to pin to, or None to track whichever version is current
            when the trigger fires.
        caller: who is creating it; their name becomes `created_by`.

    Returns:
        The persisted subscription, flushed so its generated id is readable.

    Raises:
        NameTaken: when this caller already has a subscription under this name.
        user_pipeline_errors.PipelineNotFoundError: the target is not a live pipeline of theirs.
        user_pipeline_errors.PipelineError: the pin names nothing pinnable.
    """
    condition = definition[db_models.DefinitionKey.CONDITION]
    # Raises before the insert when the condition is malformed, matching the update path: a
    # rejected definition never reaches a row.
    evaluation.event_names(condition=condition)

    # Before the insert, so a bad target or a bad pin is a sentence rather than a constraint
    # name surfacing from the flush.
    canonical_pipeline_id, version_key = (
        _validate_pipeline_is_live_owned_and_resolve_version_key(
            session=session,
            pipeline_id=pipeline_task_spec_from_user_pipeline_id,
            pipeline_version=pipeline_task_spec_from_user_pipeline_version_key,
            caller=caller,
        )
    )

    subscription = db_models.TriggerSubscription(
        name=definition[db_models.DefinitionKey.NAME],
        definition=definition,
        created_by=caller.name,
        # The checked id, not the one the request spelled: the two differ whenever a caller
        # sends a UUID without dashes or in upper case, and storing the request's spelling
        # writes a foreign key that names no pipeline row.
        pipeline_task_spec_from_user_pipeline_id=canonical_pipeline_id,
        pipeline_task_spec_from_user_pipeline_version_key=version_key,
    )
    session.add(subscription)
    # The event states reference the generated id, so it has to exist first. This is also where
    # a duplicate name is rejected — before any event state is written against a row that is
    # not going to survive the request.
    _flush_or_name_taken(session=session, name=subscription.name)
    event_state.sync(
        session=session, subscription_id=subscription.id, condition=condition
    )
    logger.info(f"Created subscription={subscription.id} created_by={caller.name}")
    return subscription


def delete_subscription(
    *,
    session: orm.Session,
    subscription: db_models.TriggerSubscription,
    caller: Caller,
) -> None:
    """Delete a subscription and its pending event states, keeping its history.

    The asymmetry is the schema's: `trigger_event_state` goes with the subscription because it
    is only pending state, while `trigger_history` has no foreign key to cascade through, so
    the record that this subscription started runs outlives it. `matched_events` snapshots the
    definition, so those rows stay readable with the subscription gone.

    The event states are deleted here rather than left to the schema's `ON DELETE CASCADE`,
    which is inert on a SQLite connection that has not enabled foreign keys — see
    `event_state.delete_all`.

    Args:
        session: the caller's transaction. Nothing is committed here.
        subscription: the row to delete.
        caller: who is asking.

    Raises:
        NotAuthorized: unless the caller is the creator or an admin.
    """
    ensure_may_write(subscription=subscription, caller=caller, action="Delete")
    subscription_id = subscription.id
    # Children first, then the parent: safe whether or not the database enforces the cascade,
    # and a no-op for the second writer if it does.
    removed = event_state.delete_all(session=session, subscription_id=subscription_id)
    session.delete(subscription)
    logger.info(
        f"Deleted subscription={subscription_id} event_states={removed} by={caller.name}"
    )


def update_subscription(
    *,
    session: orm.Session,
    subscription: db_models.TriggerSubscription,
    caller: Caller,
    name: str | None = None,
    enabled: bool | None = None,
    condition: dict[str, Any] | None = None,
    pipeline_task_spec_from_user_pipeline_id: str | None = None,
    pin_edit: tuple[bool, str | None] = (False, None),
    templates: Mapping[str, str] | None = None,
    now: datetime.datetime | None = None,
    pipeline_service: user_pipeline_services.UserPipelineService | None = None,
) -> TriggerResult:
    """Apply an edit, and re-evaluate only when the edit could have changed the answer.

    Independent parts — name, enabled, condition, target, templates — and one decision at the
    end. Every field is optional, so this is equally the path for a rename, a toggle, a new
    condition, or all of them at once.

    Four things prompt a re-check, and none implies another:

    - **The condition changed.** It is evaluated against the state the edit *leaves*, never the
      state it found, so an edit that removes the last unfilled event — `all(A, B)` with only A
      filled becoming `all(A)` — triggers on the spot rather than waiting for an emission that
      may never come. A configuration call starting a pipeline run is deliberate: the
      subscription is satisfied the instant the edit commits.
    - **`enabled` went off->on.** While disabled the subscription refuses to trigger, so a
      condition satisfied in the meantime is left holding filled events; because every event it
      waits on is already filled, no further emission will arrive to prompt a re-check, and
      without evaluating on the transition it would sit satisfied and dormant forever.
    - **The target or the pin changed.** The odd one out, because it changes *what* a satisfied
      condition launches rather than whether it is satisfied. It re-evaluates because a
      subscription whose pipeline was deleted keeps its arrival and loses only its run, so
      repointing it is the one move that can rescue a condition already satisfied. See
      `_maybe_update_pipeline_id_and_version_pin`, which used to document the opposite rule.
    - **The templates changed.** The same reason as the target, and the same reversal: a
      template key the pipeline does not declare makes run submission refuse the whole run, so
      the subscription keeps its arrival and loses only its run. Editing the templates is the
      repair, and nothing else will prompt one — the emission settled FAIL, so no redelivery is
      coming.

    Everything else is inert by construction. A rename cannot change whether the condition
    holds, and neither can resending the condition, the target or the templates already stored,
    so none re-evaluates — an unrelated metadata edit must not start a run the caller never
    asked about.

    `enabled` freezes rather than clears: switching off leaves the event states exactly as they
    are, so switching back on resumes where it was instead of waiting for every event again.
    Nothing here bumps `cycle` — only a trigger does that, and a bump would hand the
    subscription a fresh fence slot it has not earned.

    Args:
        session: the caller's transaction. Nothing is committed here.
        subscription: the row being edited, already loaded.
        caller: who is asking; the creator or an admin.
        name: the new name, or None to leave it.
        enabled: the new enabled flag, or None to leave it.
        condition: the new condition, stored verbatim and replacing the stored one wholesale,
            or None to leave it.
        pipeline_task_spec_from_user_pipeline_id: the new target, or None to leave it. Not
            nullable in the other sense: the column is NOT NULL, so there is no "unset the
            target" and None can only mean omitted.
        pin_edit: (whether the caller addressed the pin at all, the version they set it to).
            The pair is needed because `null` and omitted mean different things here and a
            plain `str | None` cannot tell them apart -- see `SubscriptionUpdateRequest`.
        templates: the argument templates to store, already validated, or None to leave the
            stored ones alone. An empty mapping is not None: it clears them.
        now: the instant freshness is judged against; defaults to the current time.

    Returns:
        Whether this call triggered, and either the cycle and its evidence or what it still
        waits for. Untriggered whenever the edit could not have changed the answer.

    Raises:
        NotAuthorized: unless the caller is the creator or an admin.
        NameTaken: when the edit renames onto a name this caller already holds.
        user_pipeline_errors.PipelineNotFoundError: the new target is not a live pipeline of
            theirs.
        user_pipeline_errors.PipelineError: the new pin names nothing pinnable.
    """
    ensure_may_write(subscription=subscription, caller=caller, action="Update")
    was_enabled = subscription.enabled
    target_changed = _maybe_update_pipeline_id_and_version_pin(
        session=session,
        subscription=subscription,
        caller=caller,
        new_pipeline_id=pipeline_task_spec_from_user_pipeline_id,
        pin_edit=pin_edit,
    )
    # Decided before anything is written, and against the stored blob rather than a
    # reconstruction of it: both sides come from the same `model_dump(mode="json",
    # exclude_unset=True)`, so a caller who resends what is already there compares equal.
    # Reordered children count as a change, which is the safe direction to be wrong in — it
    # re-evaluates unnecessarily rather than skipping a re-check that was due.
    condition_changed = (
        condition is not None
        and condition != subscription.definition[db_models.DefinitionKey.CONDITION]
    )

    if condition_changed:
        # Raises before anything is written when the new condition is malformed, so a rejected
        # edit cannot leave the event set synced against it.
        evaluation.event_names(condition=condition)
        event_state.sync(
            session=session,
            subscription_id=subscription.id,
            condition=condition,
        )
        # Reassigned rather than written in place, so the blob has one update idiom.
        subscription.definition = {
            **subscription.definition,
            db_models.DefinitionKey.CONDITION: condition,
        }

    if name is not None:
        subscription.name = name
        subscription.definition = {
            **subscription.definition,
            db_models.DefinitionKey.NAME: name,
        }

    # Compared against the stored map for the same reason the condition is: resending what is
    # already there is not an edit and must not start a run.
    templates_changed = templates is not None and dict(
        templates
    ) != pipeline_templates.get_pipeline_templates(original=subscription.definition)

    if templates_changed:
        subscription.definition = pipeline_templates.set_pipeline_templates(
            original=subscription.definition, updates=templates
        )

    if enabled is not None:
        subscription.enabled = enabled

    # Two jobs, which is why it sits outside both branches above rather than inside either.
    #
    # UNIQUE is checked on UPDATE exactly as on INSERT, so a rename onto a name this caller
    # already holds is refused here — the same 409 the create path decides, rather than an
    # IntegrityError surfacing as a 500 at the route's commit.
    #
    # And every session in this service is autoflush=False, so this is also what puts `sync`'s
    # writes on the wire before `maybe_trigger` reads them back: without it a widened expiry
    # would be invisible to the very evaluation the edit exists to prompt. Unconditional
    # because a flush with nothing dirty emits no SQL at all, and gating it on `name` is
    # precisely how the second job gets lost.
    _flush_or_name_taken(session=session, name=subscription.name)

    logger.info(
        f"Updated subscription={subscription.id} name={name} enabled={enabled} "
        f"condition_changed={condition_changed} target_changed={target_changed}"
    )

    if (
        condition_changed
        or target_changed
        or templates_changed
        or (enabled and not was_enabled)
    ):
        return maybe_trigger(
            session=session,
            subscription=subscription,
            now=now,
            **(
                {"pipeline_service": pipeline_service}
                if pipeline_service is not None
                else {}
            ),
        )
    # Nothing here touched the condition, so nothing here evaluated it: `reason` and `missing`
    # both stay None, which is the honest answer to a question this edit never asked.
    return TriggerResult(triggered=False, reason=None, cycle=None, missing=None)


def _maybe_update_pipeline_id_and_version_pin(
    *,
    session: orm.Session,
    subscription: db_models.TriggerSubscription,
    caller: Caller,
    new_pipeline_id: str | None,
    pin_edit: tuple[bool, str | None],
) -> bool:
    """Apply the target half of an edit: which pipeline, and which version of it.

    **This re-evaluates, and that is a reversal.** The rule used to be that a target edit must
    never start a run: a pin changes *what* a satisfied condition launches, never *whether* it
    is satisfied, so re-evaluating looked like work with no possible new answer. It has one,
    and it is the only recovery there is. When `_trigger` finds the target deleted it rolls the
    fence and the half-built run back to the savepoint but leaves the arrival committed, so the
    subscription sits with a satisfied condition and no run. Repointing it at a live pipeline is
    exactly the news that changes the answer, and refusing to re-evaluate would strand it: the
    emission was settled FAIL and there is no redelivery behind it.

    Safe by construction, not by luck. A successful trigger clears the event states in the same
    transaction that claims the cycle, so there is never a consumed condition left lying around
    for an edit to fire a second time on. A target edit can only trigger when a *genuinely
    unconsumed* satisfied condition is waiting — which is precisely the stranded case.

    The two fields are one edit rather than two, because a pin is only meaningful relative to a
    pipeline -- the composite foreign key keys on the pair. Four combinations, and the third is
    the one with a decision in it:

    - neither addressed: nothing happens, and in particular no guard runs. Re-checking the
      stored target on an unrelated rename would make a subscription un-editable the moment its
      pipeline was deleted, which is the opposite of useful.
    - pin only: resolved against the stored target.
    - target moved, pin not addressed: **the pin is cleared**. The stored key was resolved
      against the pipeline being moved away from, and a version is content-addressed, so
      carrying it across either fails the composite foreign key at flush or -- when the new
      pipeline holds byte-identical content, which copying a source file makes routine --
      silently switches to a version nobody chose. Dropping to "track current" is the only
      reading that is always what was asked for, and the response echoes the resulting null so
      the caller can see it.
    - both: resolved against the *new* target.

    Note `target_moved` compares ids rather than testing that one was sent. A client that reads
    a subscription, edits one field and PATCHes the whole object back sends the same pipeline
    id every time; treating that as a move would drop its pin on an unrelated rename. That
    comparison is also what keeps the re-evaluation above honest — a no-op PATCH returns False
    here and does not re-evaluate. It is why the incoming id is canonicalized before anything
    is decided from it: the stored id is canonical, so an equivalent UUID in another spelling
    would otherwise read as a move -- dropping the pin and re-evaluating on news that is not
    news.

    Returns:
        Whether the target or the pin actually changed, which is what the caller gates its
        re-evaluation on.
    """
    pin_addressed, pin_version = pin_edit
    stored_id = subscription.pipeline_task_spec_from_user_pipeline_id
    target_id = (
        user_pipeline_services.normalize_pipeline_id(new_pipeline_id)
        if new_pipeline_id is not None
        else stored_id
    )
    target_moved = target_id != stored_id
    if not target_moved and not pin_addressed:
        return False

    # Guarded against the target as it will be after this request, never as it was stored:
    # guarding the stored id would let a caller move onto a soft-deleted or someone else's
    # pipeline.
    canonical_pipeline_id, version_key = (
        _validate_pipeline_is_live_owned_and_resolve_version_key(
            session=session,
            pipeline_id=target_id,
            pipeline_version=pin_version if pin_addressed else None,
            caller=caller,
        )
    )
    stored_version_key = subscription.pipeline_task_spec_from_user_pipeline_version_key
    subscription.pipeline_task_spec_from_user_pipeline_id = canonical_pipeline_id
    subscription.pipeline_task_spec_from_user_pipeline_version_key = version_key
    # Compared after resolution rather than before: `pin_addressed` only says the caller
    # mentioned the pin, and resending the version already stored is not a change.
    return target_moved or version_key != stored_version_key


def lock_subscription_until_commit(
    *,
    session: orm.Session,
    subscription_id: str,
) -> db_models.TriggerSubscription | None:
    """Read the subscription with `SELECT ... FOR UPDATE`, the first lock every writer takes.

    It locks the row, not a column — a record lock on the primary-key index entry for this `id`:

        SELECT * FROM trigger_subscription WHERE id = 'sub-a' FOR UPDATE;   -- taken here
        UPDATE trigger_subscription SET cycle = cycle + 1 WHERE id = 'sub-a';
        INSERT INTO trigger_history (subscription_id, cycle) VALUES ('sub-a', 1);
        COMMIT;                                                             -- released here

    `cycle` is read and then written, which is why the read has to be the locking one: two
    writers that both read cycle 0 would both try to insert history row 0.

    **One lock order, everywhere** — subscription, then trigger_event_state, then
    trigger_history. Without it an arriving emission locks an event state and then the
    subscription while a `PATCH` does the reverse: opposite orders on the same two rows, the
    shape a deadlock needs.

    **The duplicate-key collision stops being the normal case.** A second writer waits, re-reads
    `cycle`, and inserts the next one. On MySQL that matters beyond tidiness: a failed unique
    insert leaves a shared lock on the conflicting index record, and two writers each holding one
    while wanting the other is the deadlock. The fence's SAVEPOINT remains the
    correctness backstop, since a lock is not a guarantee across a lock-wait timeout.

    No in-process retry rides along: the only caller with real concurrency is the emission
    consumer, which retries by letting its claim expire and re-announcing. And SQLite renders no
    `FOR UPDATE` at all, so the tests assert the lock *order* — what a reader of this code can
    get wrong — rather than the lock itself.

    Args:
        session: the caller's transaction; the lock is held until it ends.
        subscription_id: the row to lock.

    Returns:
        The locked subscription, or None when it does not exist.
    """
    return session.get(
        db_models.TriggerSubscription, subscription_id, with_for_update=True
    )


# The write failures that say nothing about whether the write is possible — the lock, the
# connection, the pool. A later attempt may well succeed, while a value the column rejects would
# fail identically every time. InnoDB reports a deadlock victim as an OperationalError, which is
# the case the fan-out below expects to see.
#
# It lives here rather than in the sink because this is now the layer that catches them: the
# fan-out contains such a failure to the one subscription it happened on. `emissions.outcome_
# recorder` keeps its own copy for its own writes; the sink imports this one rather than a third.
RETRYABLE_WRITE_FAILURES: Final[tuple[type[Exception], ...]] = (
    sql_exc.OperationalError,
    sql_exc.InterfaceError,
    sql_exc.InternalError,
    sql_exc.TimeoutError,
)


@dataclasses.dataclass(frozen=True)
class SubscriptionOutcome:
    """What one subscription did about an arriving emission.

    Attributes:
        subscription_id: whose event state the arrival was written to.
        result: what evaluating the condition afterwards decided.
    """

    subscription_id: str
    result: TriggerResult


@dataclasses.dataclass(frozen=True)
class FanOut:
    """What one emission's fan-out managed, split by whether it is durable.

    Three lists rather than one, because "did not trigger", "was not recorded" and "could not
    run" are different facts with different consequences, and a caller that reads one as another
    reports a lost signal as a normal delivery. An entry in `outcomes` is a committed arrival
    whatever its condition decided; an id in `deferred` is a subscription that never got the
    signal at all; an id in `failed` got it, and then could not act on it.

    `deferred` is retried automatically and `failed` is not: a retryable write failure clears
    by itself, an unbuildable target does not. Recovery for `failed` is a human acting on the
    subscription or its target — repointing it, re-saving the pipeline, fixing the bug behind a
    `run_start_failed` — after which the still-satisfied condition re-evaluates and starts the
    run. Worth doing, in other words, but not by this process and not on this delivery.

    Attributes:
        outcomes: one entry per subscription whose arrival committed, in subscription id order.
        deferred: the subscriptions whose write hit a retryable failure and were skipped, in the
            same order. Empty on the ordinary path, and the only thing worth retrying.
        failed: the subscriptions whose arrival committed but whose run could not be started,
            in the same order. A subset of `outcomes`, surfaced separately so one dead target
            does not pass as a quiet non-trigger.
    """

    outcomes: list[SubscriptionOutcome]
    deferred: list[str]
    failed: list[str] = dataclasses.field(default_factory=list)


class FanOutIncomplete(Exception):
    """The fan-out ran out of its time budget with subscriptions left, and stopped.

    Raised rather than returned, and that is the whole design. A returned value would be
    reported, and reporting settles the emission -- the remaining subscriptions would never
    hear the signal. Raising past the settle leaves the emission claimed and unsettled, so its
    claim lease expires, another consumer reclaims it, and the fan-out runs again.

    Safe to stop mid-way because every subscription already served committed its own
    transaction and `event_state.fill` recognises the emission on the way back round: those
    subscriptions answer `arrival_already_recorded` or `run_already_started` in a couple of
    reads, and the budget is spent on the ones still waiting.

    The subscription list is re-derived on the next pass, not replayed from `remaining`. It can
    legitimately differ: a subscription created since is served, one deleted since is gone, and
    an edit that dropped the event name drops it from the list. `remaining` is therefore a
    record of what this pass did not reach, not an instruction for the next one.

    Attributes:
        served: the subscriptions whose arrival committed before the budget ran out, in
            subscription id order.
        remaining: the subscriptions the budget did not reach, in the same order.
    """

    def __init__(self, *, served: list[str], remaining: list[str]) -> None:
        """Record which subscriptions this pass reached, for the log the caller writes."""
        super().__init__(
            f"fan-out budget exhausted after {len(served)} subscription(s),"
            f" {len(remaining)} left"
        )
        self.served = served
        self.remaining = remaining


@contextlib.contextmanager
def _subscription_transaction(*, session: orm.Session) -> Iterator[None]:
    """One subscription, one transaction, committed on the way out however the block is left.

    `continue` leaves the block, so a path that decided there was nothing to write still ends
    its transaction — which it must. The commit is what releases the `SELECT ... FOR UPDATE`
    lock taken inside, and what gives the next subscription's plain reads a fresh snapshot
    instead of this one's: under REPEATABLE READ a stranded read transaction would have the
    next iteration evaluate its condition against a view of the world from before this one ran.

    On a raise it rolls back and re-raises, so this subscription's partial write is dropped
    while every subscription already committed keeps its arrival.
    """
    try:
        yield
    except BaseException:
        session.rollback()
        raise
    else:
        session.commit()


def _run_started_by(
    *,
    session: orm.Session,
    subscription_id: str,
    event_name: str,
    emission_event_id: str | None,
) -> sql.Row[tuple[int, str | None, dict[str, Any] | None]] | None:
    """The trigger this emission already caused for this subscription, if it caused one.

    Asked on the redelivery path, where `event_state.fill` has refused the arrival because the
    row has seen this emission before. Refusing is right -- re-evaluating would start a second
    run off one readiness signal -- but "already seen" is not the same fact as "nothing
    happened", and reporting the second when the first is true is what makes the outcome ledger
    deny a run that is executing.

    Keyed on the *emission*, never on the subscription alone. "Has this subscription ever
    triggered" is true for almost every subscription in steady state and would attach a stale,
    unrelated run id to the report.

    Two-step rather than one query: an indexed prefix seek by subscription, newest cycle first,
    then the JSON match in Python. `triggered_by` has no index and no portable containment
    operator across SQLite, MySQL and Postgres, so pushing the match into SQL would buy a full
    scan and three dialect branches. See `_NUM_PAST_CYCLES_TO_SCAN` for the bound.

    Three columns rather than the entity: this runs once per deferred subscription per attempt,
    and `matched_events` plus `definition`-sized JSON would be fetched and deserialized for a
    caller that reads none of it.

    Args:
        session: the caller's transaction, inside the subscription's lock. Nothing is written.
        subscription_id: whose history to read.
        event_name: the key `triggered_by` is keyed by -- one entry per matched leaf event.
        emission_event_id: the redelivered emission. None short-circuits: an arrival with no id
            was never recorded against a history row, so there is nothing to find.

    Returns:
        `(cycle, pipeline_run_id, triggered_by)` for the row whose `triggered_by[event_name]`
        is this emission, or None when no row in range names it -- which reads as "banked,
        never fired".
    """
    if emission_event_id is None:
        return None
    rows = session.execute(
        sql.select(
            db_models.TriggerHistory.cycle,
            db_models.TriggerHistory.pipeline_run_id,
            db_models.TriggerHistory.triggered_by,
        )
        .where(db_models.TriggerHistory.subscription_id == subscription_id)
        .order_by(db_models.TriggerHistory.cycle.desc())
        .limit(_NUM_PAST_CYCLES_TO_SCAN)
    ).all()
    for row in rows:
        if (row.triggered_by or {}).get(event_name) == emission_event_id:
            return row
    return None


def record_event_and_maybe_start_runs(
    *,
    session: orm.Session,
    event_name: str,
    emission_event_id: str | None,
    now: datetime.datetime | None = None,
    deadline: float | None = None,
    pipeline_service: user_pipeline_services.UserPipelineService | None = None,
) -> FanOut:
    """Write an arriving emission onto every event state waiting for it, then evaluate each.

    Evaluation goes through `maybe_trigger`, the same door edits come through, so an emission
    and a `PATCH` cannot disagree about what "the condition holds" means. A disabled
    subscription still records the arrival; only the run is withheld.

    **The one function in this module that commits, and the exception the module docstring
    names.** Nothing may be open when it is called, and it leaves nothing open. One transaction
    per subscription rather than one for the batch, so a failure serving the second cannot throw
    away the first's arrival — and each commit (write, fence, cycle bump, clear) can start
    compute.

    `event_state.fill` refuses two kinds of arrival, writing nothing; they are different facts
    and are reported as such. `_run_started_by` and the stored id tell them apart:

        this emission again, no run started      triggered=False  arrival_already_recorded
        a distinct older emission arriving late  triggered=False  arrival_superseded
        either one, but a run did start          triggered=True   run_already_started + run id

    The third row is why the first cannot swallow the others: a redelivery is by construction
    the case where the first delivery started a run and failed to record it, so answering
    "nothing triggered" settles the emission on a ledger row denying a run already executing.

    **A retryable failure is contained to the subscription it happened on** and returned in
    `deferred` rather than raised: the loop commits as it goes, and the caller retries the whole
    fan-out, so propagating would starve the same tail on every attempt instead of delaying it.
    Re-running is safe — an arrival already recorded is a no-op. Once the caller's attempts are
    spent, though, those subscriptions have missed the signal and the outcome detail naming them
    is the only trace: a diagnosis, not a cure.

    Two limits worth naming. **The fan-out is bounded only by how many subscriptions name the
    event**, acceptable while `trigger_subscription` stays a human-authored config table;
    programmatic creation wants keyset batching or per-subscription jobs, since every extra
    subscription holds the consumer's claim on the emission row for longer. And **event names
    are a shared namespace on purpose**, so anyone who can annotate a pipeline node can make
    someone else's subscription trigger earlier than intended — but not choose what it runs,
    since the subscription names its own target. The fix, if that trade stops being acceptable,
    is a namespace on both the annotation and the subscription, not an owner check here.

    Example:
        two subscriptions wait on "orders-ready"; the emission em-9 arrives at 12:00

            sub-a  {"event": "orders-ready"}         -> triggered, cycle 0
            sub-b  all("orders-ready", "fx-ready")   -> awaiting_events, missing ["fx-ready"]

        Both event states are written. Only sub-a's condition held.

    Args:
        session: the session each per-subscription transaction is opened on. Nothing is
            expected to be open when this is called.
        event_name: the readiness event name the emission carried. Lowercase alphanumeric
            kebab, per `api_routes._EVENT_NAME_PATTERN`.
        emission_event_id: the arriving emission, recorded for correlation; None leaves the
            column NULL rather than inventing an id.
        now: the arrival instant, defaulting to the current time. Injectable so a test can
            place an expiry either side of it without sleeping.
        deadline: a `time.monotonic()` instant this fan-out must not take on new work past, or
            None for no budget. Checked between subscriptions, never inside one, so the
            subscription in flight when it expires still finishes.

    Returns:
        The committed arrivals and the skipped subscriptions, both in subscription id order.
        An empty `outcomes` does not mean nobody was listening — check `deferred` first.

    Raises:
        FanOutIncomplete: the budget ran out with subscriptions still to serve. The caller must
            not settle the emission on this — see the exception's own docstring.
    """
    now = now or db_utils.utc_now()
    subscription_ids = event_state.subscription_ids_waiting_on(
        session=session, event_name=event_name
    )
    # The seek is a plain read, so it opens a transaction and pins a REPEATABLE READ view.
    # Ending it here is what lets the first iteration's lock be the first statement of its own
    # transaction: otherwise the lock would be taken inside the seek's view, and the plain read
    # after it would still answer from before an edit that committed in between. Later
    # iterations already get a fresh view from the previous subscription's commit; this gives
    # the first one the same footing, and leaves nothing open on the early return below.
    session.commit()
    if not subscription_ids:
        logger.info(
            f"Trigger arrival ignored event={event_name} emission={emission_event_id} (no subscription)"
        )
        return FanOut(outcomes=[], deferred=[], failed=[])

    outcomes: list[SubscriptionOutcome] = []
    deferred: list[str] = []
    failed: list[str] = []
    for index, subscription_id in enumerate(subscription_ids):
        # `index > 0` is the termination proof, not a nicety. Without it a budget already spent
        # when the fan-out starts serves nobody, raises, and the redelivery it asks for arrives
        # to the same spent budget -- an emission that is retried for ever and never progresses.
        # With it, every pass serves at least one subscription, so a list of N is exhausted in
        # at most N passes however small the budget is.
        if deadline is not None and index > 0 and time.monotonic() >= deadline:
            logger.warning(
                f"Trigger fan-out budget exhausted event={event_name}"
                f" emission={emission_event_id} served={len(outcomes)}"
                f" remaining={len(subscription_ids) - index}"
            )
            raise FanOutIncomplete(
                served=[outcome.subscription_id for outcome in outcomes],
                remaining=list(subscription_ids[index:]),
            )
        try:
            with _subscription_transaction(session=session):
                # The lock comes first, which is what puts this path in the same order as the
                # update path, and it is also what makes the two reads below trustworthy: a
                # concurrent edit that changed this event's expiry or removed it entirely has
                # already committed or is still waiting behind this lock.
                subscription = lock_subscription_until_commit(
                    session=session, subscription_id=subscription_id
                )
                if subscription is None:
                    # Deleted between the seek and the lock. Its event states cascaded away with
                    # it, so there is nothing left to write to.
                    continue
                state = session.get(
                    db_models.TriggerEventState, (subscription_id, event_name)
                )
                if state is None:
                    # An edit dropped this event from the condition while the arrival was in
                    # flight. The subscription is no longer waiting for it, so there is nothing
                    # to record.
                    logger.info(
                        f"Trigger arrival stale event={event_name} subscription={subscription_id} (event dropped)"
                    )
                    continue
                # A disabled subscription keeps listening: the arrival is stored exactly as it
                # would be for a live one, and only the decision to start a run is withheld.
                # maybe_trigger owns that refusal (see its `enabled` guard), so the verdict lives
                # in one place and this loop does not need to know it is writing to a switched-off
                # subscription.
                if event_state.fill(
                    state=state, emission_event_id=emission_event_id, now=now
                ):
                    # Flushed, not left pending: maybe_trigger re-reads the event states with a
                    # SELECT, and the consumer's session factory has autoflush off
                    # (emissions/consumer_main.py), so without this the arrival just written is
                    # invisible to the query that decides whether the condition holds — every
                    # trigger would be one delivery late.
                    session.flush()
                    trigger_metrics.record_after_commit(
                        session=session,
                        counter=trigger_metrics.event_filled,
                        attributes={
                            trigger_metrics.SUBSCRIPTION_ID_LABEL: subscription_id,
                            trigger_metrics.EVENT_NAME_LABEL: event_name,
                        },
                    )
                    result = maybe_trigger(
                        session=session,
                        subscription=subscription,
                        now=now,
                        **(
                            {"pipeline_service": pipeline_service}
                            if pipeline_service is not None
                            else {}
                        ),
                    )
                else:
                    # This exact emission was already recorded against this event, so the work is
                    # done and re-evaluating would trigger a second cycle off one readiness signal.
                    #
                    # Which leaves the question this branch exists to answer: did the delivery
                    # that recorded it start a run? The sink's own dedup cannot say -- it is a
                    # dict that dies with the process, so it covers a retry within one delivery
                    # and nothing across two. And a redelivery is exactly the case where the
                    # first delivery got far enough to start a run and not far enough to write
                    # its outcome row. Answering "nothing triggered" there is a durable lie: the
                    # ledger settles on this delivery's answer, and the run it denies is
                    # already executing.
                    prior = _run_started_by(
                        session=session,
                        subscription_id=subscription_id,
                        event_name=event_name,
                        emission_event_id=emission_event_id,
                    )
                    if prior is None:
                        # `fill` refuses two different things and they must not be reported as
                        # one: this emission arriving twice, and a distinct older one arriving
                        # after the row moved past it. The first is a duplicate; the second is
                        # a readiness signal that will never be acted on. `fill` wrote nothing,
                        # so the stored id is still the earlier arrival's and tells them apart.
                        result = TriggerResult(
                            triggered=False,
                            reason=(
                                TriggerReason.ARRIVAL_ALREADY_RECORDED
                                if state.last_emission_event_id == emission_event_id
                                else TriggerReason.ARRIVAL_SUPERSEDED
                            ),
                            cycle=subscription.cycle,
                        )
                    else:
                        # `prior.cycle`, not `subscription.cycle`: the run belongs to the cycle
                        # it fired on, and the subscription has moved past it -- by one fire at
                        # least, since the trigger itself bumped it.
                        result = TriggerResult(
                            triggered=True,
                            reason=TriggerReason.RUN_ALREADY_STARTED,
                            cycle=prior.cycle,
                            pipeline_run_id=prior.pipeline_run_id,
                        )
        except RETRYABLE_WRITE_FAILURES:
            # Contained, not propagated. `_subscription_transaction` has already rolled this
            # subscription's partial write back, so the session is clean for the next one, and
            # every subscription that committed before this keeps its arrival. Skipping forward
            # rather than unwinding is the whole point: a raise here would strand the tail of the
            # fan-out, and the caller's retry restarts from the top, so the tail would be starved
            # again on every attempt instead of merely arriving late.
            #
            # A dead connection defers the rest of the loop one by one rather than bailing out,
            # which is wasted work but not wrong: they all land in `deferred`, the caller retries
            # them, and a connection that is still dead fails the seek before the next loop starts.
            logger.warning(
                f"Trigger arrival deferred event={event_name} emission={emission_event_id}"
                f" subscription={subscription_id}"
            )
            deferred.append(subscription_id)
            continue
        else:
            # Reached only when the block left cleanly, so this subscription's write is
            # committed: an arrival in `outcomes` is a durable one. A retryable failure takes
            # the `except` above, and the two `continue`s inside the block skip this entirely.
            outcomes.append(
                SubscriptionOutcome(subscription_id=subscription_id, result=result)
            )
            if result.reason in RUN_NOT_STARTED_REASONS:
                # The arrival committed, so this is not a delivery to retry — but it is not a
                # normal non-trigger either, and the caller has to be able to tell them apart.
                # Membership rather than one comparison: every permanent "no run started"
                # reason belongs here, and the tuple is what keeps this in step with the sink's
                # own recompute of the same list.
                failed.append(subscription_id)
            logger.info(
                f"Trigger arrival event={event_name} emission={emission_event_id}"
                f" subscription={subscription_id} triggered={result.triggered} reason={result.reason}"
            )
    return FanOut(outcomes=outcomes, deferred=deferred, failed=failed)


def maybe_trigger(
    *,
    session: orm.Session,
    subscription: db_models.TriggerSubscription,
    now: datetime.datetime | None = None,
    pipeline_service: user_pipeline_services.UserPipelineService | None = None,
) -> TriggerResult:
    """Trigger the subscription if its condition holds against the events already emitted.

    The one path from "the condition holds" to a run, whether the news arrived as an emission
    or as an edit. An update is not a second way to start a run; it is a second way to notice.

    A disabled subscription stops here, and stops here rather than in each caller precisely
    because this is the only path: an edit that disables and satisfies the condition in the same
    PATCH would otherwise start a run, clear the arrival it consumed and bump the cycle, leaving
    a switched-off subscription that has just fired.
    """
    now = now or db_utils.utc_now()
    if not subscription.enabled:
        # The run is withheld, not the arrival: record_event_and_maybe_start_runs has already
        # stored whatever landed, so re-enabling resumes with a current event set rather
        # than a frozen one. `missing` stays None because this returns above the evaluation:
        # the condition may well be satisfied, and saying `[]` would imply it was weighed.
        return TriggerResult(
            triggered=False,
            reason=TriggerReason.SUBSCRIPTION_DISABLED,
            cycle=subscription.cycle,
            missing=None,
        )
    condition = subscription.definition[db_models.DefinitionKey.CONDITION]
    filled = event_state.filled_events(
        session=session, subscription_id=subscription.id, now=now
    )
    emitted = filled.emitted
    found = evaluation.evaluate(condition=condition, emitted=emitted.keys())
    if found is None:
        # The condition did not hold, so it is worth knowing whether something arrived and
        # lapsed while the rest of the events were still coming. That is the failure with no
        # error attached to it: every delivery succeeded and nothing ever triggers. The
        # expired names came back with the fresh ones, so noticing this costs no extra query.
        for lapsed in filled.lapsed:
            trigger_metrics.record_after_commit(
                session=session,
                counter=trigger_metrics.event_expired,
                attributes={
                    trigger_metrics.SUBSCRIPTION_ID_LABEL: subscription.id,
                    trigger_metrics.EVENT_NAME_LABEL: lapsed,
                },
            )
        return TriggerResult(
            triggered=False,
            reason=TriggerReason.AWAITING_EVENTS,
            missing=tuple(event_state.missing(condition=condition, emitted=emitted)),
        )
    return _trigger(
        session=session,
        subscription=subscription,
        found=found,
        emitted=emitted,
        #: The evaluation's own clock, so a retry renders at the moment it retried.
        trigger_time=now,
        **(
            {"pipeline_service": pipeline_service}
            if pipeline_service is not None
            else {}
        ),
    )


class _FenceLost(Exception):
    """The unique (subscription_id, cycle) rejected our history insert.

    Raised in place of the raw `IntegrityError` so that the one clause meaning "another writer
    started this run" cannot also catch a constraint the *run* insert violated. Both surface as
    `IntegrityError` from inside the same savepoint, and only the fence's is a lost race:

        flush the fence  -> IntegrityError -> cycle_already_triggered  (a run exists)
        insert the run   -> IntegrityError -> run_start_failed         (no run anywhere)

    Same move as `NameTaken` above: translate at the raise site rather than unpick the error
    after the fact.
    """


def _permanent_failure(
    *,
    session: orm.Session,
    subscription_id: str,
    cycle: int,
    reason: TriggerReason,
    error: str,
) -> TriggerResult:
    """Count a condition that held where no run started, and build the result that says so.

    The ending shared by every reason in `RUN_NOT_STARTED_REASONS`. They differ only in who
    repairs them, so they differ only in the `reason` label -- one series, not one counter per
    cause, which is what lets a single alert cover all of them (see `metrics.REASON_LABEL`).
    A branch that returned without coming through here would be a subscription stopped for
    good with nothing on the dashboard saying so.

    Queued on the commit that makes the arrival durable, not recorded here. If the outer
    transaction rolls back, the delivery is retried and the failure is counted then; a count
    taken now would report a subscription as stopped that is about to try again.

    Args:
        session: the caller's session -- the outer one, not the rolled-back savepoint.
        subscription_id: the stopped subscription, carried as the counter's label.
        cycle: the cycle that was not claimed. Reported, not spent: the caller's savepoint took
            the fence with it, so the repair re-evaluates into this same cycle.
        reason: which of the three permanent branches this is.
        error: the detail for the caller, already rendered -- `str(error)` where the message
            stands alone, `_error_detail` where the exception's class is the diagnosis.
    """
    trigger_metrics.record_after_commit(
        session=session,
        counter=trigger_metrics.run_not_started,
        attributes={
            trigger_metrics.SUBSCRIPTION_ID_LABEL: subscription_id,
            trigger_metrics.REASON_LABEL: reason,
        },
    )
    return TriggerResult(triggered=False, reason=reason, cycle=cycle, error=error)


def _trigger(
    *,
    session: orm.Session,
    subscription: db_models.TriggerSubscription,
    found: evaluation.Match,
    emitted: dict[str, str | None],
    trigger_time: datetime.datetime,
    pipeline_service: user_pipeline_services.UserPipelineService | None = None,
) -> TriggerResult:
    """Claim this cycle, start the run, clear the event states.

    The history insert is the fence -- unique (subscription_id, cycle) -- and shares a SAVEPOINT
    with the run, so the two are one write and neither survives alone. The savepoint does not
    cover the arrival in the caller's transaction: that still commits, which is what lets a
    later repoint recover a subscription whose target is gone.

    Clause order, so containment never reaches a failure that would have cleared on its own:

        _FenceLost               -> cycle_already_triggered   the winner did the work
        PipelineError            -> user_pipeline_deleted     subscriber repoints it
        pydantic.ValidationError -> target_unbuildable        pipeline owner re-saves it
        RETRYABLE_WRITE_FAILURES -> re-raised                 the sink retries it
        Exception                -> run_start_failed          on-call reads the traceback

    Only the re-raise escapes, because a raise aborts the fan-out over every subscription
    waiting on this event name and settles the emission unredelivered. The three middle reasons
    are `RUN_NOT_STARTED_REASONS`, which callers classify on; each returns through
    `_permanent_failure`.
    """
    cycle = subscription.cycle
    #: Pure and non-raising: a template that fails is reported in `rendered.failures`, never
    #: as an exception, so this cannot abort the fan-out. A subscription is never scheduled,
    #: so its clock carries no schedule_time.
    templates = pipeline_templates.get_pipeline_templates(
        original=subscription.definition
    )
    clock = sources.Clock(
        kind=sources.Kind.SUBSCRIPTION,
        trigger_time=trigger_time,
        now=db_utils.utc_now(),
    )
    rendered = rendering.render(templates=templates, arguments={}, clock=clock)
    #: A data-quality event, not a control-flow one: the cycle still advances and each
    #: failed key keeps whatever the target's spec already had.
    #:
    #: Rendering here and reporting on the success path below are deliberate: the values are
    #: needed to build the run, the count is only true once one exists.
    history = db_models.TriggerHistory(
        subscription_id=subscription.id,
        cycle=cycle,
        matched_events=evaluation.matched_events(
            found=found, definition=subscription.definition
        ),
        triggered_by={name: emitted[name] for name in found.events},
    )
    try:
        with session.begin_nested():
            session.add(history)
            # Flush the fence before any run work and translate its rejection here. Without
            # `_FenceLost`, an IntegrityError from the run insert would also report
            # `cycle_already_triggered`: emission settled a success, no run anywhere.
            try:
                session.flush()
            except sql.exc.IntegrityError:
                raise _FenceLost from None
            run = (
                pipeline_service or user_pipeline_services.UserPipelineService()
            ).create_from_pipeline_no_commit(
                session=session,
                pipeline_id=subscription.pipeline_task_spec_from_user_pipeline_id,
                user_id=None,
                file_path=None,
                version=subscription.pipeline_task_spec_from_user_pipeline_version_key,
                run_arguments=dict(rendered.arguments),
                # Provenance keys are injected for us; `trigger_history.pipeline_run_id` is
                # the join from this run back to the subscription and cycle.
                pipeline_run_annotations=template_annotations.for_firing(
                    templates=templates, rendered=rendered
                ),
                created_by=subscription.created_by,
            )
            history.pipeline_run_id = run.id
    except _FenceLost:
        # The winner's insert, clear and cycle bump committed together. Writes nothing: every
        # read above is stale, clearing would wipe an arrival banked for the next cycle, and
        # bumping could lower a cycle a third writer has already claimed.
        logger.warning(
            f"Trigger fence lost subscription={subscription.id} cycle={cycle} (skipped)"
        )
        trigger_metrics.record_after_commit(
            session=session,
            counter=trigger_metrics.cycle_collisions,
            attributes={trigger_metrics.SUBSCRIPTION_ID_LABEL: subscription.id},
        )
        return TriggerResult(
            triggered=False,
            reason=TriggerReason.CYCLE_ALREADY_TRIGGERED,
            cycle=cycle,
            # Evaluated and held -- lost to a writer, not to a missing event. Empty, not None.
            missing=(),
        )
    except user_pipeline_errors.PipelineError as error:
        # Target gone. The savepoint took the fence and the half-built run, and the clear and
        # bump below never run, so no cycle is spent. The arrival stays committed on purpose.
        logger.warning(
            f"Trigger target unavailable subscription={subscription.id} cycle={cycle}: {error}"
        )
        # `str`, not `_error_detail`: the message stands alone; the class name adds nothing.
        return _permanent_failure(
            session=session,
            subscription_id=subscription.id,
            cycle=cycle,
            reason=TriggerReason.USER_PIPELINE_DELETED,
            error=str(error),
        )
    except pydantic.ValidationError as error:
        # Target exists; its stored task spec does not parse (`TaskSpec.from_json_dict` is a
        # pydantic validation, so an older-schema or hand-written spec raises here, not as a
        # `PipelineError`). Same shape as above, different reason because a different person
        # fixes it: the pipeline's owner re-saves it, a deleted target is repointed.
        logger.warning(
            f"Trigger target unbuildable subscription={subscription.id} cycle={cycle}: {error}"
        )
        return _permanent_failure(
            session=session,
            subscription_id=subscription.id,
            cycle=cycle,
            reason=TriggerReason.TARGET_UNBUILDABLE,
            error=_error_detail(error),
        )
    except RETRYABLE_WRITE_FAILURES:
        # Above the catch-all on purpose. A deadlock victim, pool timeout or dropped connection
        # says nothing about whether the run can start: the fan-out turns it into `deferred` and
        # the sink retries with backoff. Falling into `except Exception` would settle a
        # millisecond-long lock conflict as a trigger that never fires.
        raise
    except Exception as error:
        # Backstop: one bad subscription must not abort the fan-out. `Exception`, never
        # `BaseException` -- shutdown keeps flying. A run-insert IntegrityError lands here,
        # not on `_FenceLost`, and is a bug: hence the traceback and FAIL.
        logger.exception(
            f"Trigger run start failed subscription={subscription.id} cycle={cycle}"
        )
        #: Templating's twin of `trigger.run_not_started`. Guarded: this backstop also
        #: catches failures templating had no part in. `templates` is read above the `try`.
        if templates:
            render_observer.submission_rejected(kind=sources.Kind.SUBSCRIPTION.value)
        return _permanent_failure(
            session=session,
            subscription_id=subscription.id,
            cycle=cycle,
            reason=TriggerReason.RUN_START_FAILED,
            error=_error_detail(error),
        )

    event_state.clear(session=session, subscription_id=subscription.id)
    # The fence is (subscription_id, cycle), so the bump is what opens the next slot.
    subscription.cycle = cycle + 1
    logger.info(
        f"Triggered subscription={subscription.id} cycle={cycle} branch={found.branch} run={run.id}"
    )
    trigger_metrics.record_after_commit(
        session=session,
        counter=trigger_metrics.triggered,
        attributes={trigger_metrics.SUBSCRIPTION_ID_LABEL: subscription.id},
    )
    #: Reported here rather than at the render, so `keys_rendered` counts keys that reached a
    #: run. Every exit above renders first and starts nothing: a lost fence, a deleted or
    #: unbuildable target, a refused submission. Counting at the render site would inflate all
    #: four, and the lost-fence case would double-count -- the winner renders the same
    #: templates and counts them in its own session, so one real run would be counted twice.
    #:
    #: Queued, not reported: a retryable write failure rolls this transaction back and the sink
    #: retries the delivery, so reporting eagerly would count one eventual firing once per
    #: attempt. Same reasoning, and the same queue, as every `record_after_commit` here.
    trigger_metrics.report_after_commit(
        session=session,
        report=functools.partial(
            render_observer.report,
            rendered=rendered,
            templates=templates,
            clock=clock,
            identity={
                "subscription_id": subscription.id,
                "cycle": cycle,
                "pipeline_id": subscription.pipeline_task_spec_from_user_pipeline_id,
            },
        ),
    )
    return TriggerResult(
        triggered=True,
        cycle=cycle,
        matched_events=history.matched_events,
        missing=(),
        pipeline_run_id=run.id,
    )
