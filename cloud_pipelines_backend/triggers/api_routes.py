"""The trigger configuration API: the one place a posted definition is validated.

Everything downstream — `triggers.service`, `triggers.event_state`, `triggers.evaluation` —
trusts the shape it is handed and raises plain `ValueError` when that trust is misplaced. A
`ValueError` escaping a route is a 500, so the request models here are the boundary that turns
a malformed condition into a 422 while it is still a request.

The condition grammar is mirrored from `triggers.evaluation`, which walks the tree as authored:

    {"op": "all", "children": [                                     branch
        {"op": "any", "children": [                                 branch
            {"event": "orders-us-ready"},                           leaf
            {"event": "orders-eu-ready"}]},                         leaf
        {"event": "refunds-ready", "expire_seconds": 86400}]}       leaf with an expiry

Two deliberate strictnesses, because the definition is stored *verbatim* rather than re-dumped
from the validated model — so anything these models let through is what the evaluator will
later read back:

  - `extra="forbid"`, so a misspelled key is a 422 rather than a silently ignored field. It is
    also what rejects a request trying to set `created_by` itself.
  - `strict=True` on `expire_seconds`, because pydantic would otherwise coerce `True` to 1 and
    `"60"` to 60, store the uncoerced original in the blob, and leave
    `evaluation._expire_seconds` to raise on it at trigger time.

Not validated on purpose: an event name's *spelling* is (`_EVENT_NAME_PATTERN`), but it is
never checked against a registry of producers, so a subscription may be created before the
thing that emits its events ships; and the tree has no cap on how *many* nodes it holds, only
on how deep they nest (`_MAX_CONDITION_DEPTH`).
"""

import collections.abc
import datetime
import logging
from typing import Annotated, Any, Final, Literal, Union

import fastapi
import pydantic
import sqlalchemy
from sqlalchemy import orm
from starlette import status

from cloud_pipelines_backend import api_router
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.templating.arguments import envelopes, sources
from cloud_pipelines_backend.triggers import db_models, evaluation, event_state, service
from cloud_pipelines_backend.user_pipelines import db_models as user_pipeline_db_models
from cloud_pipelines_backend.user_pipelines import errors as user_pipeline_errors
from cloud_pipelines_backend.user_pipelines import services as user_pipeline_services
from cloud_pipelines_backend.utils import db as db_utils
from cloud_pipelines_backend.utils import pipeline_templates

logger = logging.getLogger(__name__)

_API_BASE: Final[str] = "/api/triggers/subscriptions"
# The natural-key family. A literal segment rather than a second path parameter, and it has to
# be registered before the `{subscription_id}` routes — see the note in `setup_trigger_routes`.
_LOOKUP_PATH: Final[str] = f"{_API_BASE}/lookup"
_TAG: Final[str] = "triggers"
_MAX_NAME_LENGTH: Final[int] = bts._STR_MAX_LENGTH
# RFC 1123 label, copied from `quota.api_routes._NAME_PATTERN`.
#
#     allowed:  "orders-ready"  "orders-eu-ready"  "fx1"  "a"
#     rejected: "Orders-Ready"  "orders_ready"  "café"  "-a"  "a-"  " a"  "a.b"
#
# `event_name` is half of `trigger_event_state`'s primary key. MySQL compares it under
# `utf8mb4_0900_ai_ci`, which is case- and accent-insensitive; SQLite compares it
# byte-exact. So two rows can be one key on one engine and two on the other. Allowing a
# single legal spelling makes the key engine-agnostic. Cap stays `_MAX_NAME_LENGTH`.
_EVENT_NAME_PATTERN: Final[str] = r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?$"
# Half of the natural key, and the half a caller types. Stripped at the door, the same way
# `user_pipelines.services.normalize_file_path` strips a path.
#
# The key is matched byte-exact -- by the WHERE in `_load_by_natural_key` and again by
# `_reject_inexact_key` -- so a name stored with its padding is a row nobody can address again.
# Without the strip (measured, this is what the tests in `TestNameWhitespace` hold down):
#
#     POST   name='  nightly  '        -> 201, stored '  nightly  '
#     GET    lookup?name='nightly'     -> 404   <- the row exists and cannot be reached
#     POST   name='nightly'            -> 201   <- and a second row quietly takes the name
#     POST   name='   '                -> 201, stored '   '
#
# Stripped on both sides -- here and on `_NameQuery` below -- one typed name is one row:
#
#     POST   name='  nightly  '        -> 201, stored 'nightly'
#     GET    lookup?name='nightly'     -> 200
#     GET    lookup?name='  nightly  ' -> 200
#     POST   name='nightly'            -> 409   <- same key, refused by the unique index
#     POST   name='   '                -> 422   <- `min_length` is checked after the strip
_SubscriptionName = Annotated[
    str,
    pydantic.StringConstraints(
        strip_whitespace=True, min_length=1, max_length=_MAX_NAME_LENGTH
    ),
]
# The same name arriving as the lookup key. Spelled `Annotated[..., Query()]` and keyword-only
# rather than `name: _SubscriptionName = fastapi.Query()`: with the constraints in the
# annotation and `Query()` as the *default*, FastAPI builds the field from `Query()` alone and
# the strip is silently dropped -- measured, and the reason the padded-lookup test exists.
_NameQuery = Annotated[_SubscriptionName, fastapi.Query()]
# A freshness window, not a retention policy. Bounded because the value outlives the request
# that wrote it: it is stored in an INTEGER column and later added to a datetime, so an
# unbounded one is a 500 at fill time instead of a 422 here.
_MAX_EXPIRE_SECONDS: Final[int] = 365 * 24 * 60 * 60
# How deeply a condition may nest. Twenty is far past anything a person writes, and a cap
# rather than a style preference because pydantic-core's own guards fail badly without one:
# at 128 the row commits and the *response* serializer 500s, poisoning every later read of it;
# past 253 the refusal is a `Field required` that echoes the payload back a level at a time,
# so past ~485 the refusal itself 500s. `_refuse_a_condition_too_deep_to_validate` applies
# this cap to the raw body first, so nothing deeper ever reaches pydantic. Measured with it:
# 21..4000 -> a 141-byte 422; past ~4500 -> a 400 from the body parser.
_MAX_CONDITION_DEPTH: Final[int] = 20


class EventCondition(pydantic.BaseModel):
    """A leaf: one event, and optionally how long its arrival stays fresh."""

    model_config = pydantic.ConfigDict(extra="forbid")

    event: str = pydantic.Field(
        min_length=1, max_length=_MAX_NAME_LENGTH, pattern=_EVENT_NAME_PATTERN
    )
    # Strict: `evaluation._expire_seconds` rejects bool, float and str, and the blob it reads is
    # the request's, not this model's. Bounded above for a different reason: `gt=0` alone admits
    # values that overflow the INTEGER column on MySQL, and that SQLite stores happily so they
    # detonate later in `event_state._expires_at` instead.
    expire_seconds: int | None = pydantic.Field(
        default=None, gt=0, le=_MAX_EXPIRE_SECONDS, strict=True
    )


class BranchCondition(pydantic.BaseModel):
    """A branch: `all` of its children, or `any` of them.

    `children` is non-empty by construction — `evaluation._branch` documents an empty child list
    as something the API rejects before it gets there, since `all([])` is vacuously true and
    would trigger a run on a condition naming no events at all.
    """

    model_config = pydantic.ConfigDict(extra="forbid")

    op: Literal["all", "any"]
    children: list["ConditionNode"] = pydantic.Field(min_length=1)


# A leaf is tried first: it is the only variant with an `event` key, so the union cannot be
# ambiguous, and a node that is neither shape is a 422 naming both.
ConditionNode = Annotated[
    Union[EventCondition, BranchCondition],
    pydantic.Field(union_mode="left_to_right"),
]

BranchCondition.model_rebuild()


class SubscriptionCreateRequest(pydantic.BaseModel):
    """A new subscription. `created_by` is stamped from the caller, never accepted here."""

    model_config = pydantic.ConfigDict(extra="forbid")

    name: _SubscriptionName
    condition: ConditionNode
    # The target, spelled exactly as scheduling/pipelines/api_routes.py spells it, so one grep
    # finds every API that points at a user pipeline. Required, mirroring the NOT NULL on the
    # column: a subscription that starts nothing is not a subscription.
    #
    # Bounded by the column's own width rather than the generic string maximum, so an
    # over-long id is a 422 here instead of a "Data too long" 500 at the insert.
    pipeline_task_spec_from_user_pipeline_id: str = pydantic.Field(
        min_length=1, max_length=user_pipeline_db_models.PIPELINE_ID_LENGTH
    )
    # The version, as the pipelines API publishes it. Omitted means track the pipeline's
    # current version, whichever that is when the trigger fires -- and all a pipeline without
    # version history can offer.
    pipeline_task_spec_from_user_pipeline_version_key: str | None = pydantic.Field(
        default=None,
        min_length=user_pipeline_db_models.DIGEST_LENGTH,
        max_length=user_pipeline_db_models.DIGEST_LENGTH,
    )

    # The argument templates, in the same `{"arguments": {...}}` envelope the schedules API
    # takes. `extra="forbid"` above is why this field has to exist before a client may send
    # it: on a schedule an unknown top-level key is ignored, here it is a 422.
    pipeline_templates: dict[str, Any] | None = None

    @pydantic.model_validator(mode="after")
    def _walkable(self) -> "SubscriptionCreateRequest":
        _reject_what_the_evaluator_cannot_walk(condition=self.condition)
        return self


class SubscriptionUpdateRequest(pydantic.BaseModel):
    """An edit. Every field is optional, so a metadata-only change need not resend a condition.

    A supplied `condition` replaces the stored one wholesale rather than merging into it; the
    event set is reconciled against what the edit leaves.
    """

    model_config = pydantic.ConfigDict(extra="forbid")

    name: _SubscriptionName | None = None
    condition: ConditionNode | None = None
    enabled: bool | None = None
    # Optional in the ordinary sense: the column is NOT NULL, so there is no "unset the
    # target" and None can only mean omitted.
    pipeline_task_spec_from_user_pipeline_id: str | None = pydantic.Field(
        default=None,
        min_length=1,
        max_length=user_pipeline_db_models.PIPELINE_ID_LENGTH,
    )
    # Nullable *and* optional, which the rest of this model is not: null means unpin, omitted
    # means leave the pin alone. The two are only distinguishable through model_fields_set, so
    # the route asks that question rather than reading the attribute.
    pipeline_task_spec_from_user_pipeline_version_key: str | None = pydantic.Field(
        default=None,
        min_length=user_pipeline_db_models.DIGEST_LENGTH,
        max_length=user_pipeline_db_models.DIGEST_LENGTH,
    )

    # Omitted leaves the stored templates alone; `{"arguments": {}}` clears them. Two
    # different intents, which is why this is nullable rather than defaulted to an empty
    # envelope.
    pipeline_templates: dict[str, Any] | None = None

    def pin_edit(self) -> tuple[bool, str | None]:
        """(whether the pin was addressed at all, the version it was set to)."""
        return (
            "pipeline_task_spec_from_user_pipeline_version_key"
            in self.model_fields_set,
            self.pipeline_task_spec_from_user_pipeline_version_key,
        )

    @pydantic.model_validator(mode="after")
    def _walkable(self) -> "SubscriptionUpdateRequest":
        if self.condition is not None:
            _reject_what_the_evaluator_cannot_walk(condition=self.condition)
        return self


def definition_from(
    *,
    name: str,
    condition: ConditionNode,
    templates: collections.abc.Mapping[str, str],
) -> dict[str, Any]:
    """The `definition` blob to store: the validated condition, as authored.

    `mode="json"` with `exclude_unset` keeps the payload the caller actually sent -- an omitted
    `expire_seconds` stays omitted rather than being written back as an explicit null -- so the
    blob round-trips through an edit unchanged.

    Keys and shape: `db_models.DefinitionKey`.
    """
    return pipeline_templates.set_pipeline_templates(
        original={
            db_models.DefinitionKey.NAME: name,
            db_models.DefinitionKey.CONDITION: condition.model_dump(
                mode="json", exclude_unset=True
            ),
        },
        updates=templates,
    )


_CURSOR_SEPARATOR: Final[str] = "~"


def _encode_cursor(*, subscription: db_models.TriggerSubscription) -> str:
    updated_at = subscription.updated_at
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=datetime.timezone.utc)
    return f"{updated_at.isoformat()}{_CURSOR_SEPARATOR}{subscription.id}"


def _decode_cursor(*, cursor: str) -> tuple[datetime.datetime, str]:
    """The scheduler's keyset convention, `updated_at~id`, so one client idiom serves both."""
    if _CURSOR_SEPARATOR not in cursor:
        raise fastapi.HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Unrecognized page_token format: '{cursor}'. Expected 'updated_at~id' cursor.",
        )
    updated_at_str, subscription_id = cursor.split(_CURSOR_SEPARATOR, 1)
    try:
        updated_at = datetime.datetime.fromisoformat(updated_at_str)
    except ValueError:
        raise fastapi.HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Unrecognized page_token timestamp: '{updated_at_str}'",
        ) from None
    if updated_at.tzinfo is not None:
        updated_at = updated_at.astimezone(datetime.timezone.utc)
    return updated_at, subscription_id


class SubscriptionResponse(pydantic.BaseModel):
    """A subscription as the API returns it.

    `condition` is lifted out of the stored blob rather than echoed as `definition`, so the
    response shape is flat for a caller and the blob stays an implementation detail.
    """

    id: str
    name: str
    condition: dict[str, Any]
    enabled: bool
    cycle: int
    pipeline_task_spec_from_user_pipeline_id: str
    # The pin, spoken back in the vocabulary it arrived in. No lookup is needed to translate:
    # the resolver refuses the mutable head, and every other version row is keyed by its own
    # content, so a stored key is a version by construction.
    pipeline_task_spec_from_user_pipeline_version_key: str | None
    # False once the target user pipeline is soft-deleted. Computed per read, never stored:
    # the pipeline can be deleted long after this row was written, and the foreign key happily
    # keeps pointing at the tombstone. Writes already refuse a dead target, so without this a
    # reader sees `enabled: true` and a `missing` list on a subscription that can no longer
    # start anything -- the row looks armed and is not.
    target_pipeline_live: bool
    # Always the full envelope, never omitted, so a client can tell "no templates" from an
    # older server that does not know the field.
    pipeline_templates: dict[str, Any] = pydantic.Field(default_factory=dict)
    created_by: str
    created_at: datetime.datetime
    updated_at: datetime.datetime


def _to_response(
    *, subscription: db_models.TriggerSubscription, target_pipeline_live: bool
) -> SubscriptionResponse:
    return SubscriptionResponse(
        id=subscription.id,
        name=subscription.name,
        condition=subscription.definition[db_models.DefinitionKey.CONDITION],
        enabled=subscription.enabled,
        cycle=subscription.cycle,
        pipeline_task_spec_from_user_pipeline_id=(
            subscription.pipeline_task_spec_from_user_pipeline_id
        ),
        pipeline_task_spec_from_user_pipeline_version_key=(
            subscription.pipeline_task_spec_from_user_pipeline_version_key
        ),
        target_pipeline_live=target_pipeline_live,
        pipeline_templates={
            envelopes.ARGUMENTS: pipeline_templates.get_pipeline_templates(
                original=subscription.definition
            )
        },
        created_by=subscription.created_by,
        created_at=subscription.created_at,
        updated_at=subscription.updated_at,
    )


def _target_pipeline_is_live(
    *, session: orm.Session, subscription: db_models.TriggerSubscription
) -> bool:
    """Whether this subscription's target pipeline is still there.

    One row's worth of the batched question, kept separate so the single-subscription routes
    read the same source of truth the listing does.
    """
    target = subscription.pipeline_task_spec_from_user_pipeline_id
    return target in user_pipeline_services.live_pipeline_ids(
        session=session, pipeline_ids=[target]
    )


def _caller(*, user_details: api_router.UserDetails) -> service.Caller:
    """The service's view of who is asking.

    `permissions` is a plain dict, so a missing `admin` key reads as not an admin rather than
    raising — the same `.get` the scheduler and `app.ensure_admin_user` use.
    """
    return service.Caller(
        name=user_details.name,
        is_admin=bool(user_details.permissions.get("admin")),
    )


def _forbidden(*, error: service.NotAuthorized) -> fastapi.HTTPException:
    """Turn the service's domain refusal into the 403 the scheduler's routes already return.

    The message is the service's, which already names the creator and the caller.
    """
    return fastapi.HTTPException(
        status_code=status.HTTP_403_FORBIDDEN, detail=str(error)
    )


def _conflict(*, error: service.NameTaken) -> fastapi.HTTPException:
    """Turn the natural key's refusal into a 409 rather than the 500 it would be uncaught.

    409 is the status for a request that is well-formed but clashes with what is already
    there — distinct from the 422 a malformed body gets, and from the 500 that says the server
    broke. It also tells the caller the retry is pointless until they change something, which
    a 500 does not: a 500 invites a loop that will fail identically forever.

    The message is the service's, which already names the name and the per-creator scope.
    """
    return fastapi.HTTPException(
        status_code=status.HTTP_409_CONFLICT, detail=str(error)
    )


def _pipeline_problem(
    *, error: user_pipeline_errors.PipelineError
) -> fastapi.HTTPException:
    """The HTTP answer to a bad pipeline target or a bad pin.

    Two statuses, and the split is deliberate. A pipeline that is missing, soft-deleted, or
    someone else's is a 404 -- the same answer for all three, so the response cannot be used to
    discover which other tenants own which pipeline ids. Anything else is the caller naming a
    version that does not exist or cannot be pinned, which is a 422: the pipeline was found, so
    saying so costs nothing and the message is actionable.

    Mapped here rather than left to the domain's own exception handlers, because those are
    registered by the application and these routes are mounted into test apps that are not it.
    Unmapped, a bad pin reaches the caller as the composite foreign key's IntegrityError -- a
    500 for what is a well-formed request with a wrong value in it.
    """
    if isinstance(error, user_pipeline_errors.PipelineNotFoundError):
        return fastapi.HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=str(error)
        )
    return fastapi.HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(error)
    )


def _load(
    *,
    session: orm.Session,
    subscription_id: str,
) -> db_models.TriggerSubscription:
    """The subscription, or a 404. One place, so every route words it the same way.

    Unlocked, on every route including the ones that go on to write: a writer hands what this
    returns to `_lock_for_write`, which authorizes first and locks after.
    """
    subscription = session.get(db_models.TriggerSubscription, subscription_id)
    if subscription is None:
        raise fastapi.HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Subscription not found",
        )
    return subscription


def _lock_for_write(
    *,
    session: orm.Session,
    subscription: db_models.TriggerSubscription,
    caller: service.Caller,
    action: str,
) -> db_models.TriggerSubscription:
    """Authorize on the row as read, then take the write lock on it and return it re-read.

    Two things in one place because their order is the point.

    *Authorize first.* Found by review: locking before the authorization check leaves a caller
    who is about to be told "not yours" holding a `SELECT ... FOR UPDATE` on somebody else's
    subscription until the request unwinds — and on that row every arriving emission blocks.
    The check needs nothing the lock provides: it reads `created_by`, which no route can patch,
    so there is no answer the lock would stop from changing underneath it.

    *Lock second, and through the shared helper.* `update_subscription` and `delete_subscription`
    both write trigger_event_state, and an arriving emission locks the subscription before it
    touches those same rows. A write that never took this lock would approach the pair from the
    opposite end, which is the shape a deadlock needs. The plain read above takes no lock, so it
    does not enter that ordering.

    The lock is a second statement, so the row can be deleted between the two. That comes back
    as `None` and 404s — the right answer, and the same one the request would have got a moment
    earlier.
    """
    try:
        service.ensure_may_write(
            subscription=subscription, caller=caller, action=action
        )
    except service.NotAuthorized as error:
        # Translated here as well as in `_apply_update`/`_apply_delete`, because the check now
        # runs before them and an untranslated domain error is a 500. The service keeps its own
        # call: this one decides *when* the refusal happens, not who enforces it.
        raise _forbidden(error=error) from error
    locked = service.lock_subscription_until_commit(
        session=session, subscription_id=subscription.id
    )
    if locked is None:
        raise fastapi.HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Subscription not found",
        )
    return locked


def _load_by_natural_key(
    *,
    session: orm.Session,
    name: str,
    created_by: str,
) -> db_models.TriggerSubscription:
    """The subscription a creator filed under this name, or a 404.

    `(created_by, name)` is the natural key — `uq_trigger_subscription_created_by_name` in
    `db_models` — so `one_or_none` cannot raise here. That holds on every dialect for the same
    reason it holds in the constraint: whatever collation decides two names are equal is the
    collation the unique index was built with, so a pair this query would find ambiguous is a
    pair the database refused to store in the first place.

    A name filed by somebody else is a 404 rather than a 403, matching `_load`: what exists
    under another creator is not something a failed lookup should confirm.

    Unlocked, like `_load` and for the same reason: the name cannot be locked at all — the lock
    every writer shares is keyed by id — so a write resolves the row here and hands it to
    `_lock_for_write`, which authorizes and then locks by the id this found.
    """
    subscription = session.scalars(
        sqlalchemy.select(db_models.TriggerSubscription).where(
            db_models.TriggerSubscription.created_by == created_by,
            db_models.TriggerSubscription.name == name,
        )
    ).one_or_none()
    if subscription is None:
        raise fastapi.HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No subscription named '{name}' for '{created_by}'",
        )
    return subscription


def _reject_inexact_key(
    *,
    subscription: db_models.TriggerSubscription,
    name: str,
    created_by: str,
) -> None:
    """Refuse a write whose key matched only because the database ignores case.

    Whether `?name=nightly` resolves a row stored as `Nightly` depends on the dialect:

    - **MySQL** (`utf8mb4_0900_ai_ci`) — case- *and* accent-insensitive, so it matches.
    - **PostgreSQL** (deterministic collation, e.g. `en_US.utf8`) — case-sensitive, so it 404s.
    - **SQLite** (`BINARY`) — case-sensitive, so it 404s.

    MySQL is the outlier and production is what runs it, so the local 404 is the misleading one.
    There is not single SQL agnostic way to use WHERE clause, closest thing would be COLLATE,
    but that still depends on the dialect.

    A read may live with that asymmetry: the caller gets the row back and can see which one it
    is. A PATCH or a DELETE may not. The same command would edit a different subscription
    depending on the dialect underneath it, and the destructive half of that is not
    recoverable — so the write half of this family insists the key is byte-exact and leaves the
    read half permissive.

    409 rather than 404 because something *was* found; echoing the stored spelling is what
    makes the corrected retry obvious rather than a guessing game.
    """
    if subscription.name == name and subscription.created_by == created_by:
        return
    raise fastapi.HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail=(
            f"No subscription is stored as '{created_by}'/'{name}'; the key matched "
            f"'{subscription.created_by}'/'{subscription.name}', which differs only by case or "
            f"accent. Give the name exactly as stored to edit or delete it."
        ),
    )


def _detail_response(
    *,
    session: orm.Session,
    subscription: db_models.TriggerSubscription,
) -> "SubscriptionDetailResponse":
    """A subscription plus what it is still waiting for.

    Shared by both ways of addressing one, so the detail a caller reads back cannot depend on
    whether they arrived by id or by natural key.

    Everything down to `updated_at` is the stored row. `live`, `missing` and the two
    `last_triggered_*` fields are computed per call — the last two are both null for a
    subscription that has never fired.
    """
    now = db_utils.utc_now()
    emitted = event_state.events_emitted(
        session=session, subscription_id=subscription.id, now=now
    )
    condition = subscription.definition[db_models.DefinitionKey.CONDITION]
    # Two columns, not the row: `matched_events` and `triggered_by` snapshot the whole
    # definition, and nothing below reads them.
    #
    # `limit(1)` and `.first()` are not redundant. `limit(1)` is the SQL half — it puts LIMIT 1
    # in the statement so the database stops at the newest cycle instead of sorting and
    # returning every history row. `.first()` is the Python half — `session.execute` hands back
    # a Result cursor rather than a row, so something still has to pull the single Row out of
    # it, and yield None for a subscription that has never triggered.
    last = session.execute(
        sqlalchemy.select(
            db_models.TriggerHistory.cycle, db_models.TriggerHistory.created_at
        )
        .where(db_models.TriggerHistory.subscription_id == subscription.id)
        .order_by(db_models.TriggerHistory.cycle.desc())
        .limit(1)
    ).first()
    base = _to_response(
        subscription=subscription,
        target_pipeline_live=_target_pipeline_is_live(
            session=session, subscription=subscription
        ),
    )
    return SubscriptionDetailResponse(
        **base.model_dump(),
        live=dict(emitted),
        missing=event_state.missing(condition=condition, emitted=emitted),
        last_triggered_cycle=last.cycle if last else None,
        last_triggered_at=last.created_at if last else None,
    )


def _apply_update(
    *,
    session: orm.Session,
    subscription: db_models.TriggerSubscription,
    request: SubscriptionUpdateRequest,
    caller: service.Caller,
    pipeline_service: user_pipeline_services.UserPipelineService | None = None,
) -> "SubscriptionUpdateResponse":
    """Apply an edit to an already-resolved subscription.

    Shared by both ways of addressing one, so whatever the service decides about re-evaluating
    holds however the row was found.

    The three fields are passed through as the caller sent them, `None` and all: deciding what
    an omitted field means belongs to the service, which is the only place that can compare a
    supplied condition against the stored one.
    """
    # Read before the commit. Once a commit has failed, touching an attribute on an expired
    # instance goes back to the database for it and raises again -- so the id the 404 needs
    # has to be in hand before anything can go wrong.
    subscription_id = subscription.id
    try:
        result = service.update_subscription(
            session=session,
            subscription=subscription,
            caller=caller,
            name=request.name,
            enabled=request.enabled,
            # Dumped with the same options `definition_from` uses on the create path, so a
            # caller who resends the condition unchanged produces a blob that compares equal
            # to the stored one and is correctly read as "no change".
            condition=(
                request.condition.model_dump(mode="json", exclude_unset=True)
                if request.condition is not None
                else None
            ),
            pipeline_task_spec_from_user_pipeline_id=(
                request.pipeline_task_spec_from_user_pipeline_id
            ),
            # The pair, not the attribute: `null` unpins and omitted leaves the pin alone, and
            # only `model_fields_set` can tell those apart.
            pin_edit=request.pin_edit(),
            # Validated here rather than in the service, because rejecting a template is a 422
            # and the service raises nothing HTTP. None stays None: an envelope naming no
            # `arguments` leaves the stored templates alone, and only the service acts on that.
            templates=(
                envelopes.validated_arguments(
                    envelope=request.pipeline_templates,
                    kind=sources.Kind.SUBSCRIPTION,
                )
                if envelopes.names_arguments(envelope=request.pipeline_templates)
                else None
            ),
            **(
                {"pipeline_service": pipeline_service}
                if pipeline_service is not None
                else {}
            ),
        )
        session.commit()
        session.refresh(subscription)
    except service.NotAuthorized as error:
        raise _forbidden(error=error) from error
    except service.NameTaken as error:
        raise _conflict(error=error) from error
    except user_pipeline_errors.PipelineError as error:
        raise _pipeline_problem(error=error) from error
    except (orm.exc.StaleDataError, sqlalchemy.exc.InvalidRequestError):
        # Somebody deleted the row between `_load` and here. Which statement notices is a
        # matter of timing, which is why the whole edit is inside the `try` and not just the
        # commit. Both of these were measured against a concurrent DELETE in
        # `TestPatchRacesDelete`:
        #
        #   - the delete lands before the edit reaches the database -> the UPDATE matches no
        #     rows and the flush inside `service._flush_or_name_taken` raises
        #     `StaleDataError: expected to update 1 row(s); 0 were matched`. `commit` is the
        #     same statement one step later, so it raises the same thing;
        #   - it lands after the edit is flushed -> there is nothing left to write and
        #     `refresh` raises `InvalidRequestError: Could not refresh instance`.
        #
        # Every one of them is an ordinary "it's gone" rather than a server fault: a caller
        # who had sent the same request a moment later would have been 404'd by `_load`.
        session.rollback()
        # Raises that 404 if the row really is gone. `InvalidRequestError` is a broad base
        # class, so anything else wearing it -- a genuine misuse of the session -- finds the
        # row still there and is re-raised untouched rather than mislabelled a 404.
        _load(session=session, subscription_id=subscription_id)
        raise
    # After the write, not before: a PATCH can repoint the subscription, and the caller wants
    # to hear about the target they now have.
    base = _to_response(
        subscription=subscription,
        target_pipeline_live=_target_pipeline_is_live(
            session=session, subscription=subscription
        ),
    )
    return SubscriptionUpdateResponse(
        **base.model_dump(),
        triggered=result.triggered,
        # Only when a run actually started: `maybe_trigger` reports the current cycle
        # whatever it decides, so copying it unconditionally would claim cycle 0 was
        # triggered on a PATCH that disabled the subscription.
        triggered_cycle=result.cycle if result.triggered else None,
        reason=result.reason,
        # None all the way through, rather than an empty list: an edit that never evaluated
        # the condition -- a rename, a switch-off, a PATCH on a disabled subscription -- has no
        # answer to give, and `[]` would be read as "waiting for nothing". The detail route is
        # where a caller asks that question of the row's current state.
        missing=None if result.missing is None else list(result.missing),
        pipeline_run_id=result.pipeline_run_id,
    )


def _apply_delete(
    *,
    session: orm.Session,
    subscription: db_models.TriggerSubscription,
    caller: service.Caller,
) -> None:
    """Delete an already-resolved subscription. Shared by both ways of addressing one."""
    try:
        service.delete_subscription(
            session=session, subscription=subscription, caller=caller
        )
    except service.NotAuthorized as error:
        raise _forbidden(error=error) from error
    session.commit()


def _depth(*, condition: object) -> int:
    """How many levels the tree has, counting a bare leaf as one.

    Walked with an explicit stack rather than by recursion. A recursive version would in fact
    be safe today -- pydantic refuses anything past 253 before this function is ever called --
    but that is a coincidence of another library's guard, and this is the guard that is
    supposed to make the depth safe.
    """
    deepest = 0
    stack: list[tuple[object, int]] = [(condition, 1)]
    while stack:
        node, level = stack.pop()
        deepest = max(deepest, level)
        if not isinstance(node, dict):
            continue
        children = node.get("children")
        if isinstance(children, list):
            stack.extend((child, level + 1) for child in children)
    return deepest


def _too_deep(*, depth: int) -> str:
    """The one sentence both depth checks refuse with, so they cannot drift apart."""
    return f"condition nests {depth} levels deep, more than the {_MAX_CONDITION_DEPTH} allowed"


async def _refuse_a_condition_too_deep_to_validate(
    request: fastapi.Request,
) -> None:
    """Apply `_MAX_CONDITION_DEPTH` to the raw body, before pydantic recurses into it.

    `_reject_what_the_evaluator_cannot_walk` applies the same cap, but only to a model pydantic
    managed to build. Past 253 levels pydantic-core's own guard trips first, and the error it
    raises carries the whole payload under `input`; rendering that echo costs the encoder one
    stack frame per level, so from ~485 the refusal is what 500s. Refused here it is the same
    sentence with nothing deep attached to render.

    Wired as a route dependency rather than as an app-wide exception handler so that no other
    route's 422 changes shape. Costs one walk and not a second parse: FastAPI has already
    parsed the body by the time a dependency runs (`fastapi/routing.py:439`) and the result is
    cached on the request (`starlette/requests.py:249`).

    Raises:
        fastapi.exceptions.RequestValidationError: rendered by FastAPI as an ordinary 422, so a
            caller cannot tell this cap from one pydantic applied itself.
    """
    # An empty body is FastAPI's to complain about, and asking for `.json()` on one raises.
    if not await request.body():
        return
    body = await request.json()
    if not isinstance(body, dict) or "condition" not in body:
        return
    depth = _depth(condition=body["condition"])
    if depth > _MAX_CONDITION_DEPTH:
        raise fastapi.exceptions.RequestValidationError(
            [
                {
                    "type": "value_error",
                    "loc": ("body", "condition"),
                    # Prefixed as pydantic prefixes a `ValueError` from a validator, so the two
                    # refusals read identically.
                    "msg": f"Value error, {_too_deep(depth=depth)}",
                }
            ]
        )


# Belongs on every route that takes a `condition` in its body, and on no others: the three
# below are the whole set. A route that grows a condition body later has to add it too, which
# is the cost of scoping the guard instead of registering it app-wide.
_CONDITION_DEPTH_GUARD = fastapi.Depends(_refuse_a_condition_too_deep_to_validate)


def _reject_what_the_evaluator_cannot_walk(*, condition: "ConditionNode") -> None:
    """Run the checks the field rules cannot, so nothing that validates here breaks later.

    Two of them, both about the tree as a whole rather than one node.

    The evaluator's own: the field rules accept an event named twice with two different
    expiries, which is a shape `evaluation.event_expiries` refuses. Left to the service that
    raise is a plain `ValueError` inside a request — a 500 for what is a bad payload. Calling
    the evaluator here makes it a 422, and reusing its function rather than restating the rule
    is what stops the boundary drifting from the walker it guards.

    And the API's own: `_MAX_CONDITION_DEPTH`, whose comment explains what an uncapped tree
    does downstream. That one is not an evaluator rule — the evaluator walks a 900-deep tree
    without complaint. Over HTTP the same cap has already been applied to the raw body by
    `_refuse_a_condition_too_deep_to_validate`, which is the copy that matters because it runs
    before pydantic; this one is what holds for a model built directly, in a test or a caller
    that never went through a route.

    Raises:
        ValueError: which pydantic turns into a validation error, and FastAPI into a 422.
    """
    blob = condition.model_dump(mode="json", exclude_unset=True)
    depth = _depth(condition=blob)
    if depth > _MAX_CONDITION_DEPTH:
        raise ValueError(_too_deep(depth=depth))
    evaluation.event_expiries(condition=blob)


def setup_trigger_routes(
    *,
    app: fastapi.FastAPI,
    get_session: (
        collections.abc.Callable[..., orm.Session]
        | collections.abc.Callable[..., collections.abc.Iterator[orm.Session]]
    ),
    user_details_getter: collections.abc.Callable[..., api_router.UserDetails],
    pipeline_service: user_pipeline_services.UserPipelineService | None = None,
) -> None:
    """Mount the trigger configuration API on `app`.

    Mirrors `scheduling.pipelines.api_routes.setup_pipeline_schedule_routes`: the session and
    the caller arrive as dependencies, so this module is mountable without importing the app.
    """
    router = fastapi.APIRouter()

    @router.post(
        _API_BASE,
        status_code=status.HTTP_201_CREATED,
        tags=[_TAG],
        dependencies=[_CONDITION_DEPTH_GUARD],
    )
    def create_subscription(
        request: SubscriptionCreateRequest,
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> SubscriptionResponse:
        """Create a subscription and open its event states.

        `created_by` comes from the authenticated caller. A request body carrying its own
        `created_by` never reaches here: `extra="forbid"` on the request model makes it a 422.
        """
        definition = definition_from(
            name=request.name,
            condition=request.condition,
            templates=envelopes.validated_arguments(
                envelope=request.pipeline_templates,
                kind=sources.Kind.SUBSCRIPTION,
            ),
        )
        try:
            subscription = service.create_subscription(
                session=session,
                definition=definition,
                pipeline_task_spec_from_user_pipeline_id=(
                    request.pipeline_task_spec_from_user_pipeline_id
                ),
                pipeline_task_spec_from_user_pipeline_version_key=request.pipeline_task_spec_from_user_pipeline_version_key,
                caller=_caller(user_details=user_details),
            )
        except service.NameTaken as error:
            raise _conflict(error=error) from error
        except user_pipeline_errors.PipelineError as error:
            raise _pipeline_problem(error=error) from error
        session.commit()
        session.refresh(subscription)
        # A create refuses a dead target, so this is True by construction -- reported anyway,
        # because a field that only some routes carry is a field callers have to special-case.
        return _to_response(
            subscription=subscription,
            target_pipeline_live=_target_pipeline_is_live(
                session=session, subscription=subscription
            ),
        )

    @router.get(_API_BASE, tags=[_TAG])
    def list_subscriptions(
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
        page_size: int = fastapi.Query(default=10, ge=1, le=100),
        page_token: str | None = fastapi.Query(default=None),
        enabled: bool | None = fastapi.Query(default=None),
        event_name: str | None = fastapi.Query(
            default=None, max_length=_MAX_NAME_LENGTH
        ),
        name_contains: str | None = fastapi.Query(
            default=None, max_length=_MAX_NAME_LENGTH
        ),
    ) -> SubscriptionListResponse:
        """A page of subscriptions, newest edit first.

        Reads are open: a subscription is a scheduling rule a team needs to reason about, and
        the listing is never filtered by creator. `user_details` is present because the caller
        must still be authenticated — `get_user_details` raises 401 when it cannot name one.

        Two predicates here are O(rows) on purpose, and both are bounded by the same fact:
        `trigger_subscription` is a configuration table, one row per subscription a person
        wrote by hand, so it is sized in hundreds, not in merchant-scale millions.

          - `name_contains` is a substring `LIKE '%x%'`. A leading wildcard cannot use a
            B-tree, which is also why `name` carries no index (see `db_models`). The wildcards
            are the query's own: the caller's text is escaped, so it is matched literally.
          - `total_count` runs a COUNT over everything the filters match, matching the
            scheduler's list contract (`scheduling/pipelines/api_routes.py:338`).

        `next_page_token` is not one of the two: it comes from a `page_size + 1` probe, so a
        final page that happens to be exactly `page_size` long ends the walk instead of
        advertising an empty page after it. This is the one place the shape deliberately
        differs from the scheduler, which reads `>= page_size` off the page it returns
        (`scheduling/pipelines/api_routes.py:343`) and so spends that extra request.

        If this table ever stops being human-authored — bulk-created subscriptions, one per
        shop — both need to change before it becomes an incident: substring search wants a
        real search index, and `total_count` wants dropping in favour of the probe that is
        already here, since by then the exact total is the only thing still counting rows.
        """
        filters = []
        if enabled is not None:
            filters.append(db_models.TriggerSubscription.enabled == enabled)
        if name_contains:
            # autoescape: the parameter is a literal substring, so `%` and `_` are characters
            # to search for, not wildcards. Without it `?name_contains=%` matches every row
            # and `total_count` reports the unfiltered total beside it.
            filters.append(
                db_models.TriggerSubscription.name.contains(
                    name_contains, autoescape=True
                )
            )
        if event_name:
            # A subscription matches if any of its event states names this event. EXISTS rather
            # than a JOIN, so a condition mentioning the event once cannot duplicate the row.
            filters.append(
                db_models.TriggerSubscription.id.in_(
                    sqlalchemy.select(
                        db_models.TriggerEventState.subscription_id
                    ).where(db_models.TriggerEventState.event_name == event_name)
                )
            )

        query = sqlalchemy.select(db_models.TriggerSubscription).where(*filters)
        if page_token:
            cursor_updated_at, cursor_id = _decode_cursor(cursor=page_token)
            query = query.where(
                sqlalchemy.tuple_(
                    db_models.TriggerSubscription.updated_at,
                    db_models.TriggerSubscription.id,
                )
                < sqlalchemy.tuple_(
                    sqlalchemy.literal(cursor_updated_at),
                    sqlalchemy.literal(cursor_id),
                )
            )
        query = query.order_by(
            db_models.TriggerSubscription.updated_at.desc(),
            db_models.TriggerSubscription.id.desc(),
        ).limit(page_size + 1)

        # One row more than asked for. The extra is never returned and never encoded into a
        # cursor; it exists only to answer "is there a next page" without a second query. Read
        # off the returned page instead, a full last page is indistinguishable from a full
        # middle one, and a client paging until the token is absent pays a final empty request.
        rows = list(session.scalars(query).all())
        has_more = len(rows) > page_size
        subscriptions = rows[:page_size]
        # Counts what the filters match, so a filtered page's total is about the filtered set.
        # This differs from the scheduler, whose list has no filters to respect.
        total_count = session.scalar(
            sqlalchemy.select(
                sqlalchemy.func.count(db_models.TriggerSubscription.id)
            ).where(*filters)
        )
        # Keyed off the last row actually returned, not the probe: the probe is the first row
        # of the next page, and encoding it would skip it.
        next_page_token = (
            _encode_cursor(subscription=subscriptions[-1]) if has_more else None
        )
        # One query for the whole page rather than one per row: liveness is the only part of
        # a listed subscription that lives in another table.
        live_targets = user_pipeline_services.live_pipeline_ids(
            session=session,
            pipeline_ids=[
                s.pipeline_task_spec_from_user_pipeline_id for s in subscriptions
            ],
        )
        return SubscriptionListResponse(
            subscriptions=[
                _to_response(
                    subscription=s,
                    target_pipeline_live=(
                        s.pipeline_task_spec_from_user_pipeline_id in live_targets
                    ),
                )
                for s in subscriptions
            ],
            total_count=total_count or 0,
            next_page_token=next_page_token,
        )

    # --- addressing a subscription by its natural key ---------------------------------
    #
    # These three are registered *before* the `{subscription_id}` family, and the order is
    # load-bearing. `/lookup` and an id are both a single path segment, so Starlette matches
    # them in registration order: declared second, the literal route is shadowed and every
    # lookup instead arrives at `get_subscription` as an id that cannot exist, i.e. a 404 with
    # a misleading message. Nothing warns about this at import time, so the ordering is pinned
    # by a test rather than by this comment alone.
    #
    # The key travels in the query string rather than in the path because neither half of it is
    # constrained: `name` is any 1-255 characters, and `created_by` is whatever the
    # authenticator calls a user, in practice an email. A name containing `/` would need `%2F`
    # in a path segment, which proxies routinely normalise or reject; a query parameter carries
    # the same characters through untouched.
    #
    # `name` is stripped here exactly as it is on the write side, so both halves of the key are
    # spelled the same way by the time `_reject_inexact_key` compares them.

    @router.get(_LOOKUP_PATH, tags=[_TAG])
    def get_subscription_by_name(
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
        *,
        name: _NameQuery,
        created_by: str | None = fastapi.Query(
            default=None, max_length=_MAX_NAME_LENGTH
        ),
    ) -> SubscriptionDetailResponse:
        """One subscription, addressed by `(created_by, name)`. Open to any caller.

        `created_by` defaults to the caller, so the everyday request is `?name=nightly`. Naming
        somebody else's is allowed, because reads are open here exactly as they are on the
        listing, which already returns every row's `created_by`.
        """
        caller = _caller(user_details=user_details)
        subscription = _load_by_natural_key(
            session=session,
            name=name,
            created_by=created_by if created_by is not None else caller.name,
        )
        return _detail_response(session=session, subscription=subscription)

    @router.patch(_LOOKUP_PATH, tags=[_TAG], dependencies=[_CONDITION_DEPTH_GUARD])
    def update_subscription_by_name(
        request: SubscriptionUpdateRequest,
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
        *,
        name: _NameQuery,
        created_by: str | None = fastapi.Query(
            default=None, max_length=_MAX_NAME_LENGTH
        ),
    ) -> SubscriptionUpdateResponse:
        """Edit the subscription filed under `(created_by, name)`. Creator or admin only.

        A body and a query string on one request is ordinary HTTP: the query selects the row,
        the body says what to change about it. The key must be byte-exact — see
        `_reject_inexact_key` for why the write half is stricter than the read half.
        """
        caller = _caller(user_details=user_details)
        owner = created_by if created_by is not None else caller.name
        subscription = _load_by_natural_key(
            session=session, name=name, created_by=owner
        )
        _reject_inexact_key(subscription=subscription, name=name, created_by=owner)
        return _apply_update(
            session=session,
            subscription=_lock_for_write(
                session=session,
                subscription=subscription,
                caller=caller,
                action="Update",
            ),
            request=request,
            caller=caller,
            **(
                {"pipeline_service": pipeline_service}
                if pipeline_service is not None
                else {}
            ),
        )

    @router.delete(
        _LOOKUP_PATH,
        status_code=status.HTTP_204_NO_CONTENT,
        tags=[_TAG],
    )
    def delete_subscription_by_name(
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
        *,
        name: _NameQuery,
        created_by: str | None = fastapi.Query(
            default=None, max_length=_MAX_NAME_LENGTH
        ),
    ) -> None:
        """Delete the subscription filed under `(created_by, name)`. Creator or admin only.

        The key must be byte-exact. This is the call `_reject_inexact_key` exists for: a
        case-insensitive match that deletes the wrong row leaves nothing to inspect afterwards.
        """
        caller = _caller(user_details=user_details)
        owner = created_by if created_by is not None else caller.name
        subscription = _load_by_natural_key(
            session=session, name=name, created_by=owner
        )
        _reject_inexact_key(subscription=subscription, name=name, created_by=owner)
        _apply_delete(
            session=session,
            subscription=_lock_for_write(
                session=session,
                subscription=subscription,
                caller=caller,
                action="Delete",
            ),
            caller=caller,
        )

    @router.get(f"{_API_BASE}/{{subscription_id}}", tags=[_TAG])
    def get_subscription(
        subscription_id: str,
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> SubscriptionDetailResponse:
        """One subscription, with what it is currently waiting for. Open to any caller."""
        subscription = _load(session=session, subscription_id=subscription_id)
        return _detail_response(session=session, subscription=subscription)

    @router.patch(
        f"{_API_BASE}/{{subscription_id}}",
        tags=[_TAG],
        dependencies=[_CONDITION_DEPTH_GUARD],
    )
    def update_subscription(
        subscription_id: str,
        request: SubscriptionUpdateRequest,
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> SubscriptionUpdateResponse:
        """Edit a subscription. The creator or an admin only.

        `created_by` is not patchable: `extra="forbid"` makes an attempt a 422 rather than a
        silently dropped field. A supplied condition replaces the stored one wholesale — the
        blob is reassigned, never mutated in place, so the edit round-trips.
        """
        caller = _caller(user_details=user_details)
        return _apply_update(
            session=session,
            subscription=_lock_for_write(
                session=session,
                subscription=_load(session=session, subscription_id=subscription_id),
                caller=caller,
                action="Update",
            ),
            request=request,
            caller=caller,
            **(
                {"pipeline_service": pipeline_service}
                if pipeline_service is not None
                else {}
            ),
        )

    @router.delete(
        f"{_API_BASE}/{{subscription_id}}",
        status_code=status.HTTP_204_NO_CONTENT,
        tags=[_TAG],
    )
    def delete_subscription(
        subscription_id: str,
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> None:
        """Delete a subscription. The creator or an admin only.

        Pending event states go with it; the history of what it started does not.
        """
        caller = _caller(user_details=user_details)
        _apply_delete(
            session=session,
            subscription=_lock_for_write(
                session=session,
                subscription=_load(session=session, subscription_id=subscription_id),
                caller=caller,
                action="Delete",
            ),
            caller=caller,
        )

    app.include_router(router)


class SubscriptionListResponse(pydantic.BaseModel):
    """A page of subscriptions. `total_count` counts what the filters match, not the whole table."""

    subscriptions: list[SubscriptionResponse]
    total_count: int
    next_page_token: str | None = None


class SubscriptionUpdateResponse(SubscriptionResponse):
    """A subscription after an edit, and what the edit did about the condition.

    An edit is judged against the state it leaves, so a change that removes the last unfilled
    event triggers on the spot. Reporting that here is what stops a run being a surprise.

    Attributes:
        triggered: whether this edit started a run.
        triggered_cycle: the cycle it claimed, when it did. Distinct from the row's `cycle`,
            which is the fence counter and has already been bumped past it.
        reason: why it did not trigger, when it did not. `user_pipeline_deleted` means the
            condition held but the target pipeline is gone; repointing this subscription at a
            live one re-evaluates and can still start the run.
        missing: what the condition is still waiting for, or None when this edit did not
            evaluate it. Three-valued on purpose, because two of the states are not the same
            question: `["a"]` is "waiting for a", `[]` is "evaluated, waiting for nothing",
            and null is "not evaluated -- ask the detail route". An edit evaluates when it
            changes the condition, re-enables the subscription, or re-points it at another
            pipeline; a rename, or an edit that switches it off, answers null -- in step with
            `reason`, which is already null on those paths.
        pipeline_run_id: the run this edit started, or None when it started none.
    """

    triggered: bool
    triggered_cycle: int | None = None
    reason: str | None = None
    missing: list[str] | None = None
    pipeline_run_id: str | None = None


class SubscriptionDetailResponse(SubscriptionResponse):
    """One subscription, plus what it is currently waiting for.

    `live` and `missing` are computed, not stored: freshness is a function of `now`, so a row
    that was live a minute ago may be missing on the next read without anything having written.

    Attributes:
        live: event name -> the emission that filled it, for arrivals that are still fresh.
        missing: the events the condition is still waiting for.
        last_triggered_cycle: the cycle of the most recent trigger, or None if it never has.
        last_triggered_at: when that trigger happened.
    """

    live: dict[str, str | None]
    missing: list[str]
    last_triggered_cycle: int | None = None
    last_triggered_at: datetime.datetime | None = None
