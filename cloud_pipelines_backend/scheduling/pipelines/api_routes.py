import collections.abc
import dataclasses
import datetime
import enum
import logging
from collections.abc import Mapping
from typing import Any, Final

import fastapi
import pydantic
import sqlalchemy
from apscheduler.triggers import cron
from sqlalchemy import orm
from starlette import status

from cloud_pipelines_backend import (
    api_router,
    component_structures,
    errors,
)
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend._compat import StrEnum
from cloud_pipelines_backend.scheduling.pipelines import (
    database_migrations,
    db_models,
    executor,
    schedule_paths,
    schedule_queries,
    services,
)
from cloud_pipelines_backend.templating.arguments import envelopes, sources
from cloud_pipelines_backend.user_pipelines import db_models as user_pipeline_db_models
from cloud_pipelines_backend.user_pipelines import errors as user_pipeline_errors
from cloud_pipelines_backend.user_pipelines import services as user_pipeline_services
from cloud_pipelines_backend.utils import pipeline_templates

logger = logging.getLogger(__name__)

MAX_DAILY_TRIGGERS = 50

_API_BASE: Final[str] = "/api/schedules/pipelines"
_TAG: Final[str] = "pipeline-scheduling"

_CURSOR_SEPARATOR: Final[str] = "~"

_user_pipeline_service = user_pipeline_services.UserPipelineService()


class SchedulerErrorCode(StrEnum):
    """Stable logical codes for a closed schema tier.

    Deliberately not physical index names: operators read the readiness report,
    callers get a code they can branch on. Leaking `uq_...`/`ix_...` would couple
    the public API to a schema detail and tell the caller nothing actionable.

    `StrEnum`, so the wire value is unchanged and every existing comparison
    against the literal string still holds -- these are a published contract.
    """

    SCHEDULE_PATH_WRITES_UNAVAILABLE = "schedule_path_writes_unavailable"
    PIPELINE_REFERENCE_WRITES_UNAVAILABLE = "pipeline_reference_writes_unavailable"
    SCHEDULE_PATH_LOCK_CONTENTION = "schedule_path_lock_contention"


# MySQL's two lock-failure codes, and the entire set this endpoint reinterprets.
#
# 1213 ER_LOCK_DEADLOCK      -- InnoDB chose this transaction as the victim.
# 1205 ER_LOCK_WAIT_TIMEOUT  -- the wait for a conflicting record lock expired.
#
# Both are guaranteed to have left NOTHING written, which is the whole basis for
# telling the caller the request is safe to repeat verbatim. No other
# `OperationalError` carries that guarantee: a dropped connection (2013) or an
# exhausted pool can fail with a write already applied and only the response
# lost. Those must keep escaping as a 500, because a 500 honestly says the
# outcome is unknown while a retry-safe 503 would be a promise the server cannot
# make.
_RETRY_SAFE_LOCK_ERRNOS: Final[frozenset[int]] = frozenset({1205, 1213})


class SchemaTier(enum.Enum):
    """A schema tier an operation depends on, bound to the code it reports.

    Two problems, one type. The gate used to take `tier: str` and resolve it with
    `getattr`, so `path_write_ready` for `path_writes_ready` was a 500 on the
    very path the gate exists to keep clean, invisible to a type checker and to
    an IDE rename. `is_ready` uses real attribute access instead, so a rename of
    `MigrationReport.path_writes_ready` now fails to type-check here.

    Tier and code also travelled as separate arguments, which made it possible to
    report a path-tier outage with the reference code. They cannot disagree now,
    because the code is derived from the tier.
    """

    SCHEDULE_PATH = enum.auto()
    PIPELINE_REFERENCE = enum.auto()

    @property
    def code(self) -> SchedulerErrorCode:
        if self is SchemaTier.SCHEDULE_PATH:
            return SchedulerErrorCode.SCHEDULE_PATH_WRITES_UNAVAILABLE
        return SchedulerErrorCode.PIPELINE_REFERENCE_WRITES_UNAVAILABLE

    def is_ready(self, *, schema_report: database_migrations.MigrationReport) -> bool:
        if self is SchemaTier.SCHEDULE_PATH:
            return schema_report.path_writes_ready
        return schema_report.reference_writes_ready


#: Retained as the public spelling used by callers and tests. They ARE the enum
#: members, so `== "schedule_path_writes_unavailable"` still holds.
SCHEDULE_PATH_WRITES_UNAVAILABLE: Final[SchedulerErrorCode] = (
    SchedulerErrorCode.SCHEDULE_PATH_WRITES_UNAVAILABLE
)
PIPELINE_REFERENCE_WRITES_UNAVAILABLE: Final[SchedulerErrorCode] = (
    SchedulerErrorCode.PIPELINE_REFERENCE_WRITES_UNAVAILABLE
)


def _require_schema_tier(
    *,
    schema_report: database_migrations.MigrationReport,
    tier: SchemaTier,
    feature: str,
) -> None:
    """Refuse an operation whose schema tier is not ready, with 503 and a code.

    503, never 409 or 422: the request is well-formed and the conflict is not with
    request state, so telling the caller to change its request could not help.

    **No ``Retry-After``.** Recovery is operator-controlled and not time-bounded,
    so any interval would be a false promise. Clients should apply bounded
    exponential backoff and then surface the code -- never retry forever.

    Most callers here are writes, but not all: the path-FILTERED read is gated
    too, and shares the path write tier rather than declaring a second one. It
    depends on the identical verified index, and duplicating that predicate
    would only invite the two copies to drift.

    An earlier version of this docstring said reads were deliberately never
    gated, on the grounds that a filtered lookup without the index merely scans.
    That was wrong, and it is still wrong now that the reason has moved to the
    other half of the identity: a path lookup is bounded to one row, and on a
    database where the PATH collation has not been converted yet the row it
    finds may sit at a neighbouring spelling, so the caller is told their own
    path does not exist. The failure is a confident wrong answer, not a slow
    one, and the gate is what makes the one-row bound sound.

    The unfiltered list IS still ungated -- it applies no path predicate, so the
    hazard cannot arise, and it keeps the data inspectable while the migration
    is outstanding. Its owner predicate needs no gate: owner comparison is the
    database's own and depends on no object this module installs.
    """
    if tier.is_ready(schema_report=schema_report):
        return
    raise fastapi.HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail={
            "code": tier.code.value,
            "message": (
                f"{feature} is temporarily unavailable because the scheduler"
                " database is not ready for it. This is an operator-resolved"
                " condition, not a problem with this request; retry with bounded"
                " backoff and surface this code if it persists."
            ),
        },
    )


def _encode_cursor(*, schedule: db_models.ScheduledPipelineRun) -> str:
    updated_at = schedule.updated_at
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=datetime.timezone.utc)
    return f"{updated_at.isoformat()}{_CURSOR_SEPARATOR}{schedule.id}"


def _decode_cursor(
    *,
    cursor: str,
) -> tuple[datetime.datetime, str]:
    if _CURSOR_SEPARATOR not in cursor:
        raise fastapi.HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"Unrecognized page_token format: '{cursor}'. Expected 'updated_at~id' cursor."
            ),
        )
    updated_at_str, schedule_id = cursor.split(_CURSOR_SEPARATOR, 1)
    updated_at = datetime.datetime.fromisoformat(updated_at_str)
    if updated_at.tzinfo is not None:
        updated_at = updated_at.astimezone(datetime.timezone.utc)
    return updated_at, schedule_id


class PipelineScheduleCreateRequest(pydantic.BaseModel):
    """A new schedule. Extras are refused rather than dropped.

    `extra="forbid"` matches the subscription request models. Without it a misspelled
    field is accepted and silently ignored -- `pipeline_template` for `pipeline_templates`
    returns 201 with no templates stored, and the mistake surfaces at the first fire as a
    run using the spec's default value.
    """

    model_config = pydantic.ConfigDict(extra="forbid")

    name: str = pydantic.Field(min_length=1, max_length=bts._STR_MAX_LENGTH)
    cron_expression: str = pydantic.Field(min_length=1, max_length=bts._STR_MAX_LENGTH)
    timezone: str = pydantic.Field(
        default=db_models.DEFAULT_TIMEZONE_UTC, max_length=bts._STR_MAX_LENGTH
    )
    # Exactly one source. `pipeline_task_spec` is no longer required because a
    # reference is now an equally first-class way to say what a schedule runs;
    # the one-source rule is enforced in `_validate_source` rather than by the
    # field types, so the error names the rule instead of listing field names.
    pipeline_task_spec: dict[str, Any] | None = None
    pipeline_task_spec_from_pipeline_run_id: str | None = pydantic.Field(
        default=None, max_length=bts._STR_MAX_LENGTH
    )
    pipeline_task_spec_from_user_pipeline_id: str | None = pydantic.Field(
        default=None, max_length=db_models.PIPELINE_ID_LENGTH
    )
    #: NULL means "follow the pipeline's current version". Non-NULL pins a
    #: digest, and pinning additionally requires the pipeline to be in FULL mode.
    pipeline_task_spec_from_user_pipeline_version_key: str | None = pydantic.Field(
        default=None, max_length=db_models.PIPELINE_VERSION_KEY_LENGTH
    )
    #: Optional, so this endpoint stays backward compatible for every existing
    #: caller. Omitted (or explicitly null) means *derive one*, not *store null*:
    #: every successful create stores a canonical non-null path either way, so the
    #: set of rows needing backfill stops growing.
    #:
    #: An explicitly supplied value is validated strictly and never repaired, so
    #: `""` or `"   "` is a 422 rather than being treated as omission -- a caller
    #: that sent the field meant to choose the identity.
    schedule_path: str | None = pydantic.Field(
        default=None, max_length=schedule_paths.MAX_RAW_SCHEDULE_PATH_LENGTH
    )
    #: Envelope, not a bare map: `{"arguments": {"as_of_date": "{{ schedule_time | date }}"}}`.
    #: `arguments` is the only key this version reads, and unknown siblings are ignored so
    #: a newer client can roll out against an older server. Omitted means "no templates";
    #: `{}` and null collapse to the same thing at the read boundary.
    pipeline_templates: dict[str, Any] | None = None


#: The create-only source fields, named here so PATCH can refuse them with a reason
#: instead of letting `extra="forbid"` call them "extra inputs". They are real fields on
#: the create model, so a caller sending one to PATCH has made a reasonable mistake.
_SOURCE_FIELDS_NOT_PATCHABLE: Final[tuple[str, ...]] = (
    "pipeline_task_spec_from_pipeline_run_id",
    "pipeline_task_spec_from_user_pipeline_id",
    "pipeline_task_spec_from_user_pipeline_version_key",
)


class PipelineScheduleUpdateRequest(pydantic.BaseModel):
    """An edit. Extras are refused, and a repoint attempt is refused with a reason.

    A PATCH naming a source field used to return 200 and do nothing, so the caller
    believed the schedule had been repointed. A schedule carries exactly one source and
    switching it would mean clearing the old column and the version pin together, which
    PATCH does not do.
    """

    model_config = pydantic.ConfigDict(extra="forbid")

    @pydantic.model_validator(mode="before")
    @classmethod
    def _refuse_source_transitions(cls, data: Any) -> Any:
        """Runs ahead of `extra="forbid"`, so these three get the truthful message and
        everything else gets pydantic's generic one."""
        if isinstance(data, Mapping):
            named = [field for field in _SOURCE_FIELDS_NOT_PATCHABLE if field in data]
            if named:
                raise ValueError(
                    f"{', '.join(named)}: a schedule takes its spec from exactly one"
                    " source, and PATCH changes neither which source that is nor the"
                    " version it is pinned to. Send pipeline_task_spec if this schedule"
                    " is inline-sourced, otherwise create a new schedule."
                )
        return data

    name: str | None = pydantic.Field(
        default=None, min_length=1, max_length=bts._STR_MAX_LENGTH
    )
    cron_expression: str | None = pydantic.Field(
        default=None, min_length=1, max_length=bts._STR_MAX_LENGTH
    )
    timezone: str | None = pydantic.Field(default=None, max_length=bts._STR_MAX_LENGTH)
    pipeline_task_spec: dict[str, Any] | None = None
    paused: bool | None = None
    #: Adoption only: settable while the stored value is NULL, and re-sending the
    #: same canonical value is a no-op. Changing an existing path is refused --
    #: it is a stable identity, so rewriting it would silently break any caller
    #: addressing the schedule by it. Source transitions are deliberately NOT
    #: available through PATCH in this change.
    schedule_path: str | None = pydantic.Field(
        default=None, max_length=schedule_paths.MAX_RAW_SCHEDULE_PATH_LENGTH
    )
    #: Envelope, not a bare map: `{"arguments": {"as_of_date": "{{ schedule_time | date }}"}}`.
    #: `arguments` is the only key this version reads, and unknown siblings are ignored so
    #: a newer client can roll out against an older server. Omitted leaves the stored
    #: templates alone; `{}` clears them, which is the only way to remove one.
    pipeline_templates: dict[str, Any] | None = None


class PipelineScheduleResponse(pydantic.BaseModel):
    id: str
    name: str
    cron_expression: str
    timezone: str
    pipeline_task_spec: dict[str, Any] | None = None
    pipeline_task_spec_from_pipeline_run_id: str | None = None
    pipeline_task_spec_from_user_pipeline_id: str | None = None
    # Emitted even when null, unlike `pipeline_task_spec`. The null is the
    # information: it is what distinguishes a current-following reference from a
    # pinned one, so omitting it would erase the distinction the column exists to
    # record. Same for `schedule_path`, where null means "no alternate identity"
    # and absence would be indistinguishable from an older server.
    pipeline_task_spec_from_user_pipeline_version_key: str | None = None
    schedule_path: str | None = None
    #: Always emitted, envelope and all, so a client can tell "no templates" from an
    #: older server that does not know the field.
    pipeline_templates: dict[str, Any] = pydantic.Field(default_factory=dict)
    paused: bool
    created_by: str
    created_at: datetime.datetime
    updated_at: datetime.datetime
    last_run_at: datetime.datetime | None = None
    last_run_submission_result: str | None = None
    next_run_at: str | None = None

    @pydantic.model_serializer(mode="wrap")
    def _omit_spec_when_none(
        self,
        handler: pydantic.SerializerFunctionWrapHandler,
    ) -> dict[str, Any]:
        data = handler(self)
        # Scoped to `pipeline_task_spec` on purpose. This omission exists because
        # the spec is bulky and suppressed on list responses; it is not a general
        # "drop nulls" policy, and extending it to the reference/path fields would
        # destroy the null-as-meaning semantics documented on them above.
        if data.get("pipeline_task_spec") is None:
            data.pop("pipeline_task_spec", None)
        return data


class PipelineScheduleListResponse(pydantic.BaseModel):
    schedules: list[PipelineScheduleResponse]
    total_count: int
    next_page_token: str | None = None


class PipelineScheduleTriggerResponse(pydantic.BaseModel):
    message: str
    schedule_id: str
    pipeline_run_response: dict[str, Any]


def _count_daily_triggers(
    *,
    trigger: cron.CronTrigger,
) -> int:
    """Count how many times a cron trigger fires in a 24-hour window."""
    now = datetime.datetime.now(datetime.timezone.utc)
    end = now + datetime.timedelta(days=1)
    count = 0
    fire_time = trigger.get_next_fire_time(None, now)
    while fire_time and fire_time < end:
        count += 1
        if count > MAX_DAILY_TRIGGERS:
            break
        fire_time = trigger.get_next_fire_time(fire_time, fire_time)
    return count


def _schedule_to_response(
    *,
    schedule: db_models.ScheduledPipelineRun,
    scheduler_svc: services.SchedulerService,
    include_spec: bool = False,
) -> PipelineScheduleResponse:
    return PipelineScheduleResponse(
        id=schedule.id,
        name=schedule.name,
        pipeline_task_spec=schedule.pipeline_task_spec if include_spec else None,
        pipeline_task_spec_from_pipeline_run_id=schedule.pipeline_task_spec_from_pipeline_run_id,
        pipeline_task_spec_from_user_pipeline_id=schedule.pipeline_task_spec_from_user_pipeline_id,
        pipeline_task_spec_from_user_pipeline_version_key=schedule.pipeline_task_spec_from_user_pipeline_version_key,
        schedule_path=schedule.schedule_path,
        pipeline_templates={
            envelopes.ARGUMENTS: pipeline_templates.get_pipeline_templates(
                original=schedule.settings
            )
        },
        cron_expression=schedule.cron_expression,
        timezone=schedule.timezone,
        paused=schedule.paused,
        created_by=schedule.created_by,
        created_at=schedule.created_at,
        updated_at=schedule.updated_at,
        last_run_at=schedule.last_run_at,
        last_run_submission_result=schedule.last_run_submission_result,
        next_run_at=scheduler_svc.get_next_run_time(schedule_id=schedule.id),
    )


def _check_ownership(
    *,
    session: orm.Session,
    schedule_id: str,
    created_by: str,
    user_details: api_router.UserDetails,
    action: str,
) -> None:
    """403 unless the caller owns this schedule (or is an admin).

    The comparison is the DATABASE's, and that is the change worth reading.
    This used to be `user_details.name != created_by` in Python, which folds
    nothing, while every path-addressed route scoped its SELECT and let the
    column's collation decide. Owner identity is not case-sensitive, so those
    two rules disagreeing meant ownership depended on how the caller addressed
    the row: `Jose@example.com` could list and PATCH their schedule by PATH and
    be 403'd on that same row by ID, told it "was created by jose@example.com,
    not Jose@example.com" -- naming them as a stranger to themselves. The CLI
    hit this as an id-route limitation before it was understood as this bug.

    It is not re-implemented more loosely; it is delegated, to
    `schedule_queries.is_owned_by`, the one predicate the list and the conflict
    classifier already use. Nothing in this process compares two owner names.

    Order matters and is deliberate:

    - Admin short-circuits FIRST, so an admin costs no probe and the bypass
      cannot be narrowed by an ownership rule changing underneath it.
    - The caller has ALREADY resolved the row and 404'd if it was absent. This
      only ever answers "is it theirs", never "does it exist", which is what
      keeps the 404-vs-403 split where the id routes have always had it: absent
      is 404, foreign is 403 naming the creator. A scoped read cannot tell those
      apart, so authorization is a second statement rather than a filter on the
      first.

    `created_by` is still taken, and is now used ONLY to phrase the refusal. It
    is deliberately not compared -- if it were, this would be back to two rules.

    `schedule_id` is taken rather than an entity so that a caller holding only
    identity columns applies the same rule as one holding the whole row.
    """
    if user_details.permissions.get("admin"):
        return
    if not schedule_queries.is_owned_by(
        session=session,
        schedule_id=schedule_id,
        created_by=user_details.name,
    ):
        raise fastapi.HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                f"{action} denied: schedule '{schedule_id}' was created by {created_by}, not {user_details.name}"
            ),
        )


def _owned_by_caller(
    *,
    user_details: api_router.UserDetails,
) -> sqlalchemy.ColumnElement[bool]:
    """The caller's own rows.

    A thin adapter from the request's identity to the shared predicate. The
    predicate itself lives in `schedule_queries.owned_by`, with the reasoning
    for why it delegates the comparison rather than making one, because the
    conflict classifier needs the same comparison and a second copy of it would
    eventually stop matching this one.
    """
    return schedule_queries.owned_by(created_by=user_details.name)


def _validate_cron(
    *,
    cron_expression: str,
    timezone: str,
) -> None:
    """Validate cron expression, timezone, and frequency (max 50 triggers/day)."""
    try:
        trigger = services.build_cron_trigger(
            cron_expression=cron_expression,
            timezone=timezone,
        )
    except Exception as e:
        raise fastapi.HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Invalid cron expression {cron_expression!r} or timezone {timezone!r}: {e}",
        )

    daily_count = _count_daily_triggers(trigger=trigger)
    if daily_count > MAX_DAILY_TRIGGERS:
        raise fastapi.HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Cron expression fires {daily_count} times per day, exceeding the maximum of {MAX_DAILY_TRIGGERS}",
        )


def _validate_pipeline_task_spec(
    *,
    pipeline_task_spec: dict[str, Any],
) -> None:
    """Reject a spec the executor could not parse, at write time.

    ``executor.execute_pipeline_schedule`` parses this column with
    ``TaskSpec.from_json_dict`` when the cron fires. Parsing it here too means a
    malformed spec fails the API call that created it, with the pydantic error
    attached, instead of failing silently hours later on a schedule nobody is
    watching.

    Validated as a ``TaskSpec`` rather than a ``ComponentSpec`` on purpose: every
    ``ComponentSpec`` field is optional, so that type accepts ``{}`` and would
    accept a bare pipeline too. ``TaskSpec.component_ref`` is the required field,
    and the one a client that confuses the two leaves out.
    """
    try:
        component_structures.TaskSpec.from_json_dict(pipeline_task_spec)
    except pydantic.ValidationError as e:
        raise fastapi.HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                "pipeline_task_spec is not a valid TaskSpec. It must be a root task"
                " — e.g. {'componentRef': {'spec': <pipeline>}} — not a bare pipeline"
                f" spec. Errors: {e.errors(include_url=False)}"
            ),
        ) from e
    except Exception as e:
        # ComponentReference.__post_init__ raises a plain TypeError when every
        # locator is absent, so a bare `{"componentRef": {}}` never reaches the
        # pydantic branch above.
        raise fastapi.HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"pipeline_task_spec is not a valid TaskSpec: {e}",
        ) from e


def _canonical_schedule_path(
    *,
    schedule_path: str,
) -> str:
    """Canonicalize, or 422 naming the broken rule.

    Used for **every** write and **every** lookup, so a client that sends
    ``Upi/Nightly`` writes and finds the same row on either backend.
    """
    try:
        return schedule_paths.canonicalize_schedule_path(schedule_path)
    except schedule_paths.SchedulePathValidationError as e:
        raise fastapi.HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=str(e),
        ) from e


def _validate_source(
    *,
    request: PipelineScheduleCreateRequest,
) -> None:
    """Enforce exactly one spec source, and the version-key dependency.

    Enforced here and not left to ``ck_scheduled_pipeline_run_source``: the CHECK
    is defence in depth that may legitimately never install on a live table, so a
    write path that relied on it would accept bad rows exactly where it matters.
    The API is the primary enforcement point; the constraint agrees with it.
    """
    sources = {
        "pipeline_task_spec": request.pipeline_task_spec is not None,
        "pipeline_task_spec_from_pipeline_run_id": (
            request.pipeline_task_spec_from_pipeline_run_id is not None
        ),
        "pipeline_task_spec_from_user_pipeline_id": (
            request.pipeline_task_spec_from_user_pipeline_id is not None
        ),
    }
    supplied = sorted(name for name, present in sources.items() if present)

    if len(supplied) != 1:
        raise fastapi.HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                "A schedule must have exactly one spec source: an inline"
                " pipeline_task_spec, a pipeline_task_spec_from_pipeline_run_id, or"
                " a pipeline_task_spec_from_user_pipeline_id."
                + (
                    " None were supplied."
                    if not supplied
                    else f" Received {len(supplied)}: {', '.join(supplied)}."
                )
            ),
        )

    if (
        request.pipeline_task_spec_from_user_pipeline_version_key is not None
        and request.pipeline_task_spec_from_user_pipeline_id is None
    ):
        raise fastapi.HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                "pipeline_task_spec_from_user_pipeline_version_key pins a version of"
                " pipeline_task_spec_from_user_pipeline_id, so it cannot be supplied"
                " without it."
            ),
        )


def _validate_run_reference(
    *,
    session: orm.Session,
    run_id: str,
    created_by: str,
) -> None:
    """Refuse a run reference the executor could not resolve.

    Delegates to ``executor.resolve_owned_run`` -- the exact rule the fire path
    uses -- so absent, unattributed and foreign runs are indistinguishable here
    too. Without this the endpoint was an existence oracle: a caller could tell a
    nonexistent run id from a real one belonging to somebody else by whether the
    create succeeded, and a successful create stored a schedule guaranteed to fail
    owner validation at every fire.

    Like the saved-pipeline preflight, this is a write-time check only. The
    authoritative check still happens at each fire, so a run deleted afterwards
    degrades to a failed fire rather than a corrupt row -- which is also why the
    deletion race between this check and the INSERT needs no locking.
    """
    try:
        executor.resolve_owned_run(
            session=session,
            run_id=run_id,
            created_by=created_by,
        )
    except errors.ItemNotFoundError as e:
        raise fastapi.HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        ) from e
    finally:
        _end_preflight_transaction(session=session)


def _end_preflight_transaction(*, session: orm.Session) -> None:
    """End the transaction a read-only preflight auto-began.

    Named rather than left as a bare `session.rollback()` in two `finally`
    blocks (thread 3898631041): "rollback" at a call site reads like error
    handling, and this runs on the success path too. Nothing is being undone --
    the preflight only read.

    Leaving the transaction open would carry its snapshot into the INSERT that
    follows, so a row created between preflight and write would be invisible to
    the check that exists to see it, and on SQLite would hold a read lock across
    APScheduler's own connection.
    """
    session.rollback()


def _reconcile_lost_adoption(
    *,
    session: orm.Session,
    schedule_id: str,
    canonical_path: str,
) -> None:
    """Decide the outcome for an adoption that lost a concurrent race.

    Reached when the compare-and-set matched no row, meaning another request
    adopted first. The loser must never overwrite the winner, so the winner's
    value is re-read and the normal set-once rules are applied to it: an identical
    canonical value is the documented no-op, and a different one is refused.

    The re-read is `FOR UPDATE` purely to force a *current* read. Under MySQL
    REPEATABLE READ a plain SELECT would return the snapshot taken before the
    winner committed -- which is exactly the stale value that caused the bug -- so
    reading it without locking would report "adopted" while having written nothing.
    SQLite ignores the clause, which is harmless because it serializes writers
    anyway.
    """
    # Selects COLUMNS, not the entity, and that is load-bearing. Loading the
    # entity would route this row through the identity map, and the obvious way to
    # defeat the stale cache -- populate_existing=True -- overwrites the loaded
    # attributes of the very instance `_apply_update` is still mutating. Every
    # session in `app.py` is built with autoflush=False, so that PATCH's other
    # field changes are still unflushed at this point and a refresh silently
    # discarded them: a same-value losing adoption returned 200 having dropped the
    # caller's `name` change. A column read cannot touch the identity map, so the
    # pending mutations survive and commit normally.
    winner = schedule_queries.lock_path_owner(session=session, schedule_id=schedule_id)
    if winner is None:
        # Deleted concurrently. Reporting it as not found is honest: the thing the
        # caller addressed no longer exists.
        raise fastapi.HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Schedule not found"
        )
    if winner.schedule_path == canonical_path:
        return
    raise fastapi.HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        detail=(
            f"schedule_path is already set to {winner.schedule_path!r} and cannot"
            f" be changed to {canonical_path!r}. It is a stable identity, set once."
        ),
    )


def _path_is_taken(
    *,
    session: orm.Session,
    created_by: str,
    canonical_path: str,
) -> bool:
    """Prove a path collision instead of inferring one from any IntegrityError.

    Called after a failed commit to classify it. Every write on this table can
    raise ``IntegrityError`` -- the run-reference foreign key most obviously -- and
    reporting all of them as "schedule_path already used" was actively misleading:
    a create naming a nonexistent run id returned 409 about a path the caller had
    never seen used.

    Queried rather than parsed out of the driver message, because the message
    differs per dialect and the constraint name must never reach a caller anyway.
    """
    return schedule_queries.path_is_taken(
        session=session,
        created_by=created_by,
        canonical_path=canonical_path,
    )


def _path_conflict(*, created_by: str, canonical_path: str) -> fastapi.HTTPException:
    """The one 409 body used wherever a path collision is proven."""
    return fastapi.HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail=(
            f"schedule_path {canonical_path!r} is already used by another"
            f" schedule owned by {created_by}. A path is a stable"
            " identity, so it cannot be shared."
        ),
    )


def _is_retry_safe_lock_failure(error: sqlalchemy.exc.OperationalError) -> bool:
    """Is this the narrow MySQL contention this endpoint is allowed to reinterpret?

    Read from the DRIVER exception, not from SQLAlchemy's wrapper: SQLAlchemy
    maps a wide range of causes onto ``OperationalError``, and the class alone
    says nothing about whether a write landed.

    Fails CLOSED in both unusual directions. A driver exception with no usable
    integer errno -- SQLite, a stub, a driver that reports differently -- is not
    recognised, so the error is re-raised and surfaces as a 500. That is the safe
    default: the cost of missing a real deadlock is one 500 on a request the
    client would have retried anyway, while the cost of guessing wrong the other
    way is a false no-write guarantee.
    """
    orig = getattr(error, "orig", None)
    args = getattr(orig, "args", ())
    if not args:
        return False
    errno = args[0]
    # Explicitly not `int(errno)`: a driver reporting the code as a string is a
    # driver this has not been verified against, and coercing would be a guess.
    return isinstance(errno, int) and errno in _RETRY_SAFE_LOCK_ERRNOS


def _classify_lock_failure(
    *,
    session: orm.Session,
    created_by: str,
    canonical_path: str | None,
    error: sqlalchemy.exc.OperationalError,
) -> fastapi.HTTPException | sqlalchemy.exc.OperationalError:
    """Turn a rolled-back OperationalError into 409-if-proven, else 503.

    Why this exists at all: the unique index on ``(created_by, schedule_path)``
    is contended by design -- two writers racing for one path is the case it
    exists to settle -- and the assumption that contention always arrives as a
    duplicate-key ``IntegrityError`` is wrong. InnoDB's duplicate check takes a
    shared lock on the conflicting index record, so a writer that finds the key
    uncommitted waits, and that wait can end as a deadlock (1213) or a lock-wait
    timeout (1205). Both are ``OperationalError``. Before this, they escaped the
    handlers and became a 500 for what was usually a taken path.
    ``triggers.service`` documents the same shape on its own unique
    insert.

    Two rules, and the order matters:

    1. **If the path is now taken, 409.** Proven by query, exactly as the
       ``IntegrityError`` handlers do, and deliberately not by reading driver
       error codes: once another schedule owns the path, 409 is the true and
       final answer whatever lock sequence produced the failure, and the caller
       must not be told to retry something that can only fail again.
    2. **Otherwise 503 with ``Retry-After``, not 500.** No row was written, so
       the request is safe to repeat verbatim, and a deadlock victim clears the
       moment the winner commits. A 500 tells the caller the server is broken
       and tells the operator nothing.

    **Unrelated ``OperationalError`` is re-raised untouched**, decided by the
    driver errno rather than by the exception class. An earlier revision turned
    every ``OperationalError`` into the retry-safe 503, which was wrong in the
    dangerous direction: only 1205/1213 guarantee nothing was written, so a
    dropped connection would have been answered with a no-write promise the
    server could not keep, and a caller repeating that request could duplicate a
    write it was told had not happened.

    The 503 carries a POSITIVE code rather than being identifiable by the
    absence of one. Infrastructure 503s -- a proxy, a load balancer, a pod shut
    down mid-request -- are uncoded and carry the opposite guarantee, so a
    caller classifying on absence would apply "nothing was written" to responses
    this service never issued. ``Retry-After`` does not separate them either;
    proxies emit it too.

    What this deliberately does NOT do:

    * It never consults the reference-existence check. A lock failure says
      nothing about whether a parent row exists, and answering 404 about a
      reference that was fine sends the caller to fix the wrong thing.
    * It does not retry in process. The client can resubmit far more cheaply
      than a request thread can be held through a backoff, and holding one is
      how a contended path becomes an exhausted connection pool. This is the
      same call `triggers.service` makes for the same reason.

    The caller must have rolled back before calling: the proof query needs a
    usable session, and the failed transaction is already dead.
    """
    if not _is_retry_safe_lock_failure(error):
        # Not ours to reinterpret. Returned rather than raised so every call site
        # keeps its `raise ... from e` shape and the original traceback survives.
        return error
    if canonical_path is not None and _path_is_taken(
        session=session,
        created_by=created_by,
        canonical_path=canonical_path,
    ):
        return _path_conflict(created_by=created_by, canonical_path=canonical_path)
    logger.warning(
        "schedule write failed on a lock, not a constraint"
        f" (created_by={created_by!r}, schedule_path={canonical_path!r})",
        exc_info=True,
    )
    return fastapi.HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail={
            "code": SchedulerErrorCode.SCHEDULE_PATH_LOCK_CONTENTION.value,
            "message": (
                "The schedule write could not complete because of a transient database"
                " lock conflict. Nothing was written; the same request can be repeated"
                " verbatim."
            ),
        },
        headers={"Retry-After": "1"},
    )


def _a_referenced_parent_vanished(
    *,
    session: orm.Session,
    request: PipelineScheduleCreateRequest,
    pipeline_id: str | None,
) -> bool:
    """Did a row named by a foreign-keyed reference column stop existing?

    Existence by primary key and nothing else. This runs only after an
    `IntegrityError`, to decide whether a constraint this endpoint understands
    is what failed -- so anything beyond "is the row there" would answer a
    question the constraint never asked.

    In particular a SOFT-DELETED parent counts as present. `deleted_at` leaves
    the row in place, so it satisfies the foreign key; treating it as missing
    would report a 404 for an error it did not cause.

    `pipeline_id` is the id the preflight RESOLVED, and the caller must pass it
    rather than letting this re-read the request. The two differ whenever the
    caller spelled the id in a form `normalize_pipeline_id` accepts -- a bare
    32-character hex UUID matches no primary key -- and asking "is this row
    there" with a spelling the table cannot hold answers no for a parent that is
    present. That turns any unrelated integrity fault into a 404 claiming the
    reference vanished, which is the opposite of what this branch decides: an
    error this endpoint did not cause must re-raise. The run id needs no such
    care because run ids have one spelling.
    """
    references = (
        (bts.PipelineRun, request.pipeline_task_spec_from_pipeline_run_id),
        (user_pipeline_db_models.UserPipeline, pipeline_id),
    )
    for model, identifier in references:
        if identifier is None:
            continue
        if not schedule_queries.row_exists(
            session=session, model=model, identifier=identifier
        ):
            return True
    return False


def _validate_pipeline_reference(
    *,
    session: orm.Session,
    request: PipelineScheduleCreateRequest,
    created_by: str,
    pipeline_service: user_pipeline_services.UserPipelineService | None = None,
) -> str:
    """Refuse a saved-pipeline reference the executor could not resolve, and say which row it is.

    Returns the id of the pipeline this reference actually resolved to, which is
    the value the caller must store. The request's spelling is not that value:
    the lookup runs through `normalize_pipeline_id`, so a bare 32-character hex
    UUID resolves happily, and persisting what the caller typed would write a
    string the `pipeline.id` foreign key does not match -- a create that fails
    with "referenced pipeline not found" for a pipeline the preflight had just
    found. Returning the loaded row's id rather than normalizing a second time
    here keeps one authority for what a pipeline id *is*: whatever the database
    matched. A second `normalize_pipeline_id` call at the write site would be a
    copy of the rule, and copies of a rule are what this branch has spent its
    length removing.

    Routed through ``get_pipeline_and_version`` -- the same call the executor's
    submission path uses -- rather than a reimplementation, because the writer and
    the executor agreeing is the whole point of centralizing the pin rule. A
    second copy here would be a second answer.

    ``user_id`` is passed deliberately: the service treats it as optional and only
    filters ownership when it is supplied, so omitting it would let a caller pin
    another user's pipeline. Failures are reported as *not found* rather than
    forbidden, because confirming that an id exists but belongs to someone else is
    itself a disclosure.

    This is a write-time preflight and nothing more. The authoritative check
    happens inside the locked read at every fire; a schedule whose pipeline is
    later deleted or switched out of FULL mode degrades to a failed fire, which is
    the executor's contract, not this endpoint's.

    ``validation_only=True`` keeps that routing while dropping the payload: this
    call reads no PAYLOAD attribute of what it gets back -- only ``id``, which
    ``load_only`` selects regardless of the projection because SQLAlchemy needs the
    primary key for identity. The version rows carry ``root_pipeline_task``, so
    validating a reference was loading the pipeline body it never looked at. It is
    a projection inside the shared service, not a second lookup here -- same
    queries, same shared row lock in the same place, same errors -- so the writer
    and the executor still answer from one rule .

    An earlier version of this paragraph said the call read no attribute at all.
    That stopped being true when the return value became the id: the sentence
    survived the change and would have told the next reader that touching ``id``
    here was a mistake, or that the projection could be narrowed past it.
    """
    try:
        pipeline, _, _ = (
            pipeline_service or _user_pipeline_service
        ).get_pipeline_and_version(
            session=session,
            pipeline_id=request.pipeline_task_spec_from_user_pipeline_id,
            user_id=created_by,
            file_path=None,
            version=request.pipeline_task_spec_from_user_pipeline_version_key,
            require_pinnable=True,
            validation_only=True,
        )
        # Read before the `finally` ends the transaction. `load_only` keeps the
        # primary key regardless of the projection, so this does not widen the
        # SELECT or trip `raiseload`.
        return pipeline.id
    # No `except` for PipelineNotFoundError / PipelineValidationError here.
    # `register_pipeline_exception_handlers` maps both to the same 404/422 with
    # the same `{"detail": str(exc)}` body, so local clauses produced a
    # byte-identical response while looking like this endpoint had a rule of its
    # own. `setup_pipeline_schedule_routes` installs that mapping itself, so the
    # deletion does not quietly depend on another module having been wired.
    # The `finally` below still runs on the way out.
    finally:
        _end_preflight_transaction(session=session)


def setup_pipeline_schedule_routes(
    *,
    app: fastapi.FastAPI,
    get_session: (
        collections.abc.Callable[..., orm.Session]
        | collections.abc.Callable[..., collections.abc.Iterator[orm.Session]]
    ),
    scheduler_svc: services.SchedulerService,
    user_details_getter: collections.abc.Callable[..., api_router.UserDetails],
    schema_report: database_migrations.MigrationReport,
    db_engine: sqlalchemy.Engine | None = None,
    pipeline_service: user_pipeline_services.UserPipelineService | None = None,
) -> None:
    """Wire the schedule routes.

    ``schema_report`` is injected rather than imported so tests can supply a
    constructed report; production passes the process-wide report computed once
    at import.

    ``db_engine`` is what lets a CLOSED tier notice that it opened. The boot
    report is frozen, so a pod that started while the index was still building
    used to serve 503 for that feature for its entire life -- and the stated
    remedy, "every serving pod must have booted after the migration succeeded",
    is not something the deployment can enforce or a restart respects. With an
    engine, `SchemaReadiness` re-verifies read-only, at most every
    `_READINESS_RECHECK_SECONDS`, and can only ever move a tier from closed to
    open. Omit it and the behaviour is exactly the frozen report as before.
    """
    readiness = database_migrations.SchemaReadiness(
        report=schema_report, db_engine=db_engine
    )
    # Registered here rather than assumed. These routes raise user-pipeline
    # domain errors when resolving a saved-pipeline reference, and their public
    # 404/422 mapping lives in `user_pipelines.errors`. Production happens to
    # install it because `setup_user_pipeline_routes` runs first, but relying on
    # that made this module's error contract depend on an unrelated module having
    # been wired -- mount these routes alone and the same request became a 500.
    # Re-registering is harmless: handlers are keyed by exception class.
    user_pipeline_errors.register_pipeline_exception_handlers(app=app)

    router = fastapi.APIRouter()

    @router.post(
        _API_BASE,
        status_code=status.HTTP_201_CREATED,
        tags=[_TAG],
    )
    def create_pipeline_schedule(
        request: PipelineScheduleCreateRequest,
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> PipelineScheduleResponse:
        _validate_cron(
            cron_expression=request.cron_expression,
            timezone=request.timezone,
        )
        _validate_source(request=request)
        # Pure: no database and no pipeline, so a malformed template is refused before
        # any lookup or write, and reported as malformed rather than as an unknown key.
        template_arguments = envelopes.validated_arguments(
            envelope=request.pipeline_templates, kind=sources.Kind.CRON
        )

        if request.pipeline_task_spec is not None:
            _validate_pipeline_task_spec(
                pipeline_task_spec=request.pipeline_task_spec,
            )

        # Every create stores a path -- supplied or derived -- so every create
        # needs the path tier, and readiness is checked before any persistence.
        # The operational consequence is stated plainly in SCHEDULER_DESIGN.md:
        # on a pod that booted before the index migration succeeded this closes
        # schedule creation entirely, including plain inline creates.
        _require_schema_tier(
            schema_report=readiness.current(),
            tier=SchemaTier.SCHEDULE_PATH,
            feature="Creating a schedule",
        )
        # Derivation is deferred until the id exists, a few lines below. An
        # explicit value is validated now so a bad path is refused before any
        # reference lookup or write happens.
        canonical_path = (
            None
            if request.schedule_path is None
            else _canonical_schedule_path(schedule_path=request.schedule_path)
        )

        if request.pipeline_task_spec_from_pipeline_run_id is not None:
            # Not gated on a schema tier: a run reference predates this work and
            # depends on none of the new columns.
            _validate_run_reference(
                session=session,
                run_id=request.pipeline_task_spec_from_pipeline_run_id,
                created_by=user_details.name,
            )

        # The resolved id, not the requested spelling: see `_validate_pipeline_reference`.
        canonical_pipeline_id = request.pipeline_task_spec_from_user_pipeline_id
        if request.pipeline_task_spec_from_user_pipeline_id is not None:
            _require_schema_tier(
                schema_report=readiness.current(),
                tier=SchemaTier.PIPELINE_REFERENCE,
                feature="Referencing a saved pipeline from a schedule",
            )
            canonical_pipeline_id = _validate_pipeline_reference(
                session=session,
                request=request,
                created_by=user_details.name,
                pipeline_service=pipeline_service,
            )

        schedule = db_models.ScheduledPipelineRun(
            name=request.name,
            cron_expression=request.cron_expression,
            timezone=request.timezone,
            pipeline_task_spec=request.pipeline_task_spec,
            pipeline_task_spec_from_pipeline_run_id=request.pipeline_task_spec_from_pipeline_run_id,
            pipeline_task_spec_from_user_pipeline_id=canonical_pipeline_id,
            pipeline_task_spec_from_user_pipeline_version_key=request.pipeline_task_spec_from_user_pipeline_version_key,
            schedule_path=canonical_path,
            settings=pipeline_templates.set_pipeline_templates(
                original=None, updates=template_arguments
            )
            or None,
            created_by=user_details.name,
        )

        # The id is normally an insert default, so it does not exist until the
        # INSERT -- but a derived path has to contain it. Generating it here and
        # assigning it explicitly (which suppresses the insert default) keeps this
        # to a SINGLE insert, with the path present in that insert.
        #
        # The alternative -- flush() to obtain the id, then update -- is precisely
        # what the comment below warns against: flush holds an uncommitted write
        # lock, which deadlocks against APScheduler's separate connection on
        # SQLite. Nor is `init=True` on the model an option: that would change the
        # model contract for every other caller to serve this one derivation.
        schedule.id = bts.generate_unique_id()
        if canonical_path is None:
            canonical_path = schedule_paths.generate_legacy_schedule_path(
                name=request.name,
                schedule_id=schedule.id,
            )
            schedule.schedule_path = canonical_path

        session.add(schedule)
        # commit() instead of flush() because APScheduler's SQLAlchemyJobStore
        # opens its own connection. SQLite only allows one writer at a time, so
        # flush() (which holds an uncommitted write lock) + APScheduler INSERT
        # = deadlock. MySQL/PostgreSQL row-level locking wouldn't have this
        # issue, but commit-first keeps the code DB-agnostic.
        #
        # This commit is deliberately OUTSIDE the try/except below. A concurrent
        # create racing for the same (created_by, schedule_path) loses here, and
        # the loser wrote no row -- so it must not reach `add_schedule`, and must
        # not run the "delete my row" compensation, which would delete the
        # WINNER's row. Widening that try to cover this commit would do exactly
        # that.
        try:
            session.commit()
        except sqlalchemy.exc.OperationalError as e:
            # Ordered BEFORE the IntegrityError branch for readability only --
            # the two exception classes are disjoint, so the order cannot change
            # which one catches. See `_classify_lock_failure` for why a lock
            # failure on this insert is usually a taken path.
            session.rollback()
            raise _classify_lock_failure(
                session=session,
                created_by=user_details.name,
                canonical_path=canonical_path,
                error=e,
            ) from e
        except sqlalchemy.exc.IntegrityError as e:
            session.rollback()
            # Only a PROVEN path collision is a 409. Anything else -- a foreign
            # key, a constraint added later -- is not this endpoint's to
            # reinterpret, and mislabelling it as a path conflict would send the
            # caller to fix the one thing that was fine.
            if _path_is_taken(
                session=session,
                created_by=user_details.name,
                canonical_path=canonical_path,
            ):
                raise _path_conflict(
                    created_by=user_details.name, canonical_path=canonical_path
                ) from e
            # Not a path collision. The other reachable cause is a referenced
            # PARENT ROW disappearing between preflight and this commit:
            # validation has to release its transaction (the executor's resolver
            # rolls back), so there is an unavoidable window in which a foreign
            # key can fail.
            #
            # BOTH reference columns are checked. An earlier version checked only
            # the run column, on the reasoning that the saved-pipeline column
            # carried no foreign key -- true when it was written, false now that
            # `fk_scheduled_pipeline_run_user_pipeline_id` exists.
            #
            # What is checked is RAW EXISTENCE BY ID, deliberately not the
            # preflight validators. Those also filter soft deletion, ownership,
            # version existence and pinnability -- none of which a single-column
            # foreign key can violate. Re-running them here would let an
            # unrelated integrity fault be reported as a 404 about a reference
            # that was never the cause, masking exactly the error this handler
            # promises to re-raise. Existence is the only question the constraint
            # can have answered.
            #
            # A soft-deleted parent therefore RE-RAISES: the row is still there,
            # so it satisfied the constraint and cannot be what failed.
            if _a_referenced_parent_vanished(
                session=session,
                request=request,
                pipeline_id=canonical_pipeline_id,
            ):
                # 404, the same STATUS the preflight uses, so a deletion race
                # degrades to "not found" instead of a 500.
                #
                # The body is deliberately not claimed to be identical to the
                # preflight's, and it is not: the preflight names the reference
                # it rejected, this cannot say which parent vanished without
                # re-querying to find out. So the two are distinguishable by
                # body. That is acceptable because neither body discloses
                # anything the caller did not supply -- both concern an id the
                # request itself named -- and the ownership-hiding requirement
                # this endpoint does have is about SCHEDULES, which is enforced
                # separately on the path and id routes.
                raise fastapi.HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Referenced pipeline or run not found",
                ) from e
            # Every referenced parent is still present, so this is some other
            # constraint and not this endpoint's to reinterpret.
            raise
        session.refresh(schedule)

        try:
            scheduler_svc.add_schedule(
                schedule_id=schedule.id,
                cron_expression=schedule.cron_expression,
                timezone=schedule.timezone,
            )
        except Exception:
            logger.exception(
                f"APScheduler add_schedule failed (schedule_id={schedule.id}), removing DB row"
            )
            session.delete(schedule)
            session.commit()
            raise

        return _schedule_to_response(
            schedule=schedule,
            scheduler_svc=scheduler_svc,
            include_spec=True,
        )

    @router.get(
        _API_BASE,
        tags=[_TAG],
    )
    def list_pipeline_schedules(
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
        page_size: int = fastapi.Query(default=10, ge=1, le=100),
        page_token: str | None = fastapi.Query(default=None),
        schedule_path: str | None = fastapi.Query(default=None),
    ) -> PipelineScheduleListResponse:
        # Every read on this endpoint is owner-scoped, filtered and unfiltered
        # alike. The unfiltered list previously returned every user's schedules;
        # narrowing it was a deliberate decision , taken because
        # the three read paths otherwise answered "may I see this?" differently
        # depending on how the request was spelled.
        #
        # In SQL, not in Python: see `_owned_by_caller`. `total_count` and the
        # cursor are computed from the same predicate, so a page, its token and
        # its count cannot disagree about who the rows belong to. That matters
        # most where the predicate is loose: a page bounded by `LIMIT` must not
        # be narrowed afterwards, or the count describes a population the page
        # was never drawn from.
        #
        # Projected, on every branch: a list response never carries
        # `pipeline_task_spec`, and reading a page of whole pipeline definitions
        # to serialize none of them is the largest cost this endpoint had
        # . `include_spec=False` below decided what to PRINT,
        # which is not the same thing as deciding what to READ -- the row was
        # already hydrated by then. The path-filtered branch shares this because
        # it shares the statement, and it reads one row of the same shape.
        query = (
            sqlalchemy.select(db_models.ScheduledPipelineRun)
            .where(_owned_by_caller(user_details=user_details))
            .options(*schedule_queries.LIST_PROJECTION)
        )
        row_limit = page_size
        # The path filter restates owner scoping, and is mandatory there for a
        # second reason: a path is unique per `created_by`, so an unscoped path
        # match could return another user's row, or several.
        #
        # A path-filtered read IS tier gated, and the reason is correctness, not
        # cost. I argued the opposite first -- that a read without the index is
        # merely slower, and that refusing it makes the feature uninspectable
        # while the migration is outstanding. That was wrong, for a reason worth
        # recording so it is not re-litigated:
        #
        # this branch narrows the read to ONE row, and on a database where the
        # path collation has not been converted yet the column folds, so `Foo`
        # and `foo` may both exist for one owner and either may be the row the
        # limit keeps. The caller asks for one of their paths and is handed the
        # other, or told it does not exist. A slow answer would have been
        # acceptable; a confidently wrong one is not.
        #
        # With the tier ready, the conversion has run and the unique index is
        # present over the converted column, so at most one row can match the
        # exact spelling asked for and the one-row bound is sound.
        #
        # The owner half needs no such gate and never did: it is a plain
        # equality the database evaluates under `created_by`'s own collation,
        # and a case variant of the caller's name IS the caller.
        path_filter: sqlalchemy.ColumnElement[bool] | None = None
        if schedule_path is not None:
            canonical_path = _canonical_schedule_path(schedule_path=schedule_path)
            path_filter = sqlalchemy.and_(
                _owned_by_caller(user_details=user_details),
                db_models.ScheduledPipelineRun.schedule_path == canonical_path,
            )
            query = query.where(path_filter)
            # A path lookup is not a page of a list. It is an exact-match read of
            # at most one row, it never offers a `next_page_token`, and its
            # `total_count` is derived from the rows in hand rather than counted.
            # Accepting a cursor alongside it silently broke all three: the
            # cursor's `WHERE (updated_at, id) < (...)` filtered out the single
            # matching row, so the endpoint answered `total_count = 0` for a
            # schedule that exists. Refusing is better than guessing which of the
            # two the caller meant, and no response we produce can lead here.
            if page_token:
                raise fastapi.HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                    detail=(
                        "page_token cannot be combined with schedule_path: a path"
                        " lookup returns at most one schedule and is never paginated."
                    ),
                )
            _require_schema_tier(
                schema_report=readiness.current(),
                tier=SchemaTier.SCHEDULE_PATH,
                feature="Schedule path lookup",
            )
            # Safe at one now, and only because of the gate above: the unique
            # index is present, it is enforced under the same collation as the
            # predicate, so at most one row can match. Not `page_size`, which
            # would have the server keep looking for rows that cannot exist.
            row_limit = 1

        if page_token:
            cursor_updated_at, cursor_id = _decode_cursor(cursor=page_token)
            query = query.where(
                sqlalchemy.tuple_(
                    db_models.ScheduledPipelineRun.updated_at,
                    db_models.ScheduledPipelineRun.id,
                )
                < sqlalchemy.tuple_(
                    sqlalchemy.literal(cursor_updated_at),
                    sqlalchemy.literal(cursor_id),
                )
            )

        query = query.order_by(
            db_models.ScheduledPipelineRun.updated_at.desc(),
            db_models.ScheduledPipelineRun.id.desc(),
        ).limit(row_limit)

        schedules = list(session.scalars(query).all())

        # No owner post-filter. There was one -- `row.created_by ==
        # user_details.name`, applied to the path branch only -- and it is gone
        # with the premise that put it there: it discarded rows whose owner
        # differed from the caller's only in case, which are the caller's own
        # rows. On a folding deployment it turned a schedule the user owns into
        # a 200 with an empty list. The query's owner predicate is the whole
        # rule now, and it is the same rule the constraint is enforced under.

        # Total across all pages (no cursor WHERE), so clients know the full
        # count.
        if path_filter is not None:
            # Not counted, DERIVED from the rows just read. Two conditions make
            # that exact rather than merely convenient: the tier gate above
            # guarantees the unique index exists, so at most one row can match;
            # and a cursor cannot have been applied, because `page_token` with a
            # path filter is refused. A COUNT would also be correct here, but it
            # is the one statement in this handler that no LIMIT can bound, and
            # it would be answering a question already answered.
            total_count = len(schedules)
        else:
            # Counted under the same owner predicate as the page, so the count
            # cannot describe a larger population than the rows can be drawn
            # from. A global COUNT here would tell a caller how many schedules
            # exist that they are not allowed to see.
            total_count = session.scalar(
                sqlalchemy.select(
                    sqlalchemy.func.count(db_models.ScheduledPipelineRun.id)
                ).where(_owned_by_caller(user_details=user_details))
            )

        next_page_token: str | None = None
        # A path-filtered response is complete by construction: at most one row
        # exists, so offering a cursor would invite a second round trip that can
        # only ever return nothing.
        if path_filter is None and len(schedules) >= page_size:
            next_page_token = _encode_cursor(schedule=schedules[-1])

        return PipelineScheduleListResponse(
            schedules=[
                _schedule_to_response(
                    schedule=s,
                    scheduler_svc=scheduler_svc,
                    include_spec=False,
                )
                for s in schedules
            ],
            total_count=total_count or 0,
            next_page_token=next_page_token,
        )

    @router.get(
        f"{_API_BASE}/{{pipeline_schedule_id}}",
        tags=[_TAG],
    )
    def get_pipeline_schedule(
        pipeline_schedule_id: str,
        include_spec: bool = fastapi.Query(default=False),
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> PipelineScheduleResponse:
        # Owner-scoped, where it previously was not: this route used to hand any
        # authenticated caller who knew an id the whole row, spec included, while
        # PATCH/DELETE/trigger on that same id refused them .
        #
        # Through the same helper the mutating verbs use, deliberately -- one
        # ownership rule for every id-addressed route, admin bypass included, so
        # the answer cannot depend on the verb. 403 rather than 404 for the same
        # reason: it is what the sibling routes already return, and this route
        # cannot be used to probe for existence any more cheaply than they can.
        schedule = _schedule_by_id_or_404(
            session=session,
            pipeline_schedule_id=pipeline_schedule_id,
            user_details=user_details,
            action="READ",
        )

        return _schedule_to_response(
            schedule=schedule,
            scheduler_svc=scheduler_svc,
            include_spec=include_spec,
        )

    def _reject_unless_exactly_identified(
        *,
        owner: str | None,
        stored_path: str | None,
        requested_path: str,
    ) -> None:
        """404 unless a path lookup returned the exact path the caller named.

        The PATH is re-checked and the owner is not, and the asymmetry is the
        product contract rather than an omission.

        The path needs a residual because the query's path predicate is the
        DATABASE's, and on a column whose collation has not been converted yet
        it folds: a lookup for 'Team/Nightly' resolves the row stored at
        'team/nightly'. The caller asked for a path that does not exist and
        silently received a different schedule of their own, which DELETE then
        makes irreversible. An earlier version of this code argued the path
        needed no residual because it was "lowercase ASCII by construction" --
        true only while the canonicalizer still folded case, and the code
        outlived the reasoning.

        The owner needs none because owner identity is not case-sensitive: a row
        the query matched belongs to the caller under the only comparator that
        decides ownership anywhere, which is the deployment's. This function did
        re-compare it, byte for byte, under the rejected premise that 'Jose' and
        'jose' are two people. That residual did not harden the route, it
        404'd callers on their own schedules.

        Deliberately 404 and not the 403 `_check_ownership` raises: on these
        routes absent and foreign must stay indistinguishable, and a 403 naming
        the creator would turn the mismatch into the very disclosure the
        404-for-both rule exists to prevent. `owner=None` is the absent case and
        takes the identical exit -- it is the only thing the owner argument is
        still read for.

        Every path locator funnels through this so the rule cannot be applied to
        one verb and forgotten on another; the locators differ only in what shape
        they read, never in who they let through.

        A stored path that is NULL takes the same exit: a path-less row cannot be
        the row a path lookup named.
        """
        if owner is None or stored_path != requested_path:
            raise fastapi.HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Schedule not found",
            )

    def _path_identity_or_404(
        *,
        session: orm.Session,
        schedule_path: str,
        user_details: api_router.UserDetails,
    ) -> schedule_queries.ScheduleIdentity:
        """Identity columns for a path in the CALLER's namespace, or 404.

        Identity columns and not the row: the lookup may return the caller's
        schedule at a neighbouring PATH spelling, and hydrating
        `pipeline_task_spec` (JSON) plus `last_run_submission_result` (TEXT) to
        then refuse the request is work done for a request that is about to be
        404'd .
        """
        canonical_path = _canonical_schedule_path(schedule_path=schedule_path)
        identity = schedule_queries.owned_schedule_identity_by_path(
            session=session,
            created_by=user_details.name,
            canonical_path=canonical_path,
        )
        _reject_unless_exactly_identified(
            owner=None if identity is None else identity.created_by,
            stored_path=None if identity is None else identity.schedule_path,
            requested_path=canonical_path,
        )
        assert identity is not None  # noqa: S101 - narrowed by the 404 above
        return identity

    def _path_stub_or_404(
        *,
        session: orm.Session,
        schedule_path: str,
        user_details: api_router.UserDetails,
    ) -> db_models.ScheduledPipelineRun:
        """A deletable persistent entity for a path, carrying no payload.

        `session.delete` takes an object, so DELETE cannot work from identity
        values alone -- but it does not need a loaded object either. Same
        authorization as every other path locator, one SELECT, no JSON.
        """
        canonical_path = _canonical_schedule_path(schedule_path=schedule_path)
        stub = schedule_queries.owned_schedule_stub_by_path(
            session=session,
            created_by=user_details.name,
            canonical_path=canonical_path,
        )
        _reject_unless_exactly_identified(
            owner=None if stub is None else stub.created_by,
            stored_path=None if stub is None else stub.schedule_path,
            requested_path=canonical_path,
        )
        assert stub is not None  # noqa: S101 - narrowed by the 404 above
        return stub

    def _schedule_by_path_or_404(
        *,
        session: orm.Session,
        schedule_path: str,
        user_details: api_router.UserDetails,
    ) -> db_models.ScheduledPipelineRun:
        """Resolve a path within the CALLER's namespace, or 404.

        Owner scoping is inside the lookup rather than an ownership check after
        it, because a path is unique only per owner: an unscoped read could match
        another user's row, so there would be nothing correct to check. Admin is
        deliberately not a cross-user path resolver either -- a path names at most
        one row per owner, so a global resolution would be ambiguous.

        Absent and foreign both yield 404, and the same detail string as the
        id-addressed routes. Distinguishing them would tell a caller that somebody
        else owns a given path.

        For the one caller that needs every column: PATCH mutates the entity,
        re-reads it after commit and returns it. It authorizes on identity
        columns first and loads the row only once it knows the caller owns it, so
        an unauthorized request still hydrates nothing. That costs the authorized
        request one extra indexed single-row SELECT and leaves its payload reads
        exactly where they were -- the trade is deliberate, and it is the reason
        the other two verbs use the lighter locators instead of this one.

        The identity read is COLUMNS, so it leaves the identity map empty and the
        `session.get` below is a real, fully populated load rather than a partial
        instance handed back out of the map.
        """
        identity = _path_identity_or_404(
            session=session,
            schedule_path=schedule_path,
            user_details=user_details,
        )
        schedule = session.get(db_models.ScheduledPipelineRun, identity.id)
        if schedule is None:
            # Deleted between the two reads. Same 404 as never having existed,
            # which is what a caller racing its own DELETE should see.
            raise fastapi.HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Schedule not found",
            )
        return schedule

    def _schedule_by_id_or_404(
        *,
        session: orm.Session,
        pipeline_schedule_id: str,
        user_details: api_router.UserDetails,
        action: str,
    ) -> db_models.ScheduledPipelineRun:
        schedule = session.get(db_models.ScheduledPipelineRun, pipeline_schedule_id)
        if not schedule:
            raise fastapi.HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Schedule not found",
            )
        # Unscoped read, then a scoped authorization probe. The read has to stay
        # unscoped so absent and foreign remain distinguishable (404 vs 403), and
        # the probe has to be SQL so the owner comparison is the deployment's --
        # the same one the path routes get for free from their scoped SELECT.
        _check_ownership(
            session=session,
            schedule_id=schedule.id,
            created_by=schedule.created_by,
            user_details=user_details,
            action=action,
        )
        return schedule

    def _identity_by_id_or_404(
        *,
        session: orm.Session,
        pipeline_schedule_id: str,
        user_details: api_router.UserDetails,
        action: str,
    ) -> schedule_queries.ScheduleIdentity:
        """`_schedule_by_id_or_404` for a caller that needs no payload.

        Same statuses and the same ownership rule; it reads four columns instead
        of the row. Used by the id-addressed trigger so both addressing modes of
        one operation do the same amount of work.
        """
        identity = schedule_queries.schedule_identity_by_id(
            session=session,
            schedule_id=pipeline_schedule_id,
        )
        if identity is None:
            raise fastapi.HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Schedule not found",
            )
        _check_ownership(
            session=session,
            schedule_id=identity.id,
            created_by=identity.created_by,
            user_details=user_details,
            action=action,
        )
        return identity

    def _apply_update(
        *,
        session: orm.Session,
        schedule: db_models.ScheduledPipelineRun,
        request: PipelineScheduleUpdateRequest,
        allow_path_adoption: bool,
    ) -> PipelineScheduleResponse:
        """The one update body, shared by the id- and path-addressed routes.

        Centralized so the two addressing modes cannot drift into different
        validation, different error codes, or different scheduler side effects.
        Only `allow_path_adoption` differs between them.
        """
        if request.cron_expression is not None or request.timezone is not None:
            effective_cron = request.cron_expression or schedule.cron_expression
            effective_tz = request.timezone or schedule.timezone
            _validate_cron(cron_expression=effective_cron, timezone=effective_tz)

        if request.name is not None:
            schedule.name = request.name
        if request.cron_expression is not None:
            schedule.cron_expression = request.cron_expression
        if request.timezone is not None:
            schedule.timezone = request.timezone
        if request.pipeline_task_spec is not None:
            _validate_pipeline_task_spec(
                pipeline_task_spec=request.pipeline_task_spec,
            )
            # Setting an inline spec on a REFERENCE-sourced row would give it two
            # sources and violate `ck_scheduled_pipeline_run_source` at commit.
            # Refused here with a truthful message instead: the constraint
            # violation surfaced as an opaque 500 (or, while adopting a path, as a
            # nonsensical path-conflict 409). PATCH does not do source transitions,
            # so there is nothing to support here beyond saying so.
            #
            # Pre-existing on this endpoint rather than introduced by paths; fixed
            # here because the adoption work is what made it reachable with a
            # misleading status.
            existing_reference = (
                schedule.pipeline_task_spec_from_pipeline_run_id
                or schedule.pipeline_task_spec_from_user_pipeline_id
            )
            if existing_reference is not None:
                raise fastapi.HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                    detail=(
                        "This schedule takes its spec from a reference, so an"
                        " inline pipeline_task_spec cannot be set on it. A"
                        " schedule has exactly one source, and PATCH does not"
                        " change which one it is."
                    ),
                )
            schedule.pipeline_task_spec = request.pipeline_task_spec
        # `{"arguments": {}}` is how a caller clears every template. An envelope that
        # names no `arguments` leaves the stored ones alone, so a newer client sending
        # only a field this version does not read cannot delete them.
        if envelopes.names_arguments(envelope=request.pipeline_templates):
            schedule.settings = pipeline_templates.set_pipeline_templates(
                original=schedule.settings,
                updates=envelopes.validated_arguments(
                    envelope=request.pipeline_templates, kind=sources.Kind.CRON
                ),
            )
        if request.paused is not None:
            # Unpausing fires exactly one coalesced run for any missed triggers
            # (misfire_grace_time=None + coalesce=True in APScheduler config).
            schedule.paused = request.paused

        adopting_path = False
        canonical_path: str | None = None
        if request.schedule_path is not None:
            if not allow_path_adoption:
                # A path-addressed PATCH cannot repath. The path is the address
                # here, so honouring a change would rewrite the very identity the
                # request was routed by.
                raise fastapi.HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                    detail=(
                        "schedule_path cannot be changed through a path-addressed"
                        " PATCH. Address the schedule by id to adopt a path."
                    ),
                )
            canonical_path = _canonical_schedule_path(
                schedule_path=request.schedule_path,
            )
            if schedule.schedule_path is None:
                # Adoption: NULL -> path. This is a write of the path column, so
                # it needs the path tier even though the rest of this PATCH does
                # not.
                _require_schema_tier(
                    schema_report=readiness.current(),
                    tier=SchemaTier.SCHEDULE_PATH,
                    feature="Setting schedule_path",
                )
                # Compare-and-set, NOT an ORM attribute write. The read above is
                # unlocked, so two concurrent adoptions could both observe NULL;
                # assigning the attribute and committing made the LAST writer win,
                # silently repathing a row whose path was supposed to be set once.
                #
                # `WHERE schedule_path IS NULL` makes the database arbitrate. It is
                # used in preference to `FOR UPDATE` because SQLite ignores row
                # locks, so a lock-based fix could not be regression-tested here at
                # all -- whereas this is a single atomic statement on every
                # dialect. `created_by` is in the predicate as well, so the
                # statement can never touch another owner's row.
                #
                # The unique constraint is now enforced at THIS statement rather
                # than at commit, so the collision is classified here.
                owner = schedule.created_by
                try:
                    adopted = session.execute(
                        sqlalchemy.update(db_models.ScheduledPipelineRun)
                        .where(
                            db_models.ScheduledPipelineRun.id == schedule.id,
                            # Through the shared predicate, not a second literal
                            # equality. `owner` is this row's own stored value,
                            # so any comparator matches it -- but a hand-written
                            # `created_by == ...` here is the shape a future
                            # owner rule would be forgotten in.
                            schedule_queries.owned_by(created_by=owner),
                            db_models.ScheduledPipelineRun.schedule_path.is_(None),
                        )
                        .values(schedule_path=canonical_path),
                        # synchronize_session=False is load-bearing. The default
                        # ('auto' -> 'evaluate') applies the statement's criteria
                        # to objects already in the session: our in-memory copy
                        # still has the stale NULL, so the ORM judged it a match
                        # and wrote the new path into memory even when the
                        # database matched no row. The response then showed a path
                        # that had not been stored.
                        execution_options={"synchronize_session": False},
                    )
                except sqlalchemy.exc.OperationalError as e:
                    # The adoption CAS writes the same unique key the create
                    # insert does, so it is exposed to the same lock outcomes.
                    session.rollback()
                    raise _classify_lock_failure(
                        session=session,
                        created_by=owner,
                        canonical_path=canonical_path,
                        error=e,
                    ) from e
                except sqlalchemy.exc.IntegrityError as e:
                    session.rollback()
                    if not _path_is_taken(
                        session=session,
                        created_by=owner,
                        canonical_path=canonical_path,
                    ):
                        raise
                    raise _path_conflict(
                        created_by=owner, canonical_path=canonical_path
                    ) from e
                if adopted.rowcount == 1:
                    adopting_path = True
                else:
                    # Lost the race: somebody adopted between our read and here.
                    # Re-read with a locking read so MySQL REPEATABLE READ returns
                    # the committed value rather than our stale snapshot, then
                    # apply the ordinary set-once rules to the winner's value.
                    _reconcile_lost_adoption(
                        session=session,
                        schedule_id=schedule.id,
                        canonical_path=canonical_path,
                    )
            elif schedule.schedule_path != canonical_path:
                # Safe to decide from the unlocked read: a non-NULL path is
                # immutable, so this value cannot have changed underneath us.
                # Set once. Rewriting a path would silently break every caller
                # addressing the schedule by the old one, so it is refused rather
                # than treated as a rename.
                raise fastapi.HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                    detail=(
                        f"schedule_path is already set to"
                        f" {schedule.schedule_path!r} and cannot be changed to"
                        f" {canonical_path!r}. It is a stable identity, set once."
                    ),
                )
            # Re-sending the same canonical value is a no-op, so a client can
            # replay a PATCH without special-casing this field.

        schedule.updated_at = datetime.datetime.now(datetime.timezone.utc)
        owner = schedule.created_by
        try:
            session.commit()
        except sqlalchemy.exc.OperationalError as e:
            session.rollback()
            # `canonical_path` is passed only when this PATCH actually adopted
            # one. Without that guard a lock failure on an unrelated PATCH would
            # be reported as 409 against a path this request never touched --
            # the same misclassification the IntegrityError branch below exists
            # to prevent, arriving through a different exception class.
            raise _classify_lock_failure(
                session=session,
                created_by=owner,
                canonical_path=canonical_path if adopting_path else None,
                error=e,
            ) from e
        except sqlalchemy.exc.IntegrityError as e:
            session.rollback()
            # `adopting_path` is NOT sufficient grounds to call this a path
            # collision. The adoption CAS has already succeeded by this point, so a
            # failure here is some *other* constraint: a combined PATCH that adopts
            # a path and also sets `pipeline_task_spec` on a row that already has a
            # reference violates `ck_scheduled_pipeline_run_source`, and that was
            # being reported as "schedule_path already used" for a path nothing had
            # ever used. Same misclassification as the create path, so it gets the
            # same proof requirement.
            if not adopting_path or not _path_is_taken(
                session=session,
                created_by=owner,
                canonical_path=canonical_path or "",
            ):
                raise
            raise _path_conflict(
                created_by=owner, canonical_path=canonical_path or ""
            ) from e
        session.refresh(schedule)

        scheduler_svc.update_schedule(
            schedule_id=schedule.id,
            cron_expression=request.cron_expression,
            timezone=request.timezone,
            paused=request.paused,
            current_cron=schedule.cron_expression,
            current_timezone=schedule.timezone,
        )

        return _schedule_to_response(
            schedule=schedule,
            scheduler_svc=scheduler_svc,
            include_spec=True,
        )

    def _apply_delete(
        *,
        session: orm.Session,
        schedule: db_models.ScheduledPipelineRun,
    ) -> None:
        schedule_id = schedule.id
        session.delete(schedule)
        session.commit()
        scheduler_svc.remove_schedule(schedule_id=schedule_id)

    def _apply_trigger(
        *,
        schedule: schedule_queries.ScheduleIdentity,
    ) -> PipelineScheduleTriggerResponse:
        """Fire one schedule now.

        Takes identity values, not the entity: the only things this reads are
        `paused` and `id`, and the executor loads the schedule itself, in its own
        session, from that id. Loading the row here to hand over one string was a
        second full read of a row the executor was about to read anyway.
        """
        if schedule.paused:
            raise fastapi.HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Schedule is paused, unpause before triggering",
            )

        schedule_id = schedule.id
        run = executor.execute_pipeline_schedule(
            pipeline_schedule_id=schedule_id,
            #: This bypasses APScheduler, so no schedule_time was published for it.
            #: Saying so stops the fire popping a concurrent scheduled firing's.
            manual=True,
        )

        if run is None:
            raise fastapi.HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Pipeline schedule trigger failed",
            )

        return PipelineScheduleTriggerResponse(
            message="Pipeline schedule triggered successfully",
            schedule_id=schedule_id,
            pipeline_run_response=dataclasses.asdict(run),
        )

    @router.patch(
        _API_BASE,
        tags=[_TAG],
    )
    def update_pipeline_schedule_by_path(
        request: PipelineScheduleUpdateRequest,
        schedule_path: str = fastapi.Query(),
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> PipelineScheduleResponse:
        # A point mutation addressed by path relies on the unique identity, so it
        # needs the path tier even though the same mutation by id does not.
        _require_schema_tier(
            schema_report=readiness.current(),
            tier=SchemaTier.SCHEDULE_PATH,
            feature="Addressing a schedule by schedule_path",
        )
        schedule = _schedule_by_path_or_404(
            session=session,
            schedule_path=schedule_path,
            user_details=user_details,
        )
        return _apply_update(
            session=session,
            schedule=schedule,
            request=request,
            allow_path_adoption=False,
        )

    @router.patch(
        f"{_API_BASE}/{{pipeline_schedule_id}}",
        tags=[_TAG],
    )
    def update_pipeline_schedule(
        pipeline_schedule_id: str,
        request: PipelineScheduleUpdateRequest,
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> PipelineScheduleResponse:
        schedule = _schedule_by_id_or_404(
            session=session,
            pipeline_schedule_id=pipeline_schedule_id,
            user_details=user_details,
            action="UPDATE",
        )
        return _apply_update(
            session=session,
            schedule=schedule,
            request=request,
            allow_path_adoption=True,
        )

    @router.delete(
        _API_BASE,
        status_code=status.HTTP_204_NO_CONTENT,
        tags=[_TAG],
    )
    def delete_pipeline_schedule_by_path(
        schedule_path: str = fastapi.Query(),
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> None:
        _require_schema_tier(
            schema_report=readiness.current(),
            tier=SchemaTier.SCHEDULE_PATH,
            feature="Addressing a schedule by schedule_path",
        )
        schedule = _path_stub_or_404(
            session=session,
            schedule_path=schedule_path,
            user_details=user_details,
        )
        _apply_delete(session=session, schedule=schedule)

    @router.delete(
        f"{_API_BASE}/{{pipeline_schedule_id}}",
        status_code=status.HTTP_204_NO_CONTENT,
        tags=[_TAG],
    )
    def delete_pipeline_schedule(
        pipeline_schedule_id: str,
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> None:
        schedule = _schedule_by_id_or_404(
            session=session,
            pipeline_schedule_id=pipeline_schedule_id,
            user_details=user_details,
            action="DELETE",
        )
        _apply_delete(session=session, schedule=schedule)

    # Registered BEFORE the id-addressed trigger. There is no dynamic POST at this
    # depth today, so the two cannot actually collide -- an earlier review warning
    # about `trigger` sitting in the id position does not apply to these shapes,
    # because it is a suffix there and a fixed collection segment here. Literal
    # first anyway, so the ordering stops mattering if a POST `/{id}` is ever added.
    @router.post(
        f"{_API_BASE}/trigger",
        tags=[_TAG],
    )
    def trigger_pipeline_schedule_by_path(
        schedule_path: str = fastapi.Query(),
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> PipelineScheduleTriggerResponse:
        _require_schema_tier(
            schema_report=readiness.current(),
            tier=SchemaTier.SCHEDULE_PATH,
            feature="Addressing a schedule by schedule_path",
        )
        identity = _path_identity_or_404(
            session=session,
            schedule_path=schedule_path,
            user_details=user_details,
        )
        return _apply_trigger(schedule=identity)

    @router.post(
        f"{_API_BASE}/{{pipeline_schedule_id}}/trigger",
        tags=[_TAG],
    )
    def trigger_pipeline_schedule(
        pipeline_schedule_id: str,
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> PipelineScheduleTriggerResponse:
        identity = _identity_by_id_or_404(
            session=session,
            pipeline_schedule_id=pipeline_schedule_id,
            user_details=user_details,
            action="TRIGGER",
        )
        return _apply_trigger(schedule=identity)

    app.include_router(router)
