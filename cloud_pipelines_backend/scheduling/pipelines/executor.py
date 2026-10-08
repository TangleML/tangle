import contextlib
import copy
import dataclasses
import datetime
import enum
import logging
import threading
import typing

import sqlalchemy
from apscheduler.executors import pool
from sqlalchemy import orm

from cloud_pipelines_backend import (
    api_server_sql,
    component_structures,
    errors,
)
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.scheduling.pipelines import db_models
from cloud_pipelines_backend.templating.arguments import (
    annotations as template_annotations,
)
from cloud_pipelines_backend.templating.arguments import rendering, sources
from cloud_pipelines_backend.templating.arguments.observability import render_observer
from cloud_pipelines_backend.user_pipelines import services as user_pipeline_services
from cloud_pipelines_backend.utils import pipeline_templates

logger = logging.getLogger(__name__)

_pipeline_run_service = api_server_sql.PipelineRunsApiService_Sql()
_user_pipeline_service = user_pipeline_services.UserPipelineService()


class _Source(enum.Enum):
    """Which of the three executable spec sources a schedule row uses.

    All three stay executable through the migration. `INLINE` is the legacy
    shape, `RUN` references an earlier pipeline run, and `PIPELINE` references a
    saved pipeline -- following its current content, or pinned to one version.
    """

    NONE = "none"
    INLINE = "inline"
    RUN = "run"
    PIPELINE = "pipeline"


def _classify_source(
    pipeline_schedule: db_models.ScheduledPipelineRun,
) -> _Source:
    """Pick the source to execute, inline first.

    Inline wins whenever it is present. The one-source invariant means a
    well-formed row never offers two, but this is the fire path: if a row is
    somehow malformed it has to run the same spec it ran yesterday rather than
    silently switch to a reference, so precedence is explicit rather than
    dependent on the invariant holding.
    """
    if pipeline_schedule.pipeline_task_spec is not None:
        return _Source.INLINE
    if pipeline_schedule.pipeline_task_spec_from_pipeline_run_id is not None:
        return _Source.RUN
    if pipeline_schedule.pipeline_task_spec_from_user_pipeline_id is not None:
        return _Source.PIPELINE
    return _Source.NONE


def resolve_owned_run(
    *,
    session: orm.Session,
    run_id: str,
    created_by: str,
) -> bts.PipelineRun:
    """The single owner rule for a run reference, for writers and the executor.

    Public because the API's create path validates a run reference *before*
    storing it, and it must use this exact rule rather than a second copy. A
    writer that accepted references this rejects would store a schedule
    guaranteed to fail at every fire, and one whose refusals differed would leak
    which run ids exist.

    `PipelineRun.created_by` is nullable while a schedule's is not, so an
    unattributed run is refused rather than compared: there is no owner to
    authorise against, and letting a NULL decide would be an authorization
    answer arrived at by accident.

    The ownership comparison is the DATABASE's, matching every other owner check
    in the scheduler. It was `pipeline_run.created_by != created_by` in Python,
    which folds nothing, so on a deployment whose collation is case-insensitive
    a schedule owned by `Alice@example.com` could not reference a run the same
    person had created as `alice@example.com` -- refused at create time, and
    refused again at every fire if the spelling ever changed. Owner identity is
    not case-sensitive, and this module does not get its own opinion about that.

    Two statements rather than one scoped read, for the same reason
    `_check_ownership` uses two: the three refusals are logged apart, and a
    scoped read collapses them into one indistinguishable miss. The classifying
    read is unscoped and answers only the operator-facing log line; the probe
    answers the authorization question.

    Raises:
        errors.ItemNotFoundError: for absent, unattributed, and foreign alike.
    """
    pipeline_run = session.get(bts.PipelineRun, run_id)
    # One outward message for all three refusals -- absent, unattributed, and
    # owned by someone else. Distinguishing them tells a schedule owner which
    # run ids exist, and "has no owner recorded" was doing exactly that: it
    # confirmed the row while a missing row said something different.
    # The reason is logged, where only operators can read it.
    if pipeline_run is None:
        logger.info(f"Run reference {run_id!r} does not exist")
    elif pipeline_run.created_by is None:
        logger.info(f"Run reference {run_id!r} exists but records no owner")
    elif not _run_is_owned_by(session=session, run_id=run_id, created_by=created_by):
        logger.info(f"Run reference {run_id!r} is owned by another user")
    else:
        return pipeline_run
    raise errors.ItemNotFoundError(
        f"Pipeline run {run_id!r} was not found for user {created_by!r}."
    )


def _run_is_owned_by(*, session: orm.Session, run_id: str, created_by: str) -> bool:
    """Does this run belong to this caller, under the deployment's comparator?

    `schedule_queries.is_owned_by` for `PipelineRun`. Not shared with it because
    that one is bound to `ScheduledPipelineRun`, and generalising over a model
    would hide which table is being authorized against in the one kind of code
    where that must stay obvious.

    A separate SELECT rather than a comparison against the row already loaded,
    because the comparison is exactly what must not happen in this process.
    """
    return (
        session.scalar(
            sqlalchemy.select(sqlalchemy.literal(1))
            .where(
                bts.PipelineRun.id == run_id,
                bts.PipelineRun.created_by == created_by,
            )
            .limit(1)
        )
        is not None
    )


def _root_task_from_run(
    *,
    session: orm.Session,
    run_id: str,
    created_by: str,
) -> component_structures.TaskSpec:
    """Read an earlier run's root TaskSpec, for the schedule's owner only.

    There is no rerun/clone service to reuse, so this reads the existing
    `root_execution` relationship directly and submits through the same
    `PipelineRunsApiService_Sql.create` an inline spec uses. No saved-pipeline
    provenance applies to this source, so `create_from_pipeline` would be the
    wrong path.
    """
    pipeline_run = resolve_owned_run(
        session=session,
        run_id=run_id,
        created_by=created_by,
    )
    # deepcopy because the mapped JSON dict is attached to this session;
    # parsing must not be able to mutate persisted state.
    return component_structures.TaskSpec.from_json_dict(
        copy.deepcopy(pipeline_run.root_execution.task_spec)
    )


@dataclasses.dataclass(frozen=True)
class ScheduleRender:
    """One firing's render, with the clock it used.

    The clock travels with the result because reporting happens at the caller, after the run
    is submitted, and the kind label has to be the one the render actually used -- a
    hand-fired cron row renders as MANUAL, so re-deriving it later could disagree.
    """

    rendered: rendering.Rendered
    clock: sources.Clock


def render_schedule_templates(
    *,
    templates: dict[str, str],
    schedule_time: datetime.datetime | None,
    trigger_time: datetime.datetime,
) -> ScheduleRender:
    """Render a schedule's stored templates for one firing.

    no templates stored          -> empty Rendered, nothing to merge
    schedule_time present        -> renders as CRON, `schedule_time` resolves
    schedule_time None (manual)  -> renders as MANUAL, `schedule_time` templates fail

    A failed key is simply absent from `rendered.arguments`, and the submission merge is
    `{**spec.arguments, **rendered.arguments}` -- so the run goes out carrying whatever
    literal the task spec stored rather than the template's intent, the trigger endpoint
    still answers success, and `render_error` in the run's annotations is the only trace.
    A manual fire of a `schedule_time` template therefore always ships the stored value.

    Renders only. Reporting is the caller's, because a render whose run is never submitted is
    not a firing worth counting.
    """
    #: A Clock refuses CRON without a schedule_time, so the kind follows the value rather
    #: than the row: a hand-fired cron schedule is a manual run for rendering purposes.
    kind = sources.Kind.CRON if schedule_time is not None else sources.Kind.MANUAL
    clock = sources.Clock(
        kind=kind,
        trigger_time=trigger_time,
        now=datetime.datetime.now(datetime.timezone.utc),
        schedule_time=schedule_time,
    )
    return ScheduleRender(
        rendered=rendering.render(templates=templates, arguments={}, clock=clock),
        clock=clock,
    )


def _submit(
    *,
    source: _Source,
    run_session: orm.Session,
    pipeline_schedule: db_models.ScheduledPipelineRun,
    annotations: dict[str, str],
    rendered: rendering.Rendered,
) -> api_server_sql.PipelineRunResponse:
    """Resolve the schedule's source and create the run through its canonical path."""
    created_by = pipeline_schedule.created_by

    if source is _Source.PIPELINE:
        version_key = (
            pipeline_schedule.pipeline_task_spec_from_user_pipeline_version_key
        )
        # user_id is passed deliberately. get_pipeline_and_version treats it as
        # optional and only filters ownership when given, so omitting it would let
        # any schedule execute any user's pipeline by id.
        return _user_pipeline_service.create_from_pipeline(
            session=run_session,
            pipeline_id=pipeline_schedule.pipeline_task_spec_from_user_pipeline_id,
            user_id=created_by,
            file_path=None,
            # None follows the pipeline's current_version_key, which under
            # DISABLED mode is the mutable `current` head -- so a following
            # schedule's content can change without the schedule changing. That
            # is what following means, and is why pins exist.
            version=version_key,
            run_arguments=dict(rendered.arguments),
            pipeline_run_annotations=annotations,
            created_by=created_by,
            # Not a preflight here. The service checks the mode inside the same
            # locked read that selects the version. A check made out here would be
            # a separate read, so the mode could change between it and the
            # selection -- pi-33 reproduced exactly that interleaving against the
            # earlier preflight.
            #
            require_pinnable_versioning=True,
            # The shared row lock that pin takes is released as soon as the
            # selected content has been copied out, instead of at commit. Opt-in
            # because it rolls this session back, which would destroy a caller's
            # pending writes -- `run_session` is created for this submission and
            # has none, which is exactly the precondition the flag documents.
            #
            # Without it the lock would span recursive execution and artifact
            # insertion for the whole graph, and a shared lock blocks every
            # exclusive one: `set_pipeline` and `delete_pipeline` on that pipeline
            # would wait on a scheduled fire, not just a mode change.
            end_lookup_transaction=True,
        )

    if source is _Source.RUN:
        root_task = _root_task_from_run(
            session=run_session,
            run_id=pipeline_schedule.pipeline_task_spec_from_pipeline_run_id or "",
            created_by=created_by,
        )
        # The read above auto-began a transaction on this session, and create()
        # below opens its own with session.begin(), which raises if one is already
        # active. End the read transaction explicitly.
        #
        # `create_from_pipeline` used to do the same thing for the same reason and
        # no longer does: it builds the run with `_create_in_transaction`, inside
        # the caller's transaction, so there is no nested `begin()` to make room
        # for. This branch still calls `create()`, which does begin one, so the
        # rollback stays here. Nothing has been written on this session yet at
        # this point, so there is nothing for it to discard.
        run_session.rollback()
    else:
        # Deep-copied because the parser does not give us deep isolation.
        # Pydantic rebuilds typed container shells, but values typed `Any` --
        # `annotations` here, at both the task and the input level -- are passed
        # through by reference, so the parsed TaskSpec shares nested dicts and
        # lists with the ORM-owned JSON. Mutating one today mutates the other.
        #
        # No current code mutates those leaves, and nested mutation usually
        # evades `MutableDict`'s top-level dirty tracking anyway, so this is an
        # in-memory ownership leak rather than a persistence bug. The copy makes
        # the boundary explicit instead of resting on both of those staying
        # true. The run-reference and saved-pipeline paths already own their
        # spec; this was the only place ORM-owned JSON crossed into a parser.
        root_task = component_structures.TaskSpec.from_json_dict(
            copy.deepcopy(pipeline_schedule.pipeline_task_spec)
        )

    #: The vendored `create` takes no run_arguments, so the rendered map is merged into the
    #: spec here. Same precedence the saved-pipeline path applies inside the service: a
    #: template beats an argument already on the task.
    root_task = dataclasses.replace(
        root_task,
        arguments={**dict(root_task.arguments or {}), **rendered.arguments},
    )
    return _pipeline_run_service.create(
        session=run_session,
        root_task=root_task,
        annotations=annotations,
        created_by=created_by,
    )


# Module-level mutable state is not ideal, but APScheduler 3.x pickles job
# state (function ref + kwargs) to the DB — functools.partial, lambdas, and
# callable kwargs all fail serialization. This rules out passing get_session
# as a function argument. Mitigation: this reference is set exactly once at
# startup and never mutated after, making it a late-bound constant.
_get_session: typing.Callable[..., typing.Iterator[orm.Session]] | None = None


#: One schedule_time per schedule id, written by the scheduler thread and popped by the
#: worker running the job. In-memory: a crash kills writer and reader together, and the
#: firing is redone from the job store on restart.
_SCHEDULE_TIMES: dict[str, datetime.datetime] = {}
_SCHEDULE_TIMES_LOCK = threading.Lock()


class _ScheduleTimeExecutor(pool.ThreadPoolExecutor):
    """Publishes the schedule_time APScheduler computes but does not pass to the job.

    This is the last frame holding `run_times`; the job is called without it.
    Recomputing from the cron expression instead is not an option: triggers only walk
    forward, and no backward lookback window suits both a sparse cron and a frequent one.

    Keyed by job id, which is unique per in-flight firing because the scheduler allows
    one instance of a schedule at a time and rejects a second before this method.
    """

    def _do_submit_job(
        self, job: typing.Any, run_times: list[datetime.datetime]
    ) -> None:
        #: Write before dispatch: a failed write aborts the submission, so a firing is
        #: missed rather than run with a fabricated schedule_time.
        with _SCHEDULE_TIMES_LOCK:
            #: More than one entry only after downtime. The last is the firing actually
            #: happening; earlier ones are being skipped.
            latest_schedule_time = run_times[-1]
            _SCHEDULE_TIMES[job.id] = latest_schedule_time
        super()._do_submit_job(job, run_times)


def take_schedule_time(schedule_id: str) -> datetime.datetime | None:
    """Consume this firing's schedule_time; `None` for a fire APScheduler did not dispatch.

    a scheduled firing, read once     -> the scheduled datetime
    the same firing, read again       -> None, it popped
    a manual fire                     -> None, nothing was ever written
    """
    with _SCHEDULE_TIMES_LOCK:
        return _SCHEDULE_TIMES.pop(schedule_id, None)


def execute_pipeline_schedule(
    *,
    pipeline_schedule_id: str,
    manual: bool = False,
) -> api_server_sql.PipelineRunResponse | None:
    """APScheduler callback that fires a pipeline run for a schedule.

    Looks up the ScheduledPipelineRun row, builds a TaskSpec from the stored
    pipeline_task_spec dict, attaches scheduling annotations, and creates
    the pipeline run as the schedule's creator.

    `manual` must default to False: job kwargs are pickled into the scheduler's job
    store, and rows stored before this parameter existed omit it.
    """
    #: A manual fire has no scheduled time; reading the map would steal a concurrent
    #: scheduled firing's. Pops, so it is read once into a local.
    schedule_time = None if manual else take_schedule_time(pipeline_schedule_id)
    #: Fixed here, at the top of the fire path, so every template in this firing reads the
    #: same trigger_time however long the run takes to reach the render.
    trigger_time = datetime.datetime.now(datetime.timezone.utc)

    if _get_session is None:
        logger.error("DB session factory not initialized, cannot execute schedule")
        return None

    # Two sessions are used because _pipeline_run_service.create() calls
    # session.begin() internally.  A Session(autocommit=False) auto-begins a
    # transaction on first use, so passing the same session to create() would
    # raise InvalidRequestError("A transaction is already begun").
    #
    # schedule_session — reads the schedule row and writes status updates.
    # run_session      — passed to create(), which manages its own transaction.
    try:
        with contextlib.contextmanager(_get_session)() as schedule_session:
            pipeline_schedule = schedule_session.get(
                db_models.ScheduledPipelineRun, pipeline_schedule_id
            )
            if not pipeline_schedule:
                logger.error(
                    f"Pipeline schedule {pipeline_schedule_id} not found, skipping"
                )
                return None
            if pipeline_schedule.paused:
                logger.info(
                    f"Pipeline schedule {pipeline_schedule_id} is paused, skipping"
                )
                return None
            source = _classify_source(pipeline_schedule)
            if source is _Source.NONE:
                logger.error(
                    f"Pipeline schedule {pipeline_schedule_id}"
                    f" ({pipeline_schedule.name})"
                    " has no pipeline_task_spec, skipping"
                )
                pipeline_schedule.last_run_at = datetime.datetime.now(
                    datetime.timezone.utc
                )
                pipeline_schedule.last_run_submission_result = (
                    f"{db_models.SubmissionResult.ERROR.value}: "
                    "pipeline_task_spec is None"
                )
                schedule_session.commit()
                return None

            annotations = {
                "tangleml.com/source/scheduler": "true",
                "tangleml.com/scheduling/id": pipeline_schedule_id,
                "tangleml.com/scheduling/name": pipeline_schedule.name,
                "tangleml.com/scheduling/cron": f"{pipeline_schedule.cron_expression} ({pipeline_schedule.timezone})",
                "tangleml.com/scheduling/updated_at": pipeline_schedule.updated_at.isoformat(),
            }

            now = datetime.datetime.now(datetime.timezone.utc)
            run: api_server_sql.PipelineRunResponse | None = None
            submission_result: str = db_models.SubmissionResult.SUCCESS.value
            #: Read before the try so the refusal guard below can consult it even when the
            #: failure happened during rendering.
            templates = pipeline_templates.get_pipeline_templates(
                original=pipeline_schedule.settings
            )

            try:
                # Separate session so create() can call session.begin() without
                # conflicting with schedule_session's active transaction. This is
                # also why every submission below takes run_session: the saved
                # pipeline path asks for `end_lookup_transaction`, and the run
                # reference path rolls back explicitly, and either rollback would
                # discard the pending last_run_at write if it landed on
                # schedule_session.
                schedule_render = render_schedule_templates(
                    templates=templates,
                    schedule_time=schedule_time,
                    trigger_time=trigger_time,
                )
                annotations.update(
                    template_annotations.for_firing(
                        templates=templates,
                        rendered=schedule_render.rendered,
                    )
                )
                with contextlib.contextmanager(_get_session)() as run_session:
                    run = _submit(
                        source=source,
                        run_session=run_session,
                        pipeline_schedule=pipeline_schedule,
                        annotations=annotations,
                        rendered=schedule_render.rendered,
                    )
                logger.info(
                    f"Pipeline schedule triggered (schedule_id={pipeline_schedule_id}, run_id={run.id})"
                )
                #: Reported here, not at the render: `runs_with_a_failed_key` is a count of
                #: runs, so a render whose submission was refused must not reach it. Direct
                #: rather than deferred, unlike the subscription twin -- `_submit` committed
                #: the run on its own session, so by here it is durable and the
                #: `schedule_session.commit()` below only stamps `last_run_at`.
                #:
                #: A data-quality event, not a control-flow one: the run still goes and each
                #: failed key keeps whatever the spec already had.
                render_observer.report(
                    rendered=schedule_render.rendered,
                    templates=templates,
                    clock=schedule_render.clock,
                    identity={
                        "schedule_id": pipeline_schedule.id,
                        "schedule_name": pipeline_schedule.name,
                    },
                )
            except Exception as e:
                logger.exception(
                    f"Pipeline schedule failed (schedule_id={pipeline_schedule_id})"
                )
                #: Nothing else counts a cron submission refusal. Guarded: this `except`
                #: also catches failures templating had no part in.
                if templates:
                    render_observer.submission_rejected(
                        kind=(
                            sources.Kind.CRON.value
                            if schedule_time is not None
                            else sources.Kind.MANUAL.value
                        )
                    )
                submission_result = f"{db_models.SubmissionResult.ERROR.value}: {e}"

            pipeline_schedule.last_run_at = now
            pipeline_schedule.last_run_submission_result = submission_result
            schedule_session.commit()
        return run
    except Exception as e:
        # The inner handler only covers create(). Anything raised before it — a
        # TaskSpec that no longer parses, a failed schedule read — would otherwise
        # leave last_run_at and last_run_submission_result untouched, so a schedule
        # that fires and fails every night still reads as "never run". Record it on
        # a fresh session, because the one that raised may be unusable.
        logger.exception(
            f"Pipeline schedule execution error (schedule_id={pipeline_schedule_id})"
        )
        try:
            with contextlib.contextmanager(_get_session)() as error_session:
                pipeline_schedule = error_session.get(
                    db_models.ScheduledPipelineRun, pipeline_schedule_id
                )
                if pipeline_schedule is not None:
                    pipeline_schedule.last_run_at = datetime.datetime.now(
                        datetime.timezone.utc
                    )
                    pipeline_schedule.last_run_submission_result = (
                        f"{db_models.SubmissionResult.ERROR.value}: {e}"
                    )
                    error_session.commit()
        except Exception:
            logger.exception(
                f"Failed to record schedule execution error (schedule_id={pipeline_schedule_id})"
            )
        return None
