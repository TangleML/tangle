import contextlib
import logging
import typing

from apscheduler.jobstores import sqlalchemy as aps_sqlalchemy
from apscheduler.schedulers import background
from apscheduler.triggers import cron
from sqlalchemy import orm

from cloud_pipelines_backend.scheduling.pipelines import db_models, executor
from cloud_pipelines_backend.user_pipelines import services as user_pipeline_services

logger = logging.getLogger(__name__)


def build_cron_trigger(
    *,
    cron_expression: str,
    timezone: str = db_models.DEFAULT_TIMEZONE_UTC,
) -> cron.CronTrigger:
    """Parse a cron expression into an APScheduler CronTrigger.

    Supports 5-field (standard: min hour day month dow) and
    6-field (extended: sec min hour day month dow) formats.
    """
    fields = cron_expression.strip().split()
    if len(fields) == 5:
        return cron.CronTrigger.from_crontab(cron_expression, timezone=timezone)
    elif len(fields) == 6:
        return cron.CronTrigger(
            second=fields[0],
            minute=fields[1],
            hour=fields[2],
            day=fields[3],
            month=fields[4],
            day_of_week=fields[5],
            timezone=timezone,
        )
    else:
        raise ValueError(
            f"Invalid cron expression {cron_expression!r}: expected 5 or 6 fields, got {len(fields)}"
        )


class SchedulerService:
    """In-process pipeline scheduler backed by APScheduler 3.x.

    Uses BackgroundScheduler (daemon thread) with SQLAlchemyJobStore for
    persistence across restarts. Runs inside the FastAPI process.
    """

    def __init__(
        self,
        *,
        get_session: typing.Callable[..., typing.Iterator[orm.Session]],
        pipeline_service: user_pipeline_services.UserPipelineService | None = None,
        job_callback: typing.Callable[
            ..., typing.Any
        ] = executor.execute_pipeline_schedule,
    ) -> None:
        self._get_session = get_session
        self._job_callback = job_callback
        self._pipeline_service = (
            pipeline_service or user_pipeline_services.UserPipelineService()
        )
        with contextlib.contextmanager(get_session)() as session:
            engine = session.get_bind()
        self._scheduler = background.BackgroundScheduler(
            #: Passed here, not via `add_executor`, which raises on a duplicate alias.
            #: The scheduler only builds its own default if none is configured by start.
            executors={"default": executor._ScheduleTimeExecutor()},
            job_defaults={
                # If multiple fires were missed, collapse them into a single execution.
                # This also fixes what a template's `schedule_time` renders to: the most
                # recent scheduled time, not the oldest unfired one.
                "coalesce": True,
                # Prevent concurrent executions of the same schedule
                "max_instances": 1,
                # Always fire missed jobs regardless of how long the server was down
                "misfire_grace_time": None,
            },
        )
        # Persist jobs to the `apscheduler_jobs` table (auto-created by APScheduler)
        # so scheduled triggers survive process restarts.
        self._scheduler.add_jobstore(aps_sqlalchemy.SQLAlchemyJobStore(engine=engine))

    def start(
        self,
    ) -> None:
        logger.info("Starting pipeline scheduler service")
        executor._get_session = self._get_session
        executor._user_pipeline_service = self._pipeline_service
        self._scheduler.start()
        logger.info("Pipeline scheduler service started")

    def shutdown(
        self,
    ) -> None:
        logger.info("Shutting down pipeline scheduler service")
        self._scheduler.shutdown(wait=False)
        logger.info("Pipeline scheduler service stopped")

    def add_schedule(
        self,
        *,
        schedule_id: str,
        cron_expression: str,
        timezone: str = db_models.DEFAULT_TIMEZONE_UTC,
    ) -> None:
        trigger = build_cron_trigger(
            cron_expression=cron_expression,
            timezone=timezone,
        )
        self._scheduler.add_job(
            self._job_callback,
            trigger=trigger,
            id=schedule_id,
            name=f"pipeline-schedule-id-{schedule_id}",
            kwargs={"pipeline_schedule_id": schedule_id},
            # Raise if a job with this ID already exists, preventing accidental overwrites
            replace_existing=False,
        )

    def update_schedule(
        self,
        *,
        schedule_id: str,
        cron_expression: str | None = None,
        timezone: str | None = None,
        paused: bool | None = None,
        current_cron: str,
        current_timezone: str,
    ) -> None:
        """Update the APScheduler job. Only cron/timezone/paused affect the
        scheduler — other fields (name, pipeline_task_spec) are DB-only
        and don't require APScheduler changes."""
        effective_cron = cron_expression or current_cron
        effective_tz = timezone or current_timezone

        if cron_expression is not None or timezone is not None:
            new_trigger = build_cron_trigger(
                cron_expression=effective_cron,
                timezone=effective_tz,
            )
            self._scheduler.reschedule_job(schedule_id, trigger=new_trigger)

        if paused is True:
            self._scheduler.pause_job(schedule_id)
        elif paused is False:
            # resume_job recalculates next_run_time from the trigger.  Combined
            # with misfire_grace_time=None and coalesce=True, this fires exactly
            # one coalesced run for all triggers missed while paused.
            self._scheduler.resume_job(schedule_id)

    def remove_schedule(
        self,
        *,
        schedule_id: str,
    ) -> None:
        try:
            self._scheduler.remove_job(schedule_id)
        except Exception:
            logger.warning(f"APScheduler job {schedule_id} not found during removal")

    def get_next_run_time(
        self,
        *,
        schedule_id: str,
    ) -> str | None:
        job = self._scheduler.get_job(schedule_id)
        if job and job.next_run_time:
            return job.next_run_time.isoformat()
        return None
