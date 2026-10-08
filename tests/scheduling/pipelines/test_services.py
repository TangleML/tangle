"""Unit tests for scheduling.pipelines.services."""

import datetime
import threading

import pytest
from apscheduler.triggers import interval

from cloud_pipelines_backend.scheduling.pipelines import executor, services


class TestBuildCronTrigger:
    def test_five_field_cron(self) -> None:
        trigger = services.build_cron_trigger(
            cron_expression="0 9 * * MON-FRI",
        )
        assert trigger is not None

    def test_six_field_cron(self) -> None:
        trigger = services.build_cron_trigger(
            cron_expression="*/10 * * * * *",
        )
        assert trigger is not None

    def test_invalid_field_count(self) -> None:
        with pytest.raises(ValueError, match="expected 5 or 6 fields"):
            services.build_cron_trigger(
                cron_expression="* * * *",
            )

    def test_invalid_cron_syntax(self) -> None:
        with pytest.raises(Exception):
            services.build_cron_trigger(
                cron_expression="99 99 99 99 99",
            )

    def test_timezone_applied(self) -> None:
        trigger = services.build_cron_trigger(
            cron_expression="0 9 * * *",
            timezone="America/Toronto",
        )
        assert str(trigger.timezone) == "America/Toronto"


#: Module level, and the probe's state with it, because SQLAlchemyJobStore pickles every
#: job: a closure cannot be serialized, so APScheduler refuses it at `add_job`.
_SCHEDULE_TIME_PROBE_FIRED = threading.Event()
_SCHEDULE_TIME_PROBE_TIMES: list[datetime.datetime | None] = []


def record_the_schedule_time_this_firing_was_given(
    *, pipeline_schedule_id: str
) -> None:
    """Stand-in for `execute_pipeline_schedule`, doing only the one thing under test."""
    _SCHEDULE_TIME_PROBE_TIMES.append(executor.take_schedule_time(pipeline_schedule_id))
    _SCHEDULE_TIME_PROBE_FIRED.set()


class TestTheScheduleTimeExecutorIsWired:
    """Without this, `schedule_time` renders as unavailable on every scheduled fire and
    nothing else fails -- the executor subclass would be dead code that looks alive.
    Found by review: deleting the registration left the whole suite green.
    """

    def test_the_default_executor_is_the_schedule_time_one(
        self, scheduler_svc: services.SchedulerService
    ) -> None:
        """Registered under "default" specifically: that is the alias `add_job` uses when
        a job names no executor (`apscheduler/job.py`), which every schedule here does.
        """
        assert isinstance(
            scheduler_svc._scheduler._executors["default"],
            executor._ScheduleTimeExecutor,
        )

    def test_a_fired_job_can_read_the_schedule_time_the_executor_published(
        self, scheduler_svc: services.SchedulerService
    ) -> None:
        """End to end through a real BackgroundScheduler rather than a hand-built
        executor, so a registration that exists but never runs still fails.
        """
        _SCHEDULE_TIME_PROBE_FIRED.clear()
        _SCHEDULE_TIME_PROBE_TIMES.clear()

        # The fixture has already started this scheduler.
        #
        # An explicit timezone, because `SQLAlchemyJobStore` pickles the job: a trigger
        # built without one takes the host's, and in a container `tzlocal` cannot name the
        # zone and hands back a keyless `ZoneInfo` that pickling rejects. Which zone this
        # is does not matter to the assertions.
        scheduler_svc._scheduler.add_job(
            record_the_schedule_time_this_firing_was_given,
            trigger=interval.IntervalTrigger(seconds=1, timezone=datetime.timezone.utc),
            id="schedule-time-probe",
            kwargs={"pipeline_schedule_id": "schedule-time-probe"},
        )
        assert _SCHEDULE_TIME_PROBE_FIRED.wait(timeout=10), "the job never fired"
        scheduler_svc._scheduler.remove_job("schedule-time-probe")

        assert _SCHEDULE_TIME_PROBE_TIMES and isinstance(
            _SCHEDULE_TIME_PROBE_TIMES[0], datetime.datetime
        )
        assert _SCHEDULE_TIME_PROBE_TIMES[0].tzinfo is not None

    def test_coalescing_is_on_so_the_schedule_time_is_the_latest_one(
        self, scheduler_svc: services.SchedulerService
    ) -> None:
        """The job default that decides which time `_ScheduleTimeExecutor` publishes: the
        scheduler cuts `run_times` to its last entry only when `coalesce` is true.
        """
        assert scheduler_svc._scheduler._job_defaults["coalesce"] is True

    def test_one_firing_at_a_time_so_a_schedule_time_cannot_be_overwritten_in_flight(
        self, scheduler_svc: services.SchedulerService
    ) -> None:
        """Keying the map by schedule id is only safe under `max_instances: 1`; a second
        concurrent firing is rejected before it reaches `_do_submit_job`.
        """
        assert scheduler_svc._scheduler._job_defaults["max_instances"] == 1


class TestSchedulerService:
    def test_add_and_get_next_run(
        self, scheduler_svc: services.SchedulerService
    ) -> None:
        scheduler_svc.add_schedule(
            schedule_id="test-001",
            cron_expression="0 9 * * *",
            timezone="UTC",
        )
        next_run = scheduler_svc.get_next_run_time(schedule_id="test-001")
        assert next_run is not None

    def test_add_then_pause_schedule(
        self, scheduler_svc: services.SchedulerService
    ) -> None:
        scheduler_svc.add_schedule(
            schedule_id="test-002",
            cron_expression="0 9 * * *",
        )
        assert scheduler_svc.get_next_run_time(schedule_id="test-002") is not None

        scheduler_svc.update_schedule(
            schedule_id="test-002",
            paused=True,
            current_cron="0 9 * * *",
            current_timezone="UTC",
        )
        assert scheduler_svc.get_next_run_time(schedule_id="test-002") is None

    def test_remove_schedule(self, scheduler_svc: services.SchedulerService) -> None:
        scheduler_svc.add_schedule(
            schedule_id="test-003",
            cron_expression="0 9 * * *",
        )
        scheduler_svc.remove_schedule(schedule_id="test-003")
        next_run = scheduler_svc.get_next_run_time(schedule_id="test-003")
        assert next_run is None

    def test_update_cron(self, scheduler_svc: services.SchedulerService) -> None:
        scheduler_svc.add_schedule(
            schedule_id="test-004",
            cron_expression="0 0 1 1 *",
            timezone="UTC",
        )
        scheduler_svc.update_schedule(
            schedule_id="test-004",
            cron_expression="* * * * *",
            current_cron="0 0 1 1 *",
            current_timezone="UTC",
        )
        job = scheduler_svc._scheduler.get_job("test-004")
        assert job is not None
        assert (
            "every minute" not in str(job.trigger).lower()
            or job.next_run_time is not None
        )

    def test_pause_and_resume(self, scheduler_svc: services.SchedulerService) -> None:
        scheduler_svc.add_schedule(
            schedule_id="test-005",
            cron_expression="0 9 * * *",
        )
        scheduler_svc.update_schedule(
            schedule_id="test-005",
            paused=True,
            current_cron="0 9 * * *",
            current_timezone="UTC",
        )
        assert scheduler_svc.get_next_run_time(schedule_id="test-005") is None

        scheduler_svc.update_schedule(
            schedule_id="test-005",
            paused=False,
            current_cron="0 9 * * *",
            current_timezone="UTC",
        )
        assert scheduler_svc.get_next_run_time(schedule_id="test-005") is not None


def test_start_configures_the_saved_pipeline_service(db_engine, monkeypatch):
    """The service used by persisted callbacks must carry the application's run hooks."""
    from sqlalchemy import orm

    sentinel = object()

    def get_session():
        with orm.Session(db_engine) as session:
            yield session

    monkeypatch.setattr(executor, "_user_pipeline_service", None)
    monkeypatch.setattr(executor, "_get_session", None)
    scheduler = services.SchedulerService(
        get_session=get_session, pipeline_service=sentinel
    )
    monkeypatch.setattr(scheduler._scheduler, "start", lambda: None)
    scheduler.start()
    assert executor._user_pipeline_service is sentinel
    assert executor._get_session is get_session


def test_injected_callback_is_the_persisted_job_function(db_engine):
    from sqlalchemy import orm
    from apscheduler import util

    def get_session():
        with orm.Session(db_engine) as session:
            yield session

    callback = record_the_schedule_time_this_firing_was_given
    scheduler = services.SchedulerService(
        get_session=get_session, job_callback=callback
    )
    scheduler.add_schedule(schedule_id="callback-path", cron_expression="0 0 * * *")
    job = scheduler._scheduler.get_job("callback-path")
    assert job.func is callback
    assert job.func_ref == util.obj_to_ref(callback)
