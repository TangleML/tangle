"""Unit tests for scheduling.pipelines.executor."""

import ast
import copy
import datetime
import inspect
import json
import logging
import textwrap
import typing
from unittest import mock

import pytest
import sqlalchemy
import sqlalchemy.orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import errors
from cloud_pipelines_backend.scheduling.pipelines import db_models, executor
from tests.scheduling.pipelines.conftest import (
    BARE_COMPONENT_SPEC,
    DEFAULT_USER,
    OTHER_USER,
    SAMPLE_PIPELINE_TASK_SPEC,
    insert_source_less_schedule_row,
)
from cloud_pipelines_backend.user_pipelines import db_models as user_pipeline_db_models
from cloud_pipelines_backend.user_pipelines import pipeline_run_annotations
from cloud_pipelines_backend.user_pipelines import services as user_pipeline_services


class TestExecutorRecordsFailures:
    """A failure before create() must still land on the row.

    The inner handler only wraps create(). A spec that no longer parses raises
    earlier, at the TaskSpec.from_json_dict call, and used to skip the last_run_*
    writes entirely — which is why a schedule firing and failing nightly still
    reported "never run".
    """

    def test_unparseable_spec_records_error_on_the_row(
        self,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        # Written straight to the DB: the API would now reject this shape, but rows
        # created before that validation existed still carry it.
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            schedule = db_models.ScheduledPipelineRun(
                name="Stored broken",
                cron_expression="0 8 * * *",
                pipeline_task_spec=BARE_COMPONENT_SPEC,
                created_by="test@example.com",
            )
            session.add(schedule)
            session.commit()
            sid = schedule.id

        result = executor.execute_pipeline_schedule(pipeline_schedule_id=sid)

        assert result is None
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            s = session.get(db_models.ScheduledPipelineRun, sid)
            assert s.last_run_at is not None
            assert s.last_run_submission_result is not None
            assert s.last_run_submission_result.startswith("Error:")
            assert "componentRef" in s.last_run_submission_result

    def test_no_pipeline_run_is_created_for_an_unparseable_spec(
        self,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            schedule = db_models.ScheduledPipelineRun(
                name="Stored broken 2",
                cron_expression="0 8 * * *",
                pipeline_task_spec=BARE_COMPONENT_SPEC,
                created_by="test@example.com",
            )
            session.add(schedule)
            session.commit()
            sid = schedule.id

        with mock.patch.object(executor._pipeline_run_service, "create") as mock_create:
            executor.execute_pipeline_schedule(pipeline_schedule_id=sid)
            mock_create.assert_not_called()


class TestExecutor:
    def test_skip_when_not_found(self, db_engine: sqlalchemy.Engine) -> None:
        result = executor.execute_pipeline_schedule(
            pipeline_schedule_id="nonexistent",
        )
        assert result is None

    def test_skip_when_paused(self, db_engine: sqlalchemy.Engine) -> None:
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            schedule = db_models.ScheduledPipelineRun(
                name="Paused",
                cron_expression="0 9 * * *",
                pipeline_task_spec=SAMPLE_PIPELINE_TASK_SPEC,
                created_by="test@example.com",
                paused=True,
            )
            session.add(schedule)
            session.commit()
            sid = schedule.id

        result = executor.execute_pipeline_schedule(
            pipeline_schedule_id=sid,
        )
        assert result is None
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            s = session.get(db_models.ScheduledPipelineRun, sid)
            assert s.last_run_at is None
            assert s.last_run_submission_result is None

    def test_annotations_attached(self, db_engine: sqlalchemy.Engine) -> None:
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            schedule = db_models.ScheduledPipelineRun(
                name="Annotation Test",
                cron_expression="0 9 * * MON-FRI",
                pipeline_task_spec=SAMPLE_PIPELINE_TASK_SPEC,
                created_by="alice@example.com",
            )
            session.add(schedule)
            session.commit()
            sid = schedule.id

        with mock.patch.object(
            executor._pipeline_run_service,
            "create",
        ) as mock_create:
            mock_create.return_value = mock.MagicMock(id="run-003")
            executor.execute_pipeline_schedule(
                pipeline_schedule_id=sid,
            )
            call_kwargs = mock_create.call_args.kwargs
            annotations = call_kwargs["annotations"]
            assert annotations["tangleml.com/source/scheduler"] == "true"
            assert annotations["tangleml.com/scheduling/id"] == sid
            assert annotations["tangleml.com/scheduling/name"] == "Annotation Test"
            assert (
                annotations["tangleml.com/scheduling/cron"] == "0 9 * * MON-FRI (UTC)"
            )
            assert "tangleml.com/scheduling/updated_at" in annotations

    def test_create_uses_separate_session(self, db_engine: sqlalchemy.Engine) -> None:
        """create() calls session.begin() internally — verify it gets its own
        session so it doesn't conflict with the schedule_session's auto-begun
        transaction (the original InvalidRequestError bug).
        """
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            schedule = db_models.ScheduledPipelineRun(
                name="Session Isolation",
                cron_expression="0 9 * * *",
                pipeline_task_spec=SAMPLE_PIPELINE_TASK_SPEC,
                created_by="alice@example.com",
            )
            session.add(schedule)
            session.commit()
            sid = schedule.id

        def fake_create(*, session: sqlalchemy.orm.Session, **kwargs):
            with session.begin():
                pass
            return mock.MagicMock(id="run-iso")

        with mock.patch.object(
            executor._pipeline_run_service,
            "create",
            side_effect=fake_create,
        ):
            result = executor.execute_pipeline_schedule(
                pipeline_schedule_id=sid,
            )

        assert result is not None
        assert result.id == "run-iso"

    def test_executor_does_not_change_updated_at(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            schedule = db_models.ScheduledPipelineRun(
                name="Updated At Stable",
                cron_expression="0 9 * * *",
                pipeline_task_spec=SAMPLE_PIPELINE_TASK_SPEC,
                created_by="alice@example.com",
            )
            session.add(schedule)
            session.commit()
            session.refresh(schedule)
            sid = schedule.id
            original_updated_at = schedule.updated_at

        with mock.patch.object(
            executor._pipeline_run_service,
            "create",
        ) as mock_create:
            mock_create.return_value = mock.MagicMock(id="run-stable")
            executor.execute_pipeline_schedule(
                pipeline_schedule_id=sid,
            )

        with sqlalchemy.orm.Session(bind=db_engine) as session:
            s = session.get(db_models.ScheduledPipelineRun, sid)
            assert s.last_run_at is not None
            assert s.last_run_submission_result == "Success"
            assert s.updated_at == original_updated_at

    def test_status_updated_on_success(self, db_engine: sqlalchemy.Engine) -> None:
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            schedule = db_models.ScheduledPipelineRun(
                name="Status Test",
                cron_expression="0 9 * * *",
                pipeline_task_spec=SAMPLE_PIPELINE_TASK_SPEC,
                created_by="alice@example.com",
            )
            session.add(schedule)
            session.commit()
            sid = schedule.id

        with mock.patch.object(
            executor._pipeline_run_service,
            "create",
        ) as mock_create:
            mock_create.return_value = mock.MagicMock(id="run-004")
            executor.execute_pipeline_schedule(
                pipeline_schedule_id=sid,
            )

        with sqlalchemy.orm.Session(bind=db_engine) as session:
            s = session.get(db_models.ScheduledPipelineRun, sid)
            assert s.last_run_at is not None
            assert s.last_run_submission_result == "Success"

    def test_status_updated_on_error(self, db_engine: sqlalchemy.Engine) -> None:
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            schedule = db_models.ScheduledPipelineRun(
                name="Error Test",
                cron_expression="0 9 * * *",
                pipeline_task_spec=SAMPLE_PIPELINE_TASK_SPEC,
                created_by="alice@example.com",
            )
            session.add(schedule)
            session.commit()
            sid = schedule.id

        with mock.patch.object(
            executor._pipeline_run_service,
            "create",
            side_effect=RuntimeError("boom"),
        ):
            executor.execute_pipeline_schedule(
                pipeline_schedule_id=sid,
            )

        with sqlalchemy.orm.Session(bind=db_engine) as session:
            s = session.get(db_models.ScheduledPipelineRun, sid)
            assert s.last_run_at is not None
            assert s.last_run_submission_result.startswith("Error")
            assert "boom" in s.last_run_submission_result

    def test_create_failure_records_error(self, db_engine: sqlalchemy.Engine) -> None:
        """If create() raises, last_run_submission_result contains the error."""
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            schedule = db_models.ScheduledPipelineRun(
                name="Create Failure",
                cron_expression="0 9 * * *",
                pipeline_task_spec=SAMPLE_PIPELINE_TASK_SPEC,
                created_by="alice@example.com",
            )
            session.add(schedule)
            session.commit()
            sid = schedule.id

        with mock.patch.object(
            executor._pipeline_run_service,
            "create",
            side_effect=RuntimeError("create failed"),
        ):
            result = executor.execute_pipeline_schedule(
                pipeline_schedule_id=sid,
            )

        assert result is None
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            s = session.get(db_models.ScheduledPipelineRun, sid)
            assert s.last_run_at is not None
            assert "Error" in s.last_run_submission_result
            assert "create failed" in s.last_run_submission_result

    def test_skip_when_pipeline_task_spec_is_none(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        # ck_scheduled_pipeline_run_source now forbids a row with no
        # body source, so this one is written past the constraint on purpose: the
        # shape is only reachable as a legacy or post-rollback artifact, and the
        # executor must still record an error rather than crash on it.
        sid = insert_source_less_schedule_row(
            db_engine=db_engine,
            name="No Spec",
            created_by="alice@example.com",
        )

        result = executor.execute_pipeline_schedule(
            pipeline_schedule_id=sid,
        )

        assert result is None
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            s = session.get(db_models.ScheduledPipelineRun, sid)
            assert s.last_run_at is not None
            assert "Error" in s.last_run_submission_result
            assert "pipeline_task_spec is None" in s.last_run_submission_result

    def test_db_error_is_caught_and_logged(self, db_engine: sqlalchemy.Engine) -> None:
        """If the DB session itself fails (e.g. connection error), the outer
        try/except catches it, logs the traceback, and returns None instead
        of letting the exception propagate to APScheduler.
        """
        with mock.patch.object(
            executor,
            "_get_session",
            side_effect=sqlalchemy.exc.OperationalError(
                "select 1",
                {},
                Exception("connection refused"),
            ),
        ):
            result = executor.execute_pipeline_schedule(
                pipeline_schedule_id="any-id",
            )

        assert result is None

    def test_returns_none_when_no_session_factory(self) -> None:
        """When _get_session is None, returns None."""
        original = executor._get_session
        try:
            executor._get_session = None
            result = executor.execute_pipeline_schedule(
                pipeline_schedule_id="any-id",
            )
            assert result is None
        finally:
            executor._get_session = original


def _spec_declaring(*inputs: str, name: str = "templated") -> dict[str, object]:
    """A task spec whose root component declares `inputs`, all optional.

    Run submission refuses an argument for an input the pipeline does not declare, and a
    declared-but-required input with no value refuses too, so anything receiving rendered
    arguments declares them optional.
    """
    spec = copy.deepcopy(SAMPLE_PIPELINE_TASK_SPEC)
    spec["componentRef"]["spec"]["name"] = name
    spec["componentRef"]["spec"]["inputs"] = [
        {"name": each, "optional": True} for each in inputs
    ]
    return spec


def _save_pipeline(
    *,
    db_engine: sqlalchemy.Engine,
    user_id: str = DEFAULT_USER,
    file_path: str = "rollup/daily.pipeline.yaml",
    name: str = "saved-pipeline",
    versioning_mode: user_pipeline_db_models.PipelineVersioningMode = (
        user_pipeline_db_models.PipelineVersioningMode.FULL
    ),
    declares: tuple[str, ...] = (),
) -> tuple[str, str]:
    """Save a pipeline through the real service and return (pipeline_id, version_key).

    `declares` names the pipeline's inputs. Run submission refuses an argument for an input
    the pipeline does not declare, so anything passing run_arguments must declare them.
    """
    spec = (
        _spec_declaring(*declares, name=name)
        if declares
        else copy.deepcopy(SAMPLE_PIPELINE_TASK_SPEC)
    )
    spec["componentRef"]["spec"]["name"] = name
    with sqlalchemy.orm.Session(bind=db_engine) as session:
        result = user_pipeline_services.UserPipelineService().set_pipeline(
            session=session,
            user_id=user_id,
            file_path=file_path,
            root_pipeline_task=spec,
            pipeline_run_annotations=None,
            versioning_mode=versioning_mode,
        )
        return result.pipeline.id, result.version.version_key


def _insert_reference_schedule(
    *,
    db_engine: sqlalchemy.Engine,
    created_by: str = DEFAULT_USER,
    run_id: str | None = None,
    pipeline_id: str | None = None,
    version_key: str | None = None,
    inline_spec: dict[str, object] | None = None,
    settings: dict[str, object] | None = None,
) -> str:
    with sqlalchemy.orm.Session(bind=db_engine) as session:
        schedule = db_models.ScheduledPipelineRun(
            name="reference-schedule",
            cron_expression="0 3 * * *",
            timezone="UTC",
            pipeline_task_spec=inline_spec,
            pipeline_task_spec_from_pipeline_run_id=run_id,
            pipeline_task_spec_from_user_pipeline_id=pipeline_id,
            pipeline_task_spec_from_user_pipeline_version_key=version_key,
            settings=settings,
            created_by=created_by,
        )
        session.add(schedule)
        session.commit()
        return schedule.id


def _insert_pipeline_run(
    *,
    db_engine: sqlalchemy.Engine,
    created_by: str | None = DEFAULT_USER,
    name: str = "referenced-run",
    declares: tuple[str, ...] = (),
) -> str:
    spec = (
        _spec_declaring(*declares, name=name)
        if declares
        else copy.deepcopy(SAMPLE_PIPELINE_TASK_SPEC)
    )
    spec["componentRef"]["spec"]["name"] = name
    with sqlalchemy.orm.Session(bind=db_engine) as session:
        run = bts.PipelineRun(
            root_execution=bts.ExecutionNode(task_spec=spec),
            created_by=created_by,
        )
        session.add(run)
        session.commit()
        return run.id


def _insert_two_source_row(
    *,
    db_engine: sqlalchemy.Engine,
    run_id: str,
    inline_spec: dict[str, object],
    created_by: str = DEFAULT_USER,
) -> str:
    """Write a row carrying both an inline spec and a reference.

    The CHECK constraint forbids this, so it goes through the driver with
    ``ignore_check_constraints`` -- the same technique, and the same reason, as
    ``insert_source_less_schedule_row``.
    """
    schedule_id = bts.generate_unique_id()
    now = (
        datetime.datetime.now(datetime.timezone.utc)
        .replace(tzinfo=None)
        .isoformat(sep=" ")
    )
    with db_engine.connect() as conn:
        conn.exec_driver_sql("PRAGMA ignore_check_constraints = ON")
        conn.execute(
            sqlalchemy.text(
                "INSERT INTO scheduled_pipeline_run"
                " (id, name, cron_expression, timezone, pipeline_task_spec,"
                "  pipeline_task_spec_from_pipeline_run_id,"
                "  paused, created_by, created_at, updated_at)"
                " VALUES (:id, 'two-source', '0 3 * * *', 'UTC', :spec,"
                "  :run_id, 0, :created_by, :now, :now)"
            ),
            {
                "id": schedule_id,
                "spec": json.dumps(inline_spec),
                "run_id": run_id,
                "created_by": created_by,
                "now": now,
            },
        )
        conn.commit()
    return schedule_id


def _row(
    db_engine: sqlalchemy.Engine, schedule_id: str
) -> db_models.ScheduledPipelineRun:
    with sqlalchemy.orm.Session(bind=db_engine) as session:
        row = session.get(db_models.ScheduledPipelineRun, schedule_id)
        assert row is not None
        session.expunge(row)
        return row


def _created_run_arguments(db_engine: sqlalchemy.Engine) -> dict[str, object]:
    """The arguments on the single created run, which is where run_arguments lands."""
    with sqlalchemy.orm.Session(bind=db_engine) as session:
        runs = session.scalars(sqlalchemy.select(bts.PipelineRun)).all()
        assert len(runs) == 1, f"expected one run, got {len(runs)}"
        return dict(runs[0].root_execution.task_spec.get("arguments") or {})


def _all_run_arguments(db_engine: sqlalchemy.Engine) -> list[dict[str, object]]:
    """Arguments of every run, for the sources that leave the referenced run in the table too."""
    with sqlalchemy.orm.Session(bind=db_engine) as session:
        runs = session.scalars(sqlalchemy.select(bts.PipelineRun)).all()
        return [
            dict(run.root_execution.task_spec.get("arguments") or {}) for run in runs
        ]


def _created_run_names(db_engine: sqlalchemy.Engine) -> list[str]:
    """Pipeline names of every run created, to prove *which* spec was executed."""
    with sqlalchemy.orm.Session(bind=db_engine) as session:
        runs = session.scalars(sqlalchemy.select(bts.PipelineRun)).all()
        names = []
        for run in runs:
            spec = run.root_execution.task_spec
            names.append(spec.get("componentRef", {}).get("spec", {}).get("name"))
        return names


class TestReferenceSourcePrecedence:
    """Inline wins, and the source is chosen explicitly rather than by invariant."""

    def test_an_inline_spec_beats_a_reference_on_the_same_row(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """A malformed row must run yesterday's spec, not silently switch source.

        Two sources on one row is forbidden by ``ck_scheduled_pipeline_run_source``
        and unreachable through the ORM -- proven by this test originally failing
        on that constraint. It is written through the driver with checks disabled
        because the fire path still has to behave predictably on a row that
        predates the constraint or was written by something that bypassed it.
        """
        run_id = _insert_pipeline_run(db_engine=db_engine, name="the-reference")
        inline = copy.deepcopy(SAMPLE_PIPELINE_TASK_SPEC)
        inline["componentRef"]["spec"]["name"] = "the-inline-spec"
        schedule_id = _insert_two_source_row(
            db_engine=db_engine, run_id=run_id, inline_spec=inline
        )

        executor.execute_pipeline_schedule(pipeline_schedule_id=schedule_id)

        assert "the-inline-spec" in _created_run_names(db_engine)
        assert _row(db_engine, schedule_id).last_run_submission_result == (
            db_models.SubmissionResult.SUCCESS.value
        )

    def test_a_source_less_row_still_reports_the_legacy_message(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """Behaviour for rows with no source at all is unchanged."""
        schedule_id = insert_source_less_schedule_row(
            db_engine=db_engine, name="no-source"
        )

        executor.execute_pipeline_schedule(pipeline_schedule_id=schedule_id)

        row = _row(db_engine, schedule_id)
        assert row.last_run_at is not None
        assert "pipeline_task_spec is None" in (row.last_run_submission_result or "")


class TestRunReferenceExecution:
    """The transitional source: execute an earlier run's root TaskSpec."""

    def test_a_run_reference_executes_the_referenced_spec(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        run_id = _insert_pipeline_run(db_engine=db_engine, name="from-the-run")
        schedule_id = _insert_reference_schedule(db_engine=db_engine, run_id=run_id)

        executor.execute_pipeline_schedule(pipeline_schedule_id=schedule_id)

        assert _created_run_names(db_engine).count("from-the-run") == 2
        assert _row(db_engine, schedule_id).last_run_submission_result == (
            db_models.SubmissionResult.SUCCESS.value
        )

    def test_another_users_run_is_not_executable(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """Ownership, not just existence. Id-only lookup would run another user's spec."""
        run_id = _insert_pipeline_run(
            db_engine=db_engine, created_by=OTHER_USER, name="not-yours"
        )
        schedule_id = _insert_reference_schedule(
            db_engine=db_engine, created_by=DEFAULT_USER, run_id=run_id
        )

        executor.execute_pipeline_schedule(pipeline_schedule_id=schedule_id)

        # Only the referenced run exists; nothing new was submitted.
        assert _created_run_names(db_engine) == ["not-yours"]
        row = _row(db_engine, schedule_id)
        assert row.last_run_submission_result is not None
        assert db_models.SubmissionResult.ERROR.value in row.last_run_submission_result
        assert row.last_run_at is not None

    def test_the_refusal_does_not_disclose_that_the_run_exists(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """Not-found rather than forbidden: existence is itself information."""
        run_id = _insert_pipeline_run(db_engine=db_engine, created_by=OTHER_USER)
        schedule_id = _insert_reference_schedule(
            db_engine=db_engine, created_by=DEFAULT_USER, run_id=run_id
        )

        executor.execute_pipeline_schedule(pipeline_schedule_id=schedule_id)

        detail = _row(db_engine, schedule_id).last_run_submission_result or ""
        assert "not found" in detail.lower()
        assert OTHER_USER not in detail

    def test_an_unattributed_run_is_refused_rather_than_matched(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """PipelineRun.created_by is nullable; NULL must not decide authorization."""
        run_id = _insert_pipeline_run(db_engine=db_engine, created_by=None)
        schedule_id = _insert_reference_schedule(db_engine=db_engine, run_id=run_id)

        executor.execute_pipeline_schedule(pipeline_schedule_id=schedule_id)

        row = _row(db_engine, schedule_id)
        assert db_models.SubmissionResult.ERROR.value in (
            row.last_run_submission_result or ""
        )

    def test_a_missing_run_is_recorded_per_fire_and_does_not_raise(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """A run id with no run row: defence in depth for a degraded schema.

        The run column's foreign key was never dropped -- `f39c690` removed only
        the saved-pipeline constraints -- so on a healthy database this row
        cannot be written at all. This is deliberately NOT the saved-pipeline
        story: there is no orphan preflight that can decline to install this
        constraint, and `PipelineRun` has no soft-delete contract, so none of
        those escape hatches apply here.

        What is left is narrower and worth keeping anyway: a load performed with
        `foreign_key_checks` disabled, manual surgery, or a restored schema that
        is missing or has corrupted the constraint. Not "rows predating it" --
        `452ac0d` introduced the column and its foreign key on adjacent lines,
        so no legitimate row was ever written without it. A fire that meets one
        of the remaining cases must record a failure, not crash the job.
        """
        schedule_id = _insert_reference_schedule(
            db_engine=db_engine, run_id="00000000-0000-0000-0000-000000000000"
        )

        assert (
            executor.execute_pipeline_schedule(pipeline_schedule_id=schedule_id) is None
        )

        row = _row(db_engine, schedule_id)
        assert row.last_run_at is not None
        assert db_models.SubmissionResult.ERROR.value in (
            row.last_run_submission_result or ""
        )

    def test_reading_the_run_does_not_mutate_its_stored_spec(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        run_id = _insert_pipeline_run(db_engine=db_engine, name="unchanged")
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            before = copy.deepcopy(
                session.get(bts.PipelineRun, run_id).root_execution.task_spec
            )
        schedule_id = _insert_reference_schedule(db_engine=db_engine, run_id=run_id)

        executor.execute_pipeline_schedule(pipeline_schedule_id=schedule_id)

        with sqlalchemy.orm.Session(bind=db_engine) as session:
            after = session.get(bts.PipelineRun, run_id).root_execution.task_spec
        assert after == before


class TestSavedPipelineReferenceExecution:
    """Current-following and pinned execution of a saved pipeline."""

    def test_a_following_reference_executes_the_current_version(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        pipeline_id, _version = _save_pipeline(db_engine=db_engine, name="v1")
        schedule_id = _insert_reference_schedule(
            db_engine=db_engine, pipeline_id=pipeline_id
        )

        executor.execute_pipeline_schedule(pipeline_schedule_id=schedule_id)

        assert "v1" in _created_run_names(db_engine)
        assert _row(db_engine, schedule_id).last_run_submission_result == (
            db_models.SubmissionResult.SUCCESS.value
        )

    def test_a_following_reference_follows_a_changed_current_version(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """The point of following: the schedule row never changes, the content does."""
        pipeline_id, _v1 = _save_pipeline(db_engine=db_engine, name="v1")
        schedule_id = _insert_reference_schedule(
            db_engine=db_engine, pipeline_id=pipeline_id
        )
        executor.execute_pipeline_schedule(pipeline_schedule_id=schedule_id)

        _save_pipeline(db_engine=db_engine, name="v2")  # same user + file path
        executor.execute_pipeline_schedule(pipeline_schedule_id=schedule_id)

        names = _created_run_names(db_engine)
        assert "v1" in names
        assert "v2" in names

    def test_a_pinned_reference_stays_on_its_version(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        pipeline_id, v1 = _save_pipeline(db_engine=db_engine, name="pinned-v1")
        schedule_id = _insert_reference_schedule(
            db_engine=db_engine, pipeline_id=pipeline_id, version_key=v1
        )
        _save_pipeline(db_engine=db_engine, name="pinned-v2")

        executor.execute_pipeline_schedule(pipeline_schedule_id=schedule_id)

        names = _created_run_names(db_engine)
        assert "pinned-v1" in names
        assert "pinned-v2" not in names

    def test_another_users_pipeline_is_not_executable(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """The escalation this guards: user_id is optional in the lookup it calls."""
        pipeline_id, _version = _save_pipeline(
            db_engine=db_engine, user_id=OTHER_USER, name="someone-elses"
        )
        schedule_id = _insert_reference_schedule(
            db_engine=db_engine,
            created_by=DEFAULT_USER,
            pipeline_id=pipeline_id,
        )

        executor.execute_pipeline_schedule(pipeline_schedule_id=schedule_id)

        assert "someone-elses" not in _created_run_names(db_engine)
        row = _row(db_engine, schedule_id)
        assert db_models.SubmissionResult.ERROR.value in (
            row.last_run_submission_result or ""
        )

    def test_a_missing_pipeline_is_recorded_per_fire(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        schedule_id = _insert_reference_schedule(
            db_engine=db_engine,
            pipeline_id="11111111-1111-1111-1111-111111111111",
        )

        assert (
            executor.execute_pipeline_schedule(pipeline_schedule_id=schedule_id) is None
        )

        row = _row(db_engine, schedule_id)
        assert row.last_run_at is not None
        assert db_models.SubmissionResult.ERROR.value in (
            row.last_run_submission_result or ""
        )

    def test_both_source_annotations_are_present_on_a_scheduled_saved_run(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """Scheduler provenance and saved-pipeline provenance are both true."""
        pipeline_id, _version = _save_pipeline(db_engine=db_engine, name="annotated")
        schedule_id = _insert_reference_schedule(
            db_engine=db_engine, pipeline_id=pipeline_id
        )

        executor.execute_pipeline_schedule(pipeline_schedule_id=schedule_id)

        with sqlalchemy.orm.Session(bind=db_engine) as session:
            runs = session.scalars(sqlalchemy.select(bts.PipelineRun)).all()
            annotations = {}
            for run in runs:
                if run.annotations and "tangleml.com/scheduling/id" in run.annotations:
                    annotations = run.annotations
        assert annotations.get("tangleml.com/source/scheduler") == "true"
        assert annotations.get(pipeline_run_annotations.SOURCE_ANNOTATION) == "true"
        assert annotations["tangleml.com/scheduling/id"] == schedule_id
        assert (
            annotations[pipeline_run_annotations.PIPELINE_ID_ANNOTATION] == pipeline_id
        )


class TestPinsRequireFullVersioning:
    """A non-`current` version row is necessary but not sufficient for a pin."""

    def test_a_pin_against_a_disabled_pipeline_is_refused(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """Switching to DISABLED keeps historical rows, so the row still resolves.

        `get_pipeline_and_version` only excludes the reserved `current` key, so
        without the service check this pin would quietly execute a version the
        owner can no longer see or manage.
        """
        pipeline_id, v1 = _save_pipeline(db_engine=db_engine, name="historical")
        _save_pipeline(db_engine=db_engine, name="historical-v2")
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            user_pipeline_services.UserPipelineService().patch_pipeline_properties(
                session=session,
                user_id=DEFAULT_USER,
                pipeline_id=pipeline_id,
                versioning_mode=(
                    user_pipeline_db_models.PipelineVersioningMode.DISABLED
                ),
            )
        # The pinned row survived the mode switch — that is the trap.
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            assert (
                session.get(
                    user_pipeline_db_models.UserPipelineVersion,
                    (pipeline_id, v1),
                )
                is not None
            )
        schedule_id = _insert_reference_schedule(
            db_engine=db_engine, pipeline_id=pipeline_id, version_key=v1
        )

        executor.execute_pipeline_schedule(pipeline_schedule_id=schedule_id)

        assert "historical" not in _created_run_names(db_engine)
        detail = _row(db_engine, schedule_id).last_run_submission_result or ""
        assert db_models.SubmissionResult.ERROR.value in detail
        assert "pinned" in detail

    def test_following_still_works_in_disabled_mode(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """Following the mutable head is legal; only pinning is refused."""
        pipeline_id, _v1 = _save_pipeline(
            db_engine=db_engine,
            name="disabled-follow",
            versioning_mode=user_pipeline_db_models.PipelineVersioningMode.DISABLED,
        )
        schedule_id = _insert_reference_schedule(
            db_engine=db_engine, pipeline_id=pipeline_id
        )

        executor.execute_pipeline_schedule(pipeline_schedule_id=schedule_id)

        assert "disabled-follow" in _created_run_names(db_engine)
        assert _row(db_engine, schedule_id).last_run_submission_result == (
            db_models.SubmissionResult.SUCCESS.value
        )

    def test_the_check_is_a_service_function_not_an_executor_detail(
        self,
    ) -> None:
        """PR3's writer must reach the same verdict; two copies would be two answers."""
        assert callable(user_pipeline_services.require_pinnable_versioning)

    def test_the_mode_check_happens_inside_the_version_selection(self) -> None:
        """Not a preflight, and not optional from the caller's side.

        pi-33 reproduced the preflight version of this: validate the pin on a
        FULL pipeline, commit FULL->DISABLED from another session, then resume
        submission — and the run was created against the historical version. The
        window existed because the check was a separate read from the selection,
        and `create_from_pipeline` ends its lookup transaction before submitting,
        which would also release any lock taken outside it.

        So the guarantee has to be structural: the mode is checked inside the
        same locked read that selects the version. This asserts that shape
        directly, because an interleaving test alone would pass again if the
        check were moved back out and merely happened to win the race.
        """
        source = inspect.getsource(
            user_pipeline_services.UserPipelineService.get_pipeline_and_version
        )
        check = source.index("require_pinnable_versioning(pipeline)")
        selection = source.index("current_version_key")
        assert check < selection, "the mode check must precede version selection"
        assert "for_share=pinned" in source

        # The lookup lives in the no-commit entry point; the committing wrapper
        # only forwards. Both halves are asserted, because a flag that stopped
        # being forwarded would silently disable the check for every caller that
        # goes through the wrapper -- which is every scheduled fire.
        no_commit = inspect.getsource(
            user_pipeline_services.UserPipelineService.create_from_pipeline_no_commit
        )
        assert "require_pinnable=require_pinnable_versioning" in no_commit
        wrapper = inspect.getsource(
            user_pipeline_services.UserPipelineService.create_from_pipeline
        )
        assert "require_pinnable_versioning=require_pinnable_versioning" in wrapper

    def test_a_mode_change_after_selection_cannot_alter_what_ran(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """The interleaving pi-33 exploited, now closed.

        SQLite has no row locks, so this exercises ordering rather than locking:
        the pipeline is disabled *before* the fire, and the pin must be refused
        rather than resolved against the surviving historical row.
        """
        pipeline_id, v1 = _save_pipeline(db_engine=db_engine, name="raced-v1")
        _save_pipeline(db_engine=db_engine, name="raced-v2")
        schedule_id = _insert_reference_schedule(
            db_engine=db_engine, pipeline_id=pipeline_id, version_key=v1
        )
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            user_pipeline_services.UserPipelineService().patch_pipeline_properties(
                session=session,
                user_id=DEFAULT_USER,
                pipeline_id=pipeline_id,
                versioning_mode=(
                    user_pipeline_db_models.PipelineVersioningMode.DISABLED
                ),
            )

        executor.execute_pipeline_schedule(pipeline_schedule_id=schedule_id)

        assert "raced-v1" not in _created_run_names(db_engine)
        assert db_models.SubmissionResult.ERROR.value in (
            _row(db_engine, schedule_id).last_run_submission_result or ""
        )


class TestRunRefusalsAreIndistinguishable:
    """Absent, unattributed and other-owned runs must look identical outward."""

    def test_all_three_refusals_share_one_detail(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """Distinguishing them tells a schedule owner which run ids exist."""
        absent_run = "99999999-9999-9999-9999-999999999999"
        unattributed_run = _insert_pipeline_run(db_engine=db_engine, created_by=None)
        other_owned_run = _insert_pipeline_run(
            db_engine=db_engine, created_by=OTHER_USER
        )
        cases = {
            "absent": absent_run,
            "unattributed": unattributed_run,
            "other-owned": other_owned_run,
        }

        # Normalize ONLY the run id -- the one value the caller already supplied
        # and which therefore cannot disclose anything. Everything else in the
        # message is compared verbatim.
        #
        # An earlier version of this test was vacuous (pi-33): it substituted the
        # schedule id rather than the run id, so nothing was replaced, and then
        # truncated at "Pipeline run", collapsing all three to the bare "ERROR: "
        # prefix. It would have passed while the details diverged completely.
        normalized: dict[str, str] = {}
        for label, run_id in cases.items():
            schedule_id = _insert_reference_schedule(db_engine=db_engine, run_id=run_id)
            executor.execute_pipeline_schedule(pipeline_schedule_id=schedule_id)
            detail = _row(db_engine, schedule_id).last_run_submission_result or ""
            assert run_id in detail, f"{label}: run id absent, normalization is a no-op"
            normalized[label] = detail.replace(run_id, "<RUN>")

        assert len(set(normalized.values())) == 1, normalized
        # Pin the template too, so the three staying identical while all becoming
        # more revealing would still fail.
        assert normalized["absent"] == (
            f"{db_models.SubmissionResult.ERROR.value}: Pipeline run '<RUN>' was not found for user {DEFAULT_USER!r}."
        )
        for label in ("unattributed", "other-owned"):
            assert "owner" not in normalized[label], label
            assert OTHER_USER not in normalized[label], label


class TestPinnedLockWindow:
    """Who ends the transaction a pinned lookup locked in, and on whose authority.

    A pinned lookup takes ``SELECT ... FOR SHARE`` on the pipeline row so the mode
    check and the version selection are one atomic read. When that lock is
    released is the question here, and #562 landing on main forced it to be
    answered twice.

    The lock used to be released by an unconditional ``session.rollback()`` in
    ``create_from_pipeline``, immediately after the selected content was copied
    out -- which also ended the implicit lookup transaction, required because the
    old delegate opened its own ``session.begin()``. An automated reviewer had
    asked for exactly that narrowing.

    That reason is gone: the run is built with ``_create_in_transaction`` inside
    the *caller's* transaction. An unconditional rollback is now a correctness
    bug, because it discards the caller's pending writes -- for the trigger path,
    the fence row claiming the cycle. See ``TestWhoOwnsTheCommit`` in
    ``tests/user_pipelines/test_saved_pipeline_execution.py``.

    Simply deleting it was not good enough either. Held to commit, the shared lock
    spans recursive execution and artifact insertion for the whole graph, and a
    shared lock blocks every exclusive one -- so ``set_pipeline`` and
    ``delete_pipeline`` on that pipeline would queue behind a scheduled fire, not
    just a mode change. That is a regression of the narrowing, and wider than the
    original rationale admitted.

    So the rollback is opt-in (``end_lookup_transaction``) and the executor opts
    in, because ``run_session`` is created per submission and provably has nothing
    pending. Default off keeps every other caller safe.

    SQLite has no row locks, so these assert structure and ordering rather than
    lock contention, exactly as the other locking tests here do.
    """

    @staticmethod
    def _rollback_calls(function: object) -> list[ast.Call]:
        """Parsed, not grepped.

        The comments around these call sites discuss the rollback in prose, so a
        substring check would pass on the explanation and fail when the
        explanation is reworded -- the exact inversion of the point.
        """
        tree = ast.parse(textwrap.dedent(inspect.getsource(function)))
        return [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "rollback"
        ]

    def test_the_rollback_is_reachable_only_behind_the_opt_in(self) -> None:
        """The guard against making it unconditional again.

        One rollback, and it must sit under `if end_lookup_transaction:`. A
        rollback outside that branch is the bug main added a regression test for.
        """
        source = textwrap.dedent(
            inspect.getsource(
                user_pipeline_services.UserPipelineService.create_from_pipeline_no_commit
            )
        )
        guarded = [
            node
            for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.If)
            and isinstance(node.test, ast.Name)
            and node.test.id == "end_lookup_transaction"
            for call in ast.walk(node)
            if isinstance(call, ast.Call)
            and isinstance(call.func, ast.Attribute)
            and call.func.attr == "rollback"
        ]
        assert (
            len(
                self._rollback_calls(
                    user_pipeline_services.UserPipelineService.create_from_pipeline_no_commit
                )
            )
            == 1
        )
        assert (
            len(guarded) == 1
        ), "the rollback must be reachable only when the caller asked for it"

        # The committing wrapper forwards the flag and never rolls back itself.
        assert (
            self._rollback_calls(
                user_pipeline_services.UserPipelineService.create_from_pipeline
            )
            == []
        )
        wrapper = inspect.getsource(
            user_pipeline_services.UserPipelineService.create_from_pipeline
        )
        assert "end_lookup_transaction=end_lookup_transaction" in wrapper

    def test_the_default_leaves_the_callers_transaction_alone(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """Executed, on the path that actually takes the lock.

        A pinned no-commit creation without the opt-in must not touch what the
        caller had pending. This is the trigger's shape: fence row first, run
        second, one transaction.
        """
        pipeline_id, version_key = _save_pipeline(
            db_engine=db_engine, name="pinned-and-fenced"
        )
        _save_pipeline(db_engine=db_engine, name="pinned-and-fenced-v2")
        marker_path = "pipelines/written-before-the-pinned-run.yaml"

        with sqlalchemy.orm.Session(bind=db_engine) as session:
            session.add(
                user_pipeline_db_models.UserPipeline(
                    user_id=DEFAULT_USER, file_path=marker_path
                )
            )
            session.flush()
            user_pipeline_services.UserPipelineService().create_from_pipeline_no_commit(
                session=session,
                pipeline_id=pipeline_id,
                user_id=DEFAULT_USER,
                file_path=None,
                version=version_key,
                run_arguments=None,
                pipeline_run_annotations=None,
                created_by=DEFAULT_USER,
                require_pinnable_versioning=True,
            )
            session.commit()

        with sqlalchemy.orm.Session(bind=db_engine) as other:
            survived = other.scalar(
                sqlalchemy.select(user_pipeline_db_models.UserPipeline).where(
                    user_pipeline_db_models.UserPipeline.file_path == marker_path
                )
            )
        assert (
            survived is not None
        ), "the caller's pending write was discarded by the run creation"
        assert "pinned-and-fenced" in _created_run_names(db_engine)

    def test_the_executor_is_the_caller_that_opts_in(self) -> None:
        """The narrow window is restored exactly where it can be proven safe.

        Asserted at the call site rather than by counting statements, because the
        precondition -- a session with nothing pending -- is a property of the
        caller, not of the service.
        """
        source = inspect.getsource(executor._submit)
        assert "end_lookup_transaction=True" in source
        assert "require_pinnable_versioning=True" in source

    def test_the_opt_in_releases_before_the_run_is_built(self) -> None:
        """Ordering, since SQLite cannot show the lock itself.

        The rollback has to come after the copies -- otherwise the ORM rows it
        expires are read afterwards -- and before parsing and insertion, which is
        the whole point of asking for it.
        """
        source = inspect.getsource(
            user_pipeline_services.UserPipelineService.create_from_pipeline_no_commit
        )
        source = ast.unparse(ast.parse(textwrap.dedent(source)))
        rollback = source.index("session.rollback()")
        copies = source.index("stored_annotations: dict[str, str] = copy.deepcopy")
        parse = source.index("TaskSpec.from_json_dict(stored_task_json)")
        insert = source.index("_create_in_transaction(")
        assert copies < rollback < parse < insert

    def test_the_run_is_built_inside_the_callers_transaction(self) -> None:
        """Why no rollback is needed, asserted rather than assumed.

        If this ever went back to the delegate that opens its own transaction,
        the missing rollback above would stop being correct and start being a
        crash -- so the two facts are checked together.
        """
        source = inspect.getsource(
            user_pipeline_services.UserPipelineService.create_from_pipeline_no_commit
        )
        assert "_create_in_transaction(" in source
        assert ".create(" not in source

    def test_nothing_after_the_copies_touches_the_orm_rows(self) -> None:
        """Under the opt-in the rollback expires `pipeline` and `version_row`.

        Reading either afterwards would silently re-query with no lock behind it,
        which is the bug this ordering could most easily introduce -- and it would
        only bite the one caller that opts in.
        """
        source = inspect.getsource(
            user_pipeline_services.UserPipelineService.create_from_pipeline_no_commit
        )
        source = ast.unparse(ast.parse(textwrap.dedent(source)))
        tail = source[
            source.index("stored_annotations: dict[str, str] = copy.deepcopy") :
        ]
        tail = tail[tail.index("\n") :]
        assert "version_row." not in tail
        assert "pipeline." not in tail

    def test_a_pinned_fire_does_not_eat_a_write_the_caller_already_made(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """The behaviour the structural guards above stand for.

        Executed rather than inspected: a row is written on the session, a pinned
        run is created through the no-commit entry point on that same session, and
        the caller commits. Both must survive. With the old rollback restored this
        fails, because the pinned path is the one that reaches the rollback.
        """
        pipeline_id, version_key = _save_pipeline(
            db_engine=db_engine, name="pinned-and-fenced"
        )
        _save_pipeline(db_engine=db_engine, name="pinned-and-fenced-v2")
        marker_path = "pipelines/written-before-the-pinned-run.yaml"

        with sqlalchemy.orm.Session(bind=db_engine) as session:
            session.add(
                user_pipeline_db_models.UserPipeline(
                    user_id=DEFAULT_USER, file_path=marker_path
                )
            )
            session.flush()
            user_pipeline_services.UserPipelineService().create_from_pipeline_no_commit(
                session=session,
                pipeline_id=pipeline_id,
                user_id=DEFAULT_USER,
                file_path=None,
                version=version_key,
                run_arguments=None,
                pipeline_run_annotations=None,
                created_by=DEFAULT_USER,
                require_pinnable_versioning=True,
            )
            session.commit()

        with sqlalchemy.orm.Session(bind=db_engine) as other:
            survived = other.scalar(
                sqlalchemy.select(user_pipeline_db_models.UserPipeline).where(
                    user_pipeline_db_models.UserPipeline.file_path == marker_path
                )
            )
        assert (
            survived is not None
        ), "the caller's pending write was discarded by the run creation"
        assert "pinned-and-fenced" in _created_run_names(db_engine)

    def test_current_following_fire_takes_no_row_lock(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """Only a pin locks, so following schedules never contend.

        `pinned = require_pinnable and version is not None`, so the contention
        surface is pinned schedules on one pipeline -- not every scheduled fire.
        """
        source = inspect.getsource(
            user_pipeline_services.UserPipelineService.get_pipeline_and_version
        )
        assert "pinned = require_pinnable and version is not None" in source
        assert "for_share=pinned" in source

        pipeline_id, _ = _save_pipeline(db_engine=db_engine, name="unlocked-follow")
        schedule_id = _insert_reference_schedule(
            db_engine=db_engine, pipeline_id=pipeline_id, version_key=None
        )

        executor.execute_pipeline_schedule(pipeline_schedule_id=schedule_id)

        assert "unlocked-follow" in _created_run_names(db_engine)

    def test_pinned_fire_still_runs_the_pinned_content(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """The narrowed window must not have broken what the lock guarantees."""
        pipeline_id, v1 = _save_pipeline(db_engine=db_engine, name="pinned-v1")
        _save_pipeline(db_engine=db_engine, name="pinned-v2")
        schedule_id = _insert_reference_schedule(
            db_engine=db_engine, pipeline_id=pipeline_id, version_key=v1
        )

        executor.execute_pipeline_schedule(pipeline_schedule_id=schedule_id)

        assert "pinned-v1" in _created_run_names(db_engine)


class TestTheInlineSpecIsOwnedByTheParserNotTheRow:
    """Regression: ORM-owned JSON must not cross into the parser by reference.

    The finding phrased its trigger conditionally -- IF `from_json_dict` mutates
    before raising, the error-status commit persists a partial mutation. That
    exact trigger does not occur with today's parser in our reproduction:
    `TypeAdapter.validate_python` does not write to its source, and a
    `MutableDict` read does not mark the attribute dirty.

    The boundary it points at is real, though. Pydantic rebuilds typed container
    shells but passes `Any`-typed values through by REFERENCE, so `annotations`
    at both the task and input level are shared between the parsed TaskSpec and
    the row's JSON. Nothing mutates those leaves today, and nested mutation would
    usually evade top-level dirty tracking anyway -- so this pins the contract
    rather than a live persistence bug.
    """

    @staticmethod
    def _insert(db_engine: sqlalchemy.Engine, spec: dict) -> str:
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            row = db_models.ScheduledPipelineRun(
                name="inline",
                cron_expression="0 8 * * *",
                pipeline_task_spec=copy.deepcopy(spec),
                created_by=DEFAULT_USER,
            )
            session.add(row)
            session.commit()
            return row.id

    def test_a_parser_that_mutates_then_raises_cannot_corrupt_the_row(
        self,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Exactly the failure the regression reproduces, forced to happen."""
        original = copy.deepcopy(SAMPLE_PIPELINE_TASK_SPEC)
        sid = self._insert(db_engine, original)
        received: list[object] = []

        def _mutate_then_raise(d: dict) -> object:
            received.append(d)
            d["injected_by_a_bad_parser"] = True
            raise ValueError("parser blew up after mutating")

        with mock.patch.object(
            executor.component_structures.TaskSpec,
            "from_json_dict",
            staticmethod(_mutate_then_raise),
        ):
            result = executor.execute_pipeline_schedule(pipeline_schedule_id=sid)

        assert result is None
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            row = session.get(db_models.ScheduledPipelineRun, sid)
            assert row is not None
            # The failure WAS recorded -- so the commit the regression covers really does
            # happen, and would have written a corrupted spec alongside it.
            assert row.last_run_at is not None
            assert row.last_run_submission_result is not None
            # ...but the stored spec is untouched.
            assert "injected_by_a_bad_parser" not in row.pipeline_task_spec
            assert row.pipeline_task_spec == original

        assert received, "the patched parser never ran; the test would be vacuous"
        assert "injected_by_a_bad_parser" in received[0], "the mutation did not happen"


class TestRunReferenceOwnershipUsesTheDatabaseComparator:
    """`resolve_owned_run` must not carry its own owner rule.

    It did: `pipeline_run.created_by != created_by`, a Python comparison that
    folds nothing, while every owner check on the schedule side is the
    database's. The whole-scheduler sweep for the #590 correction found it. The
    consequence is not a route status but a stored schedule that cannot fire: a
    schedule owned by `Alice@example.com` referencing a run the same person
    created as `alice@example.com` was refused at create time, and would be
    refused again at every fire if the two spellings ever diverged.

    Driven through `folding_run_owner_db_engine`, since on byte-exact SQLite the
    old rule and the new one agree and nothing here could fail.
    """

    OWNER_STORED = "alice@example.com"
    OWNER_CALLING = "Alice@example.com"
    STRANGER = "mallory@example.com"

    def _run_id(self, engine: sqlalchemy.Engine, *, created_by: str | None) -> str:
        spec = copy.deepcopy(SAMPLE_PIPELINE_TASK_SPEC)
        with sqlalchemy.orm.Session(bind=engine) as session:
            run = bts.PipelineRun(
                root_execution=bts.ExecutionNode(task_spec=spec),
                created_by=created_by,
            )
            session.add(run)
            session.commit()
            return run.id

    def test_a_case_variant_owner_resolves_their_own_run(
        self,
        folding_run_owner_db_engine: sqlalchemy.Engine,
    ) -> None:
        run_id = self._run_id(folding_run_owner_db_engine, created_by=self.OWNER_STORED)

        with sqlalchemy.orm.Session(bind=folding_run_owner_db_engine) as session:
            resolved = executor.resolve_owned_run(
                session=session,
                run_id=run_id,
                created_by=self.OWNER_CALLING,
            )

        assert resolved.id == run_id

    def test_another_users_run_is_still_refused(
        self,
        folding_run_owner_db_engine: sqlalchemy.Engine,
    ) -> None:
        """The negative control; deleting the check passes everything else here."""
        run_id = self._run_id(folding_run_owner_db_engine, created_by=self.OWNER_STORED)

        with sqlalchemy.orm.Session(bind=folding_run_owner_db_engine) as session:
            with pytest.raises(errors.ItemNotFoundError):
                executor.resolve_owned_run(
                    session=session,
                    run_id=run_id,
                    created_by=self.STRANGER,
                )

    def test_an_unattributed_run_is_still_refused(
        self,
        folding_run_owner_db_engine: sqlalchemy.Engine,
    ) -> None:
        """NULL must not become an authorization answer.

        `PipelineRun.created_by` is nullable and the schedule's is not. A scoped
        SQL probe returns no row for NULL, which is the right outcome -- but it
        reaches it by accident, so the explicit NULL branch stays and this pins
        that both agree.
        """
        run_id = self._run_id(folding_run_owner_db_engine, created_by=None)

        with sqlalchemy.orm.Session(bind=folding_run_owner_db_engine) as session:
            with pytest.raises(errors.ItemNotFoundError):
                executor.resolve_owned_run(
                    session=session,
                    run_id=run_id,
                    created_by=self.OWNER_CALLING,
                )

    def test_an_absent_run_is_refused_the_same_way(
        self,
        folding_run_owner_db_engine: sqlalchemy.Engine,
    ) -> None:
        """One outward message for all three refusals, unchanged by this fix."""
        with sqlalchemy.orm.Session(bind=folding_run_owner_db_engine) as session:
            with pytest.raises(
                errors.ItemNotFoundError, match="was not found for user"
            ):
                executor.resolve_owned_run(
                    session=session,
                    run_id="does-not-exist",
                    created_by=self.OWNER_CALLING,
                )

    def test_no_python_owner_comparison_survives(self) -> None:
        """Structural, so a redundant Python check re-added beside the probe shows up here.

        Parsed rather than grepped, so the docstring recounting the old
        comparison does not trip it. `created_by is None` is a NULL test, not an
        owner comparison, so identity comparisons are allowed and equality ones
        are not.
        """
        tree = ast.parse(textwrap.dedent(inspect.getsource(executor.resolve_owned_run)))
        operators = [
            type(op)
            for node in ast.walk(tree)
            if isinstance(node, ast.Compare)
            for op in node.ops
        ]

        assert set(operators) <= {ast.Is}, operators


class TestTheScheduleTimeMap:
    """`_ScheduleTimeExecutor` and `take_schedule_time`, the pair that carries APScheduler's nominal
    schedule_time to the job APScheduler refuses to pass it to."""

    @staticmethod
    def _job(*, job_id: str) -> mock.Mock:
        job = mock.Mock()
        job.id = job_id
        return job

    @pytest.fixture(autouse=True)
    def _empty_map(self) -> typing.Iterator[None]:
        """The map is module state, so a leaked entry would make the next test pass for
        the wrong reason."""
        executor._SCHEDULE_TIMES.clear()
        yield
        executor._SCHEDULE_TIMES.clear()

    def test_the_schedule_time_is_written_before_the_job_is_dispatched(
        self,
    ) -> None:
        """Order is the whole safety property: the entry must already be readable when
        the worker starts, and a write that fails must abort the dispatch. Asserted by
        reading the map from inside the dispatch call rather than after it.
        """
        seen: list[datetime.datetime | None] = []
        schedule_time = datetime.datetime(
            2026, 3, 6, 2, 0, tzinfo=datetime.timezone.utc
        )

        with mock.patch.object(
            executor.pool.ThreadPoolExecutor,
            "_do_submit_job",
            side_effect=lambda job, run_times: seen.append(
                executor._SCHEDULE_TIMES.get(job.id)
            ),
            autospec=False,
        ):
            executor._ScheduleTimeExecutor()._do_submit_job(
                self._job(job_id="sched-1"), [schedule_time]
            )

        assert seen == [schedule_time]

    def test_a_failed_write_dispatches_nothing(self) -> None:
        """Fail-closed. A run carrying a fabricated schedule_time would write to the wrong
        partition and report success, so a missed firing is the better failure.
        """
        dispatched: list[object] = []

        with mock.patch.object(
            executor.pool.ThreadPoolExecutor,
            "_do_submit_job",
            side_effect=lambda job, run_times: dispatched.append(job),
        ):
            with pytest.raises(IndexError):
                # An empty `run_times` makes `run_times[-1]` raise inside the lock,
                # standing in for any failure of the write itself.
                executor._ScheduleTimeExecutor()._do_submit_job(
                    self._job(job_id="sched-1"), []
                )

        assert dispatched == []
        assert executor._SCHEDULE_TIMES == {}
        assert not executor._SCHEDULE_TIMES_LOCK.locked()

    def test_the_latest_schedule_time_wins_when_firings_were_coalesced(
        self,
    ) -> None:
        """`coalesce: True` normally leaves one element, but the choice of end is ours to
        state: a schedule that was down 02:00->04:00 renders `schedule_time` as 04:00, the
        most recent scheduled time, not the oldest unfired one.
        """
        times = [
            datetime.datetime(2026, 3, 6, hour, 0, tzinfo=datetime.timezone.utc)
            for hour in (2, 3, 4)
        ]

        with mock.patch.object(executor.pool.ThreadPoolExecutor, "_do_submit_job"):
            executor._ScheduleTimeExecutor()._do_submit_job(
                self._job(job_id="sched-1"), times
            )

        assert executor.take_schedule_time("sched-1") == times[-1]

    def test_two_schedules_firing_together_do_not_share_a_schedule_time(
        self,
    ) -> None:
        """Keyed by job id, so a busy scheduler minute cannot cross the wires."""
        early = datetime.datetime(2026, 3, 6, 2, 0, tzinfo=datetime.timezone.utc)
        late = datetime.datetime(2026, 3, 6, 9, 0, tzinfo=datetime.timezone.utc)

        with mock.patch.object(executor.pool.ThreadPoolExecutor, "_do_submit_job"):
            time_executor = executor._ScheduleTimeExecutor()
            time_executor._do_submit_job(self._job(job_id="sched-a"), [early])
            time_executor._do_submit_job(self._job(job_id="sched-b"), [late])

        assert executor.take_schedule_time("sched-a") == early
        assert executor.take_schedule_time("sched-b") == late

    def test_taking_a_schedule_time_consumes_it(self) -> None:
        """It pops, so a retry or a second read inside one firing cannot re-use a time
        that has already been spent."""
        schedule_time = datetime.datetime(
            2026, 3, 6, 2, 0, tzinfo=datetime.timezone.utc
        )

        with mock.patch.object(executor.pool.ThreadPoolExecutor, "_do_submit_job"):
            executor._ScheduleTimeExecutor()._do_submit_job(
                self._job(job_id="sched-1"), [schedule_time]
            )

        assert executor.take_schedule_time("sched-1") == schedule_time
        assert executor.take_schedule_time("sched-1") is None

    def test_a_schedule_that_never_fired_has_no_schedule_time(self) -> None:
        """The manual path relies on this: nothing was written, so nothing is read."""
        assert executor.take_schedule_time("never-fired") is None

    def test_the_next_firing_overwrites_one_left_by_a_crashed_firing(
        self,
    ) -> None:
        """A job killed between write and read leaves one stale entry. It is bounded --
        one datetime per schedule -- and replaced before it can be read as a live one.
        """
        stale = datetime.datetime(2026, 3, 6, 2, 0, tzinfo=datetime.timezone.utc)
        fresh = datetime.datetime(2026, 3, 6, 3, 0, tzinfo=datetime.timezone.utc)

        with mock.patch.object(executor.pool.ThreadPoolExecutor, "_do_submit_job"):
            time_executor = executor._ScheduleTimeExecutor()
            time_executor._do_submit_job(self._job(job_id="sched-1"), [stale])
            time_executor._do_submit_job(self._job(job_id="sched-1"), [fresh])

        assert executor._SCHEDULE_TIMES == {"sched-1": fresh}
        assert executor.take_schedule_time("sched-1") == fresh


class TestTheManualFlag:
    """`manual` decides whether a fire reads the schedule_time map at all."""

    @pytest.fixture(autouse=True)
    def _empty_map(self) -> typing.Iterator[None]:
        executor._SCHEDULE_TIMES.clear()
        yield
        executor._SCHEDULE_TIMES.clear()

    def test_a_manual_fire_leaves_a_concurrent_scheduled_firing_its_schedule_time(
        self,
    ) -> None:
        """The map is keyed per schedule, not per fire, so a manual fire that read it
        would consume the time a scheduled firing of the same schedule is about to use.
        """
        schedule_time = datetime.datetime(
            2026, 3, 6, 2, 0, tzinfo=datetime.timezone.utc
        )
        executor._SCHEDULE_TIMES["sched-1"] = schedule_time

        with mock.patch.object(executor, "_get_session", None):
            executor.execute_pipeline_schedule(
                pipeline_schedule_id="sched-1", manual=True
            )

        assert executor.take_schedule_time("sched-1") == schedule_time

    def test_a_scheduled_fire_consumes_the_schedule_time(self) -> None:
        """The counterpart: without `manual`, the time is taken, which is what makes it
        available to render `schedule_time` and unavailable to anything after.
        """
        executor._SCHEDULE_TIMES["sched-1"] = datetime.datetime(
            2026, 3, 6, 2, 0, tzinfo=datetime.timezone.utc
        )

        with mock.patch.object(executor, "_get_session", None):
            executor.execute_pipeline_schedule(pipeline_schedule_id="sched-1")

        assert executor.take_schedule_time("sched-1") is None

    def test_manual_defaults_to_false_so_pickled_jobs_keep_their_schedule_time(
        self,
    ) -> None:
        """Rows already in `apscheduler_jobs` were pickled with only
        `{"pipeline_schedule_id": ...}`, so they call this with `manual` unset. A True
        default would silently drop `schedule_time` for every schedule after a deploy.
        """
        signature = inspect.signature(executor.execute_pipeline_schedule)

        assert signature.parameters["manual"].default is False


class TestTemplatesReachTheScheduledRun:
    """A schedule's stored templates render at fire time and arrive as the run's arguments.

    End-to-end through the real saved-pipeline path rather than a mocked service, because the
    thing under test is that `run_arguments` is threaded all the way into the run's root task.
    """

    @staticmethod
    def _fire(
        *,
        db_engine: sqlalchemy.Engine,
        settings: dict[str, object],
        at: datetime.datetime | None,
    ) -> None:
        pipeline_id, _version = _save_pipeline(
            db_engine=db_engine,
            name="templated",
            declares=("as_of_date", "region"),
        )
        schedule_id = _insert_reference_schedule(
            db_engine=db_engine, pipeline_id=pipeline_id, settings=settings
        )
        if at is not None:
            executor._SCHEDULE_TIMES[schedule_id] = at
        try:
            executor.execute_pipeline_schedule(pipeline_schedule_id=schedule_id)
        finally:
            executor._SCHEDULE_TIMES.clear()

    def test_a_scheduled_fire_renders_schedule_time_into_the_runs_arguments(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """The nominal time APScheduler computed, not the moment the worker got round to it."""
        self._fire(
            db_engine=db_engine,
            settings={
                "pipeline_templates": {"as_of_date": "{{ schedule_time | date }}"}
            },
            at=datetime.datetime(2026, 3, 6, 2, 0, tzinfo=datetime.timezone.utc),
        )

        assert _created_run_arguments(db_engine) == {"as_of_date": "2026-03-06"}

    def test_a_constant_template_is_still_rendered_and_delivered(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """A template with no expression is a value, so it reaches the run like any other."""
        self._fire(
            db_engine=db_engine,
            settings={"pipeline_templates": {"region": "ca"}},
            at=datetime.datetime(2026, 3, 6, 2, 0, tzinfo=datetime.timezone.utc),
        )

        assert _created_run_arguments(db_engine) == {"region": "ca"}

    def test_a_schedule_with_no_templates_delivers_no_arguments(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """The pre-feature path: an empty settings blob must not invent an argument."""
        self._fire(
            db_engine=db_engine,
            settings={},
            at=datetime.datetime(2026, 3, 6, 2, 0, tzinfo=datetime.timezone.utc),
        )

        assert _created_run_arguments(db_engine) == {}

    def test_a_hand_fired_schedule_drops_a_schedule_time_template_and_still_runs(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """No schedule_time exists for a manual fire, so that key fails rather than the run."""
        self._fire(
            db_engine=db_engine,
            settings={
                "pipeline_templates": {"as_of_date": "{{ schedule_time | date }}"}
            },
            at=None,
        )

        assert _created_run_arguments(db_engine) == {}
        assert "templated" in _created_run_names(db_engine)

    def test_a_hand_fired_schedule_still_renders_a_template_rooted_in_now(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """Failure is per key: losing schedule_time must not take the other templates with it."""
        self._fire(
            db_engine=db_engine,
            settings={
                "pipeline_templates": {
                    "as_of_date": "{{ schedule_time | date }}",
                    "region": "ca",
                }
            },
            at=None,
        )

        assert _created_run_arguments(db_engine) == {"region": "ca"}


class TestTemplatesReachARunBuiltFromASpec:
    """The two sources whose run is built from a TaskSpec, not a pipeline id.

    `create` is vendored and takes no run_arguments, so these two merge the rendered map into
    the spec instead. The claim is that the delivery differs and the result does not.
    """

    _AT = datetime.datetime(2026, 3, 6, 2, 0, tzinfo=datetime.timezone.utc)

    @staticmethod
    def _fire(
        *,
        db_engine: sqlalchemy.Engine,
        schedule_id: str,
        at: datetime.datetime | None,
    ) -> None:
        if at is not None:
            executor._SCHEDULE_TIMES[schedule_id] = at
        try:
            executor.execute_pipeline_schedule(pipeline_schedule_id=schedule_id)
        finally:
            executor._SCHEDULE_TIMES.clear()

    def test_an_inline_spec_receives_the_rendered_arguments(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        schedule_id = _insert_reference_schedule(
            db_engine=db_engine,
            inline_spec=_spec_declaring("as_of_date", name="inline-templated"),
            settings={
                "pipeline_templates": {"as_of_date": "{{ schedule_time | date }}"}
            },
        )

        self._fire(db_engine=db_engine, schedule_id=schedule_id, at=self._AT)

        assert _created_run_arguments(db_engine) == {"as_of_date": "2026-03-06"}

    def test_a_run_reference_receives_the_rendered_arguments(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        run_id = _insert_pipeline_run(
            db_engine=db_engine,
            name="referenced-templated",
            declares=("as_of_date",),
        )
        schedule_id = _insert_reference_schedule(
            db_engine=db_engine,
            run_id=run_id,
            settings={
                "pipeline_templates": {"as_of_date": "{{ schedule_time | date }}"}
            },
        )

        self._fire(db_engine=db_engine, schedule_id=schedule_id, at=self._AT)

        # Two runs exist: the one being referenced and the one the fire created.
        assert {"as_of_date": "2026-03-06"} in _all_run_arguments(db_engine)

    def test_a_template_beats_an_argument_already_on_the_inline_spec(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """The same precedence the saved-pipeline path applies inside the service."""
        spec = _spec_declaring("as_of_date", name="inline-preset")
        spec["arguments"] = {"as_of_date": "1970-01-01"}
        schedule_id = _insert_reference_schedule(
            db_engine=db_engine,
            inline_spec=spec,
            settings={
                "pipeline_templates": {"as_of_date": "{{ schedule_time | date }}"}
            },
        )

        self._fire(db_engine=db_engine, schedule_id=schedule_id, at=self._AT)

        assert _created_run_arguments(db_engine) == {"as_of_date": "2026-03-06"}

    def test_an_untemplated_argument_on_the_inline_spec_survives(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """Merging must add to the spec's arguments, not replace the map."""
        spec = _spec_declaring("as_of_date", "region", name="inline-mixed")
        spec["arguments"] = {"region": "ca"}
        schedule_id = _insert_reference_schedule(
            db_engine=db_engine,
            inline_spec=spec,
            settings={
                "pipeline_templates": {"as_of_date": "{{ schedule_time | date }}"}
            },
        )

        self._fire(db_engine=db_engine, schedule_id=schedule_id, at=self._AT)

        assert _created_run_arguments(db_engine) == {
            "as_of_date": "2026-03-06",
            "region": "ca",
        }

    def test_a_failed_template_leaves_the_inline_specs_own_argument_untouched(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """A hand-fired schedule has no schedule_time, so the stored value must stand."""
        spec = _spec_declaring("as_of_date", name="inline-fallback")
        spec["arguments"] = {"as_of_date": "1970-01-01"}
        schedule_id = _insert_reference_schedule(
            db_engine=db_engine,
            inline_spec=spec,
            settings={
                "pipeline_templates": {"as_of_date": "{{ schedule_time | date }}"}
            },
        )

        self._fire(db_engine=db_engine, schedule_id=schedule_id, at=None)

        assert _created_run_arguments(db_engine) == {"as_of_date": "1970-01-01"}


class TestARenderFailureNeverBlocksTheRun:
    """A template that cannot render is a data-quality event, not a control-flow one.

    The run is submitted, the failed key keeps whatever the spec had, and the row records the
    submission rather than the render error. A blocked run would be a data gap too — a louder
    one, but still one — so the choice is to run and report.
    """

    def _fire_with(
        self, *, db_engine: sqlalchemy.Engine, templates: dict[str, str]
    ) -> str:
        spec = _spec_declaring("as_of_date", "region", name="render-failure")
        spec["arguments"] = {"as_of_date": "1970-01-01"}
        schedule_id = _insert_reference_schedule(
            db_engine=db_engine,
            inline_spec=spec,
            settings={"pipeline_templates": templates},
        )
        # No entry in the map, so this is a hand-fired schedule with no schedule_time.
        executor.execute_pipeline_schedule(pipeline_schedule_id=schedule_id)
        return schedule_id

    def test_the_row_records_the_submission_not_the_render_error(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        schedule_id = self._fire_with(
            db_engine=db_engine,
            templates={"as_of_date": "{{ schedule_time | date }}"},
        )

        assert (
            _row(db_engine, schedule_id).last_run_submission_result
            == db_models.SubmissionResult.SUCCESS.value
        )

    def test_the_run_is_created_despite_the_failure(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        self._fire_with(
            db_engine=db_engine,
            templates={"as_of_date": "{{ schedule_time | date }}"},
        )

        assert "render-failure" in _created_run_names(db_engine)

    def test_failure_is_per_key_so_the_good_templates_still_render(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """Three good templates are not punished for a fourth that fails."""
        self._fire_with(
            db_engine=db_engine,
            templates={
                "as_of_date": "{{ schedule_time | date }}",
                "region": "{{ now | date }}",
            },
        )

        arguments = _created_run_arguments(db_engine)
        assert arguments["as_of_date"] == "1970-01-01"
        assert (
            arguments["region"]
            == datetime.datetime.now(datetime.timezone.utc).date().isoformat()
        )

    def test_the_failure_is_logged_with_the_schedule_and_the_keys(
        self, db_engine: sqlalchemy.Engine, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Until the annotation exists, the log line is the only signal a human gets."""
        with caplog.at_level(
            logging.WARNING,
            logger="cloud_pipelines_backend.scheduling.pipelines.executor",
        ):
            schedule_id = self._fire_with(
                db_engine=db_engine,
                templates={"as_of_date": "{{ schedule_time | date }}"},
            )

        assert any(
            schedule_id in record.message and "as_of_date" in record.message
            for record in caplog.records
        ), caplog.text


class TestTemplateCountersOnlyCountTemplating:
    """The `template.*` counters answer questions about templating, so a schedule that does not
    template must not move them, and a render whose run never started is not a firing.

    Both were reviewer findings. The `except Exception` around submission catches any failure
    -- a bad image, a deleted target, a quota refusal -- and it used to count every one of them
    as a templating event; the render used to be reported before the submission it precedes.
    """

    @staticmethod
    def _fire_with_a_failing_submission(
        *, db_engine: sqlalchemy.Engine, settings: dict[str, object]
    ) -> mock.MagicMock:
        """Fire a schedule whose run creation raises, and hand back the observer it used."""
        pipeline_id, _version = _save_pipeline(
            db_engine=db_engine,
            name=f"counted-{settings and 'templated' or 'bare'}",
            declares=("as_of_date", "region"),
        )
        schedule_id = _insert_reference_schedule(
            db_engine=db_engine, pipeline_id=pipeline_id, settings=settings
        )
        with (
            mock.patch.object(executor, "render_observer") as observer,
            mock.patch.object(
                executor._user_pipeline_service,
                "create_from_pipeline",
                side_effect=RuntimeError("submission refused"),
            ),
        ):
            executor.execute_pipeline_schedule(pipeline_schedule_id=schedule_id)
        return observer

    def test_a_refused_submission_on_a_template_free_schedule_counts_nothing(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """The finding: `template.submission_rejected` rose for failures templating never caused."""
        observer = self._fire_with_a_failing_submission(
            db_engine=db_engine, settings={}
        )

        observer.submission_rejected.assert_not_called()

    def test_a_refused_submission_on_a_templated_schedule_is_still_counted(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """The guard must narrow the counter, not silence it."""
        observer = self._fire_with_a_failing_submission(
            db_engine=db_engine,
            settings={"pipeline_templates": {"region": "ca"}},
        )

        observer.submission_rejected.assert_called_once()

    def test_a_refused_submission_reports_no_render(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """`runs_with_a_failed_key` counts runs, so a render with no run must not reach it."""
        observer = self._fire_with_a_failing_submission(
            db_engine=db_engine,
            settings={
                "pipeline_templates": {"as_of_date": "{{ schedule_time | date }}"}
            },
        )

        observer.report.assert_not_called()

    def test_a_successful_fire_reports_its_render_once(
        self, db_engine: sqlalchemy.Engine
    ) -> None:
        """The control: moving the report must not lose it on the path that does submit."""
        pipeline_id, _version = _save_pipeline(
            db_engine=db_engine, name="counted-ok", declares=("region",)
        )
        schedule_id = _insert_reference_schedule(
            db_engine=db_engine,
            pipeline_id=pipeline_id,
            settings={"pipeline_templates": {"region": "ca"}},
        )
        with mock.patch.object(executor, "render_observer") as observer:
            executor.execute_pipeline_schedule(pipeline_schedule_id=schedule_id)

        observer.report.assert_called_once()
        observer.submission_rejected.assert_not_called()
