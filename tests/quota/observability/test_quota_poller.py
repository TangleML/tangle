"""Tests for the per-group gauges polled from outside the orchestrator."""

import datetime

import pytest
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.quota import db_models, occupancy
from cloud_pipelines_backend.quota.observability import poller as quota_poller
from tests.quota.observability import probes

_OCCUPANCY = "quota.occupancy"
_CAPACITY = "quota.capacity"
_WAITERS = "quota.waiters"
_OLDEST_WAITER_AGE = "quota.oldest_waiter_age"
_ACTIVE_CLAIMS = "quota.active_claims"
_OLDEST_ACTIVE_AGE = "quota.oldest_active_age"
Status = bts.ContainerExecutionStatus


def _group(
    *,
    session: orm.Session,
    name: str,
    capacity: int,
) -> db_models.QuotaGroup:
    group = db_models.QuotaGroup(name=name, capacity=capacity, created_by="test")
    session.add(group)
    session.commit()
    return group


def _claim(
    *,
    session: orm.Session,
    group_id: str,
    node_id: str,
    node_status: Status,
    state: db_models.ClaimState,
    age_seconds: int = 0,
    admitted_seconds_ago: int = 0,
) -> None:
    """A node in a given status with a claim of a controlled age.

    `created_at` is set explicitly rather than left to its insert-time default, because claims
    made in the same millisecond would tie and the oldest-waiter assertion would be testing
    the tie-break rather than the ordering.

    `admitted_seconds_ago` backdates `updated_at` instead, which is the column the ACTIVE age
    is measured from. The two are separate arguments because the whole point of that gauge is
    that it does not measure from `created_at`: a claim can have queued an hour ago and been
    admitted a minute ago, and only one of those numbers is the age of in-progress work.
    """
    node = bts.ExecutionNode(task_spec={})
    node.id = node_id
    node.container_execution_status = node_status
    session.add(node)
    # Committed before the claim rather than with it: the claim's foreign key is checked at
    # INSERT, and SQLite has PRAGMA foreign_keys on in this suite's engine fixture.
    session.commit()
    claim = db_models.QuotaGroupClaim(
        quota_group_id=group_id, execution_node_id=node_id, state=state
    )
    if age_seconds:
        claim.created_at = datetime.datetime.now(
            datetime.timezone.utc
        ) - datetime.timedelta(seconds=age_seconds)
    session.add(claim)
    session.commit()
    if admitted_seconds_ago:
        # After the commit, because `updated_at` has an onupdate that would overwrite an
        # assignment made before it.
        claim.updated_at = datetime.datetime.now(
            datetime.timezone.utc
        ) - datetime.timedelta(seconds=admitted_seconds_ago)
        session.commit()


@pytest.fixture()
def poller(
    db_engine: sqlalchemy.Engine,
    metrics: probes.MetricsProbe,
) -> quota_poller.QuotaPoller:
    """A poller whose gauges report into the in-memory reader.

    Depends on `metrics` rather than merely coexisting with it: the gauges are built in the
    constructor from the module's meter, so the fixture has to have patched that meter before
    the poller exists.
    """
    return quota_poller.QuotaPoller(
        session_factory=lambda: orm.Session(bind=db_engine, autoflush=False)
    )


class TestReadings:
    """Each number, and where it comes from."""

    def test_occupancy_is_the_number_the_gate_would_have_read(
        self,
        session: orm.Session,
        poller: quota_poller.QuotaPoller,
        metrics: probes.MetricsProbe,
    ) -> None:
        # The point of importing the predicate rather than restating it: a dashboard and an
        # admission decision must not be able to disagree about how full a group is.
        group = _group(session=session, name="gpu", capacity=4)
        for index in range(2):
            _claim(
                session=session,
                group_id=group.id,
                node_id=f"running{index}",
                node_status=Status.RUNNING,
                state=db_models.ClaimState.ACTIVE,
            )

        poller.poll()

        assert metrics.groups_observed(name=_OCCUPANCY)["gpu"] == 2
        assert occupancy.count_occupancy(session=session, group_id=group.id) == 2

    def test_a_parked_node_is_a_waiter_and_not_an_occupant(
        self,
        session: orm.Session,
        poller: quota_poller.QuotaPoller,
        metrics: probes.MetricsProbe,
    ) -> None:
        group = _group(session=session, name="gpu", capacity=4)
        _claim(
            session=session,
            group_id=group.id,
            node_id="parked",
            node_status=Status.UNINITIALIZED,
            state=db_models.ClaimState.WAITING,
        )

        poller.poll()

        assert metrics.groups_observed(name=_WAITERS)["gpu"] == 1
        assert metrics.groups_observed(name=_OCCUPANCY)["gpu"] == 0

    def test_a_done_claim_counts_towards_nothing(
        self,
        session: orm.Session,
        poller: quota_poller.QuotaPoller,
        metrics: probes.MetricsProbe,
    ) -> None:
        # DONE will eventually be most of the claim table, so a gauge that counted it would
        # climb forever regardless of what the group is doing.
        group = _group(session=session, name="gpu", capacity=4)
        _claim(
            session=session,
            group_id=group.id,
            node_id="finished",
            node_status=Status.SUCCEEDED,
            state=db_models.ClaimState.DONE,
        )

        poller.poll()

        assert metrics.groups_observed(name=_OCCUPANCY)["gpu"] == 0
        assert metrics.groups_observed(name=_WAITERS)["gpu"] == 0

    def test_the_oldest_waiter_is_the_one_that_has_waited_longest(
        self,
        session: orm.Session,
        poller: quota_poller.QuotaPoller,
        metrics: probes.MetricsProbe,
    ) -> None:
        group = _group(session=session, name="gpu", capacity=1)
        for node_id, age in (("recent", 120), ("ancient", 600)):
            _claim(
                session=session,
                group_id=group.id,
                node_id=node_id,
                node_status=Status.UNINITIALIZED,
                state=db_models.ClaimState.WAITING,
                age_seconds=age,
            )

        poller.poll()

        assert metrics.groups_observed(name=_OLDEST_WAITER_AGE)["gpu"] == pytest.approx(
            600, abs=5
        )

    def test_capacity_is_reported_so_the_other_three_can_be_read_against_it(
        self,
        session: orm.Session,
        poller: quota_poller.QuotaPoller,
        metrics: probes.MetricsProbe,
    ) -> None:
        # Waiters climbing against a capacity of zero is somebody having left a group paused,
        # which is a different page from promotion being broken.
        _group(session=session, name="paused", capacity=0)

        poller.poll()

        assert metrics.groups_observed(name=_CAPACITY)["paused"] == 0


class TestWaitersMeansParked:
    """`waiters` counts nodes actually parked, not every claim row that says WAITING."""

    def test_a_promoted_node_is_no_longer_a_waiter_before_the_gate_admits_it(
        self,
        session: orm.Session,
        poller: quota_poller.QuotaPoller,
        metrics: probes.MetricsProbe,
    ) -> None:
        # The window `promote()` opens: the claim still says WAITING and the node has already
        # been moved to QUEUED for the orchestrator to pick up. Counting it as a waiter meant
        # a perfectly healthy group reported a queue for as long as the sweep took to come
        # round, which is exactly the reading an operator would page on.
        group = _group(session=session, name="gpu", capacity=4)
        _claim(
            session=session,
            group_id=group.id,
            node_id="parked",
            node_status=Status.UNINITIALIZED,
            state=db_models.ClaimState.WAITING,
        )
        _claim(
            session=session,
            group_id=group.id,
            node_id="promoted",
            node_status=Status.QUEUED,
            state=db_models.ClaimState.WAITING,
        )

        poller.poll()

        assert metrics.groups_observed(name=_WAITERS)["gpu"] == 1

    def test_the_oldest_waiter_ignores_one_that_has_been_promoted(
        self,
        session: orm.Session,
        poller: quota_poller.QuotaPoller,
        metrics: probes.MetricsProbe,
    ) -> None:
        # The same predicate has to reach both halves of the gauge pair. Counting the promoted
        # node in `waiters` and excluding it from `oldest_waiter` -- or the reverse -- would
        # be a dashboard that contradicts itself.
        group = _group(session=session, name="gpu", capacity=4)
        _claim(
            session=session,
            group_id=group.id,
            node_id="promoted-and-ancient",
            node_status=Status.QUEUED,
            state=db_models.ClaimState.WAITING,
            age_seconds=3_600,
        )
        _claim(
            session=session,
            group_id=group.id,
            node_id="still-parked",
            node_status=Status.UNINITIALIZED,
            state=db_models.ClaimState.WAITING,
            age_seconds=120,
        )

        poller.poll()

        assert metrics.groups_observed(name=_OLDEST_WAITER_AGE)["gpu"] == pytest.approx(
            120, abs=5
        )


class TestSettlementIsVisible:
    """`active_claims` beside `occupancy`, which is the pair that makes an outage readable."""

    def test_they_agree_when_the_group_is_merely_busy(
        self,
        session: orm.Session,
        poller: quota_poller.QuotaPoller,
        metrics: probes.MetricsProbe,
    ) -> None:
        group = _group(session=session, name="gpu", capacity=2)
        for index in range(2):
            _claim(
                session=session,
                group_id=group.id,
                node_id=f"running{index}",
                node_status=Status.RUNNING,
                state=db_models.ClaimState.ACTIVE,
            )

        poller.poll()

        assert metrics.groups_observed(name=_OCCUPANCY)["gpu"] == 2
        assert metrics.groups_observed(name=_ACTIVE_CLAIMS)["gpu"] == 2

    def test_they_diverge_when_nothing_is_settling_the_claims(
        self,
        session: orm.Session,
        poller: quota_poller.QuotaPoller,
        metrics: probes.MetricsProbe,
    ) -> None:
        # The fault the pair exists for: the nodes have finished, so the gate reads the group
        # as empty, but nothing flipped their claims to DONE. Occupancy alone says "idle" and
        # active_claims alone says "full"; only the gap between them names the cause.
        group = _group(session=session, name="gpu", capacity=2)
        for index in range(2):
            _claim(
                session=session,
                group_id=group.id,
                node_id=f"finished{index}",
                node_status=Status.SUCCEEDED,
                state=db_models.ClaimState.ACTIVE,
            )

        poller.poll()

        assert metrics.groups_observed(name=_OCCUPANCY)["gpu"] == 0
        assert metrics.groups_observed(name=_ACTIVE_CLAIMS)["gpu"] == 2

    def test_the_active_age_runs_from_admission_and_not_from_queueing(
        self,
        session: orm.Session,
        poller: quota_poller.QuotaPoller,
        metrics: probes.MetricsProbe,
    ) -> None:
        # An hour queued, a minute running. `created_at` would report 3600 and call a
        # one-minute job the oldest thing in the fleet.
        group = _group(session=session, name="gpu", capacity=1)
        _claim(
            session=session,
            group_id=group.id,
            node_id="long-queued",
            node_status=Status.RUNNING,
            state=db_models.ClaimState.ACTIVE,
            age_seconds=3_600,
            admitted_seconds_ago=60,
        )

        poller.poll()

        assert metrics.groups_observed(name=_OLDEST_ACTIVE_AGE)["gpu"] == pytest.approx(
            60, abs=5
        )

    def test_a_group_with_nothing_running_reports_zero_rather_than_vanishing(
        self,
        session: orm.Session,
        poller: quota_poller.QuotaPoller,
        metrics: probes.MetricsProbe,
    ) -> None:
        _group(session=session, name="idle", capacity=5)

        poller.poll()

        assert metrics.groups_observed(name=_ACTIVE_CLAIMS)["idle"] == 0
        assert metrics.groups_observed(name=_OLDEST_ACTIVE_AGE)["idle"] == 0


class TestSeriesLifecycle:
    """Which groups appear, and — more importantly — which stop appearing."""

    def test_a_group_with_nothing_happening_reports_zero_rather_than_vanishing(
        self,
        session: orm.Session,
        poller: quota_poller.QuotaPoller,
        metrics: probes.MetricsProbe,
    ) -> None:
        # A gauge that disappears when the condition clears makes "healthy" and "not being
        # measured" the same picture, and only one of those is safe to page on.
        _group(session=session, name="idle", capacity=5)

        poller.poll()

        assert metrics.groups_observed(name=_WAITERS) == {"idle": 0}
        assert metrics.groups_observed(name=_OLDEST_WAITER_AGE) == {"idle": 0}

    def test_a_deleted_group_stops_being_observed(
        self,
        session: orm.Session,
        poller: quota_poller.QuotaPoller,
        metrics: probes.MetricsProbe,
    ) -> None:
        # The cache is replaced wholesale rather than updated, which is what ends the series
        # instead of freezing it at its last reading.
        group = _group(session=session, name="doomed", capacity=1)
        poller.poll()
        assert "doomed" in metrics.groups_observed(name=_CAPACITY)

        session.execute(
            sqlalchemy.delete(db_models.QuotaGroup).where(
                db_models.QuotaGroup.id == group.id
            )
        )
        session.commit()
        poller.poll()

        assert metrics.groups_observed(name=_CAPACITY) == {}


class TestPollFailure:
    """What the gauges say when the database does not answer."""

    def test_a_failed_poll_keeps_the_last_reading_rather_than_zeroing_it(
        self,
        session: orm.Session,
        poller: quota_poller.QuotaPoller,
        metrics: probes.MetricsProbe,
    ) -> None:
        # Reporting every group as empty because the query broke would turn a database
        # problem into an all-clear, which is the one wrong answer this poller must not give.
        group = _group(session=session, name="gpu", capacity=1)
        _claim(
            session=session,
            group_id=group.id,
            node_id="parked",
            node_status=Status.UNINITIALIZED,
            state=db_models.ClaimState.WAITING,
        )
        poller.poll()

        def _broken_session() -> orm.Session:
            raise RuntimeError("db down")

        poller._session_factory = _broken_session
        with pytest.raises(RuntimeError, match="db down"):
            poller.poll()

        assert metrics.groups_observed(name=_WAITERS) == {"gpu": 1}
