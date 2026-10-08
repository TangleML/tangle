"""Tests for what the admission gate reports about deciding one node."""

import pytest

from cloud_pipelines_backend.quota.observability import gate_observer
from tests.quota.observability import probes

_GATE_DECISIONS = "quota.gate_decisions"
_DURATION_GATE = "quota.duration.gate"
_GPU = "gpu"
_MISSING = "<missing>"


class TestVerdicts:
    """One block, one verdict, and the label it lands under."""

    @pytest.mark.parametrize(
        ("name_the_verdict", "expected"),
        [
            (lambda gate: gate.admitted(quota_group=_GPU), "ADMITTED"),
            (
                lambda gate: gate.parked(quota_group=_GPU, was_waiting=False),
                "PARKED",
            ),
            (
                lambda gate: gate.parked(quota_group=_GPU, was_waiting=True),
                "REPARKED",
            ),
            (lambda gate: gate.contended(quota_group=_GPU), "CONTENDED"),
        ],
    )
    def test_each_verdict_is_counted_under_its_own_decision_label(
        self,
        metrics: probes.MetricsProbe,
        name_the_verdict: object,
        expected: str,
    ) -> None:
        with gate_observer.gating() as gate:
            name_the_verdict(gate)

        point = metrics.point(name=_GATE_DECISIONS, attributes={"decision": expected})
        assert point.value == 1
        assert point.attributes["quota_group"] == _GPU

    def test_a_re_park_is_a_different_series_from_a_first_park(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        # The two are split because they ask for different fixes: a high re-park rate means
        # promotion is over-optimistic and nodes are thrashing, not that the group is full.
        for was_waiting in (False, True):
            with gate_observer.gating() as gate:
                gate.parked(quota_group=_GPU, was_waiting=was_waiting)

        assert {
            point.attributes["decision"]
            for point in metrics.points(name=_GATE_DECISIONS)
        } == {"PARKED", "REPARKED"}


class TestUngated:
    """The one verdict that must never carry the name it was given."""

    def test_an_unknown_group_is_labelled_with_the_sentinel(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        # The declared name is typo- and attacker-controlled: labelling with it would let one
        # misspelled annotation mint a time series, and a loop of them exhaust the label
        # space. The name goes to extra_data and the log instead.
        with gate_observer.gating() as gate:
            gate.ungated()

        point = metrics.point(name=_GATE_DECISIONS, attributes={"decision": "UNGATED"})
        assert point.attributes["quota_group"] == _MISSING

    def test_ungated_takes_no_name_to_pass_it(
        self,
    ) -> None:
        # Structural, not behavioural: the safety is that there is no argument to get wrong.
        with gate_observer.gating() as gate:
            with pytest.raises(TypeError):
                gate.ungated(quota_group="whatever-the-node-said")


class TestDuration:
    """What is timed, and what deliberately is not."""

    def test_a_node_that_declared_no_group_is_not_measured_at_all(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        # The overwhelming majority of nodes. Counting them would bury the real verdicts under
        # traffic that says nothing about quota. Note this holds even though a raise is now
        # counted: a call that never resolved a group has no series to be counted under.
        with gate_observer.gating():
            pass

        assert metrics.points(name=_DURATION_GATE) == []
        assert metrics.points(name=_GATE_DECISIONS) == []

    def test_an_exit_with_no_verdict_still_records_its_duration(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        # The abandoned park, and now the only exit that reaches this state: the node was
        # cancelled or un-parked between the sweep reading it and the park writing to it, so
        # nothing was written and no verdict is honest. The compare-and-set giveup used to
        # land here too and is now counted as CONTENDED, which is why the gap between the
        # histogram's count and the counter's sum is no longer the contention rate.
        with gate_observer.gating() as gate:
            gate.measuring(quota_group=_GPU)

        assert (
            metrics.point(name=_DURATION_GATE, attributes={"quota_group": _GPU}).count
            == 1
        )
        assert metrics.points(name=_GATE_DECISIONS) == []

    def test_a_gate_that_raised_is_still_measured(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        # A gate that raised still spent the time it spent, and the exception must reach the
        # caller unchanged rather than being swallowed by the measurement.
        with pytest.raises(RuntimeError, match="boom"):
            with gate_observer.gating() as gate:
                gate.measuring(quota_group=_GPU)
                raise RuntimeError("boom")

        assert (
            metrics.point(name=_DURATION_GATE, attributes={"quota_group": _GPU}).count
            == 1
        )

    def test_a_second_verdict_is_refused_rather_than_double_counted(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        with gate_observer.gating() as gate:
            gate.admitted(quota_group=_GPU)
            gate.parked(quota_group=_GPU, was_waiting=False)

        assert metrics.point(name=_GATE_DECISIONS).attributes["decision"] == "ADMITTED"


class TestErrors:
    """A gate that raised, which used to be indistinguishable from a busy one."""

    def test_a_raise_with_no_verdict_named_is_counted_as_an_error(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        with pytest.raises(RuntimeError, match="boom"):
            with gate_observer.gating() as gate:
                gate.measuring(quota_group=_GPU)
                raise RuntimeError("boom")

        point = metrics.point(name=_GATE_DECISIONS, attributes={"decision": "ERROR"})
        assert point.value == 1
        assert point.attributes["quota_group"] == _GPU

    def test_a_raise_before_the_group_resolves_is_counted_under_the_sentinel(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        # The alternative is silence: the duration is dropped too when no group resolved, so a
        # gate failing on every call before it reads the group would emit nothing at all and
        # read as an idle gate. `decision` is what keeps the sentinel unambiguous -- UNGATED
        # with <missing> is a bad annotation, ERROR with <missing> is a gate that broke early.
        with pytest.raises(RuntimeError, match="boom"):
            with gate_observer.gating():
                raise RuntimeError("boom")

        point = metrics.point(name=_GATE_DECISIONS, attributes={"decision": "ERROR"})
        assert point.attributes["quota_group"] == _MISSING

    def test_a_raise_after_a_verdict_keeps_the_verdict(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        # Every verdict in the interceptor is named after the commit that makes it true, so an
        # exception on top of one describes a failure after the decision landed. Overwriting
        # would lose a real admission -- the node is running.
        with pytest.raises(RuntimeError, match="boom"):
            with gate_observer.gating() as gate:
                gate.admitted(quota_group=_GPU)
                raise RuntimeError("boom")

        assert metrics.point(name=_GATE_DECISIONS).attributes["decision"] == "ADMITTED"

    def test_a_cancellation_is_counted_too(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        # BaseException and not Exception: a gate killed mid-decision by a timeout or a
        # shutdown signal is exactly the case an operator needs the series for, and it would
        # be invisible under a bare `except Exception`.
        with pytest.raises(KeyboardInterrupt):
            with gate_observer.gating() as gate:
                gate.measuring(quota_group=_GPU)
                raise KeyboardInterrupt

        assert metrics.point(name=_GATE_DECISIONS).attributes["decision"] == "ERROR"
