"""Tests for what a promotion pass reports about un-parking waiters."""

import pytest

from cloud_pipelines_backend.quota.observability import metrics as quota_metrics
from cloud_pipelines_backend.quota.observability import promotion_observer
from tests.quota.observability import probes

_PROMOTIONS = "quota.promotions"
_DURATION_PROMOTE = "quota.duration.promote"
_GPU = "gpu"


class TestTriggers:
    """What woke the pass is the only thing separating these series.

    No count in the sentence, and none in the test names below: the enum grew from three to
    four to five during review, and every count written down had to be chased.
    """

    def test_the_vocabulary_is_exactly_what_the_dashboard_expects(self) -> None:
        # Parametrizing over the enum, as the tests below do, covers a new member for free --
        # which also means a member added by accident is covered for free. This is the one
        # assertion that has to be edited on purpose, and RECONCILE is why it is here: it is
        # the only member that is not a call site, so it is the one whose deletion would be
        # invisible in a diff that "only touched observability".
        assert [t.value for t in quota_metrics.PromotionTrigger] == [
            "SINK",
            "PATCH",
            "PROMOTE_API",
            "CLAIM_RELEASE_API",
            "RECONCILE",
        ]

    @pytest.mark.parametrize(
        "trigger",
        list(quota_metrics.PromotionTrigger),
        ids=lambda trigger: trigger.value,
    )
    def test_every_trigger_records_under_its_own_label(
        self,
        metrics: probes.MetricsProbe,
        trigger: quota_metrics.PromotionTrigger,
    ) -> None:
        with promotion_observer.promoting(quota_group=_GPU, trigger=trigger) as pass_:
            pass_.promoted(count=2)

        point = metrics.point(name=_PROMOTIONS, attributes={"trigger": trigger.value})
        assert point.value == 2
        assert point.attributes["quota_group"] == _GPU

    def test_each_trigger_is_its_own_series_on_one_group(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        # SINK drying up means completions stopped arriving; the API triggers only ever move
        # when a human does; RECONCILE moving at all means an edge was lost. Summed together,
        # none of those questions is answerable.
        for trigger in quota_metrics.PromotionTrigger:
            with promotion_observer.promoting(
                quota_group=_GPU, trigger=trigger
            ) as pass_:
                pass_.promoted(count=1)

        assert len(metrics.points(name=_PROMOTIONS)) == len(
            quota_metrics.PromotionTrigger
        )

    def test_the_claim_release_endpoint_has_its_own_trigger(
        self,
    ) -> None:
        # Added after the design's original three: DELETE .../claims/{node} frees a slot and
        # promotes as a side effect, and it is a different act from somebody pressing the
        # promote button because nothing is moving.
        assert (
            quota_metrics.PromotionTrigger.CLAIM_RELEASE_API.value
            == "CLAIM_RELEASE_API"
        )


class TestCounts:
    """The counter is nodes; the histogram is passes."""

    def test_a_batch_of_eight_is_one_add_of_eight(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        with promotion_observer.promoting(
            quota_group=_GPU, trigger=quota_metrics.PromotionTrigger.SINK
        ) as pass_:
            pass_.promoted(count=8)

        assert metrics.point(name=_PROMOTIONS).value == 8

    def test_a_pass_reporting_in_steps_records_their_total_once(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        with promotion_observer.promoting(
            quota_group=_GPU, trigger=quota_metrics.PromotionTrigger.SINK
        ) as pass_:
            pass_.promoted(count=3)
            pass_.promoted(count=4)

        assert metrics.point(name=_PROMOTIONS).value == 7

    def test_a_pass_that_moved_nothing_mints_no_series(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        # The common case — most passes find no waiters — so a zero-valued series here would
        # be the bulk of the counter's cardinality for no information.
        with promotion_observer.promoting(
            quota_group=_GPU, trigger=quota_metrics.PromotionTrigger.SINK
        ) as pass_:
            pass_.promoted(count=0)

        assert metrics.points(name=_PROMOTIONS) == []

    def test_a_pass_that_moved_nothing_is_still_timed(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        # Pass count lives on the histogram precisely because the counter skips empty passes.
        with promotion_observer.promoting(
            quota_group=_GPU, trigger=quota_metrics.PromotionTrigger.SINK
        ):
            pass

        assert metrics.point(name=_DURATION_PROMOTE).count == 1

    def test_a_pass_that_raised_is_timed_and_counts_nobody(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        # Correct rather than lossy: nothing was promoted, and the transaction it ran in will
        # not commit, so counting the nodes it had lined up would be a lie.
        with pytest.raises(RuntimeError, match="boom"):
            with promotion_observer.promoting(
                quota_group=_GPU, trigger=quota_metrics.PromotionTrigger.SINK
            ) as pass_:
                pass_.promoted(count=4)
                raise RuntimeError("boom")

        assert metrics.point(name=_DURATION_PROMOTE).count == 1
        assert metrics.points(name=_PROMOTIONS) == []

    def test_a_pass_killed_by_a_shutdown_also_counts_nobody(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        # BaseException and not Exception, the same reason `gate_observer.py:226` gives: a pass
        # cancelled mid-sweep by a shutdown signal has still not promoted what it lined up, and
        # under a bare `except Exception` `failed` would stay False and the counter would credit
        # a transaction that rolled back.
        with pytest.raises(KeyboardInterrupt):
            with promotion_observer.promoting(
                quota_group=_GPU, trigger=quota_metrics.PromotionTrigger.SINK
            ) as pass_:
                pass_.promoted(count=4)
                raise KeyboardInterrupt

        assert metrics.point(name=_DURATION_PROMOTE).count == 1
        assert metrics.points(name=_PROMOTIONS) == []


class TestTheDurationCarriesItsTrigger:
    """A promote duration without a trigger label averages unlike passes together."""

    def test_the_histogram_is_labelled_with_the_same_trigger_as_the_counter(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        # Without this the sink's sub-millisecond passes and the reconciler's sweep share one
        # distribution, and neither p99 means anything. It is also the label that separates
        # "the sink stopped emitting" from "the group is genuinely full".
        with promotion_observer.promoting(
            quota_group=_GPU, trigger=quota_metrics.PromotionTrigger.RECONCILE
        ) as pass_:
            pass_.promoted(count=1)

        assert metrics.point(name=_DURATION_PROMOTE).attributes == {
            "trigger": "RECONCILE",
            "quota_group": _GPU,
        }

    def test_the_group_label_is_not_the_callers_to_overwrite(
        self,
        metrics: probes.MetricsProbe,
    ) -> None:
        # `record` merges the caller's attributes under the group's, because the group is the
        # identity of the series: a caller passing its own would split one group's timings
        # across two and neither would be the group's real distribution.
        quota_metrics.record(
            histogram=quota_metrics.duration_promote,
            seconds=1.0,
            quota_group=_GPU,
            attributes={"quota_group": "somewhere-else"},
        )

        assert metrics.point(name=_DURATION_PROMOTE).attributes["quota_group"] == _GPU
