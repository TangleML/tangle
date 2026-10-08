"""What a promotion pass reports about un-parking waiters.

The counterpart of `gate_observer` for the other half of the quota path, and it exists for the
same reason: `quota/promotion.py` is called from four places with four different meanings, and
none of them should have to know an instrument's name to say which one it was.

`quota.promotions` counts nodes, not passes. A pass that moved nothing records no increment at
all — the SDK would take a lock for a zero delta — and a pass that moved eight records one
`add` of eight rather than eight increments, because every one of them would carry identical
labels. `quota.duration.promote` is the per-pass number, so pass count is available there.

The trigger label is the only thing separating the four, and it is what an operator reads when
promotions stop: SINK drying up means completions are not arriving, while PATCH and the two
API triggers only ever move when a human does.
"""

import contextlib
import time
import typing

from cloud_pipelines_backend.quota.observability import metrics as quota_metrics


class Promoting:
    """One promotion pass in progress, waiting to be told how many nodes it moved."""

    def __init__(
        self,
        *,
        quota_group: str,
        trigger: quota_metrics.PromotionTrigger,
    ) -> None:
        """Hold the labels every measurement of this pass carries.

        Args:
            quota_group: The group being swept.
            trigger: Which call site is running the pass.
        """
        self._quota_group = quota_group
        self._trigger = trigger
        self._promoted = 0

    def promoted(
        self,
        *,
        count: int,
    ) -> None:
        """Record how many nodes this pass un-parked.

        Accumulates rather than overwrites, so a caller that promotes in more than one step
        reports the pass's total in a single `add` at the end.

        Args:
            count: The number of nodes moved to QUEUED.
        """
        self._promoted += count

    def _flush(
        self,
        *,
        seconds: float,
        failed: bool,
    ) -> None:
        """Write the pass's duration and, if it committed anything, its node count.

        The duration is written either way — a pass that raised still spent the time. The node
        count is not: promotion runs inside the caller's transaction and the commit is inside
        the measured block, so a pass that raised is a pass whose promotions rolled back.
        Counting the nodes it had lined up would report waiters as un-parked that are still
        sitting at UNINITIALIZED.

        Args:
            seconds: How long the pass took.
            failed: Whether the block left by raising.
        """
        quota_metrics.record(
            histogram=quota_metrics.duration_promote,
            seconds=seconds,
            quota_group=self._quota_group,
            # The same label the counter carries, so "how long a pass takes" can be read per
            # trigger. A reconciler pass and a sink pass do different amounts of work, and
            # averaged together neither p99 means anything.
            attributes={quota_metrics.TRIGGER_LABEL: self._trigger.value},
        )
        if failed:
            return
        quota_metrics.add(
            counter=quota_metrics.promotions,
            amount=self._promoted,
            attributes={
                quota_metrics.QUOTA_GROUP_LABEL: self._quota_group,
                quota_metrics.TRIGGER_LABEL: self._trigger.value,
            },
        )


@contextlib.contextmanager
def promoting(
    *,
    quota_group: str,
    trigger: quota_metrics.PromotionTrigger,
) -> typing.Iterator[Promoting]:
    """Time one promotion pass and count the nodes it un-parks.

    The pass is measured whether or not it succeeded, and an exception continues to the caller
    unchanged. A pass that raised records its duration and no nodes at all, however many it had
    already reported: promotion does not commit itself, so a pass that did not reach the end of
    this block is a pass whose work rolled back.

    **Put the caller's `session.commit()` inside this block.** Left outside it, a commit that
    fails happens after the count is already recorded, and the counter claims waiters were
    un-parked that are still parked.

    Args:
        quota_group: The group being swept.
        trigger: Which call site is running the pass.

    Yields:
        The handle the caller reports its promoted count on.
    """
    pass_ = Promoting(quota_group=quota_group, trigger=trigger)
    start = time.monotonic()
    failed = False
    try:
        yield pass_
    except BaseException:
        # BaseException and not Exception, mirroring `gate_observer.py:226`: a pass killed by a
        # shutdown signal or a cancellation mid-sweep has still not promoted what it counted,
        # and letting `failed` stay False would have `_flush` credit `quota.promotions` for a
        # transaction that rolled back -- the double-count this block exists to prevent.
        failed = True
        raise
    finally:
        pass_._flush(seconds=time.monotonic() - start, failed=failed)
