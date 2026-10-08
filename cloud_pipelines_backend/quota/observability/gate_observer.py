"""What the admission gate reports about deciding one node.

`quota/interceptor.py` stays free of instrumentation: it opens one `gating()` block and names
its verdict, and every instrument, label and sentinel lives here. That is the same split
`emissions/observability/handler_observer.py` makes, and it is what lets the gate's own file
read as admission logic and nothing else.

The duration is recorded for every call that resolved a group, whether or not a verdict was
named. Three of the gate's exits are deliberately not counted as decisions:

| Exit | Why it is not a decision |
| --- | --- |
| The node declared no group at all | The overwhelming majority of nodes. Counting them would bury the four real verdicts under traffic that says nothing about quota |
| The group was deleted between resolving it and re-reading it | Launching ungated is the answer the deletion itself gave; the delete is already counted on the API side |
| The node already holds an ACTIVE slot and came back through the gate | It was counted as ADMITTED when it won the slot. Counting it again would inflate admissions on every failed launch |

One exit remains uncounted: a park abandoned because the node was cancelled or un-parked
between the sweep reading it and the park writing to it (`_park_status_update` in the
interceptor). It leaves the row exactly as it found it and logs a warning, and it is rare
enough that a series of its own would be noise.

Everything else names a verdict, including the two that used to be silent -- the exhausted
compare-and-set budget, now `CONTENDED`, and an exception on the gate path, now `ERROR`. That
matters for reading the numbers, because the gap between `sum(quota_gate_decisions_total)` and
the count of `quota.duration.gate` used to be described as the contention rate and was nothing
of the kind: it was contention plus exceptions plus abandoned parks, three unrelated
conditions summed into one line. Contention is now its own series, and exceptions are `ERROR`.

The gap that remains is three exits, not one. All three are after `gate.measuring()` at
`quota/interceptor.py:131`, so each records a duration and names no verdict:

- `:144` the group was deleted mid-flight, so the node launches ungated
- `:154` a re-entry -- the node already holds a slot, because the launch it was admitted for
  failed and the sweep re-queued it
- `:175` an abandoned park -- somebody decided about the node between the sweep reading it and
  the park writing to it

So `count(quota.duration.gate) - sum(quota_gate_decisions_total)` is those three summed. The
re-entry term is the one to watch: it tracks launch failures, which is usually the reason
somebody is reading this number in the first place.
"""

import contextlib
import logging
import time
import typing

from cloud_pipelines_backend.quota.observability import metrics as quota_metrics

logger = logging.getLogger(__name__)


class Gating:
    """One gate call in progress, waiting to be told how it ended.

    At most one verdict per call: the gate takes exactly one of its exits, and a second call
    would double-count a decision that happened once. A block that names no verdict records
    only the duration, which is how the uncounted exits stay visible as a gap.
    """

    def __init__(self) -> None:
        """Start with no verdict and no group; both arrive when the gate knows them."""
        self._quota_group: str | None = None
        self._decision: str | None = None

    def admitted(
        self,
        *,
        quota_group: str,
    ) -> None:
        """Record that the node won a slot and may launch.

        Args:
            quota_group: The group's name, read before the commit that expires it.
        """
        self._verdict(
            quota_group=quota_group,
            decision=quota_metrics.GateDecision.ADMITTED,
        )

    def parked(
        self,
        *,
        quota_group: str,
        was_waiting: bool,
    ) -> None:
        """Record that the group was full and the node was taken off the launch path.

        Args:
            quota_group: The group's name.
            was_waiting: Whether the node already had a WAITING claim when it was parked.
                True makes this a re-park — the node had been promoted and did not get in —
                which is a different operator signal from a first park, so the two are
                separate label values rather than one.
        """
        self._verdict(
            quota_group=quota_group,
            decision=(
                quota_metrics.GateDecision.REPARKED
                if was_waiting
                else quota_metrics.GateDecision.PARKED
            ),
        )

    def ungated(self) -> None:
        """Record that the node named a group that does not exist, so nothing gated it.

        Takes no group name on purpose. The declared name is typo- and attacker-controlled and
        would be unbounded cardinality, so the label is the `<missing>` sentinel and the real
        string goes to `extra_data["quota_group_missing"]` and the log line instead.
        """
        self._verdict(
            quota_group=quota_metrics.MISSING_GROUP,
            decision=quota_metrics.GateDecision.UNGATED,
        )

    def contended(
        self,
        *,
        quota_group: str,
    ) -> None:
        """Record that the gate ran out of compare-and-set attempts and gave up.

        Args:
            quota_group: The group's name.
        """
        self._verdict(
            quota_group=quota_group,
            decision=quota_metrics.GateDecision.CONTENDED,
        )

    def errored(self) -> None:
        """Record that the gate raised, unless it had already named a verdict.

        Every verdict in the interceptor is named *after* the commit that makes it true, so an
        exception arriving on top of one describes a failure after the decision landed --
        overwriting it would lose a real admission. A raise with no verdict named is the case
        this exists for, and it is counted under whatever group `measuring()` got to.

        A raise before even that is counted under the `<missing>` sentinel rather than
        dropped, which is the one place that sentinel means something other than a misspelled
        annotation. The alternative is worse: `_flush` drops the duration too when no group
        resolved, so a gate failing on every call before it reads the group would emit
        absolutely nothing and read as an idle gate. `decision` disambiguates the two uses --
        `<missing>` with `UNGATED` is a bad annotation, `<missing>` with `ERROR` is a gate
        that broke early.
        """
        if self._decision is not None:
            return
        self._verdict(
            quota_group=self._quota_group or quota_metrics.MISSING_GROUP,
            decision=quota_metrics.GateDecision.ERROR,
        )

    def measuring(
        self,
        *,
        quota_group: str,
    ) -> None:
        """Name the group this call is about, without naming a verdict.

        Called as soon as the group resolves, so an exit that records no decision still
        records its duration under the right group rather than being dropped.

        Args:
            quota_group: The group's name.
        """
        self._quota_group = quota_group

    def _verdict(
        self,
        *,
        quota_group: str,
        decision: quota_metrics.GateDecision,
    ) -> None:
        """Hold the verdict until the block closes, warning if one was already named.

        Recorded on exit rather than here so that the counter and the histogram describe the
        same call, and so an exception on the way out still leaves the decision counted.

        Args:
            quota_group: The group label for both instruments.
            decision: Which of the exclusive outcomes happened.
        """
        if self._decision is not None:
            logger.warning(
                f"Quota gate named a second verdict {decision.value} after"
                f" {self._decision}; keeping the first"
            )
            return
        self._quota_group = quota_group
        self._decision = decision.value

    def _flush(
        self,
        *,
        seconds: float,
    ) -> None:
        """Write the decision and the duration, if this call got far enough to have them.

        Args:
            seconds: How long the whole gate call took.
        """
        if self._quota_group is None:
            # The node declared no group. Nothing was gated and nothing is measured.
            return
        quota_metrics.record(
            histogram=quota_metrics.duration_gate,
            seconds=seconds,
            quota_group=self._quota_group,
        )
        if self._decision is None:
            return
        quota_metrics.increment(
            counter=quota_metrics.gate_decisions,
            attributes={
                quota_metrics.QUOTA_GROUP_LABEL: self._quota_group,
                quota_metrics.DECISION_LABEL: self._decision,
            },
        )


@contextlib.contextmanager
def gating() -> typing.Iterator[Gating]:
    """Time one gate call and record whatever verdict it names.

    The call is measured whether or not it succeeded: a gate that raised still spent the time
    it spent, and the exception continues to the caller unchanged. It is also *counted* --
    a raise with no verdict named becomes `ERROR` rather than vanishing into the gap between
    the counter and the histogram. This runs inside the orchestrator's single-threaded sweep,
    so the p99 of what it records is the sweep's throughput ceiling.

    Yields:
        The handle the gate names its verdict on.
    """
    gate = Gating()
    start = time.monotonic()
    try:
        yield gate
    except BaseException:
        # Named here and not in the interceptor so that `quota/interceptor.py` keeps having no
        # instrumentation in it at all. BaseException and not Exception: a gate killed by a
        # timeout or a shutdown signal mid-decision is exactly the case an operator needs the
        # series for, and the raise continues either way.
        gate.errored()
        raise
    finally:
        gate._flush(seconds=time.monotonic() - start)
