"""The generic reconciler contract: one level-triggered pass, run on a schedule.

A reconciler answers a standing question about stored state and corrects what it finds,
instead of reacting to the event that should have corrected it. That is the whole difference
from a handler: a handler is given an edge and acts on it, a reconciler is given nothing and
goes looking. On a healthy system a pass finds nothing and writes nothing.

Three obligations, and each of them is a bug that has already been written once:

  * **Idempotent, and re-derived.** A pass recomputes the desired state; it never replays a
    delta. Two replicas run this loop, and a pass that assumes it is the only one corrupts.
  * **Per-unit commit.** A pass that corrects many things commits each one, so a failure
    partway keeps the work already done. `reconcile()` returning is not the transaction
    boundary.
  * **No sleeping, no looping, no thread.** The service owns the schedule. A pass that blocks
    past its own interval delays every other reconciler on the service's one thread.
"""

import abc


class Reconciler(abc.ABC):
    """One standing question, asked on a timer."""

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Stable identifier, used in the log line and as the metric label.

        Unique across one service -- `ReconcilerService` refuses a duplicate at construction
        rather than letting two reconcilers silently share a label, which is the failure the
        dispatcher already guards against for `routing_key` (`dispatching/service.py:72`).
        """

    @property
    @abc.abstractmethod
    def interval_seconds(self) -> float:
        """How long to wait between the end of one pass and the start of the next.

        Per reconciler, not per service: a promotion backstop and a retention sweep are
        answering questions on completely different timescales, and forcing them onto one
        cadence makes the fast one wasteful or the slow one late.
        """

    @abc.abstractmethod
    def reconcile(self) -> int:
        """Run exactly one pass and report how many things it corrected.

        Zero is the expected answer, and it is what makes the return value worth having: a
        non-zero count is not "working", it is "something upstream failed and this caught
        it". Callers log on non-zero, not on zero.

        May raise -- the service logs the traceback and schedules the next pass -- but a pass
        that can fail halfway commits per unit, so raising loses only the unit in flight.

        Returns:
            The number of things this pass corrected.
        """
