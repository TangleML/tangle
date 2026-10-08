"""The schedule every reconciler runs on: one thread, round-robin, each on its own interval."""

import logging
import threading
import time
import typing

from cloud_pipelines_backend.emissions.reconciling import base

logger = logging.getLogger(__name__)

# What to wait when the service holds no reconcilers at all. Long enough that an empty service
# costs nothing, and finite only because `stop.wait` needs a number -- the event still wakes it
# immediately on shutdown, so this is never a delay anybody observes.
_EMPTY_TICK_SECONDS: typing.Final[float] = 60.0


class ReconcilerService:
    """Runs every reconciler on one thread, each on its own interval.

    Mirrors `ConsumerService.run(stop=...)`: this object owns the schedule but not the
    thread. `emissions/consumer_main.py` makes the thread, because that is where the stop
    event and the trapped signals live.

    One thread rather than one per reconciler, and the connection pool is what decides it.
    This runs inside the emission consumer, which builds a single engine that the consumer
    and every reconciler share (`emissions/consumer_main.py:117`), so a thread per reconciler
    would grow that pool's footprint with the list. The cost is that a slow pass delays its
    neighbour by its own duration -- acceptable because a pass on a healthy system reads one
    indexed query and writes nothing.
    """

    def __init__(self, *, reconcilers: list[base.Reconciler]) -> None:
        """Store the reconcilers, refusing a duplicate name.

        Args:
            reconcilers: The standing questions this service asks, in the order they are
                polled within a tick.

        Raises:
            ValueError: If two reconcilers claim the same name, which would make one of them
                indistinguishable from the other in the logs and the metric.
        """
        seen: set[str] = set()
        for reconciler in reconcilers:
            if reconciler.name in seen:
                raise ValueError(f"duplicate reconciler name={reconciler.name}")
            seen.add(reconciler.name)
        self._reconcilers = list(reconcilers)

    def run(self, *, stop: threading.Event) -> None:
        """Run passes until asked to stop, logging and continuing past any failure.

        `time.monotonic()` here, deliberately, and *not* the `utc_now()` that
        `PromotionReconciler` compares against. The two clocks are measuring different
        things: this one measures a duration between two points in this process, which is
        what monotonic is for, while a reconciler's own predicate compares against a
        deadline stored in a row, which has to be on the row's clock. Using wall time for
        the schedule would let an NTP step skip or repeat a pass.

        `stop.wait` and not `time.sleep`, because the thread is non-daemon and shutdown
        joins it: a sleeping thread would make SIGTERM hang for a whole interval.

        Args:
            stop: Set by the owning process's signal handler. Checked between reconcilers as
                well as between ticks, so a long list does not delay shutdown by its length.
        """
        # Everything is due immediately on the first tick: a restart is exactly when a
        # standing question is most likely to have gone unanswered for a while.
        due_at = {r.name: time.monotonic() for r in self._reconcilers}
        while not stop.is_set():
            for reconciler in self._reconcilers:
                if stop.is_set():
                    break
                if due_at[reconciler.name] > time.monotonic():
                    continue
                try:
                    corrected = reconciler.reconcile()
                except Exception:
                    logger.exception(f"Reconciler {reconciler.name}: pass failed")
                else:
                    if corrected:
                        logger.warning(
                            f"Reconciler {reconciler.name} corrected {corrected} item(s)"
                        )
                # Re-anchored after the pass, not before it. Anchoring to the scheduled time
                # would make a pass that overran its own interval instantly due again, and it
                # would starve every reconciler behind it in the list.
                due_at[reconciler.name] = time.monotonic() + reconciler.interval_seconds
            stop.wait(self._seconds_until_next(due_at=due_at))

    def _seconds_until_next(self, *, due_at: dict[str, float]) -> float:
        """Seconds until the soonest reconciler is due, floored at zero.

        Floored rather than clamped to a minimum: a zero here means something is already
        overdue, and the `while` condition plus the per-reconciler due check keep that from
        spinning -- the next iteration runs it and pushes its due time forward.

        Args:
            due_at: Monotonic deadline per reconciler name, as maintained by `run`.

        Returns:
            How long to wait on the stop event before the next tick.
        """
        if not due_at:
            # No reconcilers: wait on the stop event and nothing else, rather than spinning.
            return _EMPTY_TICK_SECONDS
        return max(0.0, min(due_at.values()) - time.monotonic())
