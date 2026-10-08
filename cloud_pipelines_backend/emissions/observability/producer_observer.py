"""What the producer reports about writing emission rows.

The producer runs inside the node's own commit, so everything here is on a hot path that must
not fail: the helpers it calls swallow their own errors, and the timing it persists rides an
insert the producer was making anyway rather than costing a write of its own.

One node status change can emit several rows, so the span covers the node while the counter
and the histogram are per row. `ProducerObserver` is the per-node object those rows report
through.
"""

import logging
import time
import typing

from opentelemetry import metrics as otel_metrics

from cloud_pipelines_backend.emissions.observability import environment
from cloud_pipelines_backend.emissions.observability import metrics as emission_metrics
from cloud_pipelines_backend.emissions.observability import timing as emission_timing
from cloud_pipelines_backend.emissions.observability import tracing as emission_tracing

logger = logging.getLogger(__name__)


class ProducerTelemetry:
    """Everything the producer reports, behind one object.

    The producer is a hot path inside someone else's commit, and its observability used to be
    spread across four module-level functions plus a module global holding a gauge alive. Read
    from `producer.py`, that made the instrumentation indistinguishable from the emission logic
    it instruments. One object fixes that by grep: every line in the producer that reports
    rather than emits goes through the same name.

    Holds the pending-transitions gauge, because the metrics SDK observes a gauge only while
    something keeps a reference to it, and an instance attribute is a clearer owner than a
    module global.
    """

    def __init__(
        self,
        *,
        pending_transitions: typing.Callable[[], int],
    ) -> None:
        """Bind the ledger this reports on; the gauge itself is opted into separately.

        Args:
            pending_transitions: Returns the current size of the producer's transition ledger.
                Called on the exporter's thread once the gauge is registered, so it must read
                the size and nothing else — never iterate the mapping, whose entries the
                emitting thread adds and removes concurrently.
        """
        self._pending_transitions = pending_transitions
        self._pending_transitions_gauge: otel_metrics.ObservableGauge | None = None

    def node_span(
        self,
        *,
        execution_node_id: str,
    ) -> typing.ContextManager[None]:
        """The span covering everything one node's status change emits.

        Per node, not per commit: one commit routinely changes several nodes, and a span around
        the whole drain could name only one of them.

        Args:
            execution_node_id: The node whose status change is being emitted for.

        Returns:
            A context manager wrapping the producer's work for that node.
        """
        return emission_tracing.producer_span(execution_node_id=execution_node_id)

    def row_observer(
        self,
    ) -> "ProducerObserver":
        """Start timing one node's emission work.

        Returns:
            The per-node observer every row that node writes reports through.
        """
        return ProducerObserver()

    def transition_recorded(
        self,
    ) -> None:
        """Count one status assignment recorded for the next commit.

        Called from the assignment listener, which runs on every write to the column — so this
        is the hottest thing the producer does. It counts and returns; anything heavier belongs
        at drain time.

        Returns:
            None.
        """
        emission_metrics.increment(
            counter=emission_metrics.producer_transitions_recorded,
            attributes={},
        )

    def transitions_drained(
        self,
        *,
        count: int,
    ) -> None:
        """Count the recorded transitions a commit drained.

        Args:
            count: How many transitions this commit took, recorded in one `add`. Zero is not
                reported — `before_commit` fires again as each savepoint is released, so most
                calls in a multi-row commit find nothing left and counting them would drown the
                signal — and the floor is enforced by `metrics.add`.

        Returns:
            None.
        """
        emission_metrics.add(
            counter=emission_metrics.producer_transitions_drained,
            amount=count,
            attributes={},
        )

    def transition_rejected_stale(
        self,
    ) -> None:
        """Count one recorded transition dropped because the node no longer agrees with it.

        This is the rolled-back assignment: the record survived, the attribute did not. It
        carries the skip reason the producer deliberately does not log — production and staging
        both run at INFO, so a debug line there could never be seen.

        Returns:
            None.
        """
        emission_metrics.increment(
            counter=emission_metrics.producer_transitions_rejected_stale,
            attributes={},
        )

    def observe_pending_transitions(
        self,
    ) -> None:
        """Start reporting the size of the producer's transition ledger, as a gauge.

        A per-process opt-in, separate from construction for the same reason `observe_backlog`
        is separate on the consumer: registering a callback the metrics SDK holds for the life
        of the process is a side effect no test that merely wires listeners should inherit.
        Idempotent, so a process that calls it twice still has one gauge.

        Returns:
            None. Side effect only: the gauge is registered and held for the life of this
            object.
        """
        if self._pending_transitions_gauge is not None:
            return

        def observe(
            options: otel_metrics.CallbackOptions,
        ) -> typing.Iterable[otel_metrics.Observation]:
            """Yield the ledger's size for one collection cycle.

            Args:
                options: The SDK's collection options. Unused: there is one value, no labels.

            Yields:
                One observation, or none at all if the size cannot be read — a collection
                callback that raises takes the whole export down with it.
            """
            del options
            try:
                yield otel_metrics.Observation(
                    self._pending_transitions(),
                    environment.with_environment(attributes={}),
                )
            except Exception:
                logger.warning(
                    "Failed to observe pending emission transitions",
                    exc_info=True,
                )

        self._pending_transitions_gauge = (
            emission_metrics.create_pending_transitions_gauge(callback=observe)
        )


class ProducerObserver:
    """Times and counts the rows one node status change writes.

    Built when the producer starts work on a node, so every row it goes on to write is
    measured from the moment that node's emission work began.
    """

    def __init__(
        self,
    ) -> None:
        """Start the clock the rows are measured against."""
        self._started_at = time.monotonic()

    def timings_for_row(
        self,
    ) -> dict:
        """Return the `extra_data` to build a row with, carrying its producer timing.

        Set on the row before the flush so the timing rides that INSERT instead of costing a
        second write. It therefore measures the work up to this row's insert and not the
        insert itself, which the histogram recorded afterwards does cover.

        Returns:
            An extra_data dict holding this role's timings, namespaced by the role that
            measured them so the producer's `total_s` and the consumer's never collide::

                {
                    "timings": {
                        "producer": {"total_s": 0.0123},
                    },
                }

            The row does not exist yet, so this is all it carries. The consumer adds its own
            half under `consumer` when it drains the row later.
        """
        return emission_timing.merged_row_timings(
            extra_data=None,
            role=emission_timing.PRODUCER_ROLE,
            timings={"total_s": time.monotonic() - self._started_at},
        )

    def row_written(
        self,
        *,
        emission_type: str,
    ) -> None:
        """Count one row the producer appended and record how long it took to get there.

        Call this only for a row that survived its savepoint, so a row rejected as a
        duplicate does not show up as an emission this producer wrote.

        Args:
            emission_type: The kind of emission the row carries.
        """
        emission_metrics.increment(
            counter=emission_metrics.producer_written,
            attributes={emission_metrics.EMISSION_TYPE_LABEL: emission_type},
        )
        emission_metrics.record(
            histogram=emission_metrics.duration_total,
            seconds=time.monotonic() - self._started_at,
            emission_type=emission_type,
            attributes={emission_metrics.ROLE_LABEL: emission_metrics.PRODUCER_ROLE},
        )
