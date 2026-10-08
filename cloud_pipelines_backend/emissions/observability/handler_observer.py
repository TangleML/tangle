"""What a handler reports about handling one emission.

Every handler measures the same things the same way — its own body, each delivery inside it, and
what that delivery reported — so the nesting, the instruments and the stage names live here once
rather than being copied into each handler. A handler's own file is then left saying only what it
does with the emission.

The stages are separate context managers rather than one, because `handle` contains the
deliveries rather than equalling them: a handler fans out to every sink the emission declared, so
`handle` minus the sum of the deliveries is what the fan-out itself cost, and a slow emission is
attributable to one sink rather than to handling in general.
"""

import contextlib
import typing

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.emissions.observability import metrics as emission_metrics
from cloud_pipelines_backend.emissions.observability import timing as emission_timing


class Handling:
    """The handler stage in progress, offering the delivery stages nested inside it."""

    def __init__(
        self,
        *,
        emission_type: str,
        node_status: bts.ContainerExecutionStatus,
    ) -> None:
        """Hold what labels every stage: the kind of emission and the status it fired on.

        Args:
            emission_type: The handler's routing key, recorded as the stage label.
            node_status: The status the originating node emitted on, as the event carries it.
                The enum rather than its value, so the label stays bounded by construction.
        """
        self._emission_type = emission_type
        self._node_status = node_status

    def sink(
        self,
        *,
        sink_key: str,
    ) -> typing.ContextManager[None]:
        """Time one sink's side effect, separately from the handler body around it.

        Args:
            sink_key: The annotation key of the sink being delivered to, recorded beside the
                emission type so one slow sink is not read as a slow handler.

        Returns:
            A context manager wrapping the sink call.
        """
        return emission_timing.stage_timer(
            histogram=emission_metrics.consumer_duration_sink,
            stage="sink",
            emission_type=self._emission_type,
            attributes={emission_metrics.SINK_LABEL: sink_key},
        )

    def delivered(
        self,
        *,
        sink_key: str,
        status: str,
        detail: typing.Mapping[str, typing.Any] | None = None,
    ) -> None:
        """Count one delivery, under the sink it went to and the verdict it reported.

        Per delivery rather than per emission: an emission declaring several sinks is counted
        once here for each of them, which is why the settled counter is a separate instrument.
        Called for a delivery a sink actually made — a declared key that reached no sink is on
        the ledger but delivered nothing, and shows up as an `incomplete` emission instead.

        Two labels are read out of the outcome's detail rather than passed as arguments: a
        sink that has an owner and a status to report puts them there, and one that has
        neither says nothing and is counted with the defaults. This layer stays ignorant of
        any sink's own code vocabulary either way.

        Both are coerced on the way in rather than copied. The detail mapping is free-form and
        has as many authors as there are sinks, while these two are metric labels: an
        unrecognized reason would mint its own time series and slip past every alert, and a
        status_code that is not an integer would raise inside `status_class`. That call is an
        argument to `increment`, so it is evaluated outside `increment`'s never-raise wrapper,
        and the exception would leave the handler and fail an emission whose delivery had
        already been recorded. Coercing is what keeps measurement from costing a delivery.

        A sink that placed several payloads in one delivery says so with a `payloads` list,
        and then the counter moves one point per payload: the batch is Tangle's idea, and a
        rate that counted batches would report one number whether a delivery carried one
        report or fifty. `outcome_status` stays the delivery's — one accepted payload makes
        the delivery a success — while `delivery_reason` and `status_class` are each payload's
        own, so `emission.delivered{outcome_status="success",delivery_reason="upstream"}` is
        readable as what it is: reports lost inside a delivery that otherwise worked.

        `node_status` says which status the node emitted on, and is independent of every
        label above: `outcome_status` is the delivery's verdict, not the node's. A report
        delivered from a FAILED node and one from a SUCCEEDED node are otherwise the same
        series, and only this label tells them apart. Every payload point in one delivery
        carries the same value, since a delivery belongs to one node.

        Nothing about the payload's identity reaches the labels. An index or an entry name
        would mint a time series per report and per table, which is a cardinality bill with no
        alert to justify it; that identity is on the outcome row, where a query can afford it.

        Args:
            sink_key: The annotation key of the sink that was called.
            status: The OutcomeStatus value it reported.
            detail: The outcome's detail, read for `reason` and `status_code` when present,
                and for a `payloads` list when the delivery carried several.
        """
        detail = detail or {}
        payloads = detail.get("payloads")
        # A list of mappings or nothing: the detail is free-form, so a `payloads` of any other
        # shape is a sink saying something this layer does not understand and is counted as
        # one delivery rather than guessed at.
        per_payload: list[typing.Mapping[str, typing.Any]] = (
            [entry for entry in payloads if isinstance(entry, typing.Mapping)]
            if isinstance(payloads, list)
            else []
        )
        for measured in per_payload or [detail]:
            emission_metrics.increment(
                counter=emission_metrics.consumer_delivered,
                attributes={
                    emission_metrics.EMISSION_TYPE_LABEL: self._emission_type,
                    emission_metrics.SINK_LABEL: sink_key,
                    emission_metrics.OUTCOME_STATUS_LABEL: status,
                    emission_metrics.DELIVERY_REASON_LABEL: (
                        emission_metrics.coerce_reason(reason=measured.get("reason"))
                    ),
                    emission_metrics.STATUS_CLASS_LABEL: emission_metrics.status_class(
                        status_code=measured.get("status_code")
                    ),
                    emission_metrics.NODE_STATUS_LABEL: self._node_status.value,
                },
            )
        if per_payload:
            emission_metrics.record_count(
                histogram=emission_metrics.consumer_payloads_per_delivery,
                count=len(per_payload),
                emission_type=self._emission_type,
                attributes={emission_metrics.SINK_LABEL: sink_key},
            )


@contextlib.contextmanager
def handling(
    *,
    emission_type: str,
    node_status: bts.ContainerExecutionStatus,
) -> typing.Iterator[Handling]:
    """Time a handler handling one emission, every delivery it makes included.

    Args:
        emission_type: The handler's routing key, recorded as the stage label.
        node_status: The event's `container_execution_status`, carried onto each delivery
            point. Required: every handler holds the event, and a default would let a new
            handler silently file its deliveries under one status.

    Yields:
        A handle offering `sink()` and `delivered()` for each delivery inside this stage.
    """
    with emission_timing.stage_timer(
        histogram=emission_metrics.consumer_duration_handle,
        stage="handle",
        emission_type=emission_type,
    ):
        yield Handling(emission_type=emission_type, node_status=node_status)
