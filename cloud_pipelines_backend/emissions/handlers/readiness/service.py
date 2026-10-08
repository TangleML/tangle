"""The readiness handler: validates a readiness emission and announces it."""

import typing

from cloud_pipelines_backend.dispatching.handlers import base as handler_base
from cloud_pipelines_backend.dispatching.handlers.sinks import base as sinks_base
from cloud_pipelines_backend.emissions import db_models
from cloud_pipelines_backend.emissions import messages as emission_messages
from cloud_pipelines_backend.emissions.handlers.readiness import (
    annotations as readiness_annotations,
)
from cloud_pipelines_backend.emissions.observability import handler_observer


class ReadinessHandler(
    handler_base.Handler[
        emission_messages.EmissionEventMessage,
        readiness_annotations.ReadinessIntent,
    ]
):
    """Handles the readiness emissions: "this named thing is ready".

    The emission_event row is itself the readiness record, so this handler's job is to validate
    what the row says and announce it to each sink the row declares. Each announcement's verdict
    is recorded on that delivery's own row, so a sink that reported a failure does not make the
    event's own status say so: the event reports how far the fan-out got.
    """

    def __init__(
        self,
        *,
        sinks: typing.Mapping[
            readiness_annotations.ReadinessSinkAnnotation,
            sinks_base.Sink[readiness_annotations.ReadinessIntent],
        ],
    ) -> None:
        """Claim the readiness routing key and take the sinks to announce through.

        Args:
            sinks: The sink behind each readiness sink key, keyed by the enum member so the
                wiring is typed. Required, so the caller wiring this handler up decides what
                each key announces to and a test substitutes its own. A member with no entry
                here is a wiring gap, reported per event as an unresolved sink.
        """
        super().__init__(routing_key=db_models.EmissionType.READINESS.value)
        self._sinks = sinks

    def parse(
        self,
        *,
        event: emission_messages.EmissionEventMessage,
    ) -> handler_base.ParseResult[readiness_annotations.ReadinessIntent]:
        """Rebuild the readiness intent from the event's annotations.

        Args:
            event: The emission event to validate.

        Returns:
            A ParseResult holding the validated intent, or None when the event carries no
            usable readiness intent, plus any validation issues.
        """
        return readiness_annotations.parse_readiness(annotations=event.annotations)

    def handle(
        self,
        *,
        event: emission_messages.EmissionEventMessage,
        intent: readiness_annotations.ReadinessIntent,
        unknown_sink_keys: tuple[str, ...],
        recorder: handler_base.OutcomeRecorder,
    ) -> handler_base.HandleResult:
        """Announce the intent to every sink it declared and report how far that got.

        Each announcement is recorded as its sink returns, so a fan-out cut short keeps what it
        already delivered and the consumer that reclaims the event announces only what is
        missing. A sink that reports a failing Outcome has that verdict recorded on its own
        delivery and does not stop the sinks after it: the readiness signal survives in the row
        and its annotations regardless of what any sink managed to do.

        A declared sink with no implementation behind it is recorded as unresolved and the
        fan-out carries on. Both that and a key the parser could not resolve at all are a
        person changing code, which is why either makes the result INCOMPLETE.

        A sink that reports itself unfinished (`DeliveryIncomplete`) does not take its peers
        down with it. The exception is caught per sink and re-raised once the loop is over, so
        every other sink still runs on this pass and the event is still left unsettled for the
        redelivery the unfinished one asked for. Catching it outside the loop instead would
        make one sink's pause skip the sinks after it in the list, and cost them a whole claim
        lease apiece for work they were ready to do now. On the redelivery those peers are
        skipped by `is_done`, so nothing is delivered twice.

        Args:
            event: The emission event being handled.
            intent: The validated readiness intent from `parse`.
            unknown_sink_keys: Declared sink keys the parser matched to no implemented sink.
            recorder: Where each announcement's Outcome is recorded.

        Returns:
            COMPLETE when every declared sink was reached, INCOMPLETE when any was not, naming
            the keys that were not.
        """
        unresolved = [*unknown_sink_keys]
        incomplete: handler_base.DeliveryIncomplete | None = None
        with handler_observer.handling(
            emission_type=self.routing_key,
            node_status=event.container_execution_status,
        ) as handled:
            for member in intent.sinks:
                # Already announced under an earlier claim on this event, so announcing again
                # is the double delivery the ledger exists to prevent.
                if recorder.is_done(sink_key=member.value):
                    continue

                sink = self._sinks.get(member)
                if sink is None:
                    recorder.record_unresolved(sink_key=member.value)
                    unresolved.append(member.value)
                    continue

                # Timed separately from the handling around it so a slow announcement is
                # attributable to the sink rather than to the fan-out, and the recording that
                # follows is outside the timer because it is this handler's cost, not the
                # sink's.
                #
                # A sink reports an expected failure as a fail Outcome and logs it itself, so
                # that verdict is recorded as it comes. An unexpected raise leaves this loop
                # and the router reports the event failed, with everything already announced
                # recorded — with one exception: a recorder that could not write raises
                # RecorderUnavailable, which the router passes through instead of converting,
                # so the event is never settled and is announced again once its claim expires.
                try:
                    with handled.sink(sink_key=member.value):
                        outcome = sink.emit(
                            intent=intent,
                            execution_node_id=event.execution_node_id,
                            emission_event_id=event.emission_event_id,
                        )
                except handler_base.DeliveryIncomplete as error:
                    # Held, not swallowed: it is re-raised below so the event is still left
                    # unsettled, and held rather than raised here so the sinks after this one
                    # in the list still get their turn on this pass. Nothing is recorded for
                    # this sink -- an unfinished delivery has no verdict to write, and leaving
                    # its row absent is what brings it back.
                    incomplete = incomplete or error
                    continue

                recorder.record(sink_key=member.value, outcome=outcome)
                handled.delivered(
                    sink_key=member.value,
                    status=outcome.status.value,
                    detail=outcome.detail,
                )

        if incomplete is not None:
            # After the loop, so every peer has run. The router passes this through rather than
            # turning it into a verdict, and the consumer leaves the row claimed and unsettled.
            raise incomplete

        return handler_base.HandleResult(
            status=(
                handler_base.HandleStatus.INCOMPLETE
                if unresolved
                else handler_base.HandleStatus.COMPLETE
            ),
            unresolved_sinks=tuple(unresolved),
        )
