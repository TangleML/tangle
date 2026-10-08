"""The quota handler: a node ended, so its group may have room for a waiter."""

import typing

from cloud_pipelines_backend.dispatching.handlers import base as handler_base
from cloud_pipelines_backend.dispatching.handlers.sinks import base as sinks_base
from cloud_pipelines_backend.emissions import db_models
from cloud_pipelines_backend.emissions import messages as emission_messages
from cloud_pipelines_backend.emissions.handlers.quota import (
    annotations as quota_annotations,
)
from cloud_pipelines_backend.emissions.observability import handler_observer


class QuotaHandler(
    handler_base.Handler[
        emission_messages.EmissionEventMessage,
        quota_annotations.QuotaIntent,
    ]
):
    """Handles quota emissions: "a member of this group has ended".

    Structurally the same fan-out as `ReadinessHandler` -- validate the row, deliver to each
    sink it declares, record each verdict on its own delivery row -- with one difference worth
    stating: quota's fan-out has exactly one destination and the node never chose it. The
    mapping is still injected rather than hardcoded, because that is what lets a test
    substitute a sink and what keeps the wiring in one place, but a caller passing an empty
    mapping is a wiring bug rather than a user's preference.
    """

    def __init__(
        self,
        *,
        sinks: typing.Mapping[
            quota_annotations.QuotaSinkAnnotation,
            sinks_base.Sink[quota_annotations.QuotaIntent],
        ],
    ) -> None:
        """Claim the quota routing key and take the sinks to deliver through.

        Args:
            sinks: The sink behind each quota sink key, keyed by the enum member so the wiring
                is typed. A member with no entry here is a wiring gap, reported per event as an
                unresolved sink.
        """
        super().__init__(routing_key=db_models.EmissionType.QUOTA.value)
        self._sinks = sinks

    def parse(
        self,
        *,
        event: emission_messages.EmissionEventMessage,
    ) -> handler_base.ParseResult[quota_annotations.QuotaIntent]:
        """Rebuild the quota intent from the event's annotations.

        Args:
            event: The emission event to validate.

        Returns:
            A ParseResult holding the validated intent, or None when the row carries no quota
            group -- which for a row of this type means a hand-edited or corrupted row, since
            the producer writes the group key on every quota emission it creates.
        """
        return quota_annotations.parse_quota(annotations=event.annotations)

    def handle(
        self,
        *,
        event: emission_messages.EmissionEventMessage,
        intent: quota_annotations.QuotaIntent,
        unknown_sink_keys: tuple[str, ...],
        recorder: handler_base.OutcomeRecorder,
    ) -> handler_base.HandleResult:
        """Deliver to every sink the intent declared and report how far that got.

        Each delivery is recorded as its sink returns, so a fan-out cut short keeps what it
        already did and a consumer that reclaims the event delivers only what is missing. That
        matters more here than for readiness: the sink's side effect is a database write, so
        `is_done` is what stops a redelivered event from promoting a second time within the
        same claim.

        Args:
            event: The emission event being handled. Supplies the execution node id the sink
                resolves the quota group through.
            intent: The validated quota intent from `parse`.
            unknown_sink_keys: Declared sink keys the parser matched to no implemented sink.
            recorder: Where each delivery's Outcome is recorded.

        Returns:
            COMPLETE when every declared sink was reached, INCOMPLETE when any was not, naming
            the keys that were not.
        """
        unresolved = [*unknown_sink_keys]
        with handler_observer.handling(
            emission_type=self.routing_key,
            node_status=event.container_execution_status,
        ) as handled:
            for member in intent.sinks:
                # Already delivered under an earlier claim on this event. For quota that is
                # not merely wasteful: promotion is idempotent per node but not per group, so
                # a second call promotes the *next* waiter and over-fills the group.
                if recorder.is_done(sink_key=member.value):
                    continue

                sink = self._sinks.get(member)
                if sink is None:
                    recorder.record_unresolved(sink_key=member.value)
                    unresolved.append(member.value)
                    continue

                # Timed separately from the handling around it so a slow delivery is
                # attributable to the sink rather than to the fan-out.
                with handled.sink(sink_key=member.value):
                    outcome = sink.emit(
                        intent=intent,
                        execution_node_id=event.execution_node_id,
                    )

                recorder.record(sink_key=member.value, outcome=outcome)
                handled.delivered(sink_key=member.value, status=outcome.status.value)

        return handler_base.HandleResult(
            status=(
                handler_base.HandleStatus.INCOMPLETE
                if unresolved
                else handler_base.HandleStatus.COMPLETE
            ),
            unresolved_sinks=tuple(unresolved),
        )
