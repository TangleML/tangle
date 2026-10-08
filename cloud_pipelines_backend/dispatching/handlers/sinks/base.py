"""The generic sink contract: the output port a handler delivers through."""

import abc
import typing

from cloud_pipelines_backend.dispatching.handlers import base as handler_base

# Deliberately unbound, unlike MessageT below. The obvious bound is emissions'
# EmissionIntent, but `dispatching/` is the generic layer and `emissions/` is one user of
# it, so naming that type here inverts the dependency -- and doing the same in
# dispatching/handlers/base.py is a hard circular import, because emissions.annotations
# imports that module at class-definition time. A bound is only worth having once the
# intent contract lives at this layer.
IntentT = typing.TypeVar("IntentT")


class Sink(abc.ABC, typing.Generic[IntentT]):
    """One output port a handler calls to perform one delivery's side effect.

    A sink consumes the handler's already-validated, typed intent — never the raw event —
    and performs a single synchronous side effect. A handler holds one per sink key it can
    deliver to, and each call reports its own Outcome.
    """

    @abc.abstractmethod
    def emit(
        self,
        *,
        intent: IntentT,
        execution_node_id: str,
        emission_event_id: str,
    ) -> handler_base.Outcome:
        """Perform the side effect for a typed intent and report the outcome.

        Never raise for an expected failure; report a failing Outcome instead. The owning
        handler applies its own policy on top of the result.

        The call is synchronous and nothing above it bounds how long it takes, so a sink that
        talks to a remote target sets that bound inside the client it calls — a request timeout,
        an RPC deadline — and reports the breach as a failing Outcome.

        A delivery is recorded after its side effect happens, so a failure in between means the
        same delivery is attempted again on a later claim. An implementation that does more than
        log makes its side effect idempotent — an idempotency key the target honours, a
        create-if-absent, or a read before the write.

        The node id is passed alongside the intent rather than folded into it, because the
        intent is what the annotations said and the id is what they were said about. A sink that
        needs more of the node than its annotations carry — an output artifact, a timestamp —
        reads it from the id; one that does not, ignores it.

        Args:
            intent: The handler's typed, already-validated intent.
            execution_node_id: The node whose status change produced this emission.
            emission_event_id: The delivery's stable identity — the emission_event row this
                intent was rebuilt from. A scalar rather than the event itself, so the rule
                above still holds: a sink sees no raw event. It is what the paragraph on
                idempotency asks for, since a sink cannot deduplicate a redelivery it cannot
                name, and it is also the id a side effect records for correlation.

        Returns:
            The Outcome of the side effect (success, fail, or ignore).
        """
