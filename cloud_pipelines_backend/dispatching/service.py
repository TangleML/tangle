"""The generic synchronous router from a message to its handler."""

import dataclasses
import logging
import typing

from cloud_pipelines_backend.dispatching.handlers import base as handler_base

logger = logging.getLogger(__name__)


class DispatcherService(typing.Generic[handler_base.MessageT]):
    """Routes one message to its handler and runs parse then handle inline.

    Generic over the message type it routes (`MessageT`, bound to DispatchableMessage): all
    handlers on one router share that message type. It is intentionally not generic over the
    handler intent type — the handlers are heterogeneous in intent (each owns its own), and the
    router never exposes the intent, so that second Handler type parameter stays Any below.

    Constructed with the handlers to route to; it builds a routing_key -> handler table
    once and looks it up in constant time per message. It runs no threads, owns no queue,
    and never touches the database.

    Two grains leave this layer. Each sink reports an `Outcome`, which the handler records
    through the recorder as that delivery finishes; the `HandleResult` that comes back up
    says how far the fan-out got, and is the only thing the router returns.

              Message + OutcomeRecorder
                 |
                 v
          DispatcherService  --- route on routing_key (constant time) --.
                 |                                                       |
                 v                                                       v
          +-----------+   +-----------+   +-----------+           (no handler)
          |  Handler  |   |  Handler  |   |  Handler  |  ...           |
          +-----------+   +-----------+   +-----------+                |
                 | parse(event) -> intent  (none: nothing_to_do)       |
                 | handle(event, intent, recorder)  (raise: failed)    |
                 v                                                     |
          +--------+ +--------+ +--------+                             |
          |  Sink  | |  Sink  | |  Sink  |  one per declared sink key  |
          +--------+ +--------+ +--------+                             |
                 |         |         |                                 |
                 '---------+---------'  Outcome each --> recorder       |
                           |                                           |
                           v                                           v
             HandleResult (complete | incomplete)     HandleResult (failed)
    """

    def __init__(
        self,
        *,
        # The second Handler type parameter is the intent type; it varies per handler and the
        # router never inspects it, so it stays Any here.
        handlers: list[handler_base.Handler[handler_base.MessageT, typing.Any]],
    ) -> None:
        """Build the routing table from the given handlers.

        Args:
            handlers: The handlers to route to, each identified by its own routing_key.

        Raises:
            ValueError: If two handlers claim the same routing_key, which would silently
                shadow one of them.
        """
        # The second Handler type parameter is the intent type; see the note in __init__'s
        # signature — it varies per handler and the router never inspects it, so it stays Any.
        self._handlers: dict[
            str, handler_base.Handler[handler_base.MessageT, typing.Any]
        ] = {}
        for handler in handlers:
            if handler.routing_key in self._handlers:
                raise ValueError(
                    f"duplicate handler for routing_key={handler.routing_key}"
                )
            self._handlers[handler.routing_key] = handler

    def dispatch(
        self,
        *,
        event: handler_base.MessageT,
        recorder: handler_base.OutcomeRecorder,
    ) -> handler_base.HandleResult:
        """Route one message to its handler and report what became of its fan-out.

        A missing handler resolves to `failed` — nothing is registered to deliver this
        message, which is a wiring gap — and a parse step that yields no intent to
        `nothing_to_do`. An exception from parse or handle is caught and mapped to `failed`, so
        one bad message can neither retry forever nor crash the caller's loop. Deliveries the
        handler already made keep their own records in every one of those cases.

        `RecorderUnavailable` is the one exception not converted: it says a delivery happened
        and the ledger could not be told, which is about the record rather than the message, so
        it is re-raised for the caller to leave the message unsettled.

        Args:
            event: The message to route and handle.
            recorder: Where the handler records each delivery's Outcome; passed straight
                through, since the router itself records nothing.

        Returns:
            The handler's HandleResult, or one the router synthesizes for the cases above.
        """
        # Step 1: Route on the routing_key in constant time.
        handler = self._handlers.get(event.routing_key)
        if handler is None:
            logger.warning(
                f"No handler registered for routing_key={event.routing_key}; ignoring"
            )
            return handler_base.HandleResult(
                status=handler_base.HandleStatus.FAILED,
                detail={"reason": "no_handler"},
            )

        try:
            # Step 2: Run the handler's pure parse step.
            result = handler.parse(event=event)

            # Step 3: Log any validation issues here, where the message id is available; the
            # parse step itself does not log. One warning per message keeps it to one line.
            if result.issues:
                issue_summary = "\n".join(
                    f"  [{issue.code}] dropped={issue.dropped}: {issue.message}"
                    for issue in result.issues
                )
                logger.warning(
                    f"Parse issues for routing_key={event.routing_key} "
                    f"{event.message_id}:\n{issue_summary}"
                )

            # Step 4: No intent means nothing to do — report that without handling.
            if result.intent is None:
                return handler_base.HandleResult(
                    status=handler_base.HandleStatus.NOTHING_TO_DO,
                    detail={
                        "reason": "parse_returned_none",
                        "issues": [issue.code for issue in result.issues],
                    },
                )

            # Step 5: Hand the parsed intent to the handler for its fan-out. The detail is
            # checked on the way out because the caller records whatever this method returns,
            # and nothing above the router is in a position to catch an unstorable one.
            handled = handler.handle(
                event=event,
                intent=result.intent,
                unknown_sink_keys=result.unknown_sink_keys,
                recorder=recorder,
            )
            detail = handler_base.safe_detail(detail=handled.detail)
            if detail is handled.detail:
                return handled
            logger.error(
                f"Handler for routing_key={event.routing_key} returned an unstorable "
                f"detail for {event.message_id}; recording the reason instead"
            )
            return dataclasses.replace(handled, detail=detail)
        except handler_base.RecorderUnavailable:
            # The one failure this layer has no verdict for: a delivery was made and the
            # ledger could not be told. Reporting `failed` would let the caller close a
            # message whose record is incomplete, so it goes up instead and the caller keeps
            # the message for another attempt.
            logger.exception(
                f"Recorder unavailable for routing_key={event.routing_key} "
                f"{event.message_id}; leaving the message unsettled"
            )
            raise
        except handler_base.DeliveryIncomplete:
            # The other one, and it is not an error: a sink stopped part-way and wants the
            # message back to finish. Passed through for the same reason as above -- any
            # verdict, including `failed`, settles a message whose work is unfinished -- but
            # logged at info, since nothing is broken and the caller is expected to redeliver.
            logger.info(
                f"Delivery incomplete for routing_key={event.routing_key} "
                f"{event.message_id}; leaving the message unsettled"
            )
            raise
        except Exception as exc:
            # Handlers report an expected delivery failure on that delivery's own record;
            # this catches the unexpected exception and reports it so the caller's loop is
            # safe. Whatever the fan-out already delivered keeps its records.
            logger.exception(
                f"Handler for routing_key={event.routing_key} "
                f"failed for {event.message_id}"
            )
            return handler_base.HandleResult(
                status=handler_base.HandleStatus.FAILED,
                detail={"error": repr(exc)},
            )
