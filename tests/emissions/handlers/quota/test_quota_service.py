"""Tests for the quota handler: what it does with a row and what it records.

Parsing is covered in test_quota_annotations.py and the promotion in
sinks/test_quota_group.py; here the handler is checked for the two things only it does —
threading the event's node id into the sink, and refusing to deliver twice.

The double-delivery guard is worth more here than in readiness. Readiness's sink logs; this
one writes, and `promote()` is idempotent *per node but not per group*, so a second delivery
of the same event promotes the next waiter and over-fills the group.
"""

import enum

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.dispatching import service as dispatching_service
from cloud_pipelines_backend.dispatching.handlers import base as handler_base
from cloud_pipelines_backend.dispatching.handlers.sinks import base as sinks_base
from cloud_pipelines_backend.emissions import db_models
from cloud_pipelines_backend.emissions import messages as emission_messages
from cloud_pipelines_backend.emissions.handlers.quota import (
    annotations as quota_annotations,
)
from cloud_pipelines_backend.emissions.handlers.quota import service as quota_service

CES = bts.ContainerExecutionStatus
QSA = quota_annotations.QuotaSinkAnnotation
_QUOTA = db_models.EmissionType.QUOTA.value
_PROMOTE = QSA.PROMOTE_WAITING_NODES


# Quota implements exactly one sink, so a multi-sink case needs a second key. A test-local
# enum stands in for a build with two, rather than adding an undeliverable member to
# QuotaSinkAnnotation.
class _FakeSinkAnnotation(str, enum.Enum):
    SECOND = f"{quota_annotations.SINK_PREFIX}second"


class _FakeSink(sinks_base.Sink[quota_annotations.QuotaIntent]):
    """A stand-in sink recording what it was handed."""

    def __init__(
        self,
        *,
        outcome: handler_base.Outcome | None = None,
        raises: bool = False,
    ) -> None:
        self._outcome = outcome or handler_base.Outcome(
            status=handler_base.OutcomeStatus.SUCCESS, detail={"promoted": 1}
        )
        self._raises = raises
        self.emitted: list[tuple[quota_annotations.QuotaIntent, str]] = []

    def emit(
        self,
        *,
        intent: quota_annotations.QuotaIntent,
        execution_node_id: str,
    ) -> handler_base.Outcome:
        self.emitted.append((intent, execution_node_id))
        if self._raises:
            raise RuntimeError("boom in sink")
        return self._outcome


class _FakeRecorder(handler_base.OutcomeRecorder):
    """Collects what the handler recorded, with no database behind it."""

    def __init__(
        self,
        *,
        done: set[str] | None = None,
    ) -> None:
        self._done = done or set()
        self.recorded: list[tuple[str, handler_base.Outcome]] = []
        self.unresolved: list[str] = []

    def is_done(
        self,
        *,
        sink_key: str,
    ) -> bool:
        return sink_key in self._done

    def record(
        self,
        *,
        sink_key: str,
        outcome: handler_base.Outcome,
    ) -> None:
        self.recorded.append((sink_key, outcome))

    def record_unresolved(
        self,
        *,
        sink_key: str,
    ) -> None:
        self.unresolved.append(sink_key)


def _make_event(
    *,
    annotations: dict[str, str] | None = None,
    execution_node_id: str = "node-1",
) -> emission_messages.EmissionEventMessage:
    """Build the message a consumer would hand the handler."""
    return emission_messages.EmissionEventMessage(
        emission_event_id="em-1",
        emission_type=_QUOTA,
        execution_node_id=execution_node_id,
        container_execution_id="ce-1",
        pipeline_run_id="run-1",
        container_execution_status=CES.SUCCEEDED,
        annotations=(
            {quota_annotations.QUOTA_GROUP_KEY: "bq"}
            if annotations is None
            else annotations
        ),
    )


class TestParse:
    def test_it_rebuilds_the_intent_from_the_rows(self) -> None:
        handler = quota_service.QuotaHandler(sinks={})
        result = handler.parse(event=_make_event())
        assert result.intent is not None
        assert result.intent.quota_group == "bq"

    def test_a_row_with_no_group_key_yields_no_intent(self) -> None:
        """Only reachable by a hand-edited or corrupted row: the producer always writes it."""
        handler = quota_service.QuotaHandler(sinks={})
        assert handler.parse(event=_make_event(annotations={})).intent is None

    def test_it_claims_the_quota_routing_key(self) -> None:
        assert quota_service.QuotaHandler(sinks={}).routing_key == "quota"


class TestTheNodeIdReachesTheSink:
    def test_the_event_supplies_it_not_the_intent(self) -> None:
        """The whole reason Sink.emit grew a second parameter: the intent is rebuilt
        identically for every node with these annotations, so it cannot identify this one.
        """
        sink = _FakeSink()
        handler = quota_service.QuotaHandler(sinks={_PROMOTE: sink})
        recorder = _FakeRecorder()

        handler.handle(
            event=_make_event(execution_node_id="the-node-that-ended"),
            intent=quota_annotations.QuotaIntent(quota_group="bq"),
            unknown_sink_keys=(),
            recorder=recorder,
        )

        assert [node_id for _, node_id in sink.emitted] == ["the-node-that-ended"]


class TestFanOut:
    def test_the_declared_sink_is_delivered_to_and_recorded(self) -> None:
        sink = _FakeSink()
        handler = quota_service.QuotaHandler(sinks={_PROMOTE: sink})
        recorder = _FakeRecorder()

        result = handler.handle(
            event=_make_event(),
            intent=quota_annotations.QuotaIntent(quota_group="bq"),
            unknown_sink_keys=(),
            recorder=recorder,
        )

        assert result.status is handler_base.HandleStatus.COMPLETE
        assert [key for key, _ in recorder.recorded] == [_PROMOTE.value]

    def test_a_delivery_already_recorded_is_not_repeated(self) -> None:
        """The guard against promoting twice for one completion."""
        sink = _FakeSink()
        handler = quota_service.QuotaHandler(sinks={_PROMOTE: sink})
        recorder = _FakeRecorder(done={_PROMOTE.value})

        result = handler.handle(
            event=_make_event(),
            intent=quota_annotations.QuotaIntent(quota_group="bq"),
            unknown_sink_keys=(),
            recorder=recorder,
        )

        assert sink.emitted == []
        assert recorder.recorded == []
        assert result.status is handler_base.HandleStatus.COMPLETE

    def test_a_sink_with_no_implementation_is_recorded_unresolved(self) -> None:
        """A wiring gap, not a user error: quota's sink key is never node-declared."""
        handler = quota_service.QuotaHandler(sinks={})
        recorder = _FakeRecorder()

        result = handler.handle(
            event=_make_event(),
            intent=quota_annotations.QuotaIntent(quota_group="bq"),
            unknown_sink_keys=(),
            recorder=recorder,
        )

        assert recorder.unresolved == [_PROMOTE.value]
        assert result.status is handler_base.HandleStatus.INCOMPLETE
        assert result.unresolved_sinks == (_PROMOTE.value,)

    def test_a_failing_outcome_is_recorded_without_raising(self) -> None:
        """A sink reports an expected failure; the row is redelivered on the next claim."""
        failure = handler_base.Outcome(
            status=handler_base.OutcomeStatus.FAIL, detail={"why": "db down"}
        )
        sink = _FakeSink(outcome=failure)
        handler = quota_service.QuotaHandler(sinks={_PROMOTE: sink})
        recorder = _FakeRecorder()

        result = handler.handle(
            event=_make_event(),
            intent=quota_annotations.QuotaIntent(quota_group="bq"),
            unknown_sink_keys=(),
            recorder=recorder,
        )

        assert recorder.recorded == [(_PROMOTE.value, failure)]
        # Every declared sink was reached, so the fan-out is complete even though the
        # delivery failed; the verdict lives on the delivery row.
        assert result.status is handler_base.HandleStatus.COMPLETE

    def test_a_key_the_parser_could_not_resolve_makes_it_incomplete(
        self,
    ) -> None:
        handler = quota_service.QuotaHandler(sinks={_PROMOTE: _FakeSink()})
        recorder = _FakeRecorder()

        result = handler.handle(
            event=_make_event(),
            intent=quota_annotations.QuotaIntent(quota_group="bq"),
            unknown_sink_keys=(_FakeSinkAnnotation.SECOND.value,),
            recorder=recorder,
        )

        assert result.status is handler_base.HandleStatus.INCOMPLETE
        assert result.unresolved_sinks == (_FakeSinkAnnotation.SECOND.value,)


class TestThroughDispatcher:
    """The handler as the dispatcher drives it."""

    def test_it_routes_on_the_quota_key(self) -> None:
        """A row typed "quota" must reach this handler. The routing key is the only thing
        connecting EmissionType.QUOTA to it, so a mismatch would route nowhere in silence.
        """
        sink = _FakeSink()
        dispatcher = dispatching_service.DispatcherService(
            handlers=[quota_service.QuotaHandler(sinks={_PROMOTE: sink})]
        )
        recorder = _FakeRecorder()

        result = dispatcher.dispatch(event=_make_event(), recorder=recorder)

        assert result.status is handler_base.HandleStatus.COMPLETE
        assert [node_id for _, node_id in sink.emitted] == ["node-1"]
        assert [key for key, _ in recorder.recorded] == [_PROMOTE.value]

    def test_a_row_with_no_group_touches_no_sink(self) -> None:
        """parse returns no intent, so the fan-out never starts."""
        sink = _FakeSink()
        dispatcher = dispatching_service.DispatcherService(
            handlers=[quota_service.QuotaHandler(sinks={_PROMOTE: sink})]
        )

        dispatcher.dispatch(event=_make_event(annotations={}), recorder=_FakeRecorder())

        assert sink.emitted == []
