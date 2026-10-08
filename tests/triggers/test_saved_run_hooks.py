"""Application run hooks reach event-triggered saved pipeline execution."""

from sqlalchemy import orm

from cloud_pipelines_backend.dispatching.handlers import base as handler_base
from cloud_pipelines_backend.emissions.handlers.readiness import (
    annotations as readiness,
)
from cloud_pipelines_backend.emissions.handlers.readiness.sinks import (
    start_pipeline_run,
)
from cloud_pipelines_backend.user_pipelines import services as pipeline_services
from tests.triggers.conftest import SEEDED_PIPELINE_ID


class RecordingHooks:
    def __init__(self):
        self.created = []

    def prepare_saved_run(self, task_json):
        return "context passed through"

    def run_created(self, run, context):
        self.created.append((run.id, context))


def test_readiness_delivery_uses_the_injected_run_service(client, db_engine):
    response = client.post(
        "/api/triggers/subscriptions",
        json={
            "name": "hooked",
            "condition": {"event": "orders-ready"},
            "pipeline_task_spec_from_user_pipeline_id": SEEDED_PIPELINE_ID,
        },
    )
    assert response.status_code == 201
    hooks = RecordingHooks()
    service = pipeline_services.UserPipelineService(hooks=hooks)
    sink = start_pipeline_run.StartPipelineRunSink(
        session_factory=orm.sessionmaker(db_engine),
        pipeline_service=service,
    )
    intent = readiness.parse_readiness(
        annotations={
            readiness.ReadinessAnnotation.EVENT.value: "orders-ready",
        }
    ).intent
    outcome = sink.emit(
        intent=intent, execution_node_id="node", emission_event_id="arrival"
    )
    assert outcome.status is handler_base.OutcomeStatus.SUCCESS
    assert len(hooks.created) == 1
    assert hooks.created[0][1] == "context passed through"
