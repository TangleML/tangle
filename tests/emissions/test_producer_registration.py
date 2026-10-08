"""Generic producer codecs remain extensible without application integration imports."""

import dataclasses

import pytest
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.dispatching.handlers import base as handler_base
from cloud_pipelines_backend.emissions import db_models, intents, producer
from cloud_pipelines_backend.emissions.handlers.readiness import (
    annotations as readiness,
)

EVENT_KEY = "example.com/events/name"
PAYLOAD_KEY = "example.com/events/payload"


@dataclasses.dataclass(frozen=True, kw_only=True)
class ExampleIntent(intents.SingleStatusIntent):
    payload: str


def parse_example(*, annotations: dict) -> handler_base.ParseResult[ExampleIntent]:
    payload = annotations.get(EVENT_KEY)
    return handler_base.ParseResult(
        intent=(
            ExampleIntent(
                on_status=bts.ContainerExecutionStatus.SUCCEEDED, payload=payload
            )
            if payload is not None
            else None
        )
    )


def serialize_example(*, intent: ExampleIntent) -> list[tuple[str, str]]:
    return [(PAYLOAD_KEY, intent.payload)]


@pytest.fixture(autouse=True)
def restore_codecs():
    previous = producer._KINDS
    yield
    producer.configure_kinds(registrations=previous)


def custom_registration() -> producer.EmissionKindRegistration:
    return producer.EmissionKindRegistration(
        emission_type=db_models.EmissionType.METADATA,
        parser=parse_example,
        serializer=serialize_example,
    )


def test_extension_preserves_opaque_annotation_data_and_builtin_emissions(
    session: orm.Session,
):
    producer.configure_kinds(
        registrations=[
            producer.builtin_kinds()[0],
            custom_registration(),
            producer.builtin_kinds()[1],
        ]
    )
    node = bts.ExecutionNode(
        task_spec={
            "annotations": {
                EVENT_KEY: '{"value":"exact application payload"}',
                readiness.ReadinessAnnotation.EVENT.value: "orders-ready",
            }
        },
        container_execution_status=bts.ContainerExecutionStatus.SUCCEEDED,
    )
    session.add(node)
    session.flush()
    producer._handle_node_status_change(
        session=session,
        execution_node=node,
        node_status=bts.ContainerExecutionStatus.SUCCEEDED,
        pipeline_run_id=None,
    )
    session.commit()
    events = list(session.scalars(sqlalchemy.select(db_models.EmissionEvent)))
    assert {e.emission_type for e in events} == {"readiness", "metadata"}
    example = next(e for e in events if e.emission_type == "metadata")
    stored = session.scalar(
        sqlalchemy.select(db_models.EmissionEventAnnotation).where(
            db_models.EmissionEventAnnotation.emission_event_id == example.id,
        )
    )
    assert (stored.key, stored.value) == (
        PAYLOAD_KEY,
        '{"value":"exact application payload"}',
    )


def test_replacing_registrations_stops_removed_extensions(session: orm.Session):
    producer.configure_kinds(registrations=[custom_registration()])
    producer.configure_kinds(registrations=producer.builtin_kinds())
    node = bts.ExecutionNode(
        task_spec={"annotations": {EVENT_KEY: "removed"}},
        container_execution_status=bts.ContainerExecutionStatus.SUCCEEDED,
    )
    session.add(node)
    session.flush()
    producer._handle_node_status_change(
        session=session,
        execution_node=node,
        node_status=bts.ContainerExecutionStatus.SUCCEEDED,
        pipeline_run_id=None,
    )
    session.commit()
    assert (
        session.scalar(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                db_models.EmissionEvent
            )
        )
        == 0
    )


def test_duplicate_kind_is_refused_without_losing_previous_registration():
    previous = producer._KINDS
    registration = custom_registration()
    with pytest.raises(ValueError, match="duplicate"):
        producer.configure_kinds(registrations=[registration, registration])
    assert producer._KINDS == previous


def test_codec_keeps_the_intent_status_predicate():
    producer.configure_kinds(registrations=[custom_registration()])
    parsed = parse_example(annotations={EVENT_KEY: "payload"})
    assert (
        producer._build_rows_for_matching_intents(
            intents=[(db_models.EmissionType.METADATA, parsed.intent)],
            node_status=bts.ContainerExecutionStatus.FAILED,
        )
        == []
    )


def test_custom_ignored_parse_code_is_not_reported(session: orm.Session, caplog):
    def opt_out(*, annotations):
        return handler_base.ParseResult(
            intent=None,
            issues=[
                handler_base.ParseIssue(
                    code="example_opt_out",
                    message="not opted in",
                    dropped=True,
                )
            ],
        )

    producer.configure_kinds(
        registrations=[
            producer.EmissionKindRegistration(
                emission_type=db_models.EmissionType.METADATA,
                parser=opt_out,
                serializer=serialize_example,
                ignored_parse_codes=frozenset({"example_opt_out"}),
            )
        ]
    )
    node = bts.ExecutionNode(
        task_spec={}, container_execution_status=bts.ContainerExecutionStatus.SUCCEEDED
    )
    session.add(node)
    session.flush()
    producer._handle_node_status_change(
        session=session,
        execution_node=node,
        node_status=bts.ContainerExecutionStatus.SUCCEEDED,
        pipeline_run_id=None,
    )
    assert "example_opt_out" not in caplog.text
