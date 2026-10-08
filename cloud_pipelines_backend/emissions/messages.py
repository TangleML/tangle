"""The emission event message: the DB-detached snapshot a handler receives."""

import dataclasses

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.dispatching.handlers import base as handler_base


@dataclasses.dataclass(frozen=True, kw_only=True)
class EmissionEventMessage(handler_base.DispatchableMessage):
    """An immutable, DB-detached snapshot of one emission event handed to a handler.

    The consumer builds it from an emission_event row plus that row's joined annotation
    rows, so a handler never touches the database or the ORM. It satisfies the generic
    dispatching contract through the `routing_key` and `message_id` aliases below.
    """

    # The emission_event row id; used to correlate logs and outcomes back to the row.
    emission_event_id: str
    # The routing key the dispatcher looks up to find this event's handler.
    emission_type: str
    # The node whose status change produced this emission.
    execution_node_id: str
    # The node's container execution, or None for a terminal status with no container.
    container_execution_id: str | None
    # The pipeline run the node belongs to.
    pipeline_run_id: str | None
    # The node's terminal status at emission time (any container status).
    container_execution_status: bts.ContainerExecutionStatus
    # The event's annotations, joined from the annotation rows and keyed as the node
    # declared them.
    annotations: dict[str, str]

    @property
    def message_id(self) -> str:
        """The generic log-correlation id the dispatcher reads.

        Returns a labelled string (e.g. `emission_event_id=42`) rather than the bare id so the
        dispatcher's generic log lines name what the id is without knowing this concrete type.
        """
        return f"emission_event_id={self.emission_event_id}"

    @property
    def routing_key(self) -> str:
        """The generic routing key the dispatcher routes on."""
        return self.emission_type
