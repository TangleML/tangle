"""SQLAlchemy event listeners for cloud_pipelines_backend models.

This module registers global SQLAlchemy event hooks.  It must be imported at
application startup (start_local.py, orchestrator_main_oasis.py, etc.) for the
listeners to take effect.  Model-specific business logic is delegated to methods
on the relevant model class rather than being inlined here.
"""

from sqlalchemy import event as sql_event
from sqlalchemy import orm

from . import backend_types_sql
from . import container_statuses


@sql_event.listens_for(backend_types_sql.ExecutionNode.container_execution_status, "set")
def _handle_container_execution_status_set(
    execution: backend_types_sql.ExecutionNode,
    value: container_statuses.ContainerExecutionStatus | None,
    _old_value: object,
    _initiator: object,
) -> None:
    if value is None:
        return
    execution.stage_status_history_entry(value=value)


@sql_event.listens_for(orm.Session, "before_flush")
def _handle_before_flush(
    session: orm.Session,
    _flush_context: object,
    _instances: object,
) -> None:
    for obj in list(session.new) + list(session.dirty):
        if isinstance(obj, backend_types_sql.ExecutionNode):
            obj.handle_before_flush()
