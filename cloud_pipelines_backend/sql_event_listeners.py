"""SQLAlchemy event listeners for cloud_pipelines_backend models.

This module registers global SQLAlchemy event hooks.  It must be imported at
application startup (start_local.py, orchestrator_main_oasis.py, etc.) for the
listeners to take effect.
"""

import datetime
import logging
import typing

from sqlalchemy import event as sql_event
from sqlalchemy import orm

from . import backend_types_sql
from .instrumentation import metrics

_logger = logging.getLogger(__name__)


@sql_event.listens_for(backend_types_sql.ExecutionNode.container_execution_status, "set")
def _handle_container_execution_status_set(
    execution: backend_types_sql.ExecutionNode,
    value: typing.Any,
    _old_value: typing.Any,
    _initiator: typing.Any,
) -> None:
    if value is None:
        return
    if execution.extra_data is None:
        execution.extra_data = {}
    history: list = execution.extra_data.get(
        backend_types_sql.EXECUTION_NODE_EXTRA_DATA_STATUS_HISTORY_KEY, []
    )
    entry = {
        "status": value.value,
        "first_observed_at": datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
    }
    execution.extra_data = {
        **execution.extra_data,
        backend_types_sql.EXECUTION_NODE_EXTRA_DATA_STATUS_HISTORY_KEY: history + [entry],
    }
    execution._status_changed = True


@sql_event.listens_for(orm.Session, "before_commit")
def _handle_before_commit(session: orm.Session) -> None:
    for obj in list(session.new) + list(session.dirty):
        if not isinstance(obj, backend_types_sql.ExecutionNode):
            continue
        if not obj._status_changed:
            continue
        history: list = (obj.extra_data or {}).get(
            backend_types_sql.EXECUTION_NODE_EXTRA_DATA_STATUS_HISTORY_KEY, []
        )
        if len(history) >= 2:
            prev = history[-2]
            curr = history[-1]
            prev_time = datetime.datetime.fromisoformat(prev["first_observed_at"])
            curr_time = datetime.datetime.fromisoformat(curr["first_observed_at"])
            try:
                metrics.execution_status_transition_duration.record(
                    (curr_time - prev_time).total_seconds(),
                    attributes={
                        "execution.status.from": prev["status"],
                        "execution.status.to": curr["status"],
                    },
                )
            except Exception:
                _logger.warning(
                    f"Failed to record status transition metric for execution {obj.id!r}",
                    exc_info=True,
                )
        obj._status_changed = False
