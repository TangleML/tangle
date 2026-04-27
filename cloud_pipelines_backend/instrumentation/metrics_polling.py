"""Metrics polling.

Periodically queries the DB and updates ObservableGauges. Currently emits
execution status counts; add new DB-backed metrics here as needed.

Only fluctuating (non-terminal) statuses are emitted as status count gauges —
terminal statuses like SUCCEEDED and FAILED only ever climb and are not useful
as gauges.
"""

import logging
import time
import typing

import sqlalchemy as sql
from opentelemetry import metrics as otel_metrics
from sqlalchemy import orm

from .. import backend_types_sql as bts
from . import metrics as app_metrics
from .opentelemetry._internal import configuration as otel_configuration

_logger = logging.getLogger(__name__)


# All statuses minus terminal (ended) ones — these fluctuate up and down
_ACTIVE_STATUSES: frozenset[bts.ContainerExecutionStatus] = (
    frozenset(bts.ContainerExecutionStatus) - bts.CONTAINER_STATUSES_ENDED
)


def _empty_status_counts() -> dict[str, int]:
    return {s.value: 0 for s in _ACTIVE_STATUSES}


class PollingService:
    """Polls the DB periodically and emits execution status count gauges."""

    def __init__(
        self,
        *,
        session_factory: typing.Callable[[], orm.Session],
        poll_interval_seconds: float = 30.0,
    ) -> None:
        self._session_factory = session_factory
        self._poll_interval_seconds = poll_interval_seconds
        # Initialize all active statuses to 0
        self._counts: dict[str, int] = _empty_status_counts()
        # Register our observe method as the gauge callback.
        # The OTel SDK stores callbacks in _callbacks; we append after creation
        # since create_observable_gauge is called at module load time in metrics.py.
        app_metrics.execution_status_count._callbacks.append(self._observe)

    def run_loop(self) -> None:
        while True:
            try:
                self._poll()
            except Exception:
                _logger.exception("Metrics PollingService: error polling DB")
            time.sleep(self._poll_interval_seconds)

    def _poll(self) -> None:
        with self._session_factory() as session:
            rows = session.execute(
                sql.select(
                    bts.ExecutionNode.container_execution_status,
                    sql.func.count().label("count"),
                )
                .where(
                    bts.ExecutionNode.container_execution_status.in_(_ACTIVE_STATUSES)
                )
                .group_by(bts.ExecutionNode.container_execution_status)
            ).all()
        new_counts = _empty_status_counts()
        for status, count in rows:
            if status is not None:
                new_counts[status.value] = count
        # CPython: attribute assignment is atomic under the GIL; no lock needed.
        # If GIL-free Python is ever adopted, revisit this.
        self._counts = new_counts
        _logger.debug(f"Metrics PollingService: polled status counts: {new_counts}")

    def _observe(
        self, _options: otel_metrics.CallbackOptions
    ) -> typing.Iterable[otel_metrics.Observation]:
        counts = self._counts.copy()
        for status_value, count in counts.items():
            yield otel_metrics.Observation(count, {"execution.status": status_value})


def run(*, db_engine: sql.Engine) -> None:
    """Check OTel config and run the metrics polling loop (blocking).

    Logs and returns immediately if no metrics endpoint is configured.
    """
    otel_config = otel_configuration.resolve()
    if otel_config is None or otel_config.metrics is None:
        _logger.info(
            f"No OTel metrics endpoint configured"
            f" (set {otel_configuration.EnvVar.METRIC_EXPORTER_ENDPOINT})"
            f" — metrics poller not starting"
        )
        return
    session_factory = orm.sessionmaker(
        autocommit=False, autoflush=False, bind=db_engine
    )
    PollingService(session_factory=session_factory).run_loop()
