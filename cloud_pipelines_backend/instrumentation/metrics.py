"""
Application-level meters and instruments.

Meters should be named after the software component they represent.
They should not change over time (avoid using __name__).

Instruments should be named after the metric they represent.
First and foremost, they should follow the semantic conventions
(https://opentelemetry.io/docs/specs/semconv/general/metrics/)
of OTel if the metric is common (e.g. http.server.duration).

For custom, application-specific measurements, choose a name after
what is being measured, and not after the software component that
measures it.

Good example:
- Meter: tangle.orchestrator
- Instrument: execution.system_errors

Bad example:
- Meter: tangle.orchestrator
- Instrument: orchestrator_execution_system_errors
"""

import datetime
import enum
import logging

from opentelemetry import metrics as otel_metrics
from sqlalchemy import event as sql_event
from sqlalchemy import orm

from .. import backend_types_sql
from . import execution_annotations
from . import execution_tracing

_logger = logging.getLogger(__name__)


class MetricUnit(str, enum.Enum):
    """UCUM-style unit strings accepted by the OTel SDK."""

    SECONDS = "s"
    ERRORS = "{error}"
    EXECUTIONS = "{execution}"


# ---------------------------------------------------------------------------
# tangle.orchestrator
# ---------------------------------------------------------------------------
orchestrator_meter = otel_metrics.get_meter("tangle.orchestrator")

execution_system_errors = orchestrator_meter.create_counter(
    name="execution.system_errors",
    description="Number of execution nodes that ended in SYSTEM_ERROR status",
    unit=MetricUnit.ERRORS,
)

container_execution_missing_workloads = orchestrator_meter.create_counter(
    name="container_execution.missing_workloads",
    description=(
        "Number of container executions that failed because their workload"
        " disappeared before it completed (e.g. the pod was deleted)"
    ),
    unit=MetricUnit.ERRORS,
)

execution_status_transition_duration = orchestrator_meter.create_histogram(
    name="execution.status_transition.duration",
    description="Duration an execution spent in a status before transitioning to the next status",
    unit=MetricUnit.SECONDS,
)

execution_status_count = orchestrator_meter.create_observable_gauge(
    name="execution.status.count",
    callbacks=[],
    description="Number of execution nodes in each active (non-terminal) status",
    unit=MetricUnit.EXECUTIONS,
)


# ---------------------------------------------------------------------------
# SQLAlchemy event listeners
# ---------------------------------------------------------------------------

_HISTORY_KEY = backend_types_sql.EXECUTION_NODE_EXTRA_DATA_STATUS_HISTORY_KEY

_RUNNING = backend_types_sql.ContainerExecutionStatus.RUNNING.value

# Every label an execution contributes has one of these when the task said nothing, so a
# query that filters on a label never silently drops the executions that lack it.
_UNSET = "none"


def _run_attempt(*, history: list[dict[str, str]]) -> str:
    """Whether the execution is waiting to START or waiting to RESUME.

    Both waits are recorded as PENDING->RUNNING, so without this one preempted task's
    resumption is indistinguishable from a slow queue -- and p99 over a 5 minute window
    is the single slowest sample in it.

        QUEUED -> PENDING -> RUNNING                  "first"   never had a machine
        ... RUNNING -> PENDING -> RUNNING             "repeat"  had one, lost it
        QUEUED -> PENDING                             "none"    not a start at all

    "first" is how long the orchestrator queued the work. "repeat" is how long a
    tenant's cluster took to give the machine back, which is not our queue.

    "none" rather than omitting the attribute, because an attribute that disappears
    forms a second series that no filter on the first one covers.

    "Attempt" counts times the execution held a machine, not times it was retried after
    an error. Read off the history list the session already holds; no query.
    """
    if not history or history[-1]["status"] != _RUNNING:
        return "none"
    return "repeat" if any(e["status"] == _RUNNING for e in history[:-1]) else "first"


def _cloud_provider(*, execution: backend_types_sql.ExecutionNode) -> str:
    """Whose machines ran the work.

        reads: cloud-pipelines.net/orchestration/cloud_provider

        annotation          target map        label
        "lt3"            →  google / lt3   →  "google"
        "gke-lt3"        →  google / lt3   →  "google"   (a)
        "h200"           →  nebius / h200  →  "nebius"
        "davies"         →  davies / b300  →  "davies"   (b)
        "retired-tpu"    →  not in the map →  "none"     (c)
        no annotation    →  --             →  "none"     (c)

        (a) an alias for the row above -- one series, not two
        (b) davies declares no clusters, so the task names one in a second
            annotation the deployment registered
        (c) labelled rather than omitted: a vanishing attribute forms a second
            series that no filter on the first one covers

    The map decides, not the annotation. The config section is `gke` and the label
    is `google`, because it answers whose machines, not which config stanza.
    """
    resolved = execution_annotations.target(execution=execution)
    return _UNSET if resolved is None else resolved.provider


def _namespace(*, execution: backend_types_sql.ExecutionNode) -> str:
    """Which Kubernetes namespace the work ran in.

        reads: cloud-pipelines.net/launchers/google/service_account
               + cloud-pipelines.net/pipeline_runs/created_by
               falling back to the namespace registered with the target

        a task with its own service account → "sidekick-oasis-runner-at-s5y-…"  (a)
        no service account, target "lt3"    → "kueue-jobs-staging"
        no service account, target "eo9"    → "tangle-jobs-production"
        an unrouted or missing target       → "none"

        (a) ~39 such namespaces exist and the set grows with onboarding: each is a
            function of a submitter's service account, so none is declared anywhere
            in this repository or the deployment's config

    Two origins, and only the first is visible in the annotations. A task carrying a
    service account gets a namespace of its own; everything else lands in its
    cluster's, which the deployment registers with the target.
    """
    own = execution_annotations.namespace(execution=execution)
    if own is not None:
        return own
    resolved = execution_annotations.target(execution=execution)
    return _UNSET if resolved is None else resolved.namespace


def _cluster(*, execution: backend_types_sql.ExecutionNode) -> str:
    """Which cluster inside that platform.

        reads: cloud-pipelines.net/orchestration/cloud_provider

        annotation       target map             label
        "lt3"         →  google / us-ce1-lt3  → "us-ce1-lt3"
        "eo9"         →  google / us-ce1-eo9  → "us-ce1-eo9"
        "h200"        →  nebius / b300-cluster→ "b300-cluster"
        "davies"      →  davies / named by    → whatever the task named   (a)
                                   the task
        "retired-tpu" →  not in the map       → "none"
        no annotation →  --                   → "none"

        (a) davies declares no clusters of its own, so the value is whatever the
            submitter typed, and "none" when the task named nothing

    The deployment registers these already slugified: config labels are display
    strings like "B300 cluster", which is legal in a label value and unpleasant in
    a query. Resolved through the map, never read off the annotation -- the
    annotation names a target, which is not the same thing as a cluster.
    """
    resolved = execution_annotations.target(execution=execution)
    return _UNSET if resolved is None else resolved.cluster


def _accelerator(*, execution: backend_types_sql.ExecutionNode) -> tuple[str, str]:
    """Which accelerator the task asked for, and how many.

        reads: cloud-pipelines.net/launchers/generic/resources.accelerators

        annotation                          type                   count
        '{"NVIDIA-B300-SXM6-PC":"8"}'    →  "NVIDIA-B300-SXM6-PC"  "8"
        '{"NVIDIA-B300-SXM6-PC": "8"}'   →  "NVIDIA-B300-SXM6-PC"  "8"    (a)
        '{"B300-SXM6-PC":"1"}'           →  "B300-SXM6-PC"         "1"    (b)
        '{"NVIDIA-B300-SXM6-PC":"0"}'    →  "NVIDIA-B300-SXM6-PC"  "0"
        absent  or  '{}'                 →  "none"                 "0"
        '{"A":"1","B":"1"}'              →  "unparseable"          "0"    (c)
        'not json'                       →  "unparseable"          "0"    (c)

        (a) one space from the row above -- one series, not two
        (b) the capacity API's spelling of the same GPU; both kept as written
        (c) labelled, not dropped, so a bad annotation shows as a spike rather
            than as missing traffic

    Parsing is what collapses the spacings: labelling the raw string would split one
    model across several series.
    """
    try:
        requested = execution_annotations.accelerators(execution=execution)
    except ValueError:
        return "unparseable", "0"
    if requested is None:
        return _UNSET, "0"
    model, count = requested
    return model, str(count)


@sql_event.listens_for(orm.Session, "before_commit")
def _handle_before_commit(session: orm.Session) -> None:
    for obj in list(session.new) + list(session.dirty):
        if not isinstance(obj, backend_types_sql.ExecutionNode):
            continue
        if not obj._status_changed:
            continue
        history: list = (obj.extra_data or {}).get(_HISTORY_KEY, [])
        if len(history) >= 2:
            prev = history[-2]
            curr = history[-1]
            prev_time = datetime.datetime.fromisoformat(prev["first_observed_at"])
            curr_time = datetime.datetime.fromisoformat(curr["first_observed_at"])
            accelerator_type, accelerator_count = _accelerator(execution=obj)
            cluster = _cluster(execution=obj)
            namespace = _namespace(execution=obj)
            try:
                execution_status_transition_duration.record(
                    (curr_time - prev_time).total_seconds(),
                    attributes={
                        "execution.status.from": prev["status"],
                        "execution.status.to": curr["status"],
                        "execution.run_attempt": _run_attempt(history=history),
                        "execution.cloud_provider": _cloud_provider(execution=obj),
                        "execution.accelerator_type": accelerator_type,
                        "execution.accelerator_count": accelerator_count,
                        "execution.cluster": cluster,
                        "k8s.namespace.name": namespace,
                    },
                )
            except Exception:
                _logger.warning(
                    f"Failed to record status transition metric for execution {obj.id!r}",
                    exc_info=True,
                )
        obj._status_changed = False
        execution_tracing.emit_execution_trace(execution=obj)
