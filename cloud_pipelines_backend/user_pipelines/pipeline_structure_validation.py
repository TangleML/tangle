"""Structural validation of task-output references inside saved pipeline definitions.

The execution backend resolves ``TaskOutputArgument`` references with unguarded
dict lookups while building execution/artifact nodes, so a reference to a
missing task id or a missing output name surfaces as a bare ``KeyError`` and a
catch-all HTTP 500 (for example ``{"error_message": "'wait_for_output'"}``).

These checks run at the user-pipeline CRUD boundary -- both when a definition is
saved and before a stored snapshot is turned into a run -- so the same structural
defect fails deterministically as a 422 with actionable, sanitized context.

Only structure is inspected: task ids, output names and argument kinds. Argument
values, annotations and other potentially sensitive payloads are never included
in error messages -- see also `describe_parse_failure`, which applies the same
rule to the pydantic parse that runs just before these checks.

The checks mirror what the backend actually resolves, so a pipeline that runs
today is never refused: output names are only validated for arguments the
consumer declares as inputs, because the backend never reads the others.
Referenced task ids are validated everywhere, since the backend's toposort
already rejects those (as an unmapped `TypeError`).
"""

from collections.abc import Iterable, Iterator, Mapping
from typing import Any

import pydantic
from cloud_pipelines_backend import component_structures
from cloud_pipelines_backend.user_pipelines.errors import (
    PipelineValidationError,
)

_IS_ENABLED_REFERENCE_SITE = "is_enabled"

#: Cap on how many task/output names an error message lists, so a 422 body stays
#: useful on a small graph without becoming a large pipeline's full inventory.
_MAX_NAMES_IN_MESSAGE = 10


def describe_parse_failure(exc: Exception) -> str:
    """Summarize a `TaskSpec` parse failure without echoing the submitted values.

    `pydantic.ValidationError.__str__` embeds `input_value` for every failing
    location, which can carry argument values or annotations into a 422 body.
    Only the location, message and error type are kept.
    """
    if not isinstance(exc, pydantic.ValidationError):
        return type(exc).__name__
    errors = exc.errors()
    shown = errors[:_MAX_NAMES_IN_MESSAGE]
    rendered = "; ".join(
        f"{'.'.join(str(part) for part in error['loc']) or '<root>'}: "
        f"{error['msg']} [{error['type']}]"
        for error in shown
    )
    remaining = len(errors) - len(shown)
    if remaining:
        rendered += f"; ... (+{remaining} more)"
    return rendered or type(exc).__name__


def validate_task_output_references(
    root_pipeline_task: component_structures.TaskSpec,
) -> None:
    """Validate every ``TaskOutputArgument`` inside a pipeline task graph.

    Raises:
        PipelineValidationError: a reference points at an unknown task id or at
            an output the referenced (inlined) producer does not declare.
    """
    for graph, graph_path in _iter_graphs(task_spec=root_pipeline_task, path="root"):
        _validate_graph(graph=graph, graph_path=graph_path)


def _iter_graphs(
    *,
    task_spec: component_structures.TaskSpec,
    path: str,
) -> Iterator[tuple[component_structures.GraphSpec, str]]:
    """Yield every inlined graph implementation reachable from ``task_spec``."""
    graph = _graph_of(task_spec)
    if graph is None:
        return
    yield graph, path
    for task_id, child_task_spec in (graph.tasks or {}).items():
        yield from _iter_graphs(
            task_spec=child_task_spec,
            path=f"{path}.tasks[{task_id!r}]",
        )


def _graph_of(
    task_spec: component_structures.TaskSpec | None,
) -> component_structures.GraphSpec | None:
    component_spec = task_spec.component_ref.spec if task_spec else None
    implementation = component_spec.implementation if component_spec else None
    if isinstance(implementation, component_structures.GraphImplementation):
        return implementation.graph
    return None


def _validate_graph(*, graph: component_structures.GraphSpec, graph_path: str) -> None:
    tasks: Mapping[str, component_structures.TaskSpec] = graph.tasks or {}
    for task_id, task_spec in tasks.items():
        declared_inputs = _declared_input_names(task_spec)
        for input_name, argument in (task_spec.arguments or {}).items():
            _validate_argument(
                argument=argument,
                tasks=tasks,
                graph_path=graph_path,
                consumer=f"task {task_id!r} argument {input_name!r}",
                # The backend resolves arguments by iterating the consumer's
                # DECLARED inputs (`api_server_sql.py`, child task input loop), so an
                # argument keyed by a name the consumer does not declare is never read
                # and a dangling output name inside it is inert. Refusing it would
                # break pipelines that run green today. Its task id is still checked:
                # `_toposort_tasks` covers every argument regardless of declared
                # inputs, so that case fails today anyway (as an unmapped TypeError).
                check_output_name=(
                    declared_inputs is None or input_name in declared_inputs
                ),
            )
        _validate_argument(
            argument=task_spec.is_enabled,
            tasks=tasks,
            graph_path=graph_path,
            consumer=f"task {task_id!r} {_IS_ENABLED_REFERENCE_SITE}",
        )
    for output_name, argument in (graph.output_values or {}).items():
        _validate_argument(
            argument=argument,
            tasks=tasks,
            graph_path=graph_path,
            consumer=f"graph output {output_name!r}",
        )


def _validate_argument(
    *,
    argument: Any,
    tasks: Mapping[str, component_structures.TaskSpec],
    graph_path: str,
    consumer: str,
    check_output_name: bool = True,
) -> None:
    if not isinstance(argument, component_structures.TaskOutputArgument):
        return
    reference = argument.task_output
    producer_task_spec = tasks.get(reference.task_id)
    if producer_task_spec is None:
        raise PipelineValidationError(
            f"Pipeline graph {graph_path}: {consumer} references output "
            f"{reference.output_name!r} of task {reference.task_id!r}, "
            "but no such task exists in this graph. "
            f"Known tasks: {_format_names(names=tasks)}."
        )
    if not check_output_name:
        return
    available_outputs = _declared_output_names(producer_task_spec)
    if available_outputs is None:
        # The producer component is referenced by name/url/digest instead of being
        # inlined, so its interface is unknown here. Skip rather than guess.
        return
    if reference.output_name not in available_outputs:
        raise PipelineValidationError(
            f"Pipeline graph {graph_path}: {consumer} references output "
            f"{reference.output_name!r} of task {reference.task_id!r}, "
            "but that task produces no such output. "
            f"Available outputs: {_format_names(names=available_outputs)}."
        )


def _declared_input_names(
    task_spec: component_structures.TaskSpec,
) -> set[str] | None:
    """Input names the consumer declares, or ``None`` when its spec is not inlined.

    ``None`` means "unknown, do not narrow": callers keep validating, which
    preserves the behaviour that applies when a component interface is
    unavailable.
    """
    component_spec = task_spec.component_ref.spec
    if component_spec is None:
        return None
    return {input_spec.name for input_spec in component_spec.inputs or []}


def _declared_output_names(
    task_spec: component_structures.TaskSpec,
) -> list[str] | None:
    """Output names the producer task will expose, or ``None`` when not inlined."""
    component_spec = task_spec.component_ref.spec
    if component_spec is None:
        return None
    implementation = component_spec.implementation
    if isinstance(implementation, component_structures.GraphImplementation):
        # A graph task only exposes the outputs its graph explicitly maps.
        return list(implementation.graph.output_values or {})
    if implementation is None:
        return None
    return [output_spec.name for output_spec in component_spec.outputs or []]


def _format_names(*, names: Iterable[str], limit: int = _MAX_NAMES_IN_MESSAGE) -> str:
    """Render names for an error message, capped so a large graph is not dumped."""
    sorted_names = sorted(names)
    if not sorted_names:
        return "none"
    shown = sorted_names[:limit]
    rendered = ", ".join(repr(name) for name in shown)
    remaining = len(sorted_names) - len(shown)
    if remaining:
        rendered += f", ... (+{remaining} more)"
    return rendered
