# v0.0.1 Converted KFPv1 component structures to dataclass. Classes are exactly as my KFP v1 classes. No additions, no cleanups. Constructors removed, but `__post_init___` methods remain
# v0.0.2 Removed all predicate expression classes. Moved from PredicateType to ArgumentType
# v0.0.3 Removed all trivial `_serialized_names` configs. Pydantic will be used to handle the `snake_case -> camelCase` aliases automatically.
# v0.0.4 Fixed some non-trivial `_serialized_names` configs.
# v0.0.5 Extracted long `__post_init__` methods.
# v0.0.6 Moved to Pydantic to solve the remaining `_serialized_names` configs.
# v0.0.7 Cleaned up the structures:
#   * Removed ComponentSpec.version
#   * Removed ContainerSpec.file_outputs
#   * IfPlaceholderStructure.then_value no longer accepts a single value
#   * IfPlaceholderStructure.else_value no longer accepts a single value
#   * Added ComponentReference.text: str | None
#   * Removed TaskOutputReference.task and made TaskOutputReference.task_id required
#   * Removed TaskOutputReference.type
#   * Disallowed `TaskOutputReference`` with only `tag`` set
# v0.1
#   * ? PrimitiveTypes now only consists of `str`
#   --- * GraphSpec.output_values only supports TaskOutputArgument now.

__all__ = [
    "InputSpec",
    "OutputSpec",
    "InputValuePlaceholder",
    "InputPathPlaceholder",
    "OutputPathPlaceholder",
    "ConcatPlaceholder",
    "IsPresentPlaceholder",
    "IfPlaceholderStructure",
    "IfPlaceholder",
    "ContainerSpec",
    "ContainerImplementation",
    "MetadataSpec",
    "ComponentSpec",
    "ComponentReference",
    "GraphInputReference",
    "GraphInputArgument",
    "TaskOutputReference",
    "TaskOutputArgument",
    "RetryStrategySpec",
    "CachingStrategySpec",
    "ExecutionOptionsSpec",
    "TaskSpec",
    "GraphSpec",
    "GraphImplementation",
    "PipelineRunSpec",
]

import dataclasses
from collections.abc import Mapping
from typing import Any, Union

import pydantic
import pydantic.alias_generators

# PrimitiveTypes = Union[str, int, float, bool]
PrimitiveTypes = str


TypeSpecType = str | dict | list


# class _BaseModel1(pydantic.BaseModel):
#     model_config = pydantic.ConfigDict(
#         alias_generator=pydantic.alias_generators.to_camel,
#     )

#     def to_json_dict(self):
#         self.model_dump(by_alias=True)


_default_pydantic_config = pydantic.ConfigDict(
    alias_generator=pydantic.alias_generators.to_camel,
    validate_assignment=True,
)


class _BaseModel:
    # Cannot refer to the derived class and referring to the base class does not work
    # _serialized_names = {}
    # __pydantic_config__ = pydantic.ConfigDict(
    #     alias_generator=lambda s: _BaseModel._serialized_names.get(s) or pydantic.alias_generators.to_camel(s),
    #     validate_assignment=True,
    # )
    __pydantic_config__ = _default_pydantic_config

    def to_json_dict(self):
        return pydantic.TypeAdapter(type(self)).dump_python(
            self,
            mode="json",
            by_alias=True,
            exclude_defaults=True,
        )

    @classmethod
    def from_json_dict(cls, d: dict):
        # return pydantic.TypeAdapter(cls).validate_python(d, strict=True)
        return pydantic.TypeAdapter(cls).validate_python(d)


@dataclasses.dataclass
class InputSpec(_BaseModel):
    """Describes the component input specification"""

    name: str
    type: TypeSpecType | None = None
    description: str | None = None
    default: PrimitiveTypes | None = None
    optional: bool | None = False
    annotations: dict[str, Any] | None = None


@dataclasses.dataclass
class OutputSpec(_BaseModel):
    """Describes the component output specification"""

    name: str
    type: TypeSpecType | None = None
    description: str | None = None
    annotations: dict[str, Any] | None = None


@dataclasses.dataclass
class InputValuePlaceholder(_BaseModel):  # Non-standard attr names
    """Represents the command-line argument placeholder that will be replaced at run-time by the input argument value."""

    _serialized_names = {
        "input_name": "inputValue",
    }
    __pydantic_config__ = _default_pydantic_config | pydantic.ConfigDict(
        alias_generator=lambda s: (
            InputValuePlaceholder._serialized_names.get(s)
            or pydantic.alias_generators.to_camel(s)
        ),
    )

    input_name: str


@dataclasses.dataclass
class InputPathPlaceholder(_BaseModel):  # Non-standard attr names
    """Represents the command-line argument placeholder that will be replaced at run-time by a local file path pointing to a file containing the input argument value."""

    _serialized_names = {
        "input_name": "inputPath",
    }
    __pydantic_config__ = _default_pydantic_config | pydantic.ConfigDict(
        alias_generator=lambda s: (
            InputPathPlaceholder._serialized_names.get(s)
            or pydantic.alias_generators.to_camel(s)
        ),
    )
    input_name: str


@dataclasses.dataclass
class OutputPathPlaceholder(_BaseModel):  # Non-standard attr names
    """Represents the command-line argument placeholder that will be replaced at run-time by a local file path pointing to a file where the program should write its output data."""

    _serialized_names = {
        "output_name": "outputPath",
    }
    __pydantic_config__ = _default_pydantic_config | pydantic.ConfigDict(
        alias_generator=lambda s: (
            OutputPathPlaceholder._serialized_names.get(s)
            or pydantic.alias_generators.to_camel(s)
        ),
    )
    output_name: str


CommandlineArgumentType = Union[
    str,
    InputValuePlaceholder,
    InputPathPlaceholder,
    OutputPathPlaceholder,
    "ConcatPlaceholder",
    "IfPlaceholder",
]


@dataclasses.dataclass
class ConcatPlaceholder(_BaseModel):
    """Represents the command-line argument placeholder that will be replaced at run-time by the concatenated values of its items."""

    concat: list[CommandlineArgumentType]


@dataclasses.dataclass
class IsPresentPlaceholder(_BaseModel):
    """Represents the command-line argument placeholder that will be replaced at run-time by a boolean value specifying whether the caller has passed an argument for the specified optional input."""

    is_present: str  # Input name that must be present


IfConditionArgumentType = bool | str | IsPresentPlaceholder | InputValuePlaceholder


@dataclasses.dataclass
class IfPlaceholderStructure(_BaseModel):  # Non-standard attr names
    """Used in by the IfPlaceholder - the command-line argument placeholder that will be replaced at run-time by the expanded value of either "then_value" or "else_value" depending on the submission-time resolved value of the "cond" predicate."""

    _serialized_names = {
        "condition": "cond",
        "then_value": "then",
        "else_value": "else",
    }
    __pydantic_config__ = _default_pydantic_config | pydantic.ConfigDict(
        alias_generator=lambda s: (
            IfPlaceholderStructure._serialized_names.get(s)
            or pydantic.alias_generators.to_camel(s)
        ),
    )

    condition: IfConditionArgumentType
    then_value: list[CommandlineArgumentType]
    else_value: list[CommandlineArgumentType] | None = None


@dataclasses.dataclass
class IfPlaceholder(_BaseModel):  # Non-standard attr names
    """Represents the command-line argument placeholder that will be replaced at run-time by the expanded value of either "then_value" or "else_value" depending on the submission-time resolved value of the "cond" predicate."""

    _serialized_names = {
        "if_structure": "if",
    }
    __pydantic_config__ = _default_pydantic_config | pydantic.ConfigDict(
        alias_generator=lambda s: (
            IfPlaceholder._serialized_names.get(s)
            or pydantic.alias_generators.to_camel(s)
        ),
    )

    if_structure: IfPlaceholderStructure


@dataclasses.dataclass
class ContainerSpec(_BaseModel):
    """Describes the container component implementation."""

    image: str
    command: list[CommandlineArgumentType] | None = None
    args: list[CommandlineArgumentType] | None = None
    env: Mapping[str, str] | None = None


@dataclasses.dataclass
class ContainerImplementation(_BaseModel):
    """Represents the container component implementation."""

    container: ContainerSpec


ImplementationType = Union[ContainerImplementation, "GraphImplementation"]


@dataclasses.dataclass
class MetadataSpec(_BaseModel):
    annotations: dict[str, str] | None = None
    labels: dict[str, str] | None = None


@dataclasses.dataclass
class ComponentSpec(_BaseModel):
    """Component specification. Describes the metadata (name, description, annotations and labels), the interface (inputs and outputs) and the implementation of the component."""

    name: str | None = None  # ? Move to metadata?
    description: str | None = None  # ? Move to metadata?
    metadata: MetadataSpec | None = None
    inputs: list[InputSpec] | None = None
    outputs: list[OutputSpec] | None = None
    implementation: ImplementationType | None = None

    def __post_init__(self):
        # _component_spec_post_init(self)
        pass


@dataclasses.dataclass
class ComponentReference(_BaseModel):
    """Component reference. Contains information that can be used to locate and load a component by name, digest or URL"""

    name: str | None = None
    digest: str | None = None
    tag: str | None = None
    url: str | None = None
    spec: ComponentSpec | None = None
    text: str | None = None

    def __post_init__(self) -> None:
        if not any([self.name, self.digest, self.url, self.spec, self.text]):
            raise TypeError("Need at least one argument.")


@dataclasses.dataclass
class GraphInputReference(_BaseModel):
    """References the input of the graph (the scope is a single graph)."""

    input_name: str
    type: TypeSpecType | None = None  # Can be used to override the reference data type


@dataclasses.dataclass
class GraphInputArgument(_BaseModel):
    """Represents the component argument value that comes from the graph component input."""

    graph_input: GraphInputReference


@dataclasses.dataclass
class TaskOutputReference(_BaseModel):
    """References the output of some task (the scope is a single graph)."""

    output_name: str
    task_id: str  # Used for linking to the upstream task in serialized component file.
    # type: Optional[TypeSpecType] = None    # Can be used to override the reference data type


@dataclasses.dataclass
class TaskOutputArgument(_BaseModel):  # Has additional constructor for convenience
    """Represents the component argument value that comes from the output of another task."""

    task_output: TaskOutputReference


DynamicDataReference = str | dict[str, Any]


@dataclasses.dataclass
class DynamicDataArgument(_BaseModel):
    """Argument that references data that's dynamically produced by the execution system at runtime.

    Examples of dynamic data:
    * Secret value
    * Container execution ID
    * Pipeline run ID
    * Loop index/item
    """

    dynamic_data: DynamicDataReference


ArgumentType = (
    PrimitiveTypes | GraphInputArgument | TaskOutputArgument | DynamicDataArgument
)


@dataclasses.dataclass
class RetryStrategySpec(_BaseModel):
    max_retries: int


@dataclasses.dataclass
class CachingStrategySpec(_BaseModel):
    max_cache_staleness: str | None = None  # RFC3339 compliant duration: P30DT1H22M3S


@dataclasses.dataclass
class ExecutionOptionsSpec(_BaseModel):
    retry_strategy: RetryStrategySpec | None = None
    caching_strategy: CachingStrategySpec | None = None


@dataclasses.dataclass
class TaskSpec(_BaseModel):
    """Task specification. Task is a "configured" component - a component supplied with arguments and other applied configuration changes."""

    component_ref: ComponentReference
    arguments: Mapping[str, ArgumentType] | None = None
    is_enabled: ArgumentType | None = None
    execution_options: ExecutionOptionsSpec | None = None
    annotations: dict[str, Any] | None = None


@dataclasses.dataclass
class GraphSpec(_BaseModel):
    """Describes the graph component implementation. It represents a graph of component tasks connected to the upstream sources of data using the argument specifications. It also describes the sources of graph output values."""

    tasks: Mapping[str, TaskSpec]
    output_values: Mapping[str, ArgumentType] | None = None

    def __post_init__(self):
        # _graph_spec_post_init(self)
        pass


@dataclasses.dataclass
class GraphImplementation(_BaseModel):
    """Represents the graph component implementation."""

    graph: GraphSpec


@dataclasses.dataclass
class PipelineRunSpec(_BaseModel):
    """The object that can be sent to the backend to start a new Run."""

    root_task: TaskSpec
    # on_exit_task: Optional[TaskSpec] = None


# def _component_spec_post_init(self: ComponentSpec):
#     #Checking input names for uniqueness
#     self._inputs_dict = {}
#     if self.inputs:
#         for input in self.inputs:
#             if input.name in self._inputs_dict:
#                 raise ValueError("Non-unique input name \"{}\"".format(input.name))
#             self._inputs_dict[input.name] = input

#     #Checking output names for uniqueness
#     self._outputs_dict = {}
#     if self.outputs:
#         for output in self.outputs:
#             if output.name in self._outputs_dict:
#                 raise ValueError("Non-unique output name \"{}\"".format(output.name))
#             self._outputs_dict[output.name] = output

#     if isinstance(self.implementation, ContainerImplementation):
#         container = self.implementation.container

#         if container.file_outputs:
#             for output_name, path in container.file_outputs.items():
#                 if output_name not in self._outputs_dict:
#                     raise TypeError("Unconfigurable output entry \"{}\" references non-existing output.".format({output_name: path}))

#         def verify_arg(arg):
#             if arg is None:
#                 pass
#             elif isinstance(arg, (str, int, float, bool)):
#                 pass
#             elif isinstance(arg, list):
#                 for arg2 in arg:
#                     verify_arg(arg2)
#             elif isinstance(arg, (InputValuePlaceholder, InputPathPlaceholder)):
#                 if arg.input_name not in self._inputs_dict:
#                     raise TypeError("Argument \"{}\" references non-existing input.".format(arg))
#             elif isinstance(arg, IsPresentPlaceholder):
#                 if arg.is_present not in self._inputs_dict:
#                     raise TypeError("Argument \"{}\" references non-existing input.".format(arg))
#             elif isinstance(arg, OutputPathPlaceholder):
#                 if arg.output_name not in self._outputs_dict:
#                     raise TypeError("Argument \"{}\" references non-existing output.".format(arg))
#             elif isinstance(arg, ConcatPlaceholder):
#                 for arg2 in arg.concat:
#                     verify_arg(arg2)
#             elif isinstance(arg, IfPlaceholder):
#                 verify_arg(arg.if_structure.condition)
#                 verify_arg(arg.if_structure.then_value)
#                 verify_arg(arg.if_structure.else_value)
#             else:
#                 raise TypeError("Unexpected argument \"{}\"".format(arg))

#         verify_arg(container.command)
#         verify_arg(container.args)

#     if isinstance(self.implementation, GraphImplementation):
#         graph = self.implementation.graph

#         if graph.output_values is not None:
#             for output_name, argument in graph.output_values.items():
#                 if output_name not in self._outputs_dict:
#                     raise TypeError("Graph output argument entry \"{}\" references non-existing output.".format({output_name: argument}))

#         if graph.tasks is not None:
#             for task in graph.tasks.values():
#                 if task.arguments is not None:
#                     for argument in task.arguments.values():
#                         if isinstance(argument, GraphInputArgument) and argument.graph_input.input_name not in self._inputs_dict:
#                             raise TypeError("Argument \"{}\" references non-existing input.".format(argument))

# def _graph_spec_post_init(graph_spec: GraphSpec):
#     #Checking task output references and preparing the dependency table
#     task_dependencies = {}
#     for task_id, task in graph_spec.tasks.items():
#         dependencies = set()
#         task_dependencies[task_id] = dependencies
#         if task.arguments is not None:
#             for argument in task.arguments.values():
#                 if isinstance(argument, TaskOutputArgument):
#                     dependencies.add(argument.task_output.task_id)
#                     if argument.task_output.task_id not in graph_spec.tasks:
#                         raise TypeError("Argument \"{}\" references non-existing task.".format(argument))

#     #Topologically sorting tasks to detect cycles
#     task_dependents = {k: set() for k in task_dependencies.keys()}
#     for task_id, dependencies in task_dependencies.items():
#         for dependency in dependencies:
#             task_dependents[dependency].add(task_id)
#     task_number_of_remaining_dependencies = {k: len(v) for k, v in task_dependencies.items()}
#     sorted_tasks = OrderedDict()
#     def process_task(task_id):
#         if task_number_of_remaining_dependencies[task_id] == 0 and task_id not in sorted_tasks:
#             sorted_tasks[task_id] = graph_spec.tasks[task_id]
#             for dependent_task in task_dependents[task_id]:
#                 task_number_of_remaining_dependencies[dependent_task] = task_number_of_remaining_dependencies[dependent_task] - 1
#                 process_task(dependent_task)
#     for task_id in task_dependencies.keys():
#         process_task(task_id)
#     if len(sorted_tasks) != len(task_dependencies):
#         tasks_with_unsatisfied_dependencies = {k: v for k, v in task_number_of_remaining_dependencies.items() if v > 0}
#         task_wth_minimal_number_of_unsatisfied_dependencies = min(tasks_with_unsatisfied_dependencies.keys(), key=lambda task_id: tasks_with_unsatisfied_dependencies[task_id])
#         raise ValueError("Task \"{}\" has cyclical dependency.".format(task_wth_minimal_number_of_unsatisfied_dependencies))

#     graph_spec._toposorted_tasks = sorted_tasks
