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
from collections import OrderedDict

from typing import Any, Dict, List, Mapping, Optional, Sequence, Union

import pydantic
import pydantic.alias_generators
from pydantic.dataclasses import dataclass as pydantic_dataclasses


PrimitiveTypes = str


TypeSpecType = Union[str, Dict, List]


_default_pydantic_config = pydantic.ConfigDict(
    alias_generator=pydantic.alias_generators.to_camel,
    validate_assignment=True,
)


class _BaseModel:
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
        return pydantic.TypeAdapter(cls).validate_python(d)


@dataclasses.dataclass
class InputSpec(_BaseModel):
    """Describes the component input specification"""

    name: str
    type: Optional[TypeSpecType] = None
    description: Optional[str] = None
    default: Optional[PrimitiveTypes] = None
    optional: Optional[bool] = False
    annotations: Optional[Dict[str, Any]] = None


@dataclasses.dataclass
class OutputSpec(_BaseModel):
    """Describes the component output specification"""

    name: str
    type: Optional[TypeSpecType] = None
    description: Optional[str] = None
    annotations: Optional[Dict[str, Any]] = None


@dataclasses.dataclass
class InputValuePlaceholder(_BaseModel):  # Non-standard attr names
    """Represents the command-line argument placeholder that will be replaced at run-time by the input argument value."""

    _serialized_names = {
        "input_name": "inputValue",
    }
    __pydantic_config__ = _default_pydantic_config | pydantic.ConfigDict(
        alias_generator=lambda s: InputValuePlaceholder._serialized_names.get(s)
        or pydantic.alias_generators.to_camel(s),
    )

    input_name: str


@dataclasses.dataclass
class InputPathPlaceholder(_BaseModel):  # Non-standard attr names
    """Represents the command-line argument placeholder that will be replaced at run-time by a local file path pointing to a file containing the input argument value."""

    _serialized_names = {
        "input_name": "inputPath",
    }
    __pydantic_config__ = _default_pydantic_config | pydantic.ConfigDict(
        alias_generator=lambda s: InputPathPlaceholder._serialized_names.get(s)
        or pydantic.alias_generators.to_camel(s),
    )
    input_name: str


@dataclasses.dataclass
class OutputPathPlaceholder(_BaseModel):  # Non-standard attr names
    """Represents the command-line argument placeholder that will be replaced at run-time by a local file path pointing to a file where the program should write its output data."""

    _serialized_names = {
        "output_name": "outputPath",
    }
    __pydantic_config__ = _default_pydantic_config | pydantic.ConfigDict(
        alias_generator=lambda s: OutputPathPlaceholder._serialized_names.get(s)
        or pydantic.alias_generators.to_camel(s),
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

    concat: List[CommandlineArgumentType]


@dataclasses.dataclass
class IsPresentPlaceholder(_BaseModel):
    """Represents the command-line argument placeholder that will be replaced at run-time by a boolean value specifying whether the caller has passed an argument for the specified optional input."""

    is_present: str  # Input name that must be present


IfConditionArgumentType = Union[bool, str, IsPresentPlaceholder, InputValuePlaceholder]


@dataclasses.dataclass
class IfPlaceholderStructure(_BaseModel):  # Non-standard attr names
    """Used in by the IfPlaceholder - the command-line argument placeholder that will be replaced at run-time by the expanded value of either "then_value" or "else_value" depending on the submission-time resolved value of the "cond" predicate."""

    _serialized_names = {
        "condition": "cond",
        "then_value": "then",
        "else_value": "else",
    }
    __pydantic_config__ = _default_pydantic_config | pydantic.ConfigDict(
        alias_generator=lambda s: IfPlaceholderStructure._serialized_names.get(s)
        or pydantic.alias_generators.to_camel(s),
    )

    condition: IfConditionArgumentType
    then_value: List[CommandlineArgumentType]
    else_value: Optional[List[CommandlineArgumentType]] = None


@dataclasses.dataclass
class IfPlaceholder(_BaseModel):  # Non-standard attr names
    """Represents the command-line argument placeholder that will be replaced at run-time by the expanded value of either "then_value" or "else_value" depending on the submission-time resolved value of the "cond" predicate."""

    _serialized_names = {
        "if_structure": "if",
    }
    __pydantic_config__ = _default_pydantic_config | pydantic.ConfigDict(
        alias_generator=lambda s: IfPlaceholder._serialized_names.get(s)
        or pydantic.alias_generators.to_camel(s),
    )

    if_structure: IfPlaceholderStructure


@dataclasses.dataclass
class ContainerSpec(_BaseModel):
    """Describes the container component implementation."""

    image: str
    command: Optional[List[CommandlineArgumentType]] = None
    args: Optional[List[CommandlineArgumentType]] = None
    env: Optional[Mapping[str, str]] = None


@dataclasses.dataclass
class ContainerImplementation(_BaseModel):
    """Represents the container component implementation."""

    container: ContainerSpec


ImplementationType = Union[ContainerImplementation, "GraphImplementation"]


@dataclasses.dataclass
class MetadataSpec(_BaseModel):
    annotations: Optional[Dict[str, str]] = None
    labels: Optional[Dict[str, str]] = None


@dataclasses.dataclass
class ComponentSpec(_BaseModel):
    """Component specification. Describes the metadata (name, description, annotations and labels), the interface (inputs and outputs) and the implementation of the component."""

    name: Optional[str] = None  # ? Move to metadata?
    description: Optional[str] = None  # ? Move to metadata?
    metadata: Optional[MetadataSpec] = None
    inputs: Optional[List[InputSpec]] = None
    outputs: Optional[List[OutputSpec]] = None
    implementation: Optional[ImplementationType] = None

    def __post_init__(self):
        # _component_spec_post_init(self)
        pass


@dataclasses.dataclass
class ComponentReference(_BaseModel):
    """Component reference. Contains information that can be used to locate and load a component by name, digest or URL"""

    name: Optional[str] = None
    digest: Optional[str] = None
    tag: Optional[str] = None
    url: Optional[str] = None
    spec: Optional[ComponentSpec] = None
    text: Optional[str] = None

    def __post_init__(self) -> None:
        if not any([self.name, self.digest, self.url, self.spec, self.text]):
            raise TypeError("Need at least one argument.")


@dataclasses.dataclass
class GraphInputReference(_BaseModel):
    """References the input of the graph (the scope is a single graph)."""

    input_name: str
    type: Optional[TypeSpecType] = (
        None  # Can be used to override the reference data type
    )


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


ArgumentType = Union[
    PrimitiveTypes, GraphInputArgument, TaskOutputArgument, DynamicDataArgument
]


@dataclasses.dataclass
class RetryStrategySpec(_BaseModel):

    max_retries: int


@dataclasses.dataclass
class CachingStrategySpec(_BaseModel):

    max_cache_staleness: Optional[str] = (
        None  # RFC3339 compliant duration: P30DT1H22M3S
    )


@dataclasses.dataclass
class ExecutionOptionsSpec(_BaseModel):

    retry_strategy: Optional[RetryStrategySpec] = None
    caching_strategy: Optional[CachingStrategySpec] = None


@dataclasses.dataclass
class TaskSpec(_BaseModel):
    """Task specification. Task is a "configured" component - a component supplied with arguments and other applied configuration changes."""

    component_ref: ComponentReference
    arguments: Optional[Mapping[str, ArgumentType]] = None
    is_enabled: Optional[ArgumentType] = None
    execution_options: Optional[ExecutionOptionsSpec] = None
    annotations: Optional[Dict[str, Any]] = None


@dataclasses.dataclass
class GraphSpec(_BaseModel):
    """Describes the graph component implementation. It represents a graph of component tasks connected to the upstream sources of data using the argument specifications. It also describes the sources of graph output values."""

    tasks: Mapping[str, TaskSpec]
    output_values: Optional[Mapping[str, ArgumentType]] = None

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
