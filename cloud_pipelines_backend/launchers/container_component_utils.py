import dataclasses
import typing
from collections.abc import Mapping, Sequence
from typing import Union

from .. import component_structures as structures


@dataclasses.dataclass
class _ResolvedCommandLineAndPaths:
    command: Sequence[str]
    args: Sequence[str]
    input_paths: Mapping[str, str]
    output_paths: Mapping[str, str]
    inputs_consumed_by_value: Mapping[str, str]


def resolve_container_command_line(
    component_spec: structures.ComponentSpec,
    provided_input_names: set[str],
    get_input_value: typing.Callable[[str], str],
    get_input_path: typing.Callable[[str], str],
    get_output_path: typing.Callable[[str], str],
) -> _ResolvedCommandLineAndPaths:
    """Resolves the command line argument placeholders. Also produces the maps of the generated input/output paths."""

    if not isinstance(
        component_spec.implementation, structures.ContainerImplementation
    ):
        raise TypeError("Only container components have command line to resolve")

    inputs_dict = {
        input_spec.name: input_spec for input_spec in component_spec.inputs or []
    }
    container_spec = component_spec.implementation.container

    # Need to preserve the order to make the kubernetes output names deterministic
    output_paths = dict()
    input_paths = dict()
    inputs_consumed_by_value = {}

    def expand_command_part(arg) -> Union[str, list[str], None]:
        if arg is None:
            return None
        if isinstance(arg, (str, int, float, bool)):
            return str(arg)

        if isinstance(
            arg, (structures.InputValuePlaceholder, structures.InputPathPlaceholder)
        ):
            input_name = arg.input_name
            if input_name in provided_input_names:
                if isinstance(arg, structures.InputValuePlaceholder):
                    input_value = get_input_value(input_name)
                    inputs_consumed_by_value[input_name] = input_value
                    return input_value
                elif isinstance(arg, structures.InputPathPlaceholder):
                    input_path = get_input_path(input_name)
                    input_paths[input_name] = input_path
                    return input_path
                else:
                    raise TypeError(f"Impossible placeholder: {arg}")
            else:
                input_spec = inputs_dict[input_name]
                if input_spec.optional:
                    return None
                else:
                    raise ValueError(
                        f"No argument provided for required input {input_name}"
                    )

        elif isinstance(arg, structures.OutputPathPlaceholder):
            output_name = arg.output_name
            output_filename = get_output_path(output_name)
            if arg.output_name in output_paths:
                if output_paths[output_name] != output_filename:
                    raise ValueError(
                        f"Conflicting output files specified for port {output_name}: {output_paths[output_name]} and {output_filename}"
                    )
            else:
                output_paths[output_name] = output_filename

            return output_filename

        elif isinstance(arg, structures.ConcatPlaceholder):
            expanded_argument_strings = expand_argument_list(arg.concat)
            return "".join(expanded_argument_strings)

        elif isinstance(arg, structures.IfPlaceholder):
            arg = arg.if_structure
            condition_result = expand_command_part(arg.condition)
            condition_result_bool = (
                condition_result and condition_result.lower() == "true"
            )
            result_node = arg.then_value if condition_result_bool else arg.else_value
            if result_node is None:
                return []
            if isinstance(result_node, list):
                expanded_result = expand_argument_list(result_node)
            else:
                expanded_result = expand_command_part(result_node)
            return expanded_result

        elif isinstance(arg, structures.IsPresentPlaceholder):
            input_name = arg.is_present
            argument_is_present = input_name in provided_input_names
            return str(argument_is_present)
        else:
            raise TypeError(f"Unrecognized argument type: {arg}")

    def expand_argument_list(argument_list):
        expanded_list = []
        if argument_list is not None:
            for part in argument_list:
                expanded_part = expand_command_part(part)
                if expanded_part is not None:
                    if isinstance(expanded_part, list):
                        expanded_list.extend(expanded_part)
                    else:
                        expanded_list.append(str(expanded_part))
        return expanded_list

    expanded_command = expand_argument_list(container_spec.command)
    expanded_args = expand_argument_list(container_spec.args)

    return _ResolvedCommandLineAndPaths(
        command=expanded_command,
        args=expanded_args,
        input_paths=input_paths,
        output_paths=output_paths,
        inputs_consumed_by_value=inputs_consumed_by_value,
    )
