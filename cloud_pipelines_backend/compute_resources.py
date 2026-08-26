import dataclasses
import json
from collections import abc
from typing import Literal

from . import component_structures as structures
from . import errors
from .launchers import kubernetes_launchers


@dataclasses.dataclass(frozen=True, kw_only=True)
class GpuResource:
    id: str
    display_name: str
    status: Literal["supported", "deprecated"]


@dataclasses.dataclass(frozen=True, kw_only=True)
class GetPipelineRunCapabilitiesResponse:
    gpus: tuple[GpuResource, ...]


GPU_RESOURCES = (
    GpuResource(
        id="NVIDIA-B300",
        display_name="NVIDIA B300",
        status="supported",
    ),
    GpuResource(
        id="NVIDIA-H200",
        display_name="NVIDIA H200",
        status="deprecated",
    ),
)

_VALID_GPU_IDS = frozenset(gpu.id for gpu in GPU_RESOURCES)
_GPU_ANNOTATION_KEY = kubernetes_launchers.RESOURCES_ACCELERATORS_ANNOTATION_KEY


def get_pipeline_run_capabilities() -> GetPipelineRunCapabilitiesResponse:
    return GetPipelineRunCapabilitiesResponse(gpus=GPU_RESOURCES)


def validate_pipeline_gpu_resources(root_task: structures.TaskSpec) -> None:
    unsupported_gpus: set[str] = set()

    for task in _walk_tasks(root_task):
        annotation_value = (task.annotations or {}).get(_GPU_ANNOTATION_KEY)
        if annotation_value is None:
            continue

        accelerators = _parse_accelerator_annotation(annotation_value)
        unsupported_gpus.update(set(accelerators) - _VALID_GPU_IDS)

    if unsupported_gpus:
        raise errors.UnsupportedGpuError(unsupported_gpus=sorted(unsupported_gpus))


def _walk_tasks(root_task: structures.TaskSpec) -> abc.Iterator[structures.TaskSpec]:
    yield root_task

    component_spec = root_task.component_ref.spec
    if component_spec and isinstance(
        component_spec.implementation, structures.GraphImplementation
    ):
        for child_task in component_spec.implementation.graph.tasks.values():
            yield from _walk_tasks(child_task)


def _parse_accelerator_annotation(annotation_value: object) -> abc.Mapping[str, object]:
    if isinstance(annotation_value, str):
        try:
            accelerators = json.loads(annotation_value)
        except json.JSONDecodeError:
            gpu_id, separator, quantity = annotation_value.rpartition(":")
            if not separator or not gpu_id or not quantity:
                raise errors.ApiValidationError(
                    f"GPU resource annotation {_GPU_ANNOTATION_KEY!r} must be a JSON object or a SkyPilot '<gpu-id>:<quantity>' string."
                )
            accelerators = {gpu_id: quantity}
    else:
        accelerators = annotation_value

    if not isinstance(accelerators, dict) or not all(
        isinstance(gpu_id, str) for gpu_id in accelerators
    ):
        raise errors.ApiValidationError(
            f"GPU resource annotation {_GPU_ANNOTATION_KEY!r} must be a JSON object with GPU identifiers as keys."
        )

    return accelerators
