### TaskSpec annotations that are relevant to multiple launchers.

from typing import Any

from . import interfaces

# Annotations added by the Orchestrator
PIPELINE_RUN_CREATED_BY_ANNOTATION_KEY = (
    "cloud-pipelines.net/orchestration/pipeline_run.created_by"
)
PIPELINE_RUN_ID_ANNOTATION_KEY = "cloud-pipelines.net/orchestration/pipeline_run.id"
EXECUTION_NODE_ID_ANNOTATION_KEY = "cloud-pipelines.net/orchestration/execution_node.id"
CONTAINER_EXECUTION_ID_ANNOTATION_KEY = (
    "cloud-pipelines.net/orchestration/container_execution.id"
)

# Annotations that configure the launchers

# Launcher-agnostic number of ADDITIONAL attempts after the first one (integer, 0..5).
# Absent or 0 means no retries (the previous behavior).
# Launchers that cannot honor a valid value ignore it with a warning. Invalid values fail closed.
RETRIES_MAX_RETRIES_ANNOTATION_KEY = (
    "tangleml.com/launchers/generic/retries.max_retries"
)
RETRIES_MAX_MAX_RETRIES = 5


def get_max_retries(annotations: dict[str, Any] | None) -> int:
    """Parses and validates the retries annotation. Fails closed on invalid values."""
    value = (annotations or {}).get(RETRIES_MAX_RETRIES_ANNOTATION_KEY)
    if value is None:
        return 0
    # Annotation values are not guaranteed to be strings.
    value_str = str(value)
    try:
        max_retries = int(value_str)
    except ValueError:
        max_retries = None
    if max_retries is None or not (0 <= max_retries <= RETRIES_MAX_MAX_RETRIES):
        raise interfaces.LauncherError(
            f"Invalid value for the {RETRIES_MAX_RETRIES_ANNOTATION_KEY} annotation. The value must be an integer between 0 and {RETRIES_MAX_MAX_RETRIES}, but got {value_str!r}."
        )
    return max_retries
