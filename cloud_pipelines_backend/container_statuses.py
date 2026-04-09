import enum


class ContainerExecutionStatus(str, enum.Enum):
    INVALID = "INVALID"  # Compatibility with Vertex AI CustomJob
    UNINITIALIZED = "UNINITIALIZED"  # Remove
    QUEUED = "QUEUED"  # Before WAITING_FOR_UPSTREAM or STARTING
    # READY_TO_START = "READY_TO_START"  # Input artifacts ready, but no job ID
    WAITING_FOR_UPSTREAM = "WAITING_FOR_UPSTREAM"
    # STARTING = "STARTING"
    PENDING = "PENDING"  # == Starting
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    # UPSTREAM_FAILED = "UPSTREAM_FAILED"
    # CONDITIONALLY_SKIPPED = "CONDITIONALLY_SKIPPED"
    SYSTEM_ERROR = "SYSTEM_ERROR"

    # new
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"
    # UPSTREAM_FAILED_OR_SKIPPED = "UPSTREAM_FAILED_OR_SKIPPED"
    SKIPPED = "SKIPPED"


CONTAINER_STATUSES_ENDED = {
    ContainerExecutionStatus.INVALID,
    ContainerExecutionStatus.SUCCEEDED,
    ContainerExecutionStatus.FAILED,
    ContainerExecutionStatus.SYSTEM_ERROR,
    ContainerExecutionStatus.CANCELLED,
    ContainerExecutionStatus.SKIPPED,
}
