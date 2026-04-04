import dataclasses

from .. import backend_types_sql as bts


@dataclasses.dataclass(kw_only=True)
class ExecutionStatusSummary:
    total_executions: int = 0
    ended_executions: int = 0
    has_ended: bool = False

    def count_execution_status(
        self, *, status: bts.ContainerExecutionStatus, count: int
    ) -> None:
        self.total_executions += count
        if status in bts.CONTAINER_STATUSES_ENDED:
            self.ended_executions += count

        self.has_ended = self.ended_executions == self.total_executions
