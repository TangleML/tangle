from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.api_server_sql import ExecutionStatusSummary


class TestExecutionStatusSummary:
    def test_initial_state(self):
        summary = ExecutionStatusSummary()
        assert summary.total_executions == 0
        assert summary.ended_executions == 0
        assert summary.has_ended is False

    def test_accumulate_all_ended_statuses(self):
        """Add each ended status with 2^i count for robust uniqueness."""
        summary = ExecutionStatusSummary()
        ended_statuses = sorted(bts.CONTAINER_STATUSES_ENDED, key=lambda s: s.value)
        expected_total = 0
        expected_ended = 0
        for i, status in enumerate(ended_statuses):
            count = 2**i
            summary.count_execution_status(status=status, count=count)
            expected_total += count
            expected_ended += count
            assert summary.total_executions == expected_total
            assert summary.ended_executions == expected_ended
            assert summary.has_ended is True

    def test_accumulate_all_in_progress_statuses(self):
        """Add each in-progress status with 2^i count for robust uniqueness."""
        summary = ExecutionStatusSummary()
        in_progress_statuses = sorted(
            set(bts.ContainerExecutionStatus) - bts.CONTAINER_STATUSES_ENDED,
            key=lambda s: s.value,
        )
        expected_total = 0
        for i, status in enumerate(in_progress_statuses):
            count = 2**i
            summary.count_execution_status(status=status, count=count)
            expected_total += count
            assert summary.total_executions == expected_total
            assert summary.ended_executions == 0
            assert summary.has_ended is False

    def test_accumulate_all_statuses(self):
        """Add every status with 2^i count. Summary math must be exact."""
        summary = ExecutionStatusSummary()
        all_statuses = sorted(bts.ContainerExecutionStatus, key=lambda s: s.value)
        expected_total = 0
        expected_ended = 0
        for i, status in enumerate(all_statuses):
            count = 2**i
            expected_total += count
            if status in bts.CONTAINER_STATUSES_ENDED:
                expected_ended += count
            summary.count_execution_status(status=status, count=count)
            assert summary.total_executions == expected_total
            assert summary.ended_executions == expected_ended
            assert summary.has_ended == (expected_ended == expected_total)
