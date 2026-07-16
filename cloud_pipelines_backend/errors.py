class ItemNotFoundError(Exception):
    pass


class ContainerExecutionNotReadyError(Exception):
    """Execution exists but its container execution is not yet available (transient)."""

    def __init__(self, *, execution_node_id: str, execution_status: str | None = None):
        self.execution_node_id = execution_node_id
        self.execution_status = execution_status
        super().__init__(
            f"Execution with {execution_node_id=} does not have "
            "container execution information."
        )


class ItemAlreadyExistsError(Exception):
    pass


class PermissionError(Exception):
    pass


class ApiValidationError(Exception):
    """Base for all filter/annotation validation errors -> 422."""

    pass
