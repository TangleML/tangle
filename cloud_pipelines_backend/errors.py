class ItemNotFoundError(Exception):
    pass


class ItemAlreadyExistsError(Exception):
    pass


class PermissionError(Exception):
    pass


class ApiValidationError(Exception):
    """Base for all filter/annotation validation errors -> 422."""

    pass


class MutuallyExclusiveFilterError(ApiValidationError):
    pass
