"""Project domain errors.

Plain exceptions and no FastAPI import, so the service can raise one without depending on the
web layer. `api_routes._register_exception_handlers` maps each to a status code -- the one named
in its docstring below -- and answers them all as `{"detail": "..."}`.

No ownership error here: everything in a deployment is visible to every authenticated reader, so
"not yours" is not a state this subsystem can be in. 401 and 403 come from the routes' auth
helper before any handler is reached.
"""


class ProjectError(Exception):
    pass


class ProjectNotFoundError(ProjectError):
    """No project, resource or workspace with that id. 404."""


class ProjectValidationError(ProjectError):
    """The request is well-formed JSON but names something this subsystem cannot store. 422.

    Also a write to an immutable field that bypassed the request model.
    """


class DuplicateResourceError(ProjectError):
    """This project already references that `(entity, entity_id)` pair. 409.

    Not 422: the same body succeeds once the colliding row is deleted.
    """


class WorkspaceNotEmptyError(ProjectError):
    """A workspace delete was refused because projects still point at it. 409.

    Deliberately not a cascade -- see `ProjectService.delete_workspace`.
    """
