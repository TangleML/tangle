import dataclasses
import enum
from collections.abc import Callable, Iterable
from typing import Any

from sqlalchemy import orm

from .. import backend_types_sql as bts
from . import checks
from . import scopes


class RunScope(str, enum.Enum):
    __hash__ = str.__hash__
    __str__ = str.__str__

    SUBMIT = "run:submit"
    CANCEL = "run:cancel"
    ANNOTATE = "run:annotate"


@dataclasses.dataclass(frozen=True, kw_only=True)
class RunParent:
    resource_type: scopes.ResourceType
    owner: str | None
    access_control: dict[str, Any] | None


RunParentResolver = Callable[[orm.Session, bts.PipelineRun], Iterable[RunParent]]

_parent_resolvers: list[RunParentResolver] = []


def register_run_parent_resolver(resolver: RunParentResolver) -> None:
    """Adds a host hook that finds the resources a run inherits run:* scopes from."""
    if resolver not in _parent_resolvers:
        _parent_resolvers.append(resolver)


def run_scopes(
    *,
    session: orm.Session,
    pipeline_run: bts.PipelineRun,
    caller: checks.Caller,
) -> frozenset[str]:
    """Run scopes the caller holds: all for the starter and admins, else those inherited from parents."""
    if (
        caller.is_admin
        or caller.email == pipeline_run.created_by
        or checks.is_owner(owner=pipeline_run.created_by, caller=caller)
    ):
        return frozenset(RunScope)
    granted: set[str] = set()
    for resolver in _parent_resolvers:
        for parent in resolver(session, pipeline_run):
            granted |= checks.effective_scopes(
                resource_type=parent.resource_type,
                owner=parent.owner,
                access_control=parent.access_control,
                caller=caller,
            )
    return frozenset(granted) & frozenset(RunScope)


def require_run_scope(
    *,
    session: orm.Session,
    pipeline_run: bts.PipelineRun,
    caller: checks.Caller,
    scope: RunScope,
) -> None:
    """Raises NotAuthorizedError unless the caller holds the scope on the run."""
    if scope not in run_scopes(
        session=session, pipeline_run=pipeline_run, caller=caller
    ):
        raise checks.NotAuthorizedError(
            f"The pipeline run {pipeline_run.id} was started by {pipeline_run.created_by} and {caller.email} does not have {scope} on it."
        )
