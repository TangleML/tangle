import dataclasses
import enum
from collections import abc
from typing import Any, Protocol

from sqlalchemy import orm

from .. import backend_types_sql as bts
from . import checks
from . import scopes


class RunScope(str, enum.Enum):
    __hash__ = str.__hash__
    __str__ = str.__str__

    CANCEL = "run:cancel"
    ANNOTATE = "run:annotate"


@dataclasses.dataclass(frozen=True, kw_only=True)
class RunParent:
    resource_type: scopes.ResourceType
    owner: str | None
    access_control: dict[str, Any] | None


class RunParentResolver(Protocol):
    def __call__(
        self, *, session: orm.Session, pipeline_run: bts.PipelineRun
    ) -> abc.Iterable[RunParent]: ...


_parent_resolvers: list[RunParentResolver] = []
_parent_annotation_keys: set[str] = set()


def register_run_parent_resolver(
    *, resolver: RunParentResolver, annotation_keys: abc.Iterable[str] = ()
) -> None:
    """Adds a host hook that finds the resources a run inherits run:* scopes from, via the given annotations."""
    if resolver not in _parent_resolvers:
        _parent_resolvers.append(resolver)
    _parent_annotation_keys.update(annotation_keys)


def _is_starter_or_admin(
    *, pipeline_run: bts.PipelineRun, caller: checks.Caller
) -> bool:
    """True for admins and whoever started the run."""
    return (
        caller.is_admin
        or caller.email == pipeline_run.created_by
        or checks.is_owner(owner=pipeline_run.created_by, caller=caller)
    )


def run_scopes(
    *,
    session: orm.Session,
    pipeline_run: bts.PipelineRun,
    caller: checks.Caller,
) -> frozenset[str]:
    """Run scopes the caller holds: all for the starter and admins, else those inherited from parents."""
    all_scopes = frozenset(scope.value for scope in RunScope)
    if _is_starter_or_admin(pipeline_run=pipeline_run, caller=caller):
        return all_scopes
    granted: set[str] = set()
    for resolver in _parent_resolvers:
        for parent in resolver(session=session, pipeline_run=pipeline_run):
            granted |= checks.effective_scopes(
                resource_type=parent.resource_type,
                owner=parent.owner,
                access_control=parent.access_control,
                caller=caller,
            )
    return frozenset(granted) & all_scopes


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
            f"The pipeline run {pipeline_run.id} was started by {pipeline_run.created_by} and {caller.email} does not have {scope} on it.",
            owner=pipeline_run.created_by,
        )


def require_annotation_change(
    *,
    session: orm.Session,
    pipeline_run: bts.PipelineRun,
    caller: checks.Caller,
    key: str,
) -> None:
    """Raises NotAuthorizedError unless the caller may set or delete the annotation.

    Annotations a parent resolver reads are kept to the starter and admins, so a grantee can't re-parent the run.
    """
    require_run_scope(
        session=session,
        pipeline_run=pipeline_run,
        caller=caller,
        scope=RunScope.ANNOTATE,
    )
    if key in _parent_annotation_keys and not _is_starter_or_admin(
        pipeline_run=pipeline_run, caller=caller
    ):
        raise checks.NotAuthorizedError(
            f"Only {pipeline_run.created_by} can change the {key} annotation on pipeline run {pipeline_run.id}.",
            owner=pipeline_run.created_by,
        )
