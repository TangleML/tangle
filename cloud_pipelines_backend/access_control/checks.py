import dataclasses
from typing import Any

from .. import errors
from . import keys
from . import scopes


class NotAuthorizedError(errors.PermissionError):
    """A failed scope check; `owner` is carried for the host to show or withhold."""

    def __init__(self, message: str, *, owner: str | None = None) -> None:
        super().__init__(message)
        self.owner = owner


@dataclasses.dataclass(frozen=True, kw_only=True)
class Caller:
    """Who is asking; is_admin holds every scope, for hosts that pass it rather than skip the check."""

    email: str | None
    is_admin: bool = False


def normalize_email(*, email: str) -> str:
    """Lowercases and trims an email so the same address always compares equal."""
    return email.strip().lower()


def is_owner(*, owner: str | None, caller: Caller) -> bool:
    """True when the caller's email matches the owner's, ignoring case and whitespace."""
    return bool(
        owner
        and caller.email
        and normalize_email(email=owner) == normalize_email(email=caller.email)
    )


def effective_scopes(
    *,
    resource_type: scopes.ResourceType,
    owner: str | None,
    access_control: dict[str, Any] | None,
    caller: Caller,
) -> frozenset[str]:
    """Fine scopes the caller holds: all for the owner and admins, else what their grant expands to."""
    if caller.is_admin or is_owner(owner=owner, caller=caller):
        return resource_type.fine_scopes
    if not caller.email:
        return frozenset()
    users = (access_control or {}).get(keys.Key.USERS) or {}
    entry = users.get(normalize_email(email=caller.email)) or {}
    granted: set[str] = set()
    for scope in entry.get(keys.Key.PERMISSIONS) or []:
        granted |= resource_type.expand(scope=scope)
    return frozenset(granted)


def require(
    *,
    scope: str,
    resource_type: scopes.ResourceType,
    resource_id: str,
    owner: str | None,
    access_control: dict[str, Any] | None,
    caller: Caller,
) -> None:
    """Raises NotAuthorizedError unless the caller holds the scope on the resource."""
    granted = effective_scopes(
        resource_type=resource_type,
        owner=owner,
        access_control=access_control,
        caller=caller,
    )
    if scope not in granted:
        raise NotAuthorizedError(
            f"{caller.email} does not have {scope} on {resource_type.name} {resource_id}.",
            owner=owner,
        )
