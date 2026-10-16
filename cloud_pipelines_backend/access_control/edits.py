import datetime
from typing import Any, Final

from .. import errors
from . import checks
from . import keys
from . import scopes

MAX_HISTORY_ENTRIES: Final[int] = 100


class StaleRevisionError(Exception):
    pass


def latest_revision(*, access_control: dict[str, Any] | None) -> int:
    """Revision of the newest history entry, which is always last, or 0 when access has never been changed."""
    history = (access_control or {}).get(keys.Key.HISTORY) or []
    return history[-1][keys.Key.REVISION] if history else 0


def _validated_users(
    *,
    resource_type: scopes.ResourceType,
    owner: str | None,
    users: dict[str, dict[str, list[str]]],
) -> dict[str, dict[str, list[str]]]:
    """Normalizes a requested user list and drops empty grants.

    Rejects blank emails, the owner, duplicates and unknown scopes.
    """
    validated: dict[str, dict[str, list[str]]] = {}
    for raw_email, entry in users.items():
        email = checks.normalize_email(email=raw_email)
        if not email:
            raise errors.ApiValidationError("User email must not be empty.")
        if owner and email == checks.normalize_email(email=owner):
            raise errors.ApiValidationError(
                f"{email} owns this {resource_type.name} and cannot be listed."
            )
        if email in validated:
            raise errors.ApiValidationError(f"{email} is listed more than once.")
        permissions = sorted(set(entry.get(keys.Key.PERMISSIONS) or []))
        unknown = [
            scope for scope in permissions if not resource_type.is_known(scope=scope)
        ]
        if unknown:
            raise errors.ApiValidationError(
                f"Unknown permissions for {resource_type.name}: {', '.join(unknown)}."
            )
        if permissions:
            validated[email] = {keys.Key.PERMISSIONS: permissions}
    return validated


def apply_users_edit(
    *,
    resource_type: scopes.ResourceType,
    owner: str | None,
    access_control: dict[str, Any] | None,
    users: dict[str, dict[str, list[str]]],
    changed_by: str,
    changed_at: datetime.datetime,
    expected_revision: int,
) -> dict[str, Any]:
    """Replaces the user list at expected_revision and logs each user's from/to as one history entry.

    The revision check only sees this snapshot, so the caller must hold a row lock across the read
    that produced access_control and the write of the result. Keeps the newest MAX_HISTORY_ENTRIES.
    """
    current = access_control or {}
    revision = latest_revision(access_control=current)
    if expected_revision != revision:
        raise StaleRevisionError(
            f"Access was changed since revision {expected_revision}; the latest is {revision}."
        )
    old_users = current.get(keys.Key.USERS) or {}
    new_users = _validated_users(resource_type=resource_type, owner=owner, users=users)
    changes = {}
    for email in sorted(old_users.keys() | new_users.keys()):
        before = (old_users.get(email) or {}).get(keys.Key.PERMISSIONS) or []
        after = (new_users.get(email) or {}).get(keys.Key.PERMISSIONS) or []
        if before != after:
            changes[email] = {keys.Key.FROM: before, keys.Key.TO: after}
    history = list(current.get(keys.Key.HISTORY) or [])
    if changes:
        history.append(
            {
                keys.Key.REVISION: revision + 1,
                keys.Key.CHANGED_BY: checks.normalize_email(email=changed_by),
                keys.Key.CHANGED_AT: changed_at.isoformat(),
                keys.Key.CHANGES: changes,
            }
        )
    return {
        keys.Key.USERS: new_users,
        keys.Key.HISTORY: history[-MAX_HISTORY_ENTRIES:],
    }
