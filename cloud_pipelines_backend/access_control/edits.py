import datetime
from typing import Any

from .. import errors
from . import checks
from . import scopes


class StaleRevisionError(Exception):
    pass


def latest_revision(access_control: dict[str, Any] | None) -> int:
    """Revision of the newest history entry, or 0 when access has never been changed."""
    history = (access_control or {}).get("history") or []
    return history[-1]["revision"] if history else 0


def replay(history: list[dict[str, Any]]) -> dict[str, dict[str, list[str]]]:
    """Rebuilds the user list by applying each history entry's per-user change in order."""
    users: dict[str, dict[str, list[str]]] = {}
    for entry in history:
        for email, change in entry["changes"].items():
            if change["to"]:
                users[email] = {"permissions": list(change["to"])}
            else:
                users.pop(email, None)
    return users


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
        email = checks.normalize_email(raw_email)
        if not email:
            raise errors.ApiValidationError("User email must not be empty.")
        if owner and email == checks.normalize_email(owner):
            raise errors.ApiValidationError(
                f"{email} owns this {resource_type.name} and cannot be listed."
            )
        if email in validated:
            raise errors.ApiValidationError(f"{email} is listed more than once.")
        permissions = sorted(set(entry.get("permissions") or []))
        unknown = [scope for scope in permissions if not resource_type.is_known(scope)]
        if unknown:
            raise errors.ApiValidationError(
                f"Unknown permissions for {resource_type.name}: {', '.join(unknown)}."
            )
        if permissions:
            validated[email] = {"permissions": permissions}
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
    """Replaces the user list at expected_revision and logs each user's from/to as one history entry."""
    current = access_control or {}
    revision = latest_revision(current)
    if expected_revision != revision:
        raise StaleRevisionError(
            f"Access was changed since revision {expected_revision}; the latest is {revision}."
        )
    old_users = current.get("users") or {}
    new_users = _validated_users(resource_type=resource_type, owner=owner, users=users)
    changes = {}
    for email in sorted(old_users.keys() | new_users.keys()):
        before = (old_users.get(email) or {}).get("permissions") or []
        after = (new_users.get(email) or {}).get("permissions") or []
        if before != after:
            changes[email] = {"from": before, "to": after}
    history = list(current.get("history") or [])
    if changes:
        history.append(
            {
                "revision": revision + 1,
                "changed_by": checks.normalize_email(changed_by),
                "changed_at": changed_at.isoformat(),
                "changes": changes,
            }
        )
    return {"users": new_users, "history": history}
