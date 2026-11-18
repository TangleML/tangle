import logging
import typing
from typing import Any, Final

from . import backend_types_sql as bts
from . import errors

_logger = logging.getLogger(__name__)

EXTRA_DATA_STORAGE_KEY: Final[str] = "storage"
STORAGE_TYPE_KEY: Final[str] = "type"
STORAGE_DETAILS_KEY: Final[str] = "details"


class SecretValueStore(typing.Protocol):
    """Keeps secret values outside the database row, for example in a cloud secret manager."""

    # Recorded in the row's `extra_data`, so later reads and writes know which store holds the value.
    type: str

    def save(
        self, *, user_id: str, secret_name: str, value: str
    ) -> tuple[str, dict[str, Any]]:
        """Saves the value; returns what the row's `secret_value` keeps, and the storage details for `get` and `delete`.

        Example, a store that keeps numbered versions: `save(user_id="alice", secret_name="HF_TOKEN",
        value="hf_FAKE123")` returns `("", {"version": "4"})` — the row keeps `""`, and `{"version": "4"}`
        is what `get` and `delete` are later given as `storage_details`.
        """
        ...

    def get(
        self, *, storage_details: dict[str, Any], user_id: str, secret_name: str
    ) -> str:
        """Returns the value that `save` saved with these storage details."""
        ...

    def delete(
        self, *, storage_details: dict[str, Any], user_id: str, secret_name: str
    ) -> None:
        """Retires the value that `save` saved with these storage details."""
        ...


def get_storage_type(*, secret: bts.Secret) -> str | None:
    """Returns the type of the store holding the row's value, or None when the value is plain in the row.

    Example: with `extra_data = {"storage": {"type": "gsm", "details": {"version": "4"}}}` it returns `"gsm"`.
    """
    storage = (secret.extra_data or {}).get(EXTRA_DATA_STORAGE_KEY)
    return storage[STORAGE_TYPE_KEY] if storage else None


def get_storage_details(*, secret: bts.Secret) -> dict[str, Any] | None:
    """Returns what the store needs to find the row's value, or None when the value is plain in the row.

    Example: with `extra_data = {"storage": {"type": "gsm", "details": {"version": "4"}}}` it returns
    `{"version": "4"}`.
    """
    storage = (secret.extra_data or {}).get(EXTRA_DATA_STORAGE_KEY)
    return storage[STORAGE_DETAILS_KEY] if storage else None


def store_secret_value(
    *, secret: bts.Secret, value: str, store: SecretValueStore | None
) -> dict[str, Any] | None:
    """Sets the row's value, through the store if one is given.

    Returns the storage details of the value it replaces in the store, or None when there was none.
    """
    if store is None:
        secret.secret_value = value
        return None
    previous_storage_details = get_storage_details(secret=secret)
    secret.secret_value, storage_details = store.save(
        user_id=secret.user_id, secret_name=secret.secret_name, value=value
    )
    # A merge: the copy keeps every other key, so only the storage entry changes.
    secret.extra_data = {
        **(secret.extra_data or {}),
        EXTRA_DATA_STORAGE_KEY: {
            STORAGE_TYPE_KEY: store.type,
            STORAGE_DETAILS_KEY: storage_details,
        },
    }
    return previous_storage_details


def raise_if_storage_type_mismatch(
    *, secret: bts.Secret, store: SecretValueStore | None
) -> None:
    """Raises when the row's value is in a store other than this server's, leaving the row unchanged.

    Writing plain text over it, or deleting the row, would leave the stored value behind and still readable.
    """
    storage_type = get_storage_type(secret=secret)
    if storage_type is None:
        return
    configured = store.type if store else None
    if storage_type != configured:
        raise errors.SecretStorageTypeMismatchError(
            f"Secret '{secret.secret_name}' is kept in the {storage_type!r} store, "
            f"but this server's secret store is {configured!r}; it cannot be changed or deleted here."
        )


def retire_secret_value(
    *,
    store: SecretValueStore,
    storage_details: dict[str, Any],
    user_id: str,
    secret_name: str,
) -> None:
    """Retires a stored value the row no longer names, logging a failure instead of raising it.

    A helper for after the row commits: the change is already saved, so raising would report a successful save
    as a failure, and the only harm of a failed retire is an old value left enabled in the store.
    """
    try:
        store.delete(
            storage_details=storage_details, user_id=user_id, secret_name=secret_name
        )
    except Exception:
        _logger.exception(
            f"Could not retire the previous stored value of secret '{secret_name}'."
        )
