import dataclasses
import datetime
from typing import Any, Protocol


@dataclasses.dataclass(frozen=True, kw_only=True)
class StoredSecret:
    """A secret as a `SecretStore` keeps it; `value` is None when not loaded, and never shown by repr, str or logs."""

    user_id: str
    secret_name: str
    value: str | None = dataclasses.field(default=None, repr=False)
    description: str | None = None
    created_at: datetime.datetime
    updated_at: datetime.datetime
    extra_data: dict[str, Any] | None = None


class SecretStore(Protocol):
    """Keeps secrets outside the database, for example in a cloud secret manager.

    With a store, new secrets go to the store; a secret still in the `secret` table stays there until moved.
    """

    def create(self, *, secret: StoredSecret) -> StoredSecret:
        """Saves a new secret and returns it without its value; raises `errors.ItemAlreadyExistsError` if it exists."""
        ...

    def update(self, *, secret: StoredSecret) -> StoredSecret:
        """Saves a new value for an existing secret and returns it without its value.

        Keeps the stored `created_at` and name, and the stored description when `description` is None.
        Raises `errors.ItemNotFoundError` when there is no such secret.
        """
        ...

    def get(self, *, user_id: str, secret_name: str) -> str | None:
        """Returns the secret's latest value, or None when there is no such secret."""
        ...

    def list_secrets(self, *, user_id: str) -> list[StoredSecret]:
        """Returns the user's secrets, without values."""
        ...

    def delete(self, *, user_id: str, secret_name: str) -> None:
        """Deletes the secret and all its values; raises `errors.ItemNotFoundError` when there is none."""
        ...
