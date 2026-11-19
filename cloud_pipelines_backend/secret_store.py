import abc
import dataclasses
import datetime
import functools
from typing import Any, Callable, Final

import sqlalchemy as sql
from sqlalchemy import orm

from . import backend_types_sql as bts
from . import errors

# The `SecretStore` methods that move the caller's rows out of the `secret` table first.
_STORE_METHODS: Final[tuple[str, ...]] = (
    "create",
    "update",
    "get",
    "list_secrets",
    "delete",
)


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


def _moves_rows_first(method: Callable[..., Any]) -> Callable[..., Any]:
    """Wraps a store method so that, called with a `session`, it first moves the caller's rows into the store."""

    @functools.wraps(method)
    def wrapper(
        self: "SecretStore", *, session: orm.Session | None = None, **kwargs: Any
    ) -> Any:
        # `move_rows` itself calls `create` without a session, so it never moves twice.
        if session is not None:
            secret = kwargs.get("secret")
            move_rows(
                session=session,
                secret_store=self,
                user_id=secret.user_id if secret else kwargs["user_id"],
                secret_name=secret.secret_name if secret else kwargs.get("secret_name"),
            )
        return method(self, **kwargs)

    return wrapper


class SecretStore(abc.ABC):
    """Keeps secrets outside the database, for example in a cloud secret manager.

    With a store, the `secret` table only holds rows not yet moved. Each method below, as a subclass defines
    it, is wrapped so that a call with `session=` first moves the caller's rows from the table into the store;
    the subclass's own code never sees the session or the table.
    """

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        for name in _STORE_METHODS:
            if name in cls.__dict__:
                setattr(cls, name, _moves_rows_first(cls.__dict__[name]))

    @abc.abstractmethod
    def create(self, *, secret: StoredSecret) -> StoredSecret:
        """Saves a new secret and returns it without its value; raises `errors.ItemAlreadyExistsError` if it exists."""

    @abc.abstractmethod
    def update(self, *, secret: StoredSecret) -> StoredSecret:
        """Saves a new value for an existing secret and returns it without its value.

        Keeps the stored `created_at` and name, and the stored description when `description` is None.
        Raises `errors.ItemNotFoundError` when there is no such secret.
        """

    @abc.abstractmethod
    def get(self, *, user_id: str, secret_name: str) -> str | None:
        """Returns the secret's latest value, or None when there is no such secret."""

    @abc.abstractmethod
    def list_secrets(self, *, user_id: str) -> list[StoredSecret]:
        """Returns the user's secrets, without values."""

    @abc.abstractmethod
    def delete(self, *, user_id: str, secret_name: str) -> None:
        """Deletes the secret and all its values; raises `errors.ItemNotFoundError` when there is none."""


def move_rows(
    *,
    session: orm.Session,
    secret_store: SecretStore,
    user_id: str,
    secret_name: str | None = None,
) -> None:
    """Moves the user's secret (all of the user's secrets when `secret_name` is None) from the `secret` table
    into the store, committing after each.

    Each row is deleted in the same commit, after the store has saved it, so a failure leaves the row to move
    next time. The store gets the row's own name, which the database may have matched ignoring case.
    """
    if secret_name is None:
        names = session.scalars(
            sql.select(bts.Secret.secret_name).filter_by(user_id=user_id)
        ).all()
    else:
        names = [secret_name]
    for name in names:
        secret = session.scalars(
            sql.select(bts.Secret)
            .filter_by(user_id=user_id, secret_name=name)
            .with_for_update()
        ).one_or_none()
        if secret is None:
            continue
        try:
            secret_store.create(
                secret=StoredSecret(
                    user_id=secret.user_id,
                    secret_name=secret.secret_name,
                    value=secret.secret_value,
                    description=secret.description,
                    created_at=secret.created_at,
                    updated_at=secret.updated_at,
                    extra_data=secret.extra_data,
                )
            )
        except errors.ItemAlreadyExistsError:
            # An earlier move saved it but failed before deleting the row; the store's copy is the same or newer.
            pass
        session.delete(secret)
        session.commit()
