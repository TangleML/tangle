import datetime
import typing
from typing import Any, Final

from . import backend_types_sql as bts

EXTRA_DATA_ENCRYPTION_KEY: Final[str] = "encryption"
ENCRYPTION_TYPE_KEY: Final[str] = "type"
ENCRYPTION_DETAILS_KEY: Final[str] = "details"


class SecretValueCipher(typing.Protocol):
    """Encrypts secret values before they are stored and decrypts them for a launch."""

    type: str

    def encrypt(
        self, *, plaintext: str, user_id: str, secret_name: str
    ) -> tuple[str, dict[str, Any]]:
        """Returns the value to store, and the details `decrypt` will be given."""
        ...

    def decrypt(
        self,
        *,
        stored: str,
        details: dict[str, Any],
        user_id: str,
        secret_name: str,
    ) -> str: ...


def store_secret_value(
    *, secret: bts.Secret, plaintext: str, cipher: SecretValueCipher | None
) -> None:
    """Sets the value, encrypted if a cipher is given, merging how into `extra_data`."""
    secret.secret_value = plaintext
    extra_data = dict(secret.extra_data or {})
    if cipher:
        secret.secret_value, details = cipher.encrypt(
            plaintext=plaintext, user_id=secret.user_id, secret_name=secret.secret_name
        )
        extra_data[EXTRA_DATA_ENCRYPTION_KEY] = {
            ENCRYPTION_TYPE_KEY: cipher.type,
            "encrypted_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            ENCRYPTION_DETAILS_KEY: details,
        }
    elif extra_data.pop(EXTRA_DATA_ENCRYPTION_KEY, None) is None:
        return
    # A merge: the copy keeps every other key, so only the encryption entry changes.
    secret.extra_data = extra_data
