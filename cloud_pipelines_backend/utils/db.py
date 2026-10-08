import datetime
from typing import Final

# generate_unique_id() emits 10 bytes = 20 hex chars.
ID_LENGTH: Final[int] = 20

# The capacity of sql.Text on MySQL, in UTF-8 *bytes*. A character count would not bound the
# column at all -- one character encodes to 1-4 bytes under utf8mb4 -- so anything checking a
# Text column's size before a write counts bytes against this.
MAX_TEXT_BYTES: Final[int] = 65_535

# Of those, the leading 6 bytes = 12 hex chars are a millisecond epoch, and the trailing 4 bytes
# are os.urandom. Only the prefix carries time order: two ids minted in the same millisecond sort
# by their random tails, which is to say arbitrarily. Anything comparing ids to decide *which came
# first* must therefore compare this prefix and not the whole id — see triggers/event_state.fill.
ID_MS_PREFIX_LENGTH: Final[int] = 12


def utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)
