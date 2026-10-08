"""Canonical ``schedule_path`` normalization.

``schedule_path`` is the stable, user-owned alternate identity of a schedule:
unique per ``created_by``, set once, and addressed as a collection query
parameter rather than a path segment. A path may contain ``/``, so it could not
be a single path segment even if we wanted it to be.

One normalizer is used for **every write and every lookup**. That is load-bearing
rather than tidy: writes and reads must agree on what a path *is*, and the only
way to guarantee that is for both to pass through this function.

**Case is preserved, and that is a deliberate identity decision.** ``Foo/Bar``
and ``foo/bar`` are two different paths, so both can exist under one owner and
each resolves only to itself. An earlier revision folded case to lowercase, which
made the two one identity and let a lookup for either return the other's row.

Case preservation is only honest if the database agrees, so it is not a
string-handling change on its own: ``schedule_path`` carries an explicit
case-sensitive collation, its unique index is built over that collation, and
``database_migrations`` verifies the live column really has one before path
writes are allowed. Preserving case over a case-insensitive index would be worse
than folding -- the API would accept ``Foo/Bar`` and then either collide with
``foo/bar`` on the unique key or hand back the wrong row on a read.

Rejecting non-ASCII remains, for a narrower reason than before. It no longer has
to retire a case question, but it still keeps accent folding, Unicode
normalization forms and width variants out of an identity that has to compare
byte-for-byte across two backends.

This module also owns the **derivation** used when a caller omits
``schedule_path`` on create. `canonicalize_schedule_path` is strict on purpose --
an explicitly supplied value is validated, never repaired -- so derivation is a
separate, lenient function rather than a fallback inside the normalizer. The two
must not be merged: repairing explicit input would silently store something the
caller did not ask for, and rejecting derived input would make omission
impossible. The derived value is passed through the strict normalizer at the end,
so both paths converge on the same guarantee about what is stored.

The same generator is intended for the PR4 reconciliation backfill, so existing
rows and newly omitted paths land on identical shapes.
"""

import re
import unicodedata
from typing import Final

MAX_SCHEDULE_PATH_LENGTH: Final[int] = 255
SEPARATOR: Final[str] = "/"

#: Bound for the RAW request value, before trimming. Deliberately larger than
#: `MAX_SCHEDULE_PATH_LENGTH`, and deliberately not the same rule.
#:
#: The canonical 255 applies to the value AFTER trimming, so enforcing 255 on raw
#: input rejects a legal path that merely arrives with surrounding whitespace: a
#: leading space plus 255 canonical characters is 256 raw. Lookups canonicalize
#: before comparing, so the same value that a create refused would resolve a row
#: -- writes and reads disagreeing at exactly the boundary.
#:
#: This exists only to bound the payload before string work, so the canonicalizer
#: stays the single authority on what a path may be, and length errors come from
#: one place with one explanation.
MAX_RAW_SCHEDULE_PATH_LENGTH: Final[int] = 1024

# A segment may not START with `.`, `-` or `_`, which is what makes `.` and `..`
# structurally impossible: path traversal is rejected by the charset rule rather
# than by a special case. Embedded dots stay legal, so `v1.2` is a valid segment.
#
# Both cases are accepted and PRESERVED. The pattern is the exact contract a
# client can rely on, so it is written out rather than assembled from flags:
# `re.IGNORECASE` here would also have made the rule invisible to anyone reading
# the pattern, on a module whose entire subject is case.
_SEGMENT_PATTERN: Final[re.Pattern[str]] = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]*\Z")


class SchedulePathValidationError(ValueError):
    """A schedule_path that cannot be canonicalized."""


def canonicalize_schedule_path(schedule_path: str) -> str:
    """Trim, reject non-ASCII, then validate structure -- preserving case exactly.

    Three steps, in this order:

    1. **Trim.** Surrounding whitespace is the one thing repaired, because it is
       almost always transport noise rather than intent.
    2. **Reject non-ASCII.** Nothing outside ASCII can be stored, so no
       normalization form, accent fold or width variant ever reaches the column.
    3. **Validate structure** (emptiness, length, separators, segment charset).

    **No case folding happens at any step.** ``Foo/Bar`` is stored as ``Foo/Bar``
    and is a different identity from ``foo/bar``; both may exist under one owner,
    and a lookup for one never returns the other. This is enforced by the
    database as well as by this function -- see the module docstring -- because a
    case-preserving API over a case-insensitive unique index would accept a path
    and then alias it.

    The removal of the fold also retired a bug that only existed because of it: a
    few non-ASCII characters lowercase *into* ASCII (U+212A KELVIN SIGN becomes
    ``k``), so the ASCII check had to run before the fold or the character would
    have been silently rewritten. With no fold there is nothing to sequence, but
    the ASCII rule is kept ahead of structural validation anyway so that a
    non-ASCII value is always reported as non-ASCII rather than as a bad segment.

    Accepted, showing what each step changes::

        "  upi/nightly  "  -> "upi/nightly"     # trimmed
        "Upi/Nightly"      -> "Upi/Nightly"     # case PRESERVED, not folded
        "TEAM/v1.2/run_a"  -> "TEAM/v1.2/run_a" # dots, digits, _ and - are fine
        "a"                -> "a"               # one segment is a valid path

    Rejected, each by a different rule::

        ""                 # empty
        "   "              # empty after trimming -- not treated as omission
        "upi//nightly"     # empty segment
        "/upi"             # leading separator
        "upi/"             # trailing separator
        "upi/.hidden"      # segment starts with "." -- this is what makes
        "upi/../etc"       # ".." structurally impossible, rather than special-cased
        "upi/-dash"        # segment starts with "-"
        "upi/night ly"     # space inside a segment
        "\u212aelvin/x"    # U+212A KELVIN SIGN, and every other non-ASCII
        "\u00e9quipe/x"    # character, including accented Latin

    Raises:
        SchedulePathValidationError: naming the exact rule that was broken.
    """
    trimmed = schedule_path.strip()

    # Ahead of structural validation so that a non-ASCII value is reported as
    # non-ASCII, rather than as an invalid segment that happens to contain one.
    if not trimmed.isascii():
        raise SchedulePathValidationError(
            "schedule_path must be ASCII; non-ASCII characters are rejected so the"
            " value compares identically on every database backend."
        )

    # Named `normalized` rather than `folded`: trimming is the only change this
    # function makes to the caller's value.
    normalized = trimmed

    if not normalized:
        raise SchedulePathValidationError("schedule_path must not be empty.")
    if len(normalized) > MAX_SCHEDULE_PATH_LENGTH:
        raise SchedulePathValidationError(
            f"schedule_path must be at most {MAX_SCHEDULE_PATH_LENGTH} characters; received {len(normalized)}."
        )
    if normalized.startswith(SEPARATOR) or normalized.endswith(SEPARATOR):
        raise SchedulePathValidationError(
            "schedule_path must not start or end with '/'."
        )

    for segment in normalized.split(SEPARATOR):
        if not segment:
            raise SchedulePathValidationError(
                "schedule_path must not contain empty segments (e.g. 'a//b')."
            )
        if not _SEGMENT_PATTERN.match(segment):
            raise SchedulePathValidationError(
                f"schedule_path segment {segment!r} is invalid. Each '/'-separated"
                " segment must match [A-Za-z0-9][A-Za-z0-9._-]* — it must start"
                " with a letter or digit, so '.', '..', leading '-'/'_' and"
                " backslashes are all rejected. Upper and lower case are both"
                " accepted and are preserved exactly, so they are distinct paths."
            )

    return normalized


#: First segment of every derived path. Keeps derived identities in an obvious
#: namespace, so an operator can tell at a glance which paths a human chose.
LEGACY_PATH_PREFIX: Final[str] = "schedules"

#: Used when a name slugifies to nothing at all (e.g. a name that is entirely
#: non-ASCII, punctuation, or whitespace). A derived path must always exist, so
#: there has to be a value here rather than an error.
FALLBACK_SLUG: Final[str] = "schedule"

_SEPARATOR_RUN = re.compile(r"[^a-z0-9._]+")
_LEADING_JUNK = re.compile(r"\A[^a-z0-9]+")
_TRAILING_JUNK = re.compile(r"[._-]+\Z")


def ascii_slug(value: str) -> str:
    """Fold an arbitrary display name into one valid path segment.

    Lenient by contract: this is applied to names that already exist and cannot
    be rejected, so every input must produce *something* usable. Compare
    `canonicalize_schedule_path`, which rejects rather than repairs.

    NFKD-folds first so accented Latin text degrades to its ASCII skeleton
    (``Ünïcode`` -> ``unicode``) instead of being dropped wholesale. Text with no
    ASCII skeleton at all (e.g. CJK) legitimately reduces to nothing, which is
    what `FALLBACK_SLUG` is for.

    This one DOES lowercase, and now that explicit paths do not, the difference
    is worth stating. A derived path is not an identity the caller chose, so
    there is no case to preserve; lowercasing keeps derived paths in a single
    predictable shape, and the id suffix -- not the slug -- is what makes them
    unique. `canonicalize_schedule_path` accepts the result either way.
    """
    folded = unicodedata.normalize("NFKD", value)
    ascii_only = folded.encode("ascii", "ignore").decode("ascii").lower()
    # Collapse every run of disallowed characters to a single '-'. '.' and '_'
    # are legal inside a segment, so they survive as themselves.
    slug = _SEPARATOR_RUN.sub("-", ascii_only)
    # A segment must START with a letter or digit, so leading '.', '-' and '_'
    # are stripped rather than collapsed -- this is also what makes '..' and
    # traversal-shaped names structurally impossible here.
    slug = _LEADING_JUNK.sub("", slug)
    slug = _TRAILING_JUNK.sub("", slug)
    return slug or FALLBACK_SLUG


def generate_legacy_schedule_path(*, name: str, schedule_id: str) -> str:
    """Derive ``schedules/<slug>-<schedule_id>`` for a schedule with no path.

    The opaque schedule id is included, and never truncated, because names are
    neither unique nor immutable: two schedules may share a name, and a name may
    be edited after creation. Deriving from the name alone would therefore either
    collide on the per-owner unique index or produce a path that stops matching
    the thing it names. The id makes every derived path unique by construction,
    so a derived create can never lose a uniqueness race.

    Only the slug is truncated, keeping the id (and so uniqueness) intact under
    the 255-character cap.

    Raises:
        SchedulePathValidationError: if the id alone cannot fit under the cap, or
            if the result is somehow not canonical -- both treated as programming
            errors rather than silently repaired, since the second would mean the
            id itself is not path-safe.
    """
    prefix = f"{LEGACY_PATH_PREFIX}{SEPARATOR}"
    normalized_id = schedule_id.strip().lower()
    suffix = f"-{normalized_id}"
    budget = MAX_SCHEDULE_PATH_LENGTH - len(prefix) - len(suffix)

    if budget < 1:
        # Unsatisfiable rather than pathological: there is no path that both fits
        # the cap and keeps the whole id. Truncating the id would silently trade
        # away the uniqueness the id exists to provide, so this refuses instead.
        # Unreachable with real 20-character ids; reachable only if the id format
        # changes, which is exactly when a loud failure is wanted.
        raise SchedulePathValidationError(
            f"Cannot derive a schedule_path: schedule id {schedule_id!r} is"
            f" {len(normalized_id)} characters, which leaves no room for a slug"
            f" under the {MAX_SCHEDULE_PATH_LENGTH} character limit."
        )

    slug = ascii_slug(name)[:budget]
    # Truncation can leave a trailing separator, which is ugly but legal; strip
    # it, then re-apply the fallback in case stripping emptied the slug.
    slug = _TRAILING_JUNK.sub("", slug) or FALLBACK_SLUG[:budget]
    return canonicalize_schedule_path(f"{prefix}{slug}{suffix}")
