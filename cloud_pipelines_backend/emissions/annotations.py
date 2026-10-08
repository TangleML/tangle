"""The shared emission annotation vocabulary.

Everything here is read by every kind of emission: the key namespace, the token that declares
a sink, the default on-status, and the helpers that normalize and validate raw annotation
values. Each kind's own keys, intent, sink enum, parsing and serialization live in
emissions/handlers/<kind>/annotations.py.

Pure: no DB, no id inference, no logging. The helpers that find a malformed opt-in RETURN their
issues so the caller can log them with the node / emission_event id.
"""

import enum
import json
from typing import Any, Final, NoReturn, TypeVar

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.dispatching.handlers import base as handler_base
from cloud_pipelines_backend.emissions import db_models
from cloud_pipelines_backend.utils.annotations import ANNOTATION_ROOT

# Path-based namespace. "/" is the only separator, no dots. Matches existing Tangle
# annotations (e.g. tangleml.com/launchers/kubernetes/...).
PREFIX: Final[str] = f"{ANNOTATION_ROOT}emission/"

# The one value that declares a sink. is_true compares against it and the serializers write
# it, so the token a node is checked for and the token a row stores cannot drift apart.
SINK_DECLARED_VALUE: Final[str] = "true"

# The container status that fires an emission when a node omits on-status. Held as the
# enum member here; only its string .value is stored when written to the DB.
DEFAULT_ON_STATUS: Final[bts.ContainerExecutionStatus] = (
    bts.ContainerExecutionStatus.SUCCEEDED
)


def clean_str(
    *,
    value: Any,
) -> str | None:
    """Normalize a raw annotation value to a clean string, or None when it is empty.

    Stripping whitespace lets a required key that is present but blank be treated the same
    as an absent key, since both come back as None.

    Args:
        value: The raw annotation value. May be any type, or None.

    Returns:
        The stripped string, or None if the value was None or blank after stripping.
    """
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def is_true(
    *,
    value: Any,
) -> bool:
    """Report whether an annotation value is an explicit opt-in.

    Only SINK_DECLARED_VALUE counts, case-insensitively and after stripping. Anything else —
    "1", "yes", "false", a blank value, an absent one — is not an opt-in, so it declares no
    sink and no delivery is made. The producer is the only writer of these values and it
    writes that one token, so a wider vocabulary would only ever match a hand-edited row.

    Args:
        value: The raw annotation value. May be any type, or None.

    Returns:
        True when the value reads as SINK_DECLARED_VALUE, False otherwise.
    """
    text = clean_str(value=value)
    return text is not None and text.lower() == SINK_DECLARED_VALUE


# Any of the per-handler sink enums. Their members are complete annotation keys, which is
# what lets one helper collect either handler's sinks.
_SinkAnnotationT = TypeVar("_SinkAnnotationT", bound=enum.Enum)


def parse_sinks(
    *,
    annotations: dict[str, Any],
    prefix: str,
    sink_annotation: type[_SinkAnnotationT],
    kind: db_models.EmissionType,
    default_sink: _SinkAnnotationT,
    unknown_sink_code: str,
) -> tuple[
    tuple[_SinkAnnotationT, ...], tuple[str, ...], list[handler_base.ParseIssue]
]:
    """Resolve the sinks a node declared, and the sink keys nothing can serve.

    Every handler's parser calls this, and it is parameterized on the prefix, the enum and the
    kind, so it belongs to none of them.

    Iterating the enum rather than the annotations is what fixes the order: the sinks come
    back in declaration order however the node wrote them.

    Declaring nothing is not an error: the node falls back to default_sink, so every node
    of the kind yields exactly one deliverable sink at minimum. The old behaviour — dropping
    the intent — made the common case of "emit this, I do not care where" impossible to write.

    Keys under the prefix that match no member are swept up separately. The producer runs in
    the API server and the consumer is a separate process, so a newer producer can write a
    sink key an older consumer has no member for; a node can also simply misspell one. Either
    way nothing can deliver it.

    The codes are the caller's because each handler carries its own code set; the messages are
    built here so one finding is worded one way wherever it is found.

    Args:
        annotations: The node's TaskSpec annotations, or the stored annotation rows.
        prefix: The sink key prefix belonging to sink_annotation.
        sink_annotation: The handler's sink enum, each member a complete annotation key.
        kind: The emission type these sinks belong to, named in the issue messages.
        default_sink: The member a node falls back to when it declares none.
        unknown_sink_code: The caller's code for declaring a sink nothing implements.

    Returns:
        The resolved members in declaration order — default_sink alone when nothing was
        declared — the unrecognized keys sorted, and the issues to report: unknown_sink_code
        (kept) when an unrecognized key sat beside a recognized one.
    """
    declared = tuple(
        member for member in sink_annotation if is_true(value=annotations.get(member))
    )
    known = {member.value for member in sink_annotation}
    unknown = tuple(
        sorted(
            key
            for key in annotations
            if isinstance(key, str) and key.startswith(prefix) and key not in known
        )
    )

    if not declared:
        declared = (default_sink,)
    if unknown:
        # The sinks that can be delivered still should be, so nothing is dropped here; the
        # caller accounts for the key that reached no implementation.
        return (
            declared,
            unknown,
            [
                handler_base.ParseIssue(
                    code=unknown_sink_code,
                    message=(
                        f"{kind.value} sink keys {list(unknown)} match no implemented sink; "
                        "delivering the resolved sinks only"
                    ),
                    dropped=False,
                )
            ],
        )
    return declared, unknown, []


def parse_missing_event_key(
    *,
    annotations: dict[str, Any],
    kind_prefix: str,
    kind: db_models.EmissionType,
    event_key: str,
    no_event_key_code: str,
) -> list[handler_base.ParseIssue]:
    """Report a node that set a key of this kind without setting its event key.

    Empty when the node named no key of the kind at all: every emission is opt-in, so a missing
    event key is also what opting out looks like, and reporting it would fire on nearly every
    node in the fleet. A node that named some other key of the kind meant to opt in, so its
    missing event key is a typo, and one that is invisible otherwise: no row is written and
    nothing downstream has anything to report against.

    There is no mirror finding for a missing sink: parse_sinks defaults one instead.

    Args:
        annotations: The node's TaskSpec annotations, or the stored annotation rows.
        kind_prefix: The prefix every key of this kind starts with.
        kind: The emission type those keys belong to, named in the message.
        event_key: The key that had to be set, named in the message so the fix is stated.
        no_event_key_code: The caller's code for a key of the kind set without its event key.

    Returns:
        One dropped issue under that code, or an empty list when the kind was not named at all.
    """
    if not any(
        isinstance(key, str) and key.startswith(kind_prefix) for key in annotations
    ):
        return []
    return [
        handler_base.ParseIssue(
            code=no_event_key_code,
            message=(
                f"{kind.value} keys are set without {event_key}; dropping the "
                f"{kind.value} intent"
            ),
            dropped=True,
        )
    ]


def parse_on_status(
    *,
    raw: str | None,
) -> bts.ContainerExecutionStatus | None:
    """Convert a raw on-status annotation string into the typed status enum.

    Args:
        raw: The on-status annotation value, or None when the annotation is absent.

    Returns:
        DEFAULT_ON_STATUS (SUCCEEDED) when raw is None; the matching
        ContainerExecutionStatus when raw names a valid status; or None when raw is a
        non-empty but unrecognized value. A None return tells the caller to record an
        unknown-status issue and drop the intent.
    """
    if raw is None:
        return DEFAULT_ON_STATUS
    try:
        return bts.ContainerExecutionStatus(raw)
    except ValueError:
        return None


def _reject_json_constant(
    constant: str,
) -> NoReturn:
    """Reject NaN / Infinity / -Infinity, which json.loads otherwise accepts.

    Args:
        constant: The name of the non-standard constant the decoder found.

    Raises:
        ValueError: Always. parse_json_object catches it and reports the payload as malformed.
    """
    raise ValueError(f"{constant} is not valid JSON")


def parse_json_object(
    *,
    payload: str,
) -> dict[str, Any] | None:
    """Decode a payload that has to be a JSON object, or None when it is not one.

    NaN, Infinity and -Infinity are rejected even though the decoder accepts them by default,
    and a payload nested too deep to decode is malformed rather than a RecursionError escaping
    into the caller. The decoded object comes back rather than a verdict, so a caller that then
    reads it pays for one parse instead of two.

    Args:
        payload: The raw payload string to validate.

    Returns:
        The decoded object, or None when the payload is malformed, nested too deeply, or
        decodes to anything other than an object.
    """
    try:
        parsed = json.loads(payload, parse_constant=_reject_json_constant)
    except (ValueError, RecursionError):
        return None
    return parsed if isinstance(parsed, dict) else None


def is_json_object(
    *,
    payload: str,
) -> bool:
    """Return True only if payload is a JSON object (a dict), not an array or scalar.

    Args:
        payload: The raw payload string to validate.

    Returns:
        True when payload decodes to a JSON object.
    """
    return parse_json_object(payload=payload) is not None
