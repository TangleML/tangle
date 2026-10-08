"""The `pipeline_templates` envelope, and the ladder every request climbs.

A schedule and a subscription carry the same envelope, so the ladder lives here once:

    {"pipeline_templates": {"arguments": {"as_of_date": "{{ schedule_time | date }}"}}}

    arguments_in    1-2  the envelope is an object; `arguments` is a str -> str map
    check_templates 3-6  classify, parse, walk the grammar, check availability

Every rung is pure: no database, and no pipeline is loaded. A key is deliberately not
checked against the target pipeline's inputs -- run submission already refuses an
argument for an undeclared input, and checking here would only work for an inline spec.

Nothing here repairs a value or touches the pipeline spec.
"""

from collections.abc import Mapping
from typing import Any, Final

import fastapi
from starlette import status

from cloud_pipelines_backend.templating.arguments import errors, grammars, sources

#: The one key this version reads inside the envelope. Unknown siblings are ignored so a
#: newer client can roll out against an older server without a 422 on every request.
ARGUMENTS: Final[str] = "arguments"


def arguments_in(*, envelope: Mapping[str, Any] | None) -> dict[str, str]:
    """Rungs 1-2: the templates a request is asking to store.

    None                                     -> {}      (field omitted)
    {}                                       -> {}      (envelope with nothing in it)
    {"arguments": {"day": "{{ now }}"}}      -> {"day": "{{ now }}"}
    {"arguments": {}, "future_key": 1}       -> {}       (unknown sibling ignored)
    {"arguments": [1, 2]}                    -> raises, got list
    {"arguments": []}                        -> raises, got list
    {"arguments": {"retries": 3}}            -> raises, 'retries' is int
    """
    #: Only an absent or null `arguments` means "none given". A falsy value of the wrong
    #: type is a malformed request, so it must not collapse into the same empty map.
    arguments = (envelope or {}).get(ARGUMENTS)
    if arguments is None:
        arguments = {}
    if not isinstance(arguments, Mapping):
        raise errors.arguments_not_a_map_of_strings(
            key=None, got=type(arguments).__name__
        )
    for key, value in arguments.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise errors.arguments_not_a_map_of_strings(
                key=str(key), got=type(value).__name__
            )
    return dict(arguments)


def check_templates(*, arguments: Mapping[str, str], kind: sources.Kind) -> None:
    """Rungs 3-6, per key. Raises on the first fault; returns None when every value is
    storable under `kind`.

        {"region": "ca"},            CRON          -> a constant, nothing to check
        {"day": "{{ now | date }}"}, SUBSCRIPTION  -> fine
        {"day": "{{ schedule_time }}"}, SUBSCRIPTION
            -> raises: a subscription is never scheduled, so this would fail every fire

    `kind` is the kind the row is *saved* under, not the one a fire happens to have.
    """
    for key, value in arguments.items():
        classified = grammars.classify(key=key, value=value)
        if not isinstance(classified, grammars.Expression):
            continue
        grammars.check_grammar(key=key, expression=classified.node)
        grammars.check_availability(key=key, expression=classified.node, kind=kind)


def names_arguments(*, envelope: Mapping[str, Any] | None) -> bool:
    """Whether an envelope asks to change the stored templates at all.

    None                         -> False   (field omitted)
    {}                           -> False   (nothing named)
    {"future_key": 1}            -> False   (a newer client's field, not ours)
    {"arguments": {}}            -> True    (the explicit clear)
    {"arguments": {"r": "ca"}}   -> True

    An edit that removes every template has to name `arguments`, so an envelope carrying
    only keys this version does not read leaves the stored templates alone.
    """
    return envelope is not None and ARGUMENTS in envelope


def validated_arguments(
    *, envelope: Mapping[str, Any] | None, kind: sources.Kind
) -> dict[str, str]:
    """The whole ladder, and its refusal as the 422 both endpoints return.

        None,                                CRON          -> {}
        {"arguments": {"region": "ca"}},     SUBSCRIPTION  -> {"region": "ca"}
        {"arguments": {"d": "{{ now }}"}},   CRON          -> {"d": "{{ now }}"}
        {"arguments": [1, 2]},               CRON          -> 422, not a map of strings
        {"arguments": {"d": "{{ schedule_time }}"}}, SUBSCRIPTION
            -> 422, a subscription is never scheduled

    `kind` is what the row is saved under, not what a fire happens to have, and is the
    only difference between the two callers. A subscription is never scheduled, so
    `schedule_time` is refused there and legal on a cron schedule, coalesce arms included.

    Raises:
        fastapi.HTTPException: 422, carrying the ladder's own message. `arguments_in` and
            `check_templates` stay callable separately for a non-HTTP caller.
    """
    try:
        arguments = arguments_in(envelope=envelope)
        check_templates(arguments=arguments, kind=kind)
    except errors.TemplateError as exception:
        raise fastapi.HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=exception.detail,
        ) from exception
    return arguments
