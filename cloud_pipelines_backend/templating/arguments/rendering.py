"""Turning saved templates into the arguments a run is created with.

One engine, built once at import, and one entry point:

    templates = {"as_of_date": "{{ schedule_time | shift('-1d') | date }}"}
    arguments = {"as_of_date": "2020-01-01", "region": "ca"}

    render(templates=templates, arguments=arguments, clock=cron_clock)
        .arguments   -> {"as_of_date": "2026-09-01", "region": "ca"}
        .overridden  -> {"as_of_date": "2020-01-01"}
        .failures    -> {}

Nothing here raises. A template that blows up costs its own key and nothing else: the
other keys still render, the failed key keeps whatever the task spec gave it, and the run
is still submitted. That is section 4.6 -- a render failure is a data-quality event, not a
control-flow one -- and it is why `render` returns failures instead of propagating them.

Grammar is not re-checked here. A stored template was validated when it was saved, so the
work at render time is the two things that save time cannot know: what the clocks read,
and whether this particular fire has a schedule_time.
"""

import dataclasses
import re
from collections.abc import Mapping
from typing import Final

import jinja2

from cloud_pipelines_backend.templating.arguments import engines, errors, sources

#: Jinja2's own wording for a name the context does not carry, measured rather than
#: assumed: `'schedule_time' is undefined`. It is the only place the missing name is
#: reported, since `UndefinedError` does not carry the name as an attribute.
_UNDEFINED_NAME: Final[re.Pattern[str]] = re.compile(r"^'([^']+)' is undefined$")


@dataclasses.dataclass(frozen=True)
class Rendered:
    """What one schedule's templates produced, split by what the caller does with it.

    `arguments` is handed to the run. `failures` is annotated, and is the only trace a human
    gets of a template that did not run -- hence a map rather than a count.

    `overridden` is computed and returned, and deliberately neither annotated nor counted: no
    caller can populate it, because a stored argument is first knowable inside the locked read
    that selects the pipeline version and rendering happens before that. It stays because it
    is the correct answer for a caller that does pass a non-empty `arguments`, and because
    deriving it after the fact -- comparing the run's `rendered` annotation against the
    pipeline's stored spec -- is what a human does instead.
    """

    arguments: dict[str, str]
    overridden: dict[str, str]
    failures: dict[str, str]
    #: key -> the failure's bounded code, for the metric label `failures` cannot carry.
    failure_codes: dict[str, str] = dataclasses.field(default_factory=dict)
    #: The clock these values came from. Carried so an annotation naming a source
    #: and the value rendered from it cannot describe different times.
    clock: sources.Clock | None = None


def render_one(*, key: str, value: str, clock: sources.Clock) -> str:
    """Render a single template, or raise `TemplateError` naming the key.

    key="as_of_date", value="{{ schedule_time | date }}"   -> "2026-09-02"
    key="as_of_date", value="{{ schedule_time }}", not scheduled -> raises, source unavailable
    key="region",     value="ca"                           -> "ca"   (a constant is a template)
    key="suffix",     value=""                             -> ""     (empty is a value)
    """
    try:
        return engines.ENGINE.from_string(value).render(clock.context(key=key))
    except errors.TemplateError:
        # coalesce with no available arm; already carries the key.
        raise
    except jinja2.UndefinedError as exception:
        raise _as_unavailable_source(exception, key=key, clock=clock) from exception
    except Exception as exception:
        # A saved template should not reach here. One can: a template stored before a
        # validator existed, or edited around the API. The run must not die for it.
        raise errors.render_failed(key=key, message=str(exception)) from exception


def _as_unavailable_source(
    exception: jinja2.UndefinedError, *, key: str, clock: sources.Clock
) -> errors.TemplateError:
    """Name the clock the template asked for and this fire does not have.

    "'schedule_time' is undefined" on a manual fire
        -> 'schedule_time' is not available for a manual run; coalesce(...) ...
    any other wording
        -> the message, verbatim, rather than a guess
    """
    match = _UNDEFINED_NAME.match(exception.message or "")
    name = match.group(1) if match else None
    if name in sources.TIME_SOURCE_NAMES:
        return errors.source_unavailable(key=key, sources=[name], kind=clock.kind.value)
    return errors.render_failed(key=key, message=str(exception))


def render(
    *,
    templates: Mapping[str, str],
    arguments: Mapping[str, str],
    clock: sources.Clock,
) -> Rendered:
    """Render every template over a copy of the run's arguments; the template wins.

        templates={"day": "{{ now | date }}"},  arguments={}
            -> arguments {"day": "2026-09-02"},  overridden {},  failures {}

        templates={"day": "{{ now | date }}"},  arguments={"day": "old"}
            -> arguments {"day": "2026-09-02"},  overridden {"day": "old"}

        templates={"day": "{{ schedule_time }}"}, not scheduled, arguments={"day": "old"}
            -> arguments {"day": "old"},  failures {"day": "... not available ..."}

    A key is recorded in `overridden` only when the rendered value actually differs from
    the one it replaced; a template that reproduces the existing value displaced nothing
    worth annotating.
    """
    rendered = dict(arguments)
    overridden: dict[str, str] = {}
    failures: dict[str, str] = {}
    failure_codes: dict[str, str] = {}

    for key, value in templates.items():
        try:
            result = render_one(key=key, value=value, clock=clock)
        except errors.TemplateError as exception:
            failures[key] = exception.detail
            failure_codes[key] = exception.code
            continue
        if key in rendered and rendered[key] != result:
            overridden[key] = rendered[key]
        rendered[key] = result

    return Rendered(
        arguments=rendered,
        overridden=overridden,
        failures=failures,
        failure_codes=failure_codes,
        clock=clock,
    )
