"""Run annotations recording what the renderer decided for one firing."""

from collections.abc import Mapping
from typing import Final

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.templating.arguments import rendering, sources

#: Both `pipeline_run_annotation.key` and `.value` are String, so they share the width every
#: other string column is declared with.
_MAX_LENGTH: Final[int] = bts._STR_MAX_LENGTH
_TRUNCATION_MARKER: Final[str] = "...[truncated]"

#: Not `tangleml.com/scheduling`, which the scheduler already owns and which a
#: subscription run has no business carrying.
_NAMESPACE: Final[str] = "tangleml.com/templating"
#: Not under `source/`: a kind is not a value a template can name, it is what
#: decides which sources exist.
KIND_KEY: Final[str] = f"{_NAMESPACE}/kind"
SOURCE_NAMESPACE: Final[str] = f"{_NAMESPACE}/source"
ARGUMENT_NAMESPACE: Final[str] = f"{_NAMESPACE}/argument"


def _fit(value: object) -> str:
    """Cut a value to the column width, marked so a cut never reads as whole.

    "2026-09-02"  -> "2026-09-02"
    "x" * 300     -> 242 x's followed by "...[truncated]"
    None          -> "None"   (see below)

    Takes `object`, not `str`, though the maps it reads are typed `str`. `render` is
    deliberately tolerant of a stored template that is not a string -- it records a
    per-key render failure and lets the other keys through -- and describing a firing
    must not be stricter than performing it, or a row a direct DB edit made malformed
    would start no run at all instead of one run with one bad key.
    """
    text = value if isinstance(value, str) else str(value)
    if len(text) <= _MAX_LENGTH:
        return text
    return text[: _MAX_LENGTH - len(_TRUNCATION_MARKER)] + _TRUNCATION_MARKER


def argument_key(*, name: str, facet: str) -> str:
    """Build the annotation key for one facet of one argument.

    name="as_of_date", facet="rendered"
        -> "tangleml.com/templating/argument/as_of_date/rendered"
    """
    return f"{ARGUMENT_NAMESPACE}/{name}/{facet}"


def source_key(*, name: str) -> str:
    """Build the annotation key for one clock source.

    name="now" -> "tangleml.com/templating/source/now"
    """
    return f"{SOURCE_NAMESPACE}/{name}"


def for_firing(
    *,
    templates: Mapping[str, str],
    rendered: rendering.Rendered,
) -> dict[str, str]:
    """Describe one firing's rendering as one annotation per fact.

    Subject before facet, so a prefix LIKE gathers one argument's whole story, and
    the same shape answers "every source this render could read".

    An argument name long enough to overflow the key column is skipped rather than
    truncated: a cut key would collide with its neighbours, and an oversized one
    would fail the insert and lose the run.

    The clock is read off the result rather than passed in, so a source that is
    annotated and a source that was rendered cannot disagree.

    a cron clock
        -> .../kind cron, and .../source/{schedule_time,trigger_time,now}
    the same schedule fired by hand
        -> .../kind manual, and no .../source/schedule_time
    a subscription or hand-fired clock
        -> no .../source/schedule_time; the other two are always present
    templates={"day": "{{ now | date }}"}, rendered day="2026-09-02"
        -> .../argument/day/template  and  .../argument/day/rendered
    a key in `failures`
        -> .../argument/<name>/render_error, and no `rendered` for it
    """
    clock = rendered.clock
    if clock is None:
        return {}
    annotations: dict[str, str] = {
        KIND_KEY: clock.kind.value,
        source_key(name=sources.TimeSource.TRIGGER_TIME.value): (
            clock.trigger_time.isoformat()
        ),
        source_key(name=sources.TimeSource.NOW.value): clock.now.isoformat(),
    }
    if clock.schedule_time is not None:
        annotations[source_key(name=sources.TimeSource.SCHEDULE_TIME.value)] = (
            clock.schedule_time.isoformat()
        )

    facets: list[tuple[str, Mapping[str, str]]] = [
        ("template", templates),
        ("rendered", rendered.arguments),
        ("render_error", rendered.failures),
    ]
    for facet, source in facets:
        for name, value in source.items():
            key = argument_key(name=name, facet=facet)
            if len(key) <= _MAX_LENGTH:
                annotations[key] = _fit(value)
    return annotations
