"""Why a template was rejected, and the message the API returns for it.

One exception type, because nothing distinguishes them by class: the API catches
`TemplateError` and returns `detail` as a 422. What is worth keeping apart is the
*messages*, so they are builders here rather than subclasses.

Every message names its key. A request carries a map of templates, so "unknown timezone
'America/Torono'" without a key leaves the caller to guess which one is wrong.

Only the causes something raises today. The rest of section 2.4's table arrives with the
code that raises it -- a builder with no caller drifts from the message it is supposed to
produce, and nothing goes red.

Nearly all are raised at save time, by validation. `source_unavailable` is the one that
cannot be: whether a schedule_time exists depends on how the run was started, and a cron
schedule fired by hand has none. It is raised while rendering and caught there -- the failure is
recorded against its key and the run continues; see implementation.md section 4.6.
"""

from collections.abc import Sequence


class TemplateError(Exception):
    """A rejected template. `detail` is the 422 body; `code` is the bounded name.

    `detail` names the key and quotes the offending text, so it is unbounded and
    cannot be a metric label. `code` is the builder's own name, a closed set.
    """

    def __init__(self, *, key: str, detail: str, code: str) -> None:
        self.key = key
        self.detail = detail
        self.code = code
        super().__init__(detail)


def _body(key: str, reason: str, code: str) -> TemplateError:
    """Something wrong inside the `{{ }}`. One prefix, so the messages line up."""
    return TemplateError(
        key=key, detail=f"Invalid template for {key!r}: {reason}", code=code
    )


def unparseable(*, key: str, message: str) -> TemplateError:
    """`message` is Jinja2's own, which is more specific than anything written here."""
    return _body(key, message, "unparseable")


def mixed_literal_and_expression(*, key: str) -> TemplateError:
    """Covers every shape that is not one bare expression: `run-{{ t | date }}`, and also
    `{% if %}` / `{% for %}`, which have no `{{ }}` at all."""
    return _body(
        key,
        "a value is either a plain string or one {{ }} expression, not a mix",
        "mixed_literal_and_expression",
    )


def unknown_filter(*, key: str, name: str) -> TemplateError:
    return _body(key, f"unknown filter {name!r}", "unknown_filter")


def operator_after_formatter(
    *, key: str, operator: str, formatter: str
) -> TemplateError:
    """A formatter returns a string, so nothing datetime-shaped can follow it."""
    return _body(
        key,
        f"operator {operator!r} after formatter {formatter!r}",
        "operator_after_formatter",
    )


def two_formatters(*, key: str, first: str, second: str) -> TemplateError:
    return _body(key, f"two formatters, {first!r} and {second!r}", "two_formatters")


def base_is_not_a_source(*, key: str, got: str) -> TemplateError:
    """The chain has to start at a clock source; `{{ "2026-09-01" | date }}` does not."""
    return _body(key, f"expected a clock source, got {got}", "base_is_not_a_source")


def unknown_source(*, key: str, name: str) -> TemplateError:
    return _body(key, f"unknown source {name!r}", "unknown_source")


def operator_argument_is_not_a_literal(*, key: str, operator: str) -> TemplateError:
    """`shift(x)` or a bare `shift`. Nothing is in scope for a name to refer to, and a
    non-literal could not be checked at save time, which is the whole point of checking.

    Not in implementation.md section 2.4; section 3.4 asserts the check exists without
    giving it a message. Recorded in section 6.
    """
    return _body(
        key,
        f"{operator}(...) takes exactly one literal argument",
        "operator_argument_is_not_a_literal",
    )


def formatter_takes_no_argument(*, key: str, formatter: str) -> TemplateError:
    """`date('x')`. Also absent from section 2.4; recorded in section 6."""
    return _body(key, f"{formatter} takes no argument", "formatter_takes_no_argument")


def source_not_allowed(
    *, key: str, source: str, kind: str, allowed: Sequence[str]
) -> TemplateError:
    """Save time: the template names a clock this kind of row can never have. Distinct from
    `source_unavailable` -- that one suggests `coalesce` as the portable form, which would
    be wrong advice here, because a coalesce over the same banned source is also refused.

    source='schedule_time', kind='subscription', allowed=['now', 'trigger_time']
        -> 'schedule_time' is not available for a subscription; available sources are
           now, trigger_time

    Not in implementation.md section 2.4; recorded in section 6.
    """
    return _body(
        key,
        f"{source!r} is not available for a {kind}; available sources are {', '.join(allowed)}",
        "source_not_allowed",
    )


def source_unavailable(*, key: str, sources: Sequence[str], kind: str) -> TemplateError:
    """A clock the template names does not exist for this kind of run -- in practice always
    `schedule_time`, since only a scheduler fire has one.

    One source reads as a suggestion because there is a portable form to suggest; several
    means every arm of a `coalesce` was unavailable, and repeating them is the answer.

    sources=['schedule_time'], kind='subscription'
        -> 'schedule_time' is not available for a subscription run;
           coalesce(schedule_time, trigger_time) is the portable form
    sources=['schedule_time', 'schedule_time'], kind='manual'
        -> no source in coalesce(schedule_time, schedule_time) is available for a manual run

    Not in implementation.md section 2.4; recorded in section 6.
    """
    if len(sources) == 1:
        return _body(
            key,
            f"{sources[0]!r} is not available for a {kind} run; "
            f"coalesce({sources[0]}, trigger_time) is the portable form",
            "source_unavailable",
        )
    joined = ", ".join(sources)
    return _body(
        key,
        f"no source in coalesce({joined}) is available for a {kind} run",
        "source_unavailable",
    )


def unknown_function(*, key: str, name: str, only: str) -> TemplateError:
    """`max(...)`, or a misspelled `coalese(...)`. The grammar has exactly one function, so
    the message can name it rather than leaving the caller to guess.

    `only` is passed in because the name belongs to the grammar, not to this module.
    Not in implementation.md section 2.4; recorded in section 6.
    """
    return _body(
        key,
        f"unknown function {name!r}; the only one is {only}",
        "unknown_function",
    )


def filter_used_as_a_function(*, key: str, name: str, suggestion: str) -> TemplateError:
    """`shift(trigger_time, '-2d')`. The filter exists, so saying "unknown" would be a lie;
    what is wrong is the syntax, and `suggestion` is the same call rewritten.

    Not in implementation.md section 2.4; recorded in section 6.
    """
    return _body(
        key,
        f"{name!r} is a filter, not a function; write {suggestion}",
        "filter_used_as_a_function",
    )


def bad_operator_argument(
    *, key: str, operator: str, argument: str, description: str, example: str
) -> TemplateError:
    """Caught at save time, so a bad offset cannot reach a fire path."""
    return _body(
        key,
        f"{operator}({argument!r}) is not {description}; expected e.g. {example!r}",
        "bad_operator_argument",
    )


def arguments_not_a_map_of_strings(*, key: str | None, got: str) -> TemplateError:
    """The envelope's `arguments` is not an object, or one of its values is not a string.

    key=None,      got="list" -> ... must be a map of string to string; got list
    key="retries", got="int"  -> ... must be a map of string to string; 'retries' is int
    """
    reason = f"{key!r} is {got}" if key is not None else f"got {got}"
    return TemplateError(
        key=key or "arguments",
        detail=f"pipeline_templates.arguments must be a map of string to string; {reason}",
        code="arguments_not_a_map_of_strings",
    )


def render_failed(*, key: str, message: str) -> TemplateError:
    """The catch-all for a render that failed some other way -- a template stored before a
    validator existed, or edited around the API. Section 4.6 requires the run to proceed,
    so every exception has to end up as text rather than as a raised error."""
    return _body(key, f"could not be rendered: {message}", "render_failed")


def unknown_timezone(*, key: str, name: str) -> TemplateError:
    return _body(key, f"unknown timezone {name!r}", "unknown_timezone")
