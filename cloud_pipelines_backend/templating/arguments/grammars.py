"""What a template value is, and whether its expression obeys the grammar.

The grammar is a *restriction* of Jinja2, so it is enforced by walking Jinja2's own AST and
refusing everything the allowlist does not name. A lookalike parser would have to be proven
equivalent to the syntax users already expect; this is correct by construction, because
anything not explicitly permitted falls through to a rejection.

Every function takes a `key` -- the pipeline argument being templated, such as
`as_of_date` -- and a `value`, which is what the user wrote for it. The key is carried
only so it can be named in messages: one request submits a map of templates, so
"unknown filter 'upper'" on its own would leave the caller guessing which one.

Pure: no clock, no database, no schedule kind. Whether a source is available on *this* kind
of schedule is a separate question, and lives with the sources.

Four checks, three of them from implementation.md section 3.4 and the fourth falling out of
having the AST at all:

    1  the base is a clock source, or coalesce(...) over clock sources
    2  filters are known, operators precede formatters, at most one formatter
    3  the source is available for this schedule kind      -- not here; needs the kind
    4  operator arguments are literals, so they are checkable now rather than at 02:00

What that adds up to, as a user sees it:

    {{ trigger_time | shift('-2d') | date }}   accepted
    {{ coalesce(schedule_time, now) }}         accepted
    {{ trigger_time | date | shift('-2d') }}   rejected, operator after a formatter
    {{ "2026-09-01" | date }}                  rejected, the base is not a clock
    {{ trigger_time.year }}                    rejected, attribute access is not in the grammar
    {% if x %}a{% endif %}                     rejected, not a value at all
"""

import dataclasses

from jinja2 import nodes
from jinja2.exceptions import TemplateSyntaxError

from cloud_pipelines_backend.templating.arguments import (
    engines,
    errors,
    filters,
    sources,
)


@dataclasses.dataclass(frozen=True)
class Empty:
    """No value. Renders to the empty string without going near the clock.

    "" -> Empty()
    """


@dataclasses.dataclass(frozen=True)
class Constant:
    """Literal text with no expression in it. Renders to itself.

    "2026-09-01" -> Constant(text="2026-09-01")
    "  Run A  "  -> Constant(text="  Run A  ")    whitespace survives, it is the value
    """

    text: str


@dataclasses.dataclass(frozen=True)
class Expression:
    """Exactly one `{{ }}` and nothing besides it.

    "{{ schedule_time }}"          -> Expression(node=Name('schedule_time'))
    "{{ trigger_time | date }}"    -> Expression(node=Filter('date'))
    """

    node: nodes.Node


Classification = Empty | Constant | Expression


def classify(*, key: str, value: str) -> Classification:
    """Decide what a value is from its parse tree, not from a search for `{{`.

    A substring test gets both hard cases wrong: `{% if x %}a{% endif %}` contains no `{{`
    and must still be rejected, and `run-{{ t | date }}` contains one but is a
    concatenation.

    Each row below is one `value`; the `key` never changes what is returned, only what
    the message says.

    value=""                              -> Empty()
    value="2026-09-01"                    -> Constant(text="2026-09-01")
    value="{{ schedule_time }}"           -> Expression(node=Name('schedule_time'))
    value="run-{{ trigger_time | date }}" -> raises
    value="{% if x %}a{% endif %}"        -> raises, though it holds no {{
    value="prod{#-canary-#}"              -> raises, a comment Jinja would strip

    With key="as_of_date", that last one reads:

        Invalid template for 'as_of_date': a value is either a plain string or one
        {{ }} expression, not a mix
    """
    body = _parse(key=key, value=value).body
    if not body:
        return Empty()
    # Anything that is not a single Output -- an {% if %}, a {% for %} -- is not a value.
    if len(body) != 1 or not isinstance(body[0], nodes.Output):
        raise errors.mixed_literal_and_expression(key=key)

    output = body[0].nodes
    if len(output) != 1:
        raise errors.mixed_literal_and_expression(key=key)
    if isinstance(output[0], nodes.TemplateData):
        #: A constant must survive rendering unchanged. `{# #}` and `{% raw %}` leave one
        #: Output node holding only the surrounding text, so the value would be stored as
        #: typed and then render shorter.
        if output[0].data != value:
            raise errors.mixed_literal_and_expression(key=key)
        return Constant(text=output[0].data)
    return Expression(node=output[0])


def check_grammar(*, key: str, expression: nodes.Node) -> None:
    """Raise on the first thing outside the allowlist; return None when it all fits.

    trigger_time | shift('-2d') | date   -> None
    trigger_time | date | shift('-2d')   -> raises, operator after formatter
    """
    base, chain = _unwrap_filters(expression)
    _check_base(key=key, node=base)
    _check_chain(key=key, chain=chain)


def check_availability(*, key: str, expression: nodes.Node, kind: sources.Kind) -> None:
    """Save time: refuse a clock this kind of row can never have. Separate from
    `check_grammar` because the grammar is the same everywhere and this is not.

    Coalesce arms are checked too -- otherwise `coalesce(schedule_time, trigger_time)` is a
    way to write a banned source and have it silently fall through on every single fire.

    kind=CRON          {{ schedule_time | date }}                    -> None
    kind=CRON          {{ coalesce(schedule_time, trigger_time) }}   -> None
    kind=SUBSCRIPTION  {{ trigger_time | date }}                     -> None
    kind=SUBSCRIPTION  {{ schedule_time | date }}                    -> raises
    kind=SUBSCRIPTION  {{ coalesce(schedule_time, trigger_time) }}   -> raises
    """
    if kind not in sources.SAVE_KINDS:
        raise ValueError(
            f"{kind.value} is a render-time kind; nothing is saved under it"
        )
    available = sources.AVAILABLE[kind]
    allowed = sorted(source.value for source in available)
    for name in sorted(_source_names_in(expression)):
        if sources.TimeSource(name) not in available:
            raise errors.source_not_allowed(
                key=key, source=name, kind=kind.value, allowed=allowed
            )


def _source_names_in(expression: nodes.Node) -> set[str]:
    """Every clock the expression names, whether directly or as a coalesce arm.

    trigger_time | date                  -> {"trigger_time"}
    coalesce(schedule_time, trigger_time) -> {"schedule_time", "trigger_time"}
    """
    base, _ = _unwrap_filters(expression)
    if isinstance(base, nodes.Name):
        return {base.name}
    if isinstance(base, nodes.Call):
        return {arg.name for arg in base.args if isinstance(arg, nodes.Name)}
    return set()


def _parse(*, key: str, value: str) -> nodes.Template:
    """Jinja2's parser, with its syntax error rewritten as ours so the key is named.

    "{{ schedule_time }}" -> Template(...)
    "{{ schedule_time"    -> raises, "unexpected end of template, expected 'end of print
                             statement'."
    """
    try:
        return engines.ENGINE.parse(value)
    except TemplateSyntaxError as error:
        # Jinja2's own message is more specific than anything written here.
        raise errors.unparseable(key=key, message=error.message or str(error)) from None


def _unwrap_filters(node: nodes.Node) -> tuple[nodes.Node, list[nodes.Filter]]:
    """Peel the filter chain off the base, reversed into the order a reader writes it.

    Jinja2 nests the *last* filter outermost, so the raw walk arrives backwards.

    trigger_time | shift('-2d') | date -> (Name('trigger_time'), [shift, date])
    schedule_time                      -> (Name('schedule_time'), [])
    """
    chain: list[nodes.Filter] = []
    while isinstance(node, nodes.Filter):
        chain.append(node)
        node = node.node
    chain.reverse()
    return node, chain


def _check_base(*, key: str, node: nodes.Node) -> None:
    """The chain has to start at a clock.

    trigger_time                          -> None
    coalesce(schedule_time, trigger_time) -> None
    "2026-09-01"                          -> raises, got a literal
    max(trigger_time)                     -> raises, got a call
    """
    if isinstance(node, nodes.Name):
        _check_source_name(key=key, name=node.name)
        return
    if isinstance(node, nodes.Call) and isinstance(node.node, nodes.Name):
        _check_call(key=key, call=node)
        return
    raise errors.base_is_not_a_source(key=key, got=_describe(node))


def _check_call(*, key: str, call: nodes.Call) -> None:
    """`coalesce` is the only function in the grammar. Every other call is one of two
    mistakes, told apart because a filter written as a function is bad syntax around a name
    that does exist -- calling it unknown would be a lie, and would not say what to write.

    {{ coalesce(schedule_time, now) }} -> accepted
    {{ coalese(schedule_time, now) }}  -> unknown function 'coalese'; the only one is coalesce
    {{ shift(trigger_time, '-2d') }}   -> 'shift' is a filter, not a function; write trigger_time | shift('-2d')
    {{ date(trigger_time) }}           -> 'date' is a filter, not a function; write trigger_time | date
    {{ max(trigger_time) }}            -> unknown function 'max'; the only one is coalesce

    Each message is prefixed "Invalid template for '<key>': " before it reaches the caller.
    """
    name = call.node.name
    if name == sources.COALESCE:
        _check_coalesce(key=key, call=call)
        return
    if name in filters.OPERATOR_NAMES | filters.FORMATTER_NAMES:
        raise errors.filter_used_as_a_function(
            key=key, name=name, suggestion=_as_filter_suggestion(call)
        )
    raise errors.unknown_function(key=key, name=name, only=sources.COALESCE)


def _as_filter_suggestion(call: nodes.Call) -> str:
    """Rewrite the call the user wrote as the filter they meant, using their own arguments.

    shift(trigger_time, '-2d') -> "trigger_time | shift('-2d')"
    date(trigger_time)         -> "trigger_time | date"
    shift()                    -> "trigger_time | shift(...)"    nothing to reuse
    """
    subject = (
        call.args[0].name
        if call.args and isinstance(call.args[0], nodes.Name)
        else "trigger_time"
    )
    rest = ", ".join(
        repr(argument.value) if isinstance(argument, nodes.Const) else "..."
        for argument in call.args[1:]
    )
    if not call.args:
        rest = "..."
    return (
        f"{subject} | {call.node.name}({rest})"
        if rest
        else f"{subject} | {call.node.name}"
    )


def _check_coalesce(*, key: str, call: nodes.Call) -> None:
    """Sources only, positional only.

    coalesce(schedule_time, trigger_time) -> None
    coalesce()                            -> raises, no clock sources
    coalesce("x")                         -> raises, got a literal
    coalesce(triger_time)                 -> raises, unknown source
    """
    if not call.args or call.kwargs or call.dyn_args or call.dyn_kwargs:
        raise errors.base_is_not_a_source(
            key=key, got=f"{sources.COALESCE} with no clock sources"
        )
    for argument in call.args:
        if not isinstance(argument, nodes.Name):
            raise errors.base_is_not_a_source(key=key, got=_describe(argument))
        _check_source_name(key=key, name=argument.name)


def _check_source_name(*, key: str, name: str) -> None:
    """Validate that a name is one of the clocks.

    'trigger_time' -> None
    'triger_time'  -> raises, naming the typo back
    """
    if name not in sources.TIME_SOURCE_NAMES:
        raise errors.unknown_source(key=key, name=name)


def _describe(node: nodes.Node) -> str:
    """How a rejected base is named back to the user.

    Only reached once the base has already failed, so every row here is a rejection. Two
    outcomes, not three: a call never arrives here, because `_check_call` names it.

    {{ "2026-09-01" | date }}   Const   -> "a literal"
    {{ trigger_time.year }}     Getattr -> "an expression"
    {{ 1 + 2 }}                 Add     -> "an expression"
    """
    if isinstance(node, nodes.Const):
        return "a literal"
    return "an expression"


def _check_chain(*, key: str, chain: list[nodes.Filter]) -> None:
    """Operators may repeat and must come first; one formatter may end the chain.

    [shift, shift, timezone, rfc3339] -> None
    [date, shift]                     -> raises, operator after formatter
    [date, rfc3339]                   -> raises, two formatters
    [upper]                           -> raises, unknown filter
    """
    formatter: str | None = None
    for node in chain:
        if node.name in filters.OPERATOR_NAMES:
            if formatter is not None:
                raise errors.operator_after_formatter(
                    key=key, operator=node.name, formatter=formatter
                )
            _check_operator_call(key=key, node=node)
        elif node.name in filters.FORMATTER_NAMES:
            if formatter is not None:
                raise errors.two_formatters(key=key, first=formatter, second=node.name)
            _check_formatter_call(key=key, node=node)
            formatter = node.name
        else:
            raise errors.unknown_filter(key=key, name=node.name)


def _check_operator_call(*, key: str, node: nodes.Filter) -> None:
    """One literal argument, checked now rather than at 02:00.

    Nothing is in scope for a name to refer to, and a non-literal could not be checked
    until it was too late to return a 422.

    shift('-2d')        -> None
    shift('-2 days')    -> raises, shift's own message
    shift(x) | shift    -> raises, takes exactly one literal argument
    shift(2)            -> a literal, so shift itself refuses it
    """
    if len(node.args) != 1 or node.kwargs or node.dyn_args or node.dyn_kwargs:
        raise errors.operator_argument_is_not_a_literal(key=key, operator=node.name)
    argument = node.args[0]
    if not isinstance(argument, nodes.Const):
        raise errors.operator_argument_is_not_a_literal(key=key, operator=node.name)
    filters.check_operator_argument(
        key=key, operator=node.name, argument=str(argument.value)
    )


def _check_formatter_call(*, key: str, node: nodes.Filter) -> None:
    """Validates formatter calls.

    `date`      -> None
    `date('x')` -> raises: a formatter takes no argument.
    """
    if node.args or node.kwargs or node.dyn_args or node.dyn_kwargs:
        raise errors.formatter_takes_no_argument(key=key, formatter=node.name)
