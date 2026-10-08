"""The grammar is a restriction of Jinja2, so what matters is what it *refuses*.

Every rejection asserts the message, not just the type: `implementation.md` section 2.4
publishes these strings as the 422 body, so a reworded message is an API change.
"""

import pytest
from jinja2 import nodes

from cloud_pipelines_backend.templating.arguments import errors, grammars, sources

_KEY = "as_of_date"


def _check(value: str) -> grammars.Classification:
    """Classify and, when there is an expression, walk it. What the API will call."""
    classified = grammars.classify(key=_KEY, value=value)
    if isinstance(classified, grammars.Expression):
        grammars.check_grammar(key=_KEY, expression=classified.node)
    return classified


class TestClassificationComesFromTheParseTree:
    """Not from a search for `{{`: one hard case has no braces and the other has braces
    but is a concatenation."""

    def test_an_empty_value_is_empty(self) -> None:
        assert _check("") == grammars.Empty()

    @pytest.mark.parametrize("value", ["plain", "2026-09-01", "a b c"])
    def test_text_without_an_expression_is_constant(self, value: str) -> None:
        assert _check(value) == grammars.Constant(text=value)

    def test_one_bare_expression_is_an_expression(self) -> None:
        classified = _check("{{ schedule_time }}")

        assert isinstance(classified, grammars.Expression)
        assert isinstance(classified.node, nodes.Name)

    def test_a_constant_keeps_its_exact_text(self) -> None:
        """It renders to itself, so whitespace and case have to survive classification."""
        assert _check("  Run A  ") == grammars.Constant(text="  Run A  ")


class TestTheShapesThatAreNotAValue:
    def test_text_concatenated_with_an_expression_is_rejected(self) -> None:
        with pytest.raises(errors.TemplateError) as caught:
            _check("run-{{ trigger_time | date }}")

        assert caught.value.detail == (
            "Invalid template for 'as_of_date': a value is either a plain string or one "
            "{{ }} expression, not a mix"
        )

    @pytest.mark.parametrize(
        "value",
        [
            "{% if x %}a{% endif %}",
            "{% for x in y %}a{% endfor %}",
            "{% set x = 1 %}",
        ],
        ids=["if", "for", "set"],
    )
    def test_a_statement_is_rejected_though_it_has_no_braces(self, value: str) -> None:
        """The reason classification is structural: none of these contain `{{`."""
        with pytest.raises(errors.TemplateError, match="not a mix"):
            _check(value)

    def test_an_unparseable_value_reports_jinjas_own_message(self) -> None:
        with pytest.raises(errors.TemplateError) as caught:
            _check("{{ schedule_time")

        assert caught.value.detail.startswith(
            "Invalid template for 'as_of_date': unexpected end of template"
        )

    def test_the_key_is_named_in_every_message(self) -> None:
        """A request carries a map of templates; a message without its key is a guess."""
        with pytest.raises(errors.TemplateError) as caught:
            grammars.classify(key="other_key", value="{{ a }}{{ b }}")

        assert "other_key" in caught.value.detail
        assert caught.value.key == "other_key"


class TestTheBaseMustBeAClockSource:
    @pytest.mark.parametrize("source", ["schedule_time", "trigger_time", "now"])
    def test_each_source_is_accepted_bare(self, source: str) -> None:
        assert isinstance(_check(f"{{{{ {source} }}}}"), grammars.Expression)

    def test_a_literal_base_is_rejected(self) -> None:
        with pytest.raises(errors.TemplateError) as caught:
            _check('{{ "2026-09-01" | date }}')

        assert caught.value.detail == (
            "Invalid template for 'as_of_date': expected a clock source, got a literal"
        )

    def test_a_misspelled_source_is_rejected_by_name(self) -> None:
        with pytest.raises(errors.TemplateError) as caught:
            _check("{{ triger_time }}")

        assert caught.value.detail == (
            "Invalid template for 'as_of_date': unknown source 'triger_time'"
        )

    @pytest.mark.parametrize(
        "arguments",
        [
            "schedule_time",
            "now",
            "schedule_time, trigger_time",
            "trigger_time, schedule_time",
            "now, now",
            "schedule_time, trigger_time, now",
            "now, trigger_time, schedule_time",
        ],
        ids=[
            "one",
            "one-now",
            "two",
            "two-reversed",
            "repeated",
            "three",
            "three-reordered",
        ],
    )
    def test_coalesce_accepts_any_number_and_order_of_sources(
        self, arguments: str
    ) -> None:
        """Arity and order are the caller's business: `coalesce` picks the first source
        that has a value, so the grammar has no reason to constrain either."""
        assert isinstance(
            _check(f"{{{{ coalesce({arguments}) }}}}"), grammars.Expression
        )

    def test_a_coalesce_keeps_its_arguments_in_written_order(self) -> None:
        """Which source wins at render time is decided by this order, so it has to survive
        parsing intact."""
        classified = _check("{{ coalesce(schedule_time, trigger_time, now) }}")

        assert isinstance(classified, grammars.Expression)
        assert [argument.name for argument in classified.node.args] == [
            "schedule_time",
            "trigger_time",
            "now",
        ]

    @pytest.mark.parametrize(
        "value",
        [
            "{{ coalesce() | date }}",
            '{{ coalesce("x") | date }}',
            "{{ coalesce(triger_time) }}",
        ],
        ids=["no-args", "literal-arg", "misspelled-arg"],
    )
    def test_coalesce_takes_clock_sources_and_nothing_else(self, value: str) -> None:
        with pytest.raises(errors.TemplateError):
            _check(value)

    def test_an_unknown_function_names_the_only_one_there_is(self) -> None:
        with pytest.raises(errors.TemplateError) as caught:
            _check("{{ max(trigger_time) | date }}")

        assert caught.value.detail == (
            "Invalid template for 'as_of_date': unknown function 'max'; "
            "the only one is coalesce"
        )

    def test_a_misspelled_coalesce_is_told_apart_from_a_bad_base(self) -> None:
        """It used to land on "got a call", the least useful message of the set, even
        though the analogous typo in a source name is named back precisely."""
        with pytest.raises(errors.TemplateError) as caught:
            _check("{{ coalese(schedule_time, now) }}")

        assert caught.value.detail == (
            "Invalid template for 'as_of_date': unknown function 'coalese'; "
            "the only one is coalesce"
        )

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("{{ shift(trigger_time, '-2d') }}", "trigger_time | shift('-2d')"),
            ("{{ date(trigger_time) }}", "trigger_time | date"),
            ("{{ rfc3339(schedule_time) }}", "schedule_time | rfc3339"),
            ("{{ shift() }}", "trigger_time | shift(...)"),
        ],
        ids=["operator", "formatter", "other-source", "no-arguments"],
    )
    def test_a_filter_written_as_a_function_is_rewritten_not_disowned(
        self, value: str, expected: str
    ) -> None:
        """The filter exists, so "unknown" would be a lie. The suggestion is built from the
        user's own arguments, which is why it names their source and their offset."""
        with pytest.raises(errors.TemplateError) as caught:
            _check(value)

        assert caught.value.detail.endswith(
            f"is a filter, not a function; write {expected}"
        )

    def test_a_call_never_reaches_the_describe_fallback(self) -> None:
        """_describe has two outcomes because every Name-callee call is named by
        _check_call first. A call reported as "a call" would mean that routing broke."""
        for value in [
            "{{ max(trigger_time) }}",
            "{{ coalese(now) }}",
            "{{ date(now) }}",
        ]:
            with pytest.raises(errors.TemplateError) as caught:
                _check(value)

            assert "got a call" not in caught.value.detail


def _chain(value: str) -> list[str]:
    """The filter names a value accepts, in written order. Asserting on this rather than on
    "it did not raise" is what makes a positive case prove something."""
    classified = _check(value)
    assert isinstance(classified, grammars.Expression)
    _, chain = grammars._unwrap_filters(classified.node)
    return [node.name for node in chain]


_BASES = [
    "trigger_time",
    "coalesce(schedule_time, trigger_time)",
    "coalesce(schedule_time, trigger_time, now)",
]


@pytest.mark.parametrize("base", _BASES, ids=["source", "coalesce", "coalesce-three"])
class TestEveryChainShapeAgainstEveryBase:
    """The base and the chain are checked independently, so a coalesce has to accept
    exactly the chains a bare source accepts. Running both through the same matrix is what
    proves that, rather than one example of each."""

    def test_one_operator_and_no_formatter(self, base: str) -> None:
        assert _chain(f"{{{{ {base} | shift('-1d') }}}}") == ["shift"]

    def test_the_same_operator_twice(self, base: str) -> None:
        """Repetition specifically -- two *different* operators would not prove it."""
        assert _chain(f"{{{{ {base} | shift('-1d') | shift('-2h') }}}}") == [
            "shift",
            "shift",
        ]

    def test_several_operators_and_no_formatter(self, base: str) -> None:
        value = (
            f"{{{{ {base} | shift('-1d') | truncate_time('1h') | timezone('UTC') }}}}"
        )

        assert _chain(value) == ["shift", "truncate_time", "timezone"]

    def test_several_operators_then_one_formatter(self, base: str) -> None:
        """The chain `_check_chain`'s own docstring uses."""
        value = f"{{{{ {base} | shift('-1d') | shift('-1d') | timezone('UTC') | rfc3339 }}}}"

        assert _chain(value) == ["shift", "shift", "timezone", "rfc3339"]

    @pytest.mark.parametrize("formatter", ["date", "rfc3339", "epoch_seconds"])
    def test_any_formatter_may_terminate_a_multi_operator_chain(
        self, base: str, formatter: str
    ) -> None:
        value = f"{{{{ {base} | shift('-1d') | timezone('UTC') | {formatter} }}}}"

        assert _chain(value) == ["shift", "timezone", formatter]

    @pytest.mark.parametrize("formatter", ["date", "rfc3339", "epoch_seconds"])
    def test_a_formatter_alone(self, base: str, formatter: str) -> None:
        assert _chain(f"{{{{ {base} | {formatter} }}}}") == [formatter]

    def test_every_operator_is_accepted_on_its_own(self, base: str) -> None:
        for operator, argument in [
            ("shift", "-1d"),
            ("truncate_time", "4h"),
            ("timezone", "Asia/Kolkata"),
        ]:
            assert _chain(f"{{{{ {base} | {operator}('{argument}') }}}}") == [operator]


class TestChainsWithoutFilters:
    @pytest.mark.parametrize(
        "base", _BASES, ids=["source", "coalesce", "coalesce-three"]
    )
    def test_a_base_needs_no_chain_at_all(self, base: str) -> None:
        assert _chain(f"{{{{ {base} }}}}") == []


class TestFilterOrder:
    def test_an_unknown_filter_is_rejected_by_name(self) -> None:
        with pytest.raises(errors.TemplateError) as caught:
            _check("{{ trigger_time | upper }}")

        assert caught.value.detail == (
            "Invalid template for 'as_of_date': unknown filter 'upper'"
        )

    def test_an_operator_after_a_formatter_is_rejected(self) -> None:
        """A formatter returns a string, so nothing datetime-shaped can follow it."""
        with pytest.raises(errors.TemplateError) as caught:
            _check("{{ trigger_time | date | shift('-2d') }}")

        assert caught.value.detail == (
            "Invalid template for 'as_of_date': operator 'shift' after formatter 'date'"
        )

    def test_two_formatters_are_rejected(self) -> None:
        with pytest.raises(errors.TemplateError) as caught:
            _check("{{ trigger_time | date | rfc3339 }}")

        assert caught.value.detail == (
            "Invalid template for 'as_of_date': two formatters, 'date' and 'rfc3339'"
        )

    def test_the_chain_is_walked_in_written_order(self) -> None:
        """Jinja2 nests filters outermost-last, so the walk reverses them. If it did not,
        this would report 'rfc3339 after date' -- the wrong pair, in the wrong order."""
        with pytest.raises(errors.TemplateError) as caught:
            _check("{{ trigger_time | date | rfc3339 }}")

        assert "'date' and 'rfc3339'" in caught.value.detail


class TestOperatorArgumentsAreCheckedAtSaveTime:
    """Check 4 of section 3.4: the argument is a literal, so `shift('-2 days')` is a 422 at
    create rather than a render failure at 02:00."""

    def test_a_bad_offset_is_rejected_with_the_operators_own_message(
        self,
    ) -> None:
        with pytest.raises(errors.TemplateError) as caught:
            _check("{{ trigger_time | shift('-2 days') }}")

        assert caught.value.detail == (
            "Invalid template for 'as_of_date': shift('-2 days') is not an offset; "
            "expected e.g. '-2d'"
        )

    def test_a_bad_timezone_is_rejected_by_name(self) -> None:
        with pytest.raises(errors.TemplateError) as caught:
            _check("{{ trigger_time | timezone('America/Torono') }}")

        assert caught.value.detail == (
            "Invalid template for 'as_of_date': unknown timezone 'America/Torono'"
        )

    @pytest.mark.parametrize(
        "value",
        [
            "{{ trigger_time | shift(x) }}",
            "{{ trigger_time | shift }}",
            "{{ trigger_time | shift('-1d', '-2d') }}",
        ],
        ids=["name", "no-argument", "two-arguments"],
    )
    def test_an_argument_that_is_not_one_literal_is_rejected(self, value: str) -> None:
        """Nothing is in scope for a name to refer to, and a non-literal could not be
        checked until it was too late to return a 422."""
        with pytest.raises(errors.TemplateError) as caught:
            _check(value)

        assert caught.value.detail == (
            "Invalid template for 'as_of_date': shift(...) takes exactly one literal argument"
        )

    def test_a_formatter_given_an_argument_is_rejected(self) -> None:
        with pytest.raises(errors.TemplateError) as caught:
            _check("{{ trigger_time | date('x') }}")

        assert caught.value.detail == (
            "Invalid template for 'as_of_date': date takes no argument"
        )

    def test_a_non_string_literal_reaches_the_operators_own_check(self) -> None:
        """`shift(2)` is a literal, so it passes check 4 and is refused by shift itself."""
        with pytest.raises(errors.TemplateError, match="is not an offset"):
            _check("{{ trigger_time | shift(2) }}")


class TestTheSandboxIsOn:
    def test_attribute_access_does_not_reach_python(self) -> None:
        """The walk rejects it before the sandbox is ever asked, which is the point: the
        sandbox is the second line, not the first."""
        with pytest.raises(errors.TemplateError):
            _check("{{ trigger_time.__class__ }}")

    def test_coalesce_is_not_a_source(self) -> None:
        """It combines sources, so a bare `{{ coalesce }}` has to stay invalid. The enum
        itself is pinned in test_sources.py; what this asserts is the grammar's reaction.
        """
        with pytest.raises(errors.TemplateError, match="unknown source 'coalesce'"):
            _check("{{ coalesce }}")


class TestAvailabilityAtSaveTime:
    """`check_grammar` is the same everywhere; this is the part that is not."""

    def _check_for(self, value: str, kind: sources.Kind) -> None:
        classified = grammars.classify(key="as_of_date", value=value)
        assert isinstance(classified, grammars.Expression)
        grammars.check_availability(
            key="as_of_date", expression=classified.node, kind=kind
        )

    @pytest.mark.parametrize(
        "value",
        [
            "{{ schedule_time | date }}",
            "{{ trigger_time | date }}",
            "{{ now | date }}",
            "{{ coalesce(schedule_time, trigger_time) | date }}",
        ],
    )
    def test_a_cron_schedule_may_name_every_clock(self, value: str) -> None:
        assert self._check_for(value, sources.Kind.CRON) is None

    @pytest.mark.parametrize(
        "value",
        [
            "{{ trigger_time | date }}",
            "{{ now | date }}",
            "{{ coalesce(now, trigger_time) }}",
        ],
    )
    def test_a_subscription_may_name_the_clocks_it_has(self, value: str) -> None:
        assert self._check_for(value, sources.Kind.SUBSCRIPTION) is None

    def test_a_subscription_may_not_name_schedule_time(self) -> None:
        with pytest.raises(errors.TemplateError) as caught:
            self._check_for("{{ schedule_time | date }}", sources.Kind.SUBSCRIPTION)

        assert caught.value.detail == (
            "Invalid template for 'as_of_date': 'schedule_time' is not available for a "
            "subscription; available sources are now, trigger_time"
        )

    def test_a_coalesce_arm_is_not_a_way_round_it(self) -> None:
        """Otherwise the ban is cosmetic: the arm would just fall through on every fire,
        which is the silent-wrong-value outcome the save-time check exists to prevent.
        """
        with pytest.raises(
            errors.TemplateError, match="'schedule_time' is not available"
        ):
            self._check_for(
                "{{ coalesce(schedule_time, trigger_time) | date }}",
                sources.Kind.SUBSCRIPTION,
            )

    def test_the_message_does_not_suggest_coalesce(self) -> None:
        """`source_unavailable` suggests the portable form and is right to; here it would
        be advice that leads straight to another 422."""
        with pytest.raises(errors.TemplateError) as caught:
            self._check_for("{{ schedule_time }}", sources.Kind.SUBSCRIPTION)

        assert "coalesce" not in caught.value.detail

    def test_manual_is_refused_as_a_save_kind(self) -> None:
        """Validating against MANUAL would ban schedule_time on every cron schedule."""
        with pytest.raises(ValueError, match="render-time kind"):
            self._check_for("{{ schedule_time }}", sources.Kind.MANUAL)


class TestAConstantMustSurviveRenderingUnchanged:
    """Jinja strips `{# #}` and unwraps `{% raw %}` while leaving one text node behind.

    Classifying on the node alone called those constants, so the value was stored as
    typed and then rendered shorter -- a silent edit of a pipeline argument.
    """

    @pytest.mark.parametrize(
        "value",
        [
            "prod{#-canary-#}",
            "prod{# canary #}",
            "{% raw %}{{ now }}{% endraw %}",
        ],
        ids=["trimmed-comment", "comment", "raw-block"],
    )
    def test_a_value_jinja_would_rewrite_is_refused(self, value: str) -> None:
        with pytest.raises(errors.TemplateError) as caught:
            grammars.classify(key="as_of_date", value=value)

        assert "plain string or one" in caught.value.detail

    @pytest.mark.parametrize(
        "value",
        ["prod", "  Run A  ", "100%", "ca-central-1", "a-b_c.d"],
        ids=["word", "padded", "percent", "region", "punctuation"],
    )
    def test_a_real_constant_is_still_classified_as_one(self, value: str) -> None:
        """The check is equality with the input, so text that merely looks template-ish
        is unaffected."""
        classified = grammars.classify(key="as_of_date", value=value)

        assert isinstance(classified, grammars.Constant)
        assert classified.text == value
