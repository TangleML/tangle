"""Unit tests for templating.arguments.rendering.

The same two clock fixtures as test_sources, and the same reason: the only interesting
axis is whether this fire has a schedule_time.

    _with_schedule_time()      a cron schedule fired by the scheduler -- schedule_time exists
    _without_schedule_time()   the same schedule fired by hand        -- it does not

    schedule_time  09:00:00.000000    trigger_time  09:00:03.250000    now  09:00:04.125000
"""

import datetime
import zoneinfo

import pytest

from cloud_pipelines_backend.templating.arguments import errors, rendering, sources

_TORONTO = zoneinfo.ZoneInfo("America/Toronto")
_SCHEDULE_TIME = datetime.datetime(2026, 9, 2, 9, 0, 0, tzinfo=_TORONTO)
_TRIGGER = datetime.datetime(2026, 9, 2, 9, 0, 3, 250000, tzinfo=_TORONTO)
_NOW = datetime.datetime(2026, 9, 2, 9, 0, 4, 125000, tzinfo=_TORONTO)


def _with_schedule_time() -> sources.Clock:
    return sources.Clock(
        kind=sources.Kind.CRON,
        trigger_time=_TRIGGER,
        now=_NOW,
        schedule_time=_SCHEDULE_TIME,
    )


def _without_schedule_time() -> sources.Clock:
    return sources.Clock(kind=sources.Kind.MANUAL, trigger_time=_TRIGGER, now=_NOW)


class TestRenderingOneTemplate:
    def test_an_expression_renders_through_its_filters(self) -> None:
        assert (
            rendering.render_one(
                key="as_of_date",
                value="{{ schedule_time | shift('-1d') | date }}",
                clock=_with_schedule_time(),
            )
            == "2026-09-01"
        )

    def test_a_constant_renders_to_itself(self) -> None:
        """A stored value with no `{{ }}` is still handed to the engine rather than
        special-cased, so there is one path to keep correct instead of two."""
        assert (
            rendering.render_one(key="region", value="ca", clock=_with_schedule_time())
            == "ca"
        )

    def test_an_empty_template_renders_to_an_empty_string(self) -> None:
        """Section 4.4: empty is a rendered value, not a failure."""
        assert (
            rendering.render_one(key="suffix", value="", clock=_with_schedule_time())
            == ""
        )

    def test_a_missing_clock_becomes_the_keyed_unavailable_error(self) -> None:
        """Jinja2 raises UndefinedError, which carries the name only inside its message.
        The caller needs our wording and the key, so it is translated."""
        with pytest.raises(errors.TemplateError) as caught:
            rendering.render_one(
                key="as_of_date",
                value="{{ schedule_time }}",
                clock=_without_schedule_time(),
            )

        assert caught.value.key == "as_of_date"
        assert caught.value.detail == (
            "Invalid template for 'as_of_date': 'schedule_time' is not available for a "
            "manual run; coalesce(schedule_time, trigger_time) is the portable form"
        )

    def test_a_coalesce_failure_passes_straight_through(self) -> None:
        """coalesce raises our own error and already knows the key, so re-wrapping it
        would bury the message that names every arm it tried."""
        with pytest.raises(errors.TemplateError) as caught:
            rendering.render_one(
                key="as_of_date",
                value="{{ coalesce(schedule_time, schedule_time) }}",
                clock=_without_schedule_time(),
            )

        assert "no source in coalesce" in caught.value.detail
        assert "could not be rendered" not in caught.value.detail

    def test_coalesce_falls_through_to_a_clock_that_exists(self) -> None:
        """The whole point of the grammar's one function, exercised through the engine
        the product actually renders with."""
        assert (
            rendering.render_one(
                key="as_of_date",
                value="{{ coalesce(schedule_time, trigger_time) | date }}",
                clock=_without_schedule_time(),
            )
            == "2026-09-02"
        )


class TestMergingIntoTheRunsArguments:
    def test_a_rendered_key_is_added_when_nothing_was_there(self) -> None:
        result = rendering.render(
            templates={"day": "{{ now | date }}"},
            arguments={},
            clock=_with_schedule_time(),
        )

        assert result.arguments == {"day": "2026-09-02"}
        assert result.overridden == {}
        assert result.failures == {}

    def test_the_template_wins_and_the_displaced_value_is_kept(self) -> None:
        """Section 4.4. The old value is not discarded: it is what the annotation reports,
        and without it a human cannot tell what the template changed."""
        result = rendering.render(
            templates={"day": "{{ now | date }}"},
            arguments={"day": "2020-01-01"},
            clock=_with_schedule_time(),
        )

        assert result.arguments == {"day": "2026-09-02"}
        assert result.overridden == {"day": "2020-01-01"}

    def test_a_template_that_reproduces_the_existing_value_overrides_nothing(
        self,
    ) -> None:
        """Nothing was displaced, so annotating an override would be noise."""
        result = rendering.render(
            templates={"day": "{{ now | date }}"},
            arguments={"day": "2026-09-02"},
            clock=_with_schedule_time(),
        )

        assert result.arguments == {"day": "2026-09-02"}
        assert result.overridden == {}

    def test_arguments_without_a_template_are_carried_through(self) -> None:
        result = rendering.render(
            templates={"day": "{{ now | date }}"},
            arguments={"region": "ca"},
            clock=_with_schedule_time(),
        )

        assert result.arguments == {"region": "ca", "day": "2026-09-02"}

    def test_the_callers_map_is_not_mutated(self) -> None:
        """The caller still needs the originals to annotate against, and a run that was
        rejected downstream must not be left holding a half-templated map."""
        arguments = {"day": "2020-01-01"}

        rendering.render(
            templates={"day": "{{ now | date }}"},
            arguments=arguments,
            clock=_with_schedule_time(),
        )

        assert arguments == {"day": "2020-01-01"}


class TestOneFailureCostsOnlyItsOwnKey:
    def test_a_failed_key_keeps_the_value_the_spec_gave_it(self) -> None:
        """Section 4.6: left exactly as it was, not blanked and not set to "None"."""
        result = rendering.render(
            templates={"day": "{{ schedule_time }}"},
            arguments={"day": "2020-01-01"},
            clock=_without_schedule_time(),
        )

        assert result.arguments == {"day": "2020-01-01"}
        assert "day" in result.failures

    def test_a_failed_key_with_no_previous_value_is_simply_absent(self) -> None:
        """Rather than present and empty, which would render as an empty CLI argument."""
        result = rendering.render(
            templates={"day": "{{ schedule_time }}"},
            arguments={},
            clock=_without_schedule_time(),
        )

        assert result.arguments == {}
        assert "day" in result.failures

    def test_the_other_templates_still_render(self) -> None:
        """Failure is per key, never per schedule -- one bad template out of three must
        not cost the two good ones."""
        result = rendering.render(
            templates={
                "bad": "{{ schedule_time }}",
                "good": "{{ trigger_time | date }}",
                "also_good": "{{ now | date }}",
            },
            arguments={},
            clock=_without_schedule_time(),
        )

        assert result.arguments == {
            "good": "2026-09-02",
            "also_good": "2026-09-02",
        }
        assert list(result.failures) == ["bad"]

    def test_render_never_raises_however_broken_the_template(self) -> None:
        """The run must be submitted regardless, so every exception has to come back as
        text. A stored template can be arbitrarily bad: it may predate a validator."""
        result = rendering.render(
            templates={
                "a": "{{ now.__class__ }}",
                "b": "{% for x in y %}{% endfor %}",
            },
            arguments={},
            clock=_with_schedule_time(),
        )

        assert set(result.failures) == {"a", "b"}
        assert result.arguments == {}

    def test_a_failure_reads_as_a_sentence_naming_its_key(self) -> None:
        """The failure text is annotated verbatim, so it is the whole of what a human
        debugging a wrong partition gets to see."""
        result = rendering.render(
            templates={"partition_date": "{{ schedule_time }}"},
            arguments={},
            clock=_without_schedule_time(),
        )

        assert result.failures["partition_date"].startswith(
            "Invalid template for 'partition_date': "
        )
