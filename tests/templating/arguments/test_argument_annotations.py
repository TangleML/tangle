"""Unit tests for templating.arguments.annotations.

The subject is what a human can search for after a run went wrong. Every test is
about one of three things: which facets exist for a key, that a value too long for
the column never reads as a complete one, and that a key too long for the column is
dropped rather than truncated into a collision with its neighbour.
"""

import datetime

from cloud_pipelines_backend.templating.arguments import annotations, rendering, sources

_TRIGGER_TIME = datetime.datetime(2026, 9, 2, 3, 13, 4, tzinfo=datetime.timezone.utc)
_SCHEDULE_TIME = datetime.datetime(2026, 9, 2, 3, 13, 0, tzinfo=datetime.timezone.utc)


def _render(
    *, templates: dict[str, str], kind: sources.Kind, scheduled: bool
) -> rendering.Rendered:
    return rendering.render(
        templates=templates,
        arguments={},
        clock=sources.Clock(
            kind=kind,
            trigger_time=_TRIGGER_TIME,
            now=_TRIGGER_TIME,
            schedule_time=_SCHEDULE_TIME if scheduled else None,
        ),
    )


def test_a_rendered_key_gets_both_its_template_and_its_value() -> None:
    """The two facets that answer "what did it say" and "what did it produce"."""
    templates = {"day": "{{ schedule_time | date }}"}
    rendered = _render(templates=templates, kind=sources.Kind.CRON, scheduled=True)

    built = annotations.for_firing(templates=templates, rendered=rendered)

    assert (
        built[annotations.argument_key(name="day", facet="template")]
        == "{{ schedule_time | date }}"
    )
    assert built[annotations.argument_key(name="day", facet="rendered")] == "2026-09-02"


def test_a_failed_key_gets_an_error_and_no_rendered_value() -> None:
    """A `rendered` annotation on a key that did not render would be a lie: the run
    executes against the spec's value, not against one this map names."""
    templates = {"day": "{{ schedule_time | date }}"}
    rendered = _render(
        templates=templates, kind=sources.Kind.SUBSCRIPTION, scheduled=False
    )

    built = annotations.for_firing(templates=templates, rendered=rendered)

    assert (
        "schedule_time"
        in built[annotations.argument_key(name="day", facet="render_error")]
    )
    assert annotations.argument_key(name="day", facet="rendered") not in built


def test_the_kind_distinguishes_a_scheduled_fire_from_a_hand_fired_one() -> None:
    """One schedule row and one unchanged template render as `cron` on a tick and
    `manual` by hand, and only the second fails. Without the kind the two runs
    differ only by an absent source, which reads the same as a subscription."""
    templates = {"day": "{{ schedule_time | date }}"}

    ticked = annotations.for_firing(
        templates=templates,
        rendered=_render(templates=templates, kind=sources.Kind.CRON, scheduled=True),
    )
    by_hand = annotations.for_firing(
        templates=templates,
        rendered=_render(
            templates=templates, kind=sources.Kind.MANUAL, scheduled=False
        ),
    )

    assert ticked[annotations.KIND_KEY] == "cron"
    assert by_hand[annotations.KIND_KEY] == "manual"
    assert annotations.argument_key(name="day", facet="render_error") in by_hand


def test_the_kind_is_not_filed_under_the_source_subject() -> None:
    """`{{ cron }}` is not a template anyone can write; filing it beside the clocks
    would say it is."""
    templates = {"day": "{{ now | date }}"}
    rendered = _render(
        templates=templates, kind=sources.Kind.SUBSCRIPTION, scheduled=False
    )

    built = annotations.for_firing(templates=templates, rendered=rendered)

    assert annotations.KIND_KEY in built
    assert not annotations.KIND_KEY.startswith(f"{annotations.SOURCE_NAMESPACE}/")


def test_all_three_clock_sources_are_annotated_under_one_subject() -> None:
    """A rendered value whose input is not recorded cannot be reproduced, and `now`
    is a source a template may name like any other."""
    templates = {"day": "{{ now | date }}"}
    rendered = _render(templates=templates, kind=sources.Kind.CRON, scheduled=True)

    built = annotations.for_firing(templates=templates, rendered=rendered)
    source_keys = {
        key for key in built if key.startswith(f"{annotations.SOURCE_NAMESPACE}/")
    }

    assert source_keys == {
        annotations.source_key(name="schedule_time"),
        annotations.source_key(name="trigger_time"),
        annotations.source_key(name="now"),
    }


def test_schedule_time_is_annotated_only_when_the_firing_had_one() -> None:
    """A subscription and a hand-fired schedule have no nominal slot, so annotating
    one would invent a time the run never had."""
    templates = {"region": "ca-central-1"}

    cron = annotations.for_firing(
        templates=templates,
        rendered=_render(templates=templates, kind=sources.Kind.CRON, scheduled=True),
    )
    manual = annotations.for_firing(
        templates=templates,
        rendered=_render(
            templates=templates, kind=sources.Kind.MANUAL, scheduled=False
        ),
    )

    schedule_time_key = annotations.source_key(name="schedule_time")

    assert cron[schedule_time_key] == _SCHEDULE_TIME.isoformat()
    assert schedule_time_key not in manual
    assert (
        manual[annotations.source_key(name="trigger_time")] == _TRIGGER_TIME.isoformat()
    )


def test_an_oversized_value_is_cut_to_the_column_and_marked() -> None:
    """The column is 255. A cut value that did not say so would be read as the whole
    template, and the missing tail is exactly where a long template's error is."""
    long_template = "{{ now | date }}" + "#" * 400
    templates = {"day": long_template}
    rendered = _render(templates=templates, kind=sources.Kind.CRON, scheduled=True)

    built = annotations.for_firing(templates=templates, rendered=rendered)
    value = built[annotations.argument_key(name="day", facet="template")]

    assert len(value) == 255
    assert value.endswith("...[truncated]")


def test_a_name_too_long_for_the_key_column_is_dropped_not_truncated() -> None:
    """Truncating the key would collide with every other name sharing that prefix,
    and an oversized key would fail the insert and lose the whole run."""
    name = "n" * 250
    templates = {name: "ca-central-1", "short": "ca-central-1"}
    rendered = _render(templates=templates, kind=sources.Kind.CRON, scheduled=True)

    built = annotations.for_firing(templates=templates, rendered=rendered)

    argument_keys = {
        key for key in built if key.startswith(f"{annotations.ARGUMENT_NAMESPACE}/")
    }

    assert all(len(key) <= 255 for key in built)
    assert argument_keys == {
        annotations.argument_key(name="short", facet="template"),
        annotations.argument_key(name="short", facet="rendered"),
    }


def test_the_key_orders_subject_before_facet_so_one_argument_prefixes() -> None:
    """A LIKE on `.../argument/<name>/%` gathers one argument's whole story; that is
    only possible while the name precedes the facet."""
    templates = {"day": "{{ now | date }}", "other": "x"}
    rendered = _render(templates=templates, kind=sources.Kind.CRON, scheduled=True)

    built = annotations.for_firing(templates=templates, rendered=rendered)
    prefix = f"{annotations.ARGUMENT_NAMESPACE}/day/"
    gathered = {key for key in built if key.startswith(prefix)}

    assert gathered == {
        f"{prefix}template",
        f"{prefix}rendered",
    }


def test_a_displaced_value_is_not_annotated_even_when_render_reports_one() -> None:
    """`overridden` is returned by `render` and deliberately never annotated.

    Driven with a non-empty `arguments` map -- which no production caller passes -- so the
    facet is populated and the assertion is about the annotator's choice, not about the map
    happening to be empty. Removing the facet was the fix for a reviewer's finding that it
    could never fire: a stored argument is first knowable inside the locked read that selects
    the pipeline version, which happens after rendering.
    """
    templates = {"env": "prod"}
    rendered = rendering.render(
        templates=templates,
        arguments={"env": "staging"},
        clock=sources.Clock(
            kind=sources.Kind.CRON,
            trigger_time=_TRIGGER_TIME,
            now=_TRIGGER_TIME,
            schedule_time=_SCHEDULE_TIME,
        ),
    )
    assert rendered.overridden == {"env": "staging"}

    built = annotations.for_firing(templates=templates, rendered=rendered)

    assert not [key for key in built if key.endswith("/overridden")]
    assert [key for key in built if key.endswith("/rendered")]
