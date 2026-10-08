"""Unit tests for templating.arguments.engines.

One engine serves both the grammar walk and the render, so what is pinned here is the
configuration itself and the fact that there is only one of it.
"""

import pytest

from cloud_pipelines_backend.templating.arguments import (
    engines,
    errors,
    filters,
    grammars,
    rendering,
)


class TestThereIsOnlyOneEngine:
    def test_the_shared_engine_is_a_single_object(self) -> None:
        """Two engines would be two configurations, and two configurations can drift --
        a template could then pass validation and fail at render."""
        assert grammars.engines.ENGINE is engines.ENGINE
        assert rendering.engines.ENGINE is engines.ENGINE

    def test_build_returns_a_fresh_one_rather_than_the_shared_engine(
        self,
    ) -> None:
        """So a test can compare against a clean engine without mutating the live one."""
        assert engines.build() is not engines.ENGINE

    def test_the_grammar_module_only_ever_parses(self) -> None:
        """The shared engine can render, and grammars must not. Save-time code that
        rendered a template would execute user input on the API path."""
        source = __import__("inspect").getsource(grammars)

        assert "ENGINE.parse" in source
        assert "from_string" not in source
        assert ".render(" not in source


class TestTheConfiguration:
    def test_reaching_through_an_object_is_refused_by_the_sandbox(self) -> None:
        """A template is user input. `{{ now.__class__ }}` on a plain Environment walks to
        the type; the sandbox stops it."""
        with pytest.raises(Exception) as caught:
            engines.ENGINE.from_string("{{ now.__class__ }}").render({"now": 1})

        assert "not safe" in str(caught.value) or "unsafe" in str(caught.value)

    def test_a_missing_name_raises_instead_of_rendering_empty(self) -> None:
        """StrictUndefined. The default Undefined renders an empty string, which would put
        an empty CLI argument into a run and report success."""
        with pytest.raises(Exception, match="is undefined"):
            engines.ENGINE.from_string("{{ nothing }}").render({})

    def test_an_ampersand_is_left_alone(self) -> None:
        """autoescape=False, because these values become CLI arguments and paths. HTML
        escaping would put `&amp;` into one."""
        assert engines.ENGINE.from_string("a&b").render({}) == "a&b"

    def test_every_filter_in_the_registry_is_wired_in(self) -> None:
        """The engine takes its filters from the registry, so a filter added there needs
        no second registration."""
        assert set(filters.as_jinja_filters()) <= set(engines.ENGINE.filters)

    def test_a_filter_is_usable_through_the_shared_engine(self) -> None:
        """The registry is wired in as callables, not just as names."""
        import datetime
        import zoneinfo

        moment = datetime.datetime(
            2026, 9, 2, 9, 0, tzinfo=zoneinfo.ZoneInfo("America/Toronto")
        )

        assert (
            engines.ENGINE.from_string("{{ t | date }}").render({"t": moment})
            == "2026-09-02"
        )


class TestTheSameEngineSeesBothPaths:
    def test_what_the_grammar_accepts_the_renderer_can_render(self) -> None:
        """The reason for sharing, stated as a test: one engine cannot disagree with
        itself about what a template means."""
        value = "{{ trigger_time | shift('-1d') | date }}"
        classified = grammars.classify(key="as_of_date", value=value)

        grammars.check_grammar(key="as_of_date", expression=classified.node)

        assert engines.ENGINE.from_string(value) is not None

    def test_what_the_grammar_rejects_never_reaches_the_renderer(self) -> None:
        """Rejection happens at save, so a template like this is never stored to render.
        The engine itself would accept it -- it only discovers the missing filter at
        compile -- which is precisely why the grammar walk runs first."""
        classified = grammars.classify(
            key="as_of_date", value="{{ trigger_time | nope }}"
        )

        with pytest.raises(errors.TemplateError, match="unknown filter"):
            grammars.check_grammar(key="as_of_date", expression=classified.node)
