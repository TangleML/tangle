"""Unit tests for templating.arguments.observability.render_observer.

Two surfaces, one call. The log tests assert the fields a reader needs and the one
the old line threw away -- the reason. The metric tests assert the counts and the
labels, and that the label set stays bounded: the free-text message is what makes a
`reason` label explode, so the code is what is recorded.
"""

import datetime
import logging

import pytest
from opentelemetry import metrics as otel_metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from cloud_pipelines_backend.templating.arguments import rendering, sources
from cloud_pipelines_backend.templating.arguments.observability import (
    metrics,
    render_observer,
)

_TRIGGER_TIME = datetime.datetime(2026, 9, 2, 2, 30, tzinfo=datetime.timezone.utc)


@pytest.fixture(scope="session")
def _reader() -> InMemoryMetricReader:
    """Install one provider for the whole session.

    The SDK refuses a second `set_meter_provider` in a process, so a per-test
    provider would bind only the first test and leave the rest reading nothing.
    The counters are created at import against a proxy meter, which binds to this
    provider when it is installed.
    """
    reader = InMemoryMetricReader()
    otel_metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))
    return reader


@pytest.fixture
def counts(_reader: InMemoryMetricReader):
    """Return a reader of counts recorded during this test only.

    Counters are cumulative across the session, so each test takes a snapshot
    before it records and subtracts it; a test that asserted on the raw totals
    would pass alone and fail in a suite.
    """

    def snapshot() -> dict[tuple[str, frozenset], int]:
        totals: dict[tuple[str, frozenset], int] = {}
        data = _reader.get_metrics_data()
        for resource in data.resource_metrics if data else []:
            for scope in resource.scope_metrics:
                if scope.scope.name != "tangle.templating":
                    continue
                for metric in scope.metrics:
                    for point in metric.data.data_points:
                        key = (metric.name, frozenset(point.attributes.items()))
                        totals[key] = point.value
        return totals

    before = snapshot()

    def read() -> dict[str, list[tuple[dict[str, str], int]]]:
        collected: dict[str, list[tuple[dict[str, str], int]]] = {}
        for (name, attributes), value in snapshot().items():
            delta = value - before.get((name, attributes), 0)
            if delta:
                collected.setdefault(name, []).append((dict(attributes), delta))
        return collected

    return read


def _report(
    *,
    templates: dict[str, str],
    kind: sources.Kind,
    scheduled: bool = False,
    identity: dict[str, object] | None = None,
    arguments: dict[str, str] | None = None,
) -> rendering.Rendered:
    clock = sources.Clock(
        kind=kind,
        trigger_time=_TRIGGER_TIME,
        now=_TRIGGER_TIME,
        schedule_time=_TRIGGER_TIME if scheduled else None,
    )
    rendered = rendering.render(
        templates=templates, arguments=arguments or {}, clock=clock
    )
    render_observer.report(
        rendered=rendered,
        templates=templates,
        clock=clock,
        identity=identity or {"subscription_id": "sub_9f2", "cycle": 7},
    )
    return rendered


def test_the_log_carries_the_reason_the_old_line_discarded(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """`Rendered.failures` held the remedy all along and the summary line dropped it;
    this is the whole point of the replacement."""
    with caplog.at_level(logging.WARNING):
        _report(
            templates={"day": "{{ schedule_time | date }}"},
            kind=sources.Kind.SUBSCRIPTION,
        )

    assert len(caplog.records) == 1
    message = caplog.records[0].message
    assert "coalesce(schedule_time, trigger_time) is the portable form" in message


def test_one_line_is_logged_per_failed_key_not_one_per_render(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Two keys can fail for two different reasons, and a summary line can only
    carry one of them."""
    with caplog.at_level(logging.WARNING):
        _report(
            templates={
                "a": "{{ schedule_time | date }}",
                "b": "{{ schedule_time | rfc3339 }}",
                "fine": "{{ now | date }}",
            },
            kind=sources.Kind.SUBSCRIPTION,
        )

    assert len(caplog.records) == 2
    assert {"key=a", "key=b"} == {
        field
        for record in caplog.records
        for field in record.message.split()
        if field.startswith("key=")
    }


def test_the_log_names_the_carrier_the_caller_passed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A schedule has an id and a name, a subscription an id and a cycle; neither
    has the other's, so the observer formats whatever it is given."""
    with caplog.at_level(logging.WARNING):
        _report(
            templates={"day": "{{ schedule_time | date }}"},
            kind=sources.Kind.MANUAL,
            identity={"schedule_id": "sched_ab", "schedule_name": "nightly-fx"},
        )

    message = caplog.records[0].message
    assert "schedule_id=sched_ab" in message
    assert "schedule_name=nightly-fx" in message
    assert "kind=manual" in message


def test_an_absent_schedule_time_is_logged_as_none_not_omitted(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A missing field reads as a logging bug; `none` reads as the answer, and the
    answer is usually why the key failed."""
    with caplog.at_level(logging.WARNING):
        _report(
            templates={"day": "{{ schedule_time | date }}"},
            kind=sources.Kind.SUBSCRIPTION,
        )

    assert "schedule_time=none" in caplog.records[0].message


def test_nothing_is_logged_when_every_key_rendered(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A warning per successful firing would bury the ones that mean something."""
    with caplog.at_level(logging.WARNING):
        _report(
            templates={"day": "{{ now | date }}"},
            kind=sources.Kind.SUBSCRIPTION,
        )

    assert caplog.records == []


def test_every_key_is_counted_once_so_the_sum_is_the_denominator(
    counts,
) -> None:
    """The outcomes are mutually exclusive; if they were not, the failure rate
    computed against this sum would be wrong."""
    templates = {
        "bad": "{{ schedule_time | date }}",
        "ok": "{{ now | date }}",
        "also_ok": "ca-central-1",
    }
    _report(templates=templates, kind=sources.Kind.SUBSCRIPTION)

    points = counts()["template.keys_rendered"]
    assert sum(value for _, value in points) == len(templates)


def test_a_pre_seeded_key_that_fails_is_counted_once_and_only_as_failed(
    counts,
) -> None:
    """`render` leaves a pre-seeded value in `arguments` when its template fails, so the
    key is in both maps. Counting it in both would inflate the denominator and report one
    key as having two verdicts. No caller passes a non-empty map today; this pins the
    invariant so the first one that does cannot break the failure rate silently.
    """
    rendered = _report(
        templates={"day": "{{ schedule_time | date }}"},
        kind=sources.Kind.MANUAL,
        arguments={"day": "already-here"},
    )
    #: The premise: the key really is in both maps, so the skip is doing the work.
    assert "day" in rendered.arguments and "day" in rendered.failures

    points = counts()["template.keys_rendered"]
    assert sum(value for _, value in points) == 1
    assert [attributes[metrics.OUTCOME_LABEL] for attributes, _ in points] == [
        metrics.Outcome.FAILED.value
    ]


def test_a_pre_seeded_key_that_renders_is_still_counted_once_as_rendered(
    counts,
) -> None:
    """The control for the test above: skipping keys present in `failures` must not also
    swallow a pre-seeded key whose template succeeded. Without this, dropping the RENDERED
    loop entirely would pass.
    """
    _report(
        templates={"day": "{{ now | date }}"},
        kind=sources.Kind.MANUAL,
        arguments={"day": "already-here"},
    )

    points = counts()["template.keys_rendered"]
    assert sum(value for _, value in points) == 1
    assert [attributes[metrics.OUTCOME_LABEL] for attributes, _ in points] == [
        metrics.Outcome.RENDERED.value
    ]


def test_the_failure_reason_label_is_the_bounded_code_not_the_message(
    counts,
) -> None:
    """The message quotes the key and the template, so it is unbounded; recording it
    as a label is the cardinality explosion this label exists to avoid."""
    _report(
        templates={"day": "{{ schedule_time | date }}"},
        kind=sources.Kind.SUBSCRIPTION,
    )

    points = counts()["template.render_failures"]
    assert [attributes[metrics.REASON_LABEL] for attributes, _ in points] == [
        "source_unavailable"
    ]


def test_a_run_losing_several_keys_counts_as_one_bad_run(counts) -> None:
    """One broken template on a three-key schedule is one bad run, not three; the
    per-key counter already answers the other question."""
    _report(
        templates={
            "a": "{{ schedule_time | date }}",
            "b": "{{ schedule_time | rfc3339 }}",
            "c": "{{ schedule_time | epoch_seconds }}",
        },
        kind=sources.Kind.SUBSCRIPTION,
    )

    points = counts()["template.runs_with_a_failed_key"]
    assert [value for _, value in points] == [1]


def test_a_clean_render_moves_the_counters_but_not_the_failure_ones(
    counts,
) -> None:
    """The denominator has to move on success, or the failure rate is one."""
    _report(templates={"day": "{{ now | date }}"}, kind=sources.Kind.SUBSCRIPTION)

    collected = counts()
    assert collected["template.keys_rendered"][0][1] == 1
    assert "template.render_failures" not in collected
    assert "template.runs_with_a_failed_key" not in collected


def test_the_kind_label_follows_the_clock_so_a_hand_fired_cron_is_manual(
    counts,
) -> None:
    """The same schedule row fires as cron on a tick and as manual by hand, and the
    two fail differently; labelling by the row would merge them."""
    _report(
        templates={"day": "{{ schedule_time | date }}"},
        kind=sources.Kind.MANUAL,
        identity={"schedule_id": "sched_ab", "schedule_name": "nightly-fx"},
    )

    points = counts()["template.render_failures"]
    assert [attributes[metrics.KIND_LABEL] for attributes, _ in points] == ["manual"]


def test_the_only_outcomes_are_rendered_and_failed() -> None:
    """Two buckets, disjoint by construction: a failed key is absent from `arguments`.

    A third bucket is what made the sum above a coincidence rather than an invariant, so this
    fails if one is reintroduced without revisiting `_count_outcomes`.
    """
    assert {outcome.value for outcome in metrics.Outcome} == {
        "rendered",
        "failed",
    }
