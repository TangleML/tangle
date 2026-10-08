"""Read-back helpers over the in-memory OTel readers the observability fixtures install.

The SDK's collected shape is several layers deep (resource, scope, metric, data, point) and
a finished span is only findable by scanning the exporter, so these two probes do that
walking once and let a test say what it means: this metric, under these labels.
"""

import typing

from opentelemetry.sdk.metrics import export as metrics_export
from opentelemetry.sdk.trace.export import in_memory_span_exporter


class MetricsProbe:
    """Reads back what the emission instruments recorded during one test."""

    def __init__(
        self,
        *,
        reader: metrics_export.InMemoryMetricReader,
    ) -> None:
        """Hold the reader to collect through.

        Args:
            reader: The in-memory reader the test meter provider exports to.
        """
        self._reader = reader

    @property
    def reader(self) -> metrics_export.InMemoryMetricReader:
        """The reader to collect through, for assertions this class does not answer itself.

        Returns:
            The test meter provider's in-memory reader.
        """
        return self._reader

    def points(
        self,
        *,
        name: str,
    ) -> list[typing.Any]:
        """Every data point recorded for one metric name.

        Collecting is what runs the observable-gauge callbacks, so a gauge reads as of this
        call rather than as of when it was created.

        Args:
            name: The full metric name, dots included.

        Returns:
            One entry per attribute combination the metric was recorded under; empty when the
            metric was never recorded.
        """
        data = self._reader.get_metrics_data()
        if data is None:
            return []
        return [
            point
            for resource_metric in data.resource_metrics
            for scope_metric in resource_metric.scope_metrics
            for metric in scope_metric.metrics
            if metric.name == name
            for point in metric.data.data_points
        ]

    def point(
        self,
        *,
        name: str,
        attributes: dict[str, str] | None = None,
    ) -> typing.Any:
        """The single data point for a metric under the given attributes.

        Args:
            name: The full metric name.
            attributes: The labels the point must carry; a subset is enough to match.

        Returns:
            The matching data point, or None when the metric was never recorded under those
            labels.

        Raises:
            AssertionError: If several points match, meaning the metric was recorded under
                labels the caller did not distinguish.
        """
        wanted = attributes or {}
        matches = [
            point
            for point in self.points(name=name)
            if all(point.attributes.get(key) == value for key, value in wanted.items())
        ]
        if len(matches) > 1:
            raise AssertionError(f"{name} has {len(matches)} points matching {wanted}")
        return matches[0] if matches else None


class SpanProbe:
    """Reads back the spans the emission code finished during one test."""

    def __init__(
        self,
        *,
        exporter: in_memory_span_exporter.InMemorySpanExporter,
    ) -> None:
        """Hold the exporter the test tracer writes to.

        Args:
            exporter: The in-memory exporter finished spans land in.
        """
        self._exporter = exporter

    def all(
        self,
    ) -> tuple[typing.Any, ...]:
        """Return every finished span, in the order they ended."""
        return self._exporter.get_finished_spans()

    def all_named(
        self,
        *,
        name: str,
    ) -> list[typing.Any]:
        """Every finished span with the given name.

        Args:
            name: The span name to match.

        Returns:
            The matching spans, in the order they ended.
        """
        return [span for span in self.all() if span.name == name]

    def named(
        self,
        *,
        name: str,
    ) -> typing.Any:
        """The single finished span with the given name.

        Args:
            name: The span name to match.

        Returns:
            The matching span, or None if no span by that name finished.

        Raises:
            AssertionError: If several spans share the name.
        """
        matches = self.all_named(name=name)
        if len(matches) > 1:
            raise AssertionError(f"{len(matches)} spans named {name!r}")
        return matches[0] if matches else None
