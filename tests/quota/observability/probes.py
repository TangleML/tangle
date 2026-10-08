"""Read-back helper over the in-memory OTel reader the quota metrics fixture installs.

The SDK's collected shape is several layers deep — resource, scope, metric, data, point — so
this walks it once and lets a test say what it means: this metric, under these labels.
"""

import typing

from opentelemetry.sdk.metrics import export as metrics_export


class MetricsProbe:
    """Reads back what the quota instruments recorded during one test."""

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

    def points(
        self,
        *,
        name: str,
    ) -> list[typing.Any]:
        """Every data point recorded for one metric name.

        Collecting is what runs the observable-gauge callbacks, so a gauge reads as of this
        call rather than as of when its poller last ran.

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

    def groups_observed(
        self,
        *,
        name: str,
    ) -> dict[str, float]:
        """A gauge's current reading for every group it observed.

        Args:
            name: The full metric name.

        Returns:
            The group label mapped to its value, which is the shape a poller assertion wants.
        """
        return {
            point.attributes["quota_group"]: point.value
            for point in self.points(name=name)
        }
