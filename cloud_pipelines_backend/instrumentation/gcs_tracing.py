"""GCS storage provider that emits one OTel span per upload/download so transfer duration is measurable."""

from __future__ import annotations

import collections.abc
import contextlib

from opentelemetry import trace
from opentelemetry.trace import StatusCode

from cloud_pipelines.orchestration.storage_providers import google_cloud_storage

_tracer = trace.get_tracer("tangle.storage")


@contextlib.contextmanager
def _gcs_operation_span(
    operation: str, uri: str
) -> collections.abc.Iterator[trace.Span]:
    with _tracer.start_as_current_span(
        f"gcs.{operation}",
        attributes={"gcs.operation": operation, "gcs.uri": uri},
    ) as span:
        try:
            yield span
        except Exception as exception:
            span.set_status(StatusCode.ERROR)
            span.record_exception(exception)
            raise


class TracingGoogleCloudStorageProvider(
    google_cloud_storage.GoogleCloudStorageProvider
):
    def upload(
        self,
        source_path: str,
        destination_uri: google_cloud_storage.GoogleCloudStorageUri,
    ) -> None:
        with _gcs_operation_span("upload", destination_uri.uri):
            super().upload(source_path, destination_uri)

    def upload_bytes(
        self, data: bytes, destination_uri: google_cloud_storage.GoogleCloudStorageUri
    ) -> None:
        with _gcs_operation_span("upload_bytes", destination_uri.uri) as span:
            span.set_attribute("gcs.bytes", len(data))
            super().upload_bytes(data, destination_uri)

    def download(
        self,
        source_uri: google_cloud_storage.GoogleCloudStorageUri,
        destination_path: str,
    ) -> None:
        with _gcs_operation_span("download", source_uri.uri):
            super().download(source_uri, destination_path)

    def download_bytes(
        self, source_uri: google_cloud_storage.GoogleCloudStorageUri
    ) -> bytes:
        with _gcs_operation_span("download_bytes", source_uri.uri) as span:
            data = super().download_bytes(source_uri)
            span.set_attribute("gcs.bytes", len(data))
            return data
