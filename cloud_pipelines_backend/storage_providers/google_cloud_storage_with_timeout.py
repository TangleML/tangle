"""Google Cloud Storage provider with explicit per-request timeouts.

The official google-cloud-storage Python client exposes timeouts as per-call
``timeout=`` arguments. This provider keeps Tangle's StorageProvider interface
but passes a configured timeout to each GCS request used by the upstream
GoogleCloudStorageProvider implementation.
"""

from __future__ import annotations

import base64
import logging
import os
import typing
from typing import Optional, TypeAlias

from cloud_pipelines.orchestration.storage_providers import google_cloud_storage
from cloud_pipelines.orchestration.storage_providers import interfaces

_LOGGER = logging.getLogger(name=__name__)

GCS_REQUEST_TIMEOUT_ENV = "TANGLE_GCS_REQUEST_TIMEOUT_SECONDS"
DEFAULT_GCS_REQUEST_TIMEOUT_SECONDS = 60.0

RequestTimeout: TypeAlias = float | tuple[float, float]

if typing.TYPE_CHECKING:
    from google.cloud import storage


def _storage_module():
    from google.cloud import storage

    return storage


class GoogleCloudStorageProviderWithTimeout(
    google_cloud_storage.GoogleCloudStorageProvider
):
    """GoogleCloudStorageProvider that passes an explicit timeout to GCS calls."""

    def __init__(
        self,
        client: Optional["storage.Client"] = None,
        *,
        request_timeout: RequestTimeout | None = None,
    ) -> None:
        """Construct a GCS provider whose every request is bounded by a timeout.

        Instantiated by the Kubernetes launchers, so any consumer that runs
        Tangle on GCP gets a storage provider where a hung GCS call can never
        block the orchestrator's poll loop indefinitely. The timeout defaults
        from ``TANGLE_GCS_REQUEST_TIMEOUT_SECONDS``, letting a consumer tune GCS
        patience per deployment without code changes.
        """
        super().__init__(client=client)
        self._request_timeout = request_timeout or _configured_request_timeout()

    def _upload_file(self, source_file_path: str, destination_blob_uri: str):
        """Upload a single local file to a GCS object.

        Use cases:
        - Staging a leaf output artifact (a single file produced by a completed
          container) into its artifact URI.
        - Writing a container's captured log file to its log URI.
        """
        storage = _storage_module()
        destination_blob = storage.Blob.from_string(
            uri=destination_blob_uri, client=self._client
        )
        destination_blob.upload_from_filename(
            filename=source_file_path,
            checksum="md5",
            timeout=self._request_timeout,
        )

    def _upload_dir(self, source_dir_path: str, destination_dir_uri: str):
        """Upload a local directory tree (recursively) to GCS.

        Used when a container output artifact is a directory rather than a
        single file (e.g. a model checkpoint directory or a multi-file dataset):
        each entry is uploaded under the destination prefix and a zero-byte
        marker object represents the directory itself.
        """
        # Creating the directory object (zero-byte object with name ending in slash)
        storage = _storage_module()
        storage.Blob.from_string(
            uri=destination_dir_uri.rstrip("/") + "/", client=self._client
        ).upload_from_string(
            data="",
            checksum="md5",
            timeout=self._request_timeout,
        )

        for dir_entry_name in os.listdir(source_dir_path):
            source_path = os.path.join(source_dir_path, dir_entry_name)
            destination_uri = destination_dir_uri.rstrip("/") + "/" + dir_entry_name
            self._upload_to_uri(
                source_path=source_path,
                destination_uri=destination_uri,
            )

    def upload_bytes(
        self, data: bytes, destination_uri: google_cloud_storage.GoogleCloudStorageUri
    ):
        """Upload in-memory bytes directly to a GCS object (no local temp file).

        Use cases:
        - Staging a small inline input-argument value into its staging URI
          before a container is launched, so the container can consume it as a
          file.
        - Persisting small artifact or log payloads that a consumer already
          holds in memory.
        """
        storage = _storage_module()
        destination_uri_str = destination_uri.uri
        destination_blob = storage.Blob.from_string(
            uri=destination_uri_str, client=self._client
        )
        _LOGGER.debug(f"Uploading data to {destination_uri_str}")
        destination_blob.upload_from_string(
            data=data,
            checksum="md5",
            timeout=self._request_timeout,
        )

    def _download_from_uri(self, source_uri: str, destination_path: str):
        """Download a GCS object — or every object under a directory prefix — to
        a local path.

        Used by the launchers to materialize a container's input artifacts on
        local disk before execution; handles both single-file and directory
        artifacts.
        """
        storage = _storage_module()
        source_blob_or_dir = storage.Blob.from_string(
            uri=source_uri, client=self._client
        )
        if source_blob_or_dir.exists(timeout=self._request_timeout):
            return _download_blob_to_filename_with_timeout(
                blob=source_blob_or_dir,
                destination_path=destination_path,
                request_timeout=self._request_timeout,
            )

        source_dir_prefix = source_blob_or_dir.name.rstrip("/") + "/"
        for source_blob in self._client.list_blobs(
            bucket_or_name=source_blob_or_dir.bucket,
            prefix=source_dir_prefix,
            timeout=self._request_timeout,
        ):
            assert source_blob.name.startswith(source_dir_prefix)
            relative_source_blob_name = source_blob.name[len(source_dir_prefix) :]
            destination_file_path = os.path.join(
                destination_path, relative_source_blob_name
            )
            if source_blob.name.endswith("/"):
                # It's a zero-size object that represents a directory
                assert source_blob.size == 0
                os.makedirs(destination_file_path, exist_ok=True)
            else:
                _download_blob_to_filename_with_timeout(
                    blob=source_blob,
                    destination_path=destination_file_path,
                    request_timeout=self._request_timeout,
                )

    def download_bytes(
        self, source_uri: google_cloud_storage.GoogleCloudStorageUri
    ) -> bytes:
        """Download a GCS object as raw bytes.

        Use cases:
        - Launchers: reading an input artifact so its value can be inlined as
          text and passed to a container.
        - Orchestrator: preloading small (<=255 byte) output artifact values for
          preservation after a container completes.
        - API server: fetching stored container log text to serve to a consumer.
        """
        storage = _storage_module()
        source_uri_str = source_uri.uri
        source_blob = storage.Blob.from_string(uri=source_uri_str, client=self._client)
        _LOGGER.debug(f"Downloading data from {source_uri_str}")
        return source_blob.download_as_bytes(timeout=self._request_timeout)

    def exists(self, uri: google_cloud_storage.GoogleCloudStorageUri) -> bool:
        """Check whether a GCS object or directory exists.

        Used by the orchestrator after a container reports success to verify
        every declared output artifact was actually produced; any missing output
        marks the execution FAILED and skips its downstream nodes.
        """
        storage = _storage_module()
        blob_uri = uri.uri.rstrip("/")
        file_blob = storage.Blob.from_string(uri=blob_uri, client=self._client)
        # The "directory objects" are expected to exist for directories
        dir_blob = storage.Blob.from_string(uri=blob_uri + "/", client=self._client)
        return file_blob.exists(timeout=self._request_timeout) or dir_blob.exists(
            timeout=self._request_timeout
        )

    def _get_info_from_uri(self, uri: str) -> interfaces.DataInfo:
        """Return size, directory flag, and content hashes for a GCS object or
        directory.

        Used by the orchestrator to record each produced output's ArtifactData
        (total size, is_dir, hash). Those hashes drive artifact caching and
        execution reuse and are surfaced to downstream consumers.
        """
        storage = _storage_module()
        file_info_list = []
        blob_or_dir = storage.Blob.from_string(uri=uri, client=self._client)
        if blob_or_dir.exists(timeout=self._request_timeout):
            blob = blob_or_dir
            blob.reload(timeout=self._request_timeout)
            return interfaces.DataInfo(
                total_size=blob.size,
                is_dir=False,
                hashes=_get_gcs_blob_hashes(blob),
            )

        dir_prefix = blob_or_dir.name.rstrip("/") + "/"
        for blob in self._client.list_blobs(
            bucket_or_name=blob_or_dir.bucket,
            prefix=dir_prefix,
            timeout=self._request_timeout,
        ):
            blob.reload(timeout=self._request_timeout)
            assert blob.name.startswith(dir_prefix)
            relative_source_blob_name = blob.name[len(dir_prefix) :]
            file_info_list.append(
                interfaces._FileInfo(
                    path=relative_source_blob_name,
                    size=blob.size,
                    hashes=_get_gcs_blob_hashes(blob),
                )
            )
        data_info = interfaces._make_data_info_for_dir(file_info_list)
        data_info._file_info_list = file_info_list
        return data_info


def _configured_request_timeout() -> RequestTimeout:
    """Resolve the per-request timeout from ``TANGLE_GCS_REQUEST_TIMEOUT_SECONDS``.

    Falls back to the default when the variable is unset or invalid, letting a
    consumer tune GCS patience per deployment via environment configuration.
    """
    raw_value = os.environ.get(
        GCS_REQUEST_TIMEOUT_ENV,
        str(DEFAULT_GCS_REQUEST_TIMEOUT_SECONDS),
    )
    try:
        timeout = float(raw_value)
    except (TypeError, ValueError):
        _LOGGER.warning(
            "Invalid %s=%r; using default %.1fs",
            GCS_REQUEST_TIMEOUT_ENV,
            raw_value,
            DEFAULT_GCS_REQUEST_TIMEOUT_SECONDS,
        )
        return DEFAULT_GCS_REQUEST_TIMEOUT_SECONDS
    if timeout <= 0:
        _LOGGER.warning(
            "Invalid %s=%r; using default %.1fs",
            GCS_REQUEST_TIMEOUT_ENV,
            raw_value,
            DEFAULT_GCS_REQUEST_TIMEOUT_SECONDS,
        )
        return DEFAULT_GCS_REQUEST_TIMEOUT_SECONDS
    return timeout


def _download_blob_to_filename_with_timeout(
    *, blob: "storage.Blob", destination_path: str, request_timeout: RequestTimeout
) -> None:
    """Download a single blob to a local file under the configured timeout.

    Creates parent directories first. Shared by the single-file and per-entry
    directory download paths so both honor the same request timeout.
    """
    os.makedirs(os.path.dirname(destination_path), exist_ok=True)
    blob.download_to_filename(filename=destination_path, timeout=request_timeout)


def _get_gcs_blob_hashes(blob: "storage.Blob") -> dict[str, str]:
    """Extract md5/crc32c hashes from a GCS blob's metadata.

    Feeds artifact integrity checks and cache-key / directory-hash computation
    in ``get_info``. (Composite GCS objects have no md5 hash, so it may be
    absent.)
    """
    hashes = {}
    # Note: Composite GCS objects do not have MD5 hash metadata.
    # See: https://docs.cloud.google.com/storage/docs/composite-objects#metadata
    if blob.md5_hash:
        # blob.md5_hash is a base64-encoded hash digest byte array. E.g. "1B2M2Y8AsgTpgAmY7PhCfg=="
        hashes["md5"] = base64.decodebytes(blob.md5_hash.encode("ascii")).hex()
    if blob.crc32c:
        # blob.crc32c is a base64-encoded hash digest byte array. E.g. "4gcgLQ=="
        hashes["crc32c"] = base64.decodebytes(blob.crc32c.encode("ascii")).hex()
    return hashes
