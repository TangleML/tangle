"""Patched GCS storage provider that handles composite objects missing MD5 hashes.

GCS composite objects (created via parallel uploads or compose operations) do not
have an MD5 hash. The upstream SDK crashes with `AttributeError: 'NoneType' object
has no attribute 'encode'` when it tries to read `blob.md5_hash` for these objects.

This subclass overrides `_get_info_from_uri` to fall back to a unique timestamp
when MD5 is missing, which forces a cache miss for any downstream task that consumes
these artifacts. See: https://cloud.google.com/storage/docs/composite-objects
"""

import base64
import datetime
import logging

from cloud_pipelines.orchestration.storage_providers import google_cloud_storage
from cloud_pipelines.orchestration.storage_providers import interfaces

_LOGGER = logging.getLogger(name=__name__)


def _blob_hash(
    *,
    blob,
) -> dict[str, str]:
    """Return a hash dict for a GCS blob, handling composite objects gracefully.

    For normal objects: returns {"md5": "<hex digest>"}.
    For composite objects (md5_hash is None): returns {"md5": "no_md5_<ISO timestamp>"}
    which produces a unique, non-repeating value that forces a cache miss.

    The key must always be "md5" because the upstream SDK's _make_data_info_for_dir
    assumes all files in a directory share the same hash key names and uses hashlib.new()
    with that key name.
    """
    if blob.md5_hash is not None:
        return {
            "md5": base64.decodebytes(blob.md5_hash.encode("ascii")).hex(),
        }

    _LOGGER.warning(
        f"Blob {blob.name} is a composite object (component_count={blob.component_count}) "
        f"with no MD5 hash. Using timestamp fallback — downstream tasks will not use caching for this artifact."
    )
    timestamp = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
    return {
        "md5": f"no_md5_{timestamp}",
    }


class PatchedGoogleCloudStorageProvider(
    google_cloud_storage.GoogleCloudStorageProvider,
):
    """GCS provider that gracefully handles composite objects with no MD5 hash.

    NOTE: _get_info_from_uri is copied verbatim from the upstream SDK
    (cloud-pipelines==0.26.3.12, google_cloud_storage.py lines 142-179)
    with the only change being: blob.md5_hash.encode("ascii") replaced
    by _blob_hash(blob=blob) to handle None md5_hash on composite objects.
    If the upstream SDK is upgraded, this override must be kept in sync.
    """

    def _get_info_from_uri(self, uri: str) -> interfaces.DataInfo:
        from google.cloud import storage

        blob_or_dir = storage.Blob.from_string(uri=uri, client=self._client)
        if blob_or_dir.exists():
            blob = blob_or_dir
            blob.reload()
            return interfaces.DataInfo(
                total_size=blob.size,
                is_dir=False,
                hashes=_blob_hash(blob=blob),
            )

        dir_prefix = blob_or_dir.name.rstrip("/") + "/"
        file_info_list = []
        for blob in self._client.list_blobs(
            bucket_or_name=blob_or_dir.bucket,
            prefix=dir_prefix,
        ):
            blob.reload()
            assert blob.name.startswith(dir_prefix)
            relative_source_blob_name = blob.name[len(dir_prefix) :]
            file_info_list.append(
                interfaces._FileInfo(
                    path=relative_source_blob_name,
                    size=blob.size,
                    hashes=_blob_hash(blob=blob),
                )
            )
        data_info = interfaces._make_data_info_for_dir(file_info_list)
        data_info._file_info_list = file_info_list
        return data_info
