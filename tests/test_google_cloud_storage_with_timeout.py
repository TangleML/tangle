import sys
import types
from unittest import mock

# cloud-pipelines-backend keeps GCS support optional. Stub google.cloud.storage
# before importing the timeout provider so these unit tests do not require the
# optional Google client package.
_google_mod = types.ModuleType("google")
_cloud_mod = types.ModuleType("google.cloud")
_storage_mod = types.ModuleType("google.cloud.storage")
_storage_mod.Blob = mock.MagicMock()
_storage_mod.Client = mock.MagicMock()
_cloud_mod.storage = _storage_mod
_google_mod.cloud = _cloud_mod
sys.modules.setdefault("google", _google_mod)
sys.modules.setdefault("google.cloud", _cloud_mod)
sys.modules.setdefault("google.cloud.storage", _storage_mod)

from cloud_pipelines_backend.storage_providers import google_cloud_storage_with_timeout


class TestGoogleCloudStorageProviderWithTimeout:
    def test_upload_bytes_passes_timeout(self) -> None:
        storage = mock.MagicMock()
        blob = mock.MagicMock()
        storage.Blob.from_string.return_value = blob
        provider = (
            google_cloud_storage_with_timeout.GoogleCloudStorageProviderWithTimeout(
                client=mock.MagicMock(), request_timeout=12
            )
        )
        uri = mock.MagicMock(uri="gs://bucket/object")

        with mock.patch.object(
            google_cloud_storage_with_timeout,
            "_storage_module",
            return_value=storage,
        ):
            provider.upload_bytes(b"data", uri)

        blob.upload_from_string.assert_called_once_with(
            data=b"data", checksum="md5", timeout=12
        )

    def test_exists_passes_timeout_to_file_and_dir_blobs(self) -> None:
        storage = mock.MagicMock()
        file_blob = mock.MagicMock()
        file_blob.exists.return_value = False
        dir_blob = mock.MagicMock()
        dir_blob.exists.return_value = True
        storage.Blob.from_string.side_effect = [file_blob, dir_blob]
        provider = (
            google_cloud_storage_with_timeout.GoogleCloudStorageProviderWithTimeout(
                client=mock.MagicMock(), request_timeout=12
            )
        )
        uri = mock.MagicMock(uri="gs://bucket/path")

        with mock.patch.object(
            google_cloud_storage_with_timeout,
            "_storage_module",
            return_value=storage,
        ):
            assert provider.exists(uri) is True

        file_blob.exists.assert_called_once_with(timeout=12)
        dir_blob.exists.assert_called_once_with(timeout=12)

    def test_file_get_info_passes_timeout_to_exists_and_reload(self) -> None:
        storage = mock.MagicMock()
        blob = mock.MagicMock()
        blob.exists.return_value = True
        blob.size = 123
        blob.md5_hash = None
        blob.crc32c = None
        storage.Blob.from_string.return_value = blob
        provider = (
            google_cloud_storage_with_timeout.GoogleCloudStorageProviderWithTimeout(
                client=mock.MagicMock(), request_timeout=12
            )
        )

        with mock.patch.object(
            google_cloud_storage_with_timeout,
            "_storage_module",
            return_value=storage,
        ):
            data_info = provider._get_info_from_uri("gs://bucket/object")

        blob.exists.assert_called_once_with(timeout=12)
        blob.reload.assert_called_once_with(timeout=12)
        assert data_info.total_size == 123
        assert data_info.is_dir is False
