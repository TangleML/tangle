import datetime
import sys
from unittest import mock

import pytest

# google-cloud-storage is not installed in the dev venv — mock the import chain
# so the module can be imported without the actual GCS SDK.
_mock_gcs_module = mock.MagicMock()
sys.modules.setdefault("google.cloud.storage", _mock_gcs_module)
sys.modules.setdefault("google.cloud", mock.MagicMock(storage=_mock_gcs_module))

from cloud_pipelines_backend.storage_providers import patched_google_cloud_storage


def _make_blob(
    *,
    name: str = "artifacts/test/output.txt",
    size: int = 1024,
    md5_hash: str | None = "1B2M2Y8AsgTpgAmY7PhCfg==",
    component_count: int | None = None,
) -> mock.MagicMock:
    blob = mock.MagicMock()
    blob.name = name
    blob.size = size
    blob.md5_hash = md5_hash
    blob.component_count = component_count
    return blob


class TestBlobHash:
    def test_normal_blob_returns_md5(self) -> None:
        blob = _make_blob(md5_hash="1B2M2Y8AsgTpgAmY7PhCfg==")
        result = patched_google_cloud_storage._blob_hash(blob=blob)

        assert "md5" in result
        assert result["md5"] == "d41d8cd98f00b204e9800998ecf8427e"

    def test_composite_blob_returns_timestamp_under_md5_key(self) -> None:
        blob = _make_blob(md5_hash=None, component_count=2)
        result = patched_google_cloud_storage._blob_hash(blob=blob)

        assert "md5" in result
        assert result["md5"].startswith("no_md5_")
        datetime.datetime.fromisoformat(result["md5"].removeprefix("no_md5_"))

    def test_composite_blob_timestamps_are_unique(self) -> None:
        blob = _make_blob(md5_hash=None, component_count=3)
        result_1 = patched_google_cloud_storage._blob_hash(blob=blob)
        result_2 = patched_google_cloud_storage._blob_hash(blob=blob)

        assert result_1["md5"] != result_2["md5"]

    def test_composite_blob_logs_warning(self) -> None:
        blob = _make_blob(
            name="artifacts/test/model.bin",
            md5_hash=None,
            component_count=5,
        )
        with mock.patch.object(
            patched_google_cloud_storage._LOGGER, "warning"
        ) as mock_warn:
            patched_google_cloud_storage._blob_hash(blob=blob)

        mock_warn.assert_called_once()
        call_args = mock_warn.call_args[0][0]
        assert "artifacts/test/model.bin" in call_args
        assert "component_count=5" in call_args


class TestPatchedProvider:
    def _make_provider(
        self,
    ) -> patched_google_cloud_storage.PatchedGoogleCloudStorageProvider:
        provider = patched_google_cloud_storage.PatchedGoogleCloudStorageProvider(
            client=mock.MagicMock(),
        )
        return provider

    def test_single_file_with_md5(self) -> None:
        provider = self._make_provider()
        blob = _make_blob(md5_hash="1B2M2Y8AsgTpgAmY7PhCfg==")
        blob.exists.return_value = True

        _mock_gcs_module.Blob.from_string.return_value = blob
        result = provider._get_info_from_uri("gs://bucket/file.txt")

        assert result.is_dir is False
        assert result.total_size == 1024
        assert result.hashes["md5"] == "d41d8cd98f00b204e9800998ecf8427e"

    def test_single_file_composite_no_md5(self) -> None:
        provider = self._make_provider()
        blob = _make_blob(md5_hash=None, component_count=2)
        blob.exists.return_value = True

        _mock_gcs_module.Blob.from_string.return_value = blob
        result = provider._get_info_from_uri("gs://bucket/file.txt")

        assert result.is_dir is False
        assert result.hashes["md5"].startswith("no_md5_")

    def test_directory_with_mixed_blobs(self) -> None:
        provider = self._make_provider()

        dir_blob = mock.MagicMock()
        dir_blob.exists.return_value = False
        dir_blob.name = "artifacts/output"
        dir_blob.bucket = "bucket"

        normal_blob = _make_blob(
            name="artifacts/output/data.csv",
            size=500,
            md5_hash="1B2M2Y8AsgTpgAmY7PhCfg==",
        )
        composite_blob = _make_blob(
            name="artifacts/output/model.bin",
            size=2000,
            md5_hash=None,
            component_count=2,
        )

        _mock_gcs_module.Blob.from_string.return_value = dir_blob
        provider._client.list_blobs.return_value = [normal_blob, composite_blob]
        result = provider._get_info_from_uri("gs://bucket/artifacts/output")

        assert result.is_dir is True
        assert result.total_size == 2500
