"""Tests for ArtifactNodesApiService_Sql.get_signed_artifact_url."""

import datetime
import sys
import types
import unittest.mock

import pytest
from sqlalchemy import orm

from cloud_pipelines_backend import api_server_sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend import database_ops

_SIGNER_ENV_VAR = "TANGLE_ARTIFACT_SIGNER_SERVICE_ACCOUNT"
_SIGNER_EMAIL = "artifact-signer@example-project.iam.gserviceaccount.com"


@pytest.fixture()
def session() -> orm.Session:
    db_engine = database_ops.create_db_engine(database_uri="sqlite://")
    bts._TableBase.metadata.create_all(db_engine)
    with orm.Session(db_engine) as s:
        yield s


def _add_artifact_node(session: orm.Session, uri: str) -> bts.IdType:
    artifact_node = bts.ArtifactNode(
        artifact_data=bts.ArtifactData(
            total_size=1, is_dir=False, hash="md5=0", uri=uri
        )
    )
    session.add(artifact_node)
    session.commit()
    return artifact_node.id


@pytest.fixture()
def google_mocks(monkeypatch):
    # google-auth and google-cloud-storage are not test dependencies, so the modules the
    # handler imports lazily are replaced in sys.modules, which import resolves first.
    modules = {
        name: types.ModuleType(name)
        for name in [
            "google",
            "google.auth",
            "google.auth.compute_engine",
            "google.auth.iam",
            "google.auth.transport",
            "google.auth.transport.requests",
            "google.cloud",
            "google.cloud.storage",
            "google.oauth2",
            "google.oauth2.service_account",
        ]
    }
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
        parent_name, _, child_name = name.rpartition(".")
        if parent_name:
            setattr(modules[parent_name], child_name, module)

    auth_default = unittest.mock.MagicMock()
    compute_credentials = unittest.mock.MagicMock()
    iam_signer = unittest.mock.MagicMock()
    service_account_credentials = unittest.mock.MagicMock()
    storage_client = unittest.mock.MagicMock()
    storage_blob = unittest.mock.MagicMock()
    modules["google.auth"].default = auth_default
    modules["google.auth.compute_engine"].Credentials = compute_credentials
    modules["google.auth.iam"].Signer = iam_signer
    modules["google.auth.transport.requests"].Request = unittest.mock.MagicMock()
    modules["google.oauth2.service_account"].Credentials = service_account_credentials
    modules["google.cloud.storage"].Client = storage_client
    modules["google.cloud.storage"].Blob = storage_blob
    blob_from_string = storage_blob.from_string

    default_credentials = unittest.mock.MagicMock()
    default_credentials.token = "developer-access-token"
    auth_default.return_value = (default_credentials, "example-project")
    blob = blob_from_string.return_value
    blob.generate_signed_url.return_value = "https://signed.example/url"
    yield unittest.mock.Mock(
        auth_default=auth_default,
        default_credentials=default_credentials,
        compute_credentials=compute_credentials,
        iam_signer=iam_signer,
        service_account_credentials=service_account_credentials,
        storage_client=storage_client,
        blob_from_string=blob_from_string,
        blob=blob,
    )


class TestGetSignedArtifactUrl:
    def test_signs_as_configured_service_account_when_env_var_set(
        self, session: orm.Session, google_mocks, monkeypatch
    ) -> None:
        monkeypatch.setenv(_SIGNER_ENV_VAR, _SIGNER_EMAIL)
        artifact_id = _add_artifact_node(session, "gs://bucket/path/file.txt")

        response = api_server_sql.ArtifactNodesApiService_Sql().get_signed_artifact_url(
            session=session, id=artifact_id
        )

        assert response.signed_url == "https://signed.example/url"
        google_mocks.auth_default.assert_called_once_with(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        google_mocks.default_credentials.refresh.assert_called_once()
        google_mocks.compute_credentials.assert_not_called()
        google_mocks.iam_signer.assert_not_called()
        google_mocks.storage_client.assert_called_once_with(
            credentials=google_mocks.default_credentials
        )
        google_mocks.blob_from_string.assert_called_once_with(
            uri="gs://bucket/path/file.txt",
            client=google_mocks.storage_client.return_value,
        )
        google_mocks.blob.generate_signed_url.assert_called_once_with(
            version="v4",
            expiration=datetime.timedelta(days=7),
            service_account_email=_SIGNER_EMAIL,
            access_token="developer-access-token",
        )

    def test_uses_compute_engine_credentials_when_env_var_unset(
        self, session: orm.Session, google_mocks, monkeypatch
    ) -> None:
        monkeypatch.delenv(_SIGNER_ENV_VAR, raising=False)
        artifact_id = _add_artifact_node(session, "gs://bucket/path/file.txt")

        response = api_server_sql.ArtifactNodesApiService_Sql().get_signed_artifact_url(
            session=session, id=artifact_id
        )

        assert response.signed_url == "https://signed.example/url"
        google_mocks.compute_credentials.assert_called_once_with()
        google_mocks.compute_credentials.return_value.refresh.assert_called_once()
        google_mocks.iam_signer.assert_called_once()
        google_mocks.auth_default.assert_not_called()
        google_mocks.storage_client.assert_called_once_with(
            credentials=google_mocks.service_account_credentials.return_value
        )
        google_mocks.blob.generate_signed_url.assert_called_once_with(
            expiration=datetime.timedelta(days=7)
        )

    @pytest.mark.parametrize("signer_email", [_SIGNER_EMAIL, None])
    def test_non_gcs_uri_raises_before_credential_work(
        self, session: orm.Session, google_mocks, monkeypatch, signer_email
    ) -> None:
        if signer_email is None:
            monkeypatch.delenv(_SIGNER_ENV_VAR, raising=False)
        else:
            monkeypatch.setenv(_SIGNER_ENV_VAR, signer_email)
        artifact_id = _add_artifact_node(session, "s3://bucket/path/file.txt")

        with pytest.raises(ValueError, match="only supports Google Cloud Storage"):
            api_server_sql.ArtifactNodesApiService_Sql().get_signed_artifact_url(
                session=session, id=artifact_id
            )

        google_mocks.auth_default.assert_not_called()
        google_mocks.compute_credentials.assert_not_called()
        google_mocks.storage_client.assert_not_called()
        google_mocks.blob.generate_signed_url.assert_not_called()
