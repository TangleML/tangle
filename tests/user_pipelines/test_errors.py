import fastapi
import fastapi.testclient
import sqlalchemy
from cloud_pipelines_backend.user_pipelines import errors


class DerivedPipelineNotFoundError(errors.PipelineNotFoundError):
    pass


class DerivedPipelineValidationError(errors.PipelineValidationError):
    pass


def test_pipeline_error_handlers_follow_exception_mro(
    app: fastapi.FastAPI,
) -> None:
    @app.get("/_test/pipeline-not-found")
    def raise_not_found() -> None:
        raise DerivedPipelineNotFoundError("pipeline missing")

    @app.get("/_test/pipeline-invalid")
    def raise_validation_error() -> None:
        raise DerivedPipelineValidationError("pipeline invalid")

    client = fastapi.testclient.TestClient(app)

    not_found = client.get("/_test/pipeline-not-found")
    invalid = client.get("/_test/pipeline-invalid")

    assert not_found.status_code == 404
    assert not_found.json() == {"detail": "pipeline missing"}
    assert invalid.status_code == 422
    assert invalid.json() == {"detail": "pipeline invalid"}


def test_unrelated_errors_are_not_translated_to_pipeline_conflicts(
    app: fastapi.FastAPI,
) -> None:
    @app.get("/_test/unrelated-error")
    def raise_unrelated_error() -> None:
        raise RuntimeError("unexpected")

    @app.get("/_test/integrity-error")
    def raise_integrity_error() -> None:
        raise sqlalchemy.exc.IntegrityError(
            "insert failed",
            params={},
            orig=RuntimeError("database failure"),
        )

    client = fastapi.testclient.TestClient(app, raise_server_exceptions=False)

    unrelated = client.get("/_test/unrelated-error")
    integrity = client.get("/_test/integrity-error")

    assert unrelated.status_code == 500
    assert integrity.status_code == 500
    assert unrelated.status_code != 409
    assert integrity.status_code != 409
