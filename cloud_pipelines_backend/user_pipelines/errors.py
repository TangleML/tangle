import typing

import fastapi
import fastapi.responses
from starlette import status


class PipelineError(Exception):
    pass


class PipelineNotFoundError(PipelineError):
    pass


class PipelineValidationError(PipelineError):
    pass


class VersionNotFoundError(PipelineError):
    """No version of this pipeline carries the requested content.

    Deliberately not a `PipelineNotFoundError`: the pipeline was found, and a caller that
    cannot tell the two apart cannot tell "you named a pipeline that is not yours" (404) from
    "you named a version that does not exist" (422).
    """


class VersionNotPinnableError(PipelineError):
    """The named version exists but is a mutable head, so it cannot be referenced durably."""


class RunTargetUnavailableError(PipelineError):
    """A stored pipeline targets a cluster or accelerator that is no longer available.

    Carries the same ``code`` and resolved ``successor`` a direct submit is rejected
    with, so a client sees one contract whichever path created the run.
    """

    def __init__(
        self,
        message: str,
        *,
        code: str,
        successor: dict[str, typing.Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.successor = successor


def register_pipeline_exception_handlers(*, app: fastapi.FastAPI) -> None:
    """Map user-pipeline domain errors to their public HTTP responses."""

    @app.exception_handler(PipelineNotFoundError)
    async def handle_pipeline_not_found(
        request: fastapi.Request,
        exc: PipelineNotFoundError,
    ) -> fastapi.responses.JSONResponse:
        del request
        return fastapi.responses.JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"detail": str(exc)},
        )

    @app.exception_handler(PipelineValidationError)
    async def handle_pipeline_validation_error(
        request: fastapi.Request,
        exc: PipelineValidationError,
    ) -> fastapi.responses.JSONResponse:
        del request
        return fastapi.responses.JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            content={"detail": str(exc)},
        )

    @app.exception_handler(RunTargetUnavailableError)
    async def handle_run_target_unavailable(
        request: fastapi.Request,
        exc: RunTargetUnavailableError,
    ) -> fastapi.responses.JSONResponse:
        del request
        return fastapi.responses.JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            content={
                "detail": str(exc),
                "code": exc.code,
                "successor": exc.successor,
            },
        )
