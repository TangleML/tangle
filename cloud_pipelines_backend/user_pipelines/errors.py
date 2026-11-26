import fastapi
import fastapi.responses
from starlette import status


class PipelineError(Exception):
    pass


class PipelineNotFoundError(PipelineError):
    pass


class PipelineValidationError(PipelineError):
    pass


class SecretChangeNotAllowedError(PipelineError):
    pass


_STATUS_CODES = {
    PipelineNotFoundError: status.HTTP_404_NOT_FOUND,
    PipelineValidationError: status.HTTP_422_UNPROCESSABLE_CONTENT,
    SecretChangeNotAllowedError: status.HTTP_403_FORBIDDEN,
}


def register_pipeline_exception_handlers(*, app: fastapi.FastAPI) -> None:
    """Maps saved-pipeline errors to HTTP responses."""

    async def handle(
        request: fastapi.Request, exc: Exception
    ) -> fastapi.responses.JSONResponse:
        return fastapi.responses.JSONResponse(
            status_code=_STATUS_CODES[type(exc)], content={"detail": str(exc)}
        )

    for error_type in _STATUS_CODES:
        app.add_exception_handler(error_type, handle)
