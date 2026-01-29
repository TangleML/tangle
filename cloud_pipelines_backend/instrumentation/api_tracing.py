"""Request context middleware for FastAPI applications.

This middleware automatically generates a request_id for each incoming HTTP request,
sets it in the logging context for the duration of the request, and includes it in
the response headers.
"""

import logging
import secrets

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from . import contextual_logging

logger = logging.getLogger(__name__)


def generate_request_id() -> str:
    """Generate a new request ID compatible with OpenTelemetry format.

    OpenTelemetry trace IDs are 16-byte (128-bit) values represented as
    32 hexadecimal characters (lowercase). We use the same format for
    request IDs to maintain compatibility.

    Returns:
        A 32-character hexadecimal string representing the request ID
    """
    return secrets.token_hex(16)


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Middleware to manage request_id for each request.

    For each incoming request:
    1. Generates a new request_id (32-character hex string)
    2. Sets it in the logging context (as 'request_id' key)
    3. Adds it to the response headers as 'x-tangle-request-id'
    4. Clears it after the request completes

    This ensures all logs during the request processing include the same request_id.
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        """Process each request with a new request_id.

        Args:
            request: The incoming HTTP request
            call_next: The next middleware or route handler

        Returns:
            The HTTP response with request_id in headers
        """
        # Generate a new request_id for this request
        request_id = generate_request_id()

        with contextual_logging.logging_context(request_id=request_id):
            response = await call_next(request)
            response.headers["x-tangle-request-id"] = request_id
            return response
