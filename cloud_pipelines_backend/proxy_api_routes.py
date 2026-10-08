"""Reusable authenticated HTTP forwarding with an explicitly configured upstream."""

from collections.abc import Awaitable, Callable, Mapping
from typing import Annotated
from urllib.parse import unquote_to_bytes

import anyio
import fastapi
import httpx
from starlette import status
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.types import Message, Receive, Scope, Send

_MAX_PATH_DECODE_PASSES = 8
# Starlette does not define this nonstandard status for a disconnected caller.
_HTTP_CLIENT_CLOSED_REQUEST = 499
_HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}


def _filtered_headers(
    *, headers: httpx.Headers, allowed: frozenset[str] | None = None
) -> list[tuple[bytes, bytes]]:
    excluded = _HOP_BY_HOP_HEADERS | {
        name.lower() for name in headers.get_list("connection", split_commas=True)
    }
    if allowed is None:
        excluded.add("set-cookie")
    filtered = []
    for name, value in headers.raw:
        name = name.lower()
        key = name.decode("ascii")
        if key not in excluded and (allowed is None or key in allowed):
            filtered.append((name, value))
    return filtered


def _validate_api_path(*, api_path: bytes) -> None:
    """Reject unsafe decoded forms without changing the forwarded path."""
    decoded = api_path
    # Bound the work required to check nested percent escapes.
    for _ in range(_MAX_PATH_DECODE_PASSES):
        if (
            b"\\" in decoded
            or any(segment in (b".", b"..") for segment in decoded.split(b"/"))
            or any(char < 32 or char == 127 for char in decoded)
        ):
            break
        unescaped = unquote_to_bytes(decoded)
        if unescaped == decoded:
            return
        decoded = unescaped
    raise fastapi.HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid proxy path"
    )


def _raw_api_path(*, request: fastapi.Request, prefix: str) -> bytes:
    raw_path = request.scope["raw_path"]
    raw_prefix = prefix.encode("ascii") + b"/"
    if not raw_path.startswith(raw_prefix):
        raise fastapi.HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid proxy path"
        )
    api_path = raw_path[len(raw_prefix) :]

    _validate_api_path(api_path=api_path)
    return api_path


def _create_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        # Non-streaming generation can take minutes before its first byte.
        timeout=httpx.Timeout(60.0, connect=10.0, read=300.0),
        follow_redirects=False,
        trust_env=False,
    )


class _ProxyResponse(Response):
    def __init__(self, *, upstream_request: httpx.Request) -> None:
        super().__init__()
        self.upstream_request = upstream_request

    async def _stream(self, *, scope: Scope, receive: Receive, send: Send) -> None:
        client = _create_client()
        upstream: httpx.Response | None = None
        try:
            try:
                upstream = await client.send(self.upstream_request, stream=True)
                chunks = upstream.aiter_raw()
                # Delay response headers until the first read succeeds so an
                # initial read failure can still return a sanitized HTTP error.
                first_chunk = await anext(chunks, None)
            except httpx.RequestError as error:
                if isinstance(error, httpx.TimeoutException):
                    error_status, detail = (
                        status.HTTP_504_GATEWAY_TIMEOUT,
                        "Upstream request timed out",
                    )
                else:
                    error_status, detail = (
                        status.HTTP_502_BAD_GATEWAY,
                        "Upstream request failed",
                    )
                await JSONResponse({"detail": detail}, status_code=error_status)(
                    scope, receive, send
                )
                return

            async def body():
                if first_chunk is not None:
                    yield first_chunk
                try:
                    async for chunk in chunks:
                        yield chunk
                except httpx.RequestError:
                    # Once streaming starts the HTTP status cannot be changed.
                    # Abort without exposing upstream request data.
                    raise RuntimeError("Upstream response stream failed") from None

            response = StreamingResponse(body(), status_code=upstream.status_code)
            response.raw_headers = _filtered_headers(headers=upstream.headers)
            await response.stream_response(send)
        finally:
            # Disconnects cancel the streaming task. Shield cleanup so both
            # resources close even during cancellation or a failed socket write.
            with anyio.CancelScope(shield=True):
                try:
                    if upstream is not None:
                        await upstream.aclose()
                finally:
                    await client.aclose()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # ASGI invokes response and send callbacks with positional arguments.
        response_started = False

        async def send_response(message: Message) -> None:
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
            await send(message)

        async with anyio.create_task_group() as tasks:

            async def stream() -> None:
                await self._stream(scope=scope, receive=receive, send=send_response)
                tasks.cancel_scope.cancel()

            tasks.start_soon(stream)
            # Watch disconnects during connection setup, reads, and writes.
            while (await receive())["type"] != "http.disconnect":
                pass
            tasks.cancel_scope.cancel()
        if not response_started:
            # The caller disconnected before an upstream response was ready.
            # Complete the ASGI exchange so middleware does not report a 500.
            await Response(status_code=_HTTP_CLIENT_CLOSED_REQUEST)(
                scope, receive, send
            )


def setup_routes(
    *,
    app: fastapi.FastAPI,
    upstream_base_url: str,
    route_prefix: str,
    allowed_headers: frozenset[str],
    headers_getter: Callable[..., Mapping[str, str] | Awaitable[Mapping[str, str]]],
) -> None:
    """Register GET/POST routes using a FastAPI dependency for trusted headers.

    The dependency must authenticate each caller (raising HTTPException on
    failure) and return server-controlled headers. The host also supplies the
    caller-header allowlist; server headers take precedence. The route prefix
    excludes a trailing slash and must appear literally in the request. The
    ASGI server must provide raw_path (as Uvicorn does). The upstream URL is
    trusted application configuration, never client input.

    Upstream reads allow five minutes between chunks (including the first
    byte). Connection setup allows ten seconds; writes and pool waits allow
    sixty seconds. Caller and ingress timeouts can impose shorter limits.
    """
    upstream = httpx.URL(upstream_base_url)
    allowed = frozenset(name.lower() for name in allowed_headers)

    async def proxy(
        *,
        request: fastapi.Request,
        path: str,
        headers: Annotated[Mapping[str, str], fastapi.Depends(headers_getter)],
    ) -> Response:
        api_path = _raw_api_path(request=request, prefix=route_prefix)
        url = upstream.copy_with(
            raw_path=upstream.raw_path.rstrip(b"/") + b"/" + api_path,
        ).copy_with(query=request.scope["query_string"] or None)
        forwarded = httpx.Headers(
            _filtered_headers(
                headers=httpx.Headers(request.headers.raw), allowed=allowed
            )
        )
        forwarded.update(headers)
        # Construct the request directly to avoid client default headers,
        # cookie persistence, query normalization, or payload rewriting.
        return _ProxyResponse(
            upstream_request=httpx.Request(
                request.method,
                url,
                headers=forwarded,
                content=await request.body(),
            )
        )

    for method in ("GET", "POST"):
        app.add_api_route(
            route_prefix + "/{path:path}",
            proxy,
            methods=[method],
            tags=["proxy"],
            response_class=Response,
        )
