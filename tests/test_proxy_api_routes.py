import asyncio
import gzip
import json

import anyio
import fastapi
import httpx
import pytest
from fastapi.testclient import TestClient
from starlette import status
from starlette.middleware.base import BaseHTTPMiddleware

from cloud_pipelines_backend import proxy_api_routes
from tests.proxy_helpers import (
    ByteStream,
    Upstream,
    asgi_request,
    response_body,
)

try:
    from builtins import ExceptionGroup
except ImportError:
    from exceptiongroup import ExceptionGroup


@pytest.fixture
def upstream(monkeypatch):
    return Upstream(monkeypatch)


@pytest.fixture
def app():
    application = fastapi.FastAPI()

    async def headers(request: fastapi.Request):
        if request.headers.get("cookie") != "session=valid":
            raise fastapi.HTTPException(
                status.HTTP_401_UNAUTHORIZED, "Authentication required"
            )
        return {"Authorization": "Bearer provider-token"}

    proxy_api_routes.setup_routes(
        app=application,
        upstream_base_url="https://provider.example/api/v2",
        route_prefix="/ai",
        allowed_headers=frozenset(),
        headers_getter=headers,
    )
    return application


def call(app, **kwargs):
    return asgi_request(
        app, b"/ai/responses", headers=[(b"cookie", b"session=valid")], **kwargs
    )


def add_passthrough_middleware(app):
    # Match the middleware style used by applications wrapping the proxy.
    async def passthrough(request, call_next):
        return await call_next(request)

    app.add_middleware(BaseHTTPMiddleware, dispatch=passthrough)


def test_another_provider_receives_only_its_configured_headers(app, upstream):
    messages = asyncio.run(call(app, method="POST", body=b'{"model":"custom"}'))
    assert messages[0]["status"] == status.HTTP_200_OK
    request = upstream.requests[0]
    assert str(request.url) == "https://provider.example/api/v2/responses"
    assert dict(request.headers) == {
        "host": "provider.example",
        "content-length": "18",
        "authorization": "Bearer provider-token",
    }
    upstream.assert_closed()


def test_provider_authentication_dependency_is_enforced(app, upstream):
    assert TestClient(app).get("/ai/models").status_code == status.HTTP_401_UNAUTHORIZED
    assert not upstream.clients


def test_raw_api_path_and_query_are_preserved(app, upstream):
    api_path = b"files/a%2Fb%3Fc%23d%25e%20f/%E2%98%83"
    query = b"tag=one&tag=two&opaque=%2f%25"
    messages = asyncio.run(
        asgi_request(
            app,
            b"/ai/" + api_path,
            query=query,
            headers=[(b"cookie", b"session=valid")],
        )
    )
    assert messages[0]["status"] == status.HTTP_200_OK
    assert upstream.requests[0].url.raw_path == b"/api/v2/" + api_path + b"?" + query
    upstream.assert_closed()


@pytest.mark.parametrize(
    "layers, value, expected_status",
    [
        (1, b"61", status.HTTP_200_OK),
        (2, b"61", status.HTTP_200_OK),
        (7, b"61", status.HTTP_200_OK),
        (8, b"61", status.HTTP_400_BAD_REQUEST),
        (8001, b"61", status.HTTP_400_BAD_REQUEST),
        (7, b"2e", status.HTTP_400_BAD_REQUEST),
    ],
)
def test_nested_path_encoding_is_bounded(app, upstream, layers, value, expected_status):
    api_path = b"models/%" + b"25" * (layers - 1) + value
    messages = asyncio.run(
        asgi_request(
            app,
            b"/ai/" + api_path,
            headers=[(b"cookie", b"session=valid")],
        )
    )
    assert messages[0]["status"] == expected_status
    if expected_status == status.HTTP_200_OK:
        assert upstream.requests[0].url.raw_path == b"/api/v2/" + api_path
        upstream.assert_closed()
    else:
        assert not upstream.requests
        assert not upstream.clients


@pytest.mark.parametrize(
    "connection_headers",
    [
        [("Connection", "Accept, Authorization")],
        [("Connection", "aCcEpT"), ("Connection", "AUTHORIZATION")],
    ],
)
def test_provider_header_allowlist_is_configurable_and_case_insensitive(
    upstream, connection_headers
):
    app = fastapi.FastAPI()
    proxy_api_routes.setup_routes(
        app=app,
        upstream_base_url="https://provider.example/v2",
        route_prefix="/proxy",
        allowed_headers=frozenset({"X-Provider-Mode", "AUTHORIZATION", "ACCEPT"}),
        headers_getter=lambda: {"Authorization": "Bearer provider-token"},
    )
    response = TestClient(app).get(
        "/proxy/models",
        headers=[
            ("x-provider-mode", "custom"),
            ("authorization", "Bearer caller-token"),
            ("Accept", "text/event-stream"),
            ("Cookie", "session=secret"),
            ("X-Unlisted-Token", "secret"),
            *connection_headers,
        ],
    )
    assert response.status_code == status.HTTP_200_OK
    assert dict(upstream.requests[0].headers) == {
        "host": "provider.example",
        "authorization": "Bearer provider-token",
        "x-provider-mode": "custom",
    }
    upstream.assert_closed()


def test_client_disables_ambient_auth_and_has_bounded_timeouts():
    async def check():
        async with proxy_api_routes._create_client() as client:
            assert client.trust_env is False
            assert client.follow_redirects is False
            assert client.timeout.connect == 10
            assert client.timeout.read == 300
            assert client.timeout.write == 60
            assert client.timeout.pool == 60

    asyncio.run(check())


def test_compressed_response_bytes_and_metadata_are_preserved(app, upstream):
    body = gzip.compress(b'data: {"delta":"hello"}\n\ndata: [DONE]\n\n')
    stream = ByteStream(body[:10], body[10:])
    upstream.streams.append(stream)
    upstream.handler = lambda request: httpx.Response(
        status.HTTP_201_CREATED,
        headers={
            "Content-Encoding": "gzip",
            "Content-Length": str(len(body)),
            "Content-Type": "text/event-stream",
            "Vary": "Accept-Encoding",
        },
        stream=stream,
    )
    messages = asyncio.run(call(app))
    assert messages[0]["status"] == status.HTTP_201_CREATED
    headers = dict(messages[0]["headers"])
    assert headers[b"content-encoding"] == b"gzip"
    assert headers[b"content-length"] == str(len(body)).encode()
    assert headers[b"content-type"] == b"text/event-stream"
    assert headers[b"vary"] == b"Accept-Encoding"
    assert response_body(messages) == body
    upstream.assert_closed()


def test_response_headers_drop_connection_fields_and_cookies(app, upstream):
    stream = ByteStream(b"body")
    upstream.streams.append(stream)
    upstream.handler = lambda request: httpx.Response(
        status.HTTP_200_OK,
        stream=stream,
        headers=[
            ("Content-Type", "application/octet-stream"),
            ("Connection", "X-Internal, Keep-Alive"),
            ("CONNECTION", "X-Second"),
            ("X-Internal", "private"),
            ("X-Second", "private"),
            ("Set-Cookie", "upstream=secret"),
            ("SET-cookie", "upstream2=secret"),
            ("Keep-Alive", "timeout=60"),
            ("Transfer-Encoding", "chunked"),
            ("Proxy-Authenticate", "Basic"),
            ("Proxy-Authorization", "Basic secret"),
            ("Proxy-Connection", "keep-alive"),
            ("TE", "trailers"),
            ("Trailer", "X-Internal"),
            ("Upgrade", "websocket"),
            ("X-Request-ID", "request-1"),
            ("Link", "<https://example.com/first>"),
            ("Link", "<https://example.com/second>"),
        ],
    )
    messages = asyncio.run(call(app))
    assert messages[0]["headers"] == [
        (b"content-type", b"application/octet-stream"),
        (b"x-request-id", b"request-1"),
        (b"link", b"<https://example.com/first>"),
        (b"link", b"<https://example.com/second>"),
    ]
    assert response_body(messages) == b"body"
    upstream.assert_closed()


@pytest.mark.parametrize("with_middleware", [False, True])
def test_sse_is_delivered_before_the_next_chunk_is_available(
    app, upstream, with_middleware
):
    if with_middleware:
        add_passthrough_middleware(app)

    async def check():
        first_sent = anyio.Event()

        class Stream(ByteStream):
            async def __aiter__(self):
                yield b'data: {"delta":"hello"}\n\n'
                await first_sent.wait()
                yield b"data: [DONE]\n\n"

        stream = Stream()
        upstream.streams.append(stream)
        upstream.handler = lambda request: httpx.Response(
            status.HTTP_200_OK,
            headers={"Content-Type": "text/event-stream"},
            stream=stream,
        )

        async def sent(message):
            if message.get("body") == b'data: {"delta":"hello"}\n\n':
                assert message["more_body"] is True
                first_sent.set()

        messages = await call(app, on_send=sent)
        assert first_sent.is_set()
        assert [m["body"] for m in messages if m["type"] == "http.response.body"] == [
            b'data: {"delta":"hello"}\n\n',
            b"data: [DONE]\n\n",
            b"",
        ]
        assert messages[-1]["more_body"] is False
        upstream.assert_closed()

    asyncio.run(check())


@pytest.mark.parametrize("phase", ["request", "first_read", "streaming"])
@pytest.mark.parametrize("with_middleware", [False, True])
def test_disconnect_closes_upstream_at_every_stage(
    app, upstream, phase, with_middleware
):
    if with_middleware:
        add_passthrough_middleware(app)

    async def check():
        disconnected = anyio.Event()

        class Stream(ByteStream):
            async def __aiter__(self):
                if phase == "streaming":
                    yield b"data: first\n\n"
                disconnected.set()
                await anyio.sleep_forever()
                yield b"unreachable"

        async def handler(request):
            if phase == "request":
                disconnected.set()
                await anyio.sleep_forever()
            stream = Stream()
            upstream.streams.append(stream)
            return httpx.Response(status.HTTP_200_OK, stream=stream)

        upstream.handler = handler
        messages = await call(app, disconnected=disconnected)
        if phase == "streaming":
            assert messages[0]["status"] == status.HTTP_200_OK
            assert response_body(messages) == b"data: first\n\n"
        else:
            assert messages[0]["status"] == 499
            assert response_body(messages) == b""
        upstream.assert_closed()

    asyncio.run(check())


def test_parent_cancellation_closes_upstream(app, upstream):
    async def check():
        started = anyio.Event()

        class Stream(ByteStream):
            async def __aiter__(self):
                started.set()
                await anyio.sleep_forever()
                yield b"unreachable"

        stream = Stream()
        upstream.streams.append(stream)
        upstream.handler = lambda request: httpx.Response(
            status.HTTP_200_OK, stream=stream
        )
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(call, app)
            await started.wait()
            tasks.cancel_scope.cancel()
        upstream.assert_closed()

    asyncio.run(check())


@pytest.mark.parametrize("message_type", ["http.response.start", "http.response.body"])
def test_failed_downstream_write_closes_resources(app, upstream, message_type):
    async def fail_send(message):
        if message["type"] == message_type:
            raise OSError("caller disconnected")

    with pytest.raises(ExceptionGroup) as error:
        asyncio.run(call(app, on_send=fail_send))
    assert isinstance(error.value.exceptions[0], OSError)
    upstream.assert_closed()


@pytest.mark.parametrize(
    "error_type, phase, expected_status, detail",
    [
        (
            httpx.ConnectError,
            "request",
            status.HTTP_502_BAD_GATEWAY,
            "Upstream request failed",
        ),
        (
            httpx.ReadError,
            "first_read",
            status.HTTP_502_BAD_GATEWAY,
            "Upstream request failed",
        ),
        (
            httpx.ConnectTimeout,
            "request",
            status.HTTP_504_GATEWAY_TIMEOUT,
            "Upstream request timed out",
        ),
        (
            httpx.ReadTimeout,
            "first_read",
            status.HTTP_504_GATEWAY_TIMEOUT,
            "Upstream request timed out",
        ),
    ],
)
def test_upstream_failures_are_sanitized_and_closed(
    app, upstream, error_type, expected_status, detail, phase
):
    class Stream(ByteStream):
        async def __aiter__(self):
            raise error_type("sensitive-token https://private.example/internal")
            yield b"unreachable"

    def handler(request):
        if phase == "request":
            raise error_type("sensitive-token https://private.example/internal")
        stream = Stream()
        upstream.streams.append(stream)
        return httpx.Response(status.HTTP_200_OK, stream=stream)

    upstream.handler = handler
    messages = asyncio.run(call(app))
    assert messages[0]["status"] == expected_status
    assert json.loads(response_body(messages)) == {"detail": detail}
    upstream.assert_closed()


@pytest.mark.parametrize("error_type", [httpx.ReadError, httpx.ReadTimeout])
def test_failure_after_headers_aborts_the_stream_without_leaking_details(
    app, upstream, error_type
):
    class Stream(ByteStream):
        async def __aiter__(self):
            yield b"data: first\n\n"
            raise error_type("sensitive-token private-url")

    stream = Stream()
    upstream.streams.append(stream)
    upstream.handler = lambda request: httpx.Response(status.HTTP_200_OK, stream=stream)
    messages = []

    async def capture(message):
        messages.append(message)

    with pytest.raises(ExceptionGroup) as error:
        asyncio.run(call(app, on_send=capture))
    assert str(error.value.exceptions[0]) == "Upstream response stream failed"
    assert messages[0]["status"] == status.HTTP_200_OK
    assert response_body(messages) == b"data: first\n\n"
    assert all(m.get("more_body", True) for m in messages)
    upstream.assert_closed()


def test_empty_upstream_response_closes_resources(app, upstream):
    stream = ByteStream()
    upstream.streams.append(stream)
    upstream.handler = lambda request: httpx.Response(
        status.HTTP_204_NO_CONTENT, stream=stream
    )
    messages = asyncio.run(call(app))
    assert messages[0]["status"] == status.HTTP_204_NO_CONTENT
    assert response_body(messages) == b""
    upstream.assert_closed()
