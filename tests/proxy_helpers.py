"""Mock upstream and raw ASGI helpers (no network or external credentials)."""

from collections.abc import Callable
from urllib.parse import unquote

import anyio
import httpx
from starlette import status

from cloud_pipelines_backend import proxy_api_routes


class ByteStream(httpx.AsyncByteStream):
    def __init__(self, *chunks: bytes):
        self.chunks = chunks
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk

    async def aclose(self):
        # A checkpoint detects cleanup accidentally running in a cancelled scope.
        await anyio.sleep(0)
        self.closed = True


class Upstream:
    def __init__(self, monkeypatch):
        self.requests: list[httpx.Request] = []
        self.clients: list[httpx.AsyncClient] = []
        self.streams: list[ByteStream] = []
        self.handler: Callable = self.ok
        monkeypatch.setattr(proxy_api_routes, "_create_client", self.create_client)

    def ok(self, request):
        stream = ByteStream(b'{"ok":true}')
        self.streams.append(stream)
        return httpx.Response(status.HTTP_200_OK, stream=stream)

    def create_client(self):
        async def handle(request):
            self.requests.append(request)
            result = self.handler(request)
            if hasattr(result, "__await__"):
                result = await result
            return result

        client = httpx.AsyncClient(
            transport=httpx.MockTransport(handle), trust_env=False
        )
        self.clients.append(client)
        return client

    def assert_closed(self):
        assert self.clients
        assert all(client.is_closed for client in self.clients)
        assert all(stream.closed for stream in self.streams)


async def asgi_request(
    app,
    raw_path: bytes,
    *,
    method="GET",
    headers=(),
    body=b"",
    query=b"",
    on_send=None,
    disconnected=None,
):
    """Capture wire bytes without HTTP client decoding or buffering a stream."""
    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": unquote(raw_path.decode("ascii")),
        "raw_path": raw_path,
        "root_path": "",
        "query_string": query,
        "headers": list(headers),
        "client": ("127.0.0.1", 1234),
        "server": ("testserver", 80),
    }
    messages = []
    received = False

    async def receive():
        nonlocal received
        if not received:
            received = True
            return {"type": "http.request", "body": body, "more_body": False}
        if disconnected is None:
            await anyio.sleep_forever()
        else:
            await disconnected.wait()
        return {"type": "http.disconnect"}

    async def send(message):
        messages.append(message)
        if on_send is not None:
            await on_send(message)

    with anyio.fail_after(5):
        await app(scope, receive, send)
    return messages


def response_body(messages):
    return b"".join(
        message.get("body", b"")
        for message in messages
        if message["type"] == "http.response.body"
    )
