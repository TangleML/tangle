"""Tests for the thread-free ``port_forward_websocket`` bridge.

Each ``test_*`` function is synchronous and drives an event loop via
``asyncio.run``. The file can run independently or through pytest:

    python tests/tangent/test_kubernetes_proxy_utils.py
    pytest tests/tangent/test_kubernetes_proxy_utils.py

The integration tests stand up two real ``websockets`` servers wired exactly
like the production path:

    browser  <-- starlette WS -->  port_forward_websocket
                                        |  inner WS client over a socketpair
                                        v
    fake API-server "portforward" subresource (v4.channel.k8s.io channels)
                                        |  raw TCP relay (what kubelet does)
                                        v
    fake pod "agent" (a real WebSocket echo server)

so they exercise the channel framing, port-confirmation skipping, the
socketpair bridge, the inner WebSocket handshake tunnelled through it, query
string preservation, and teardown from both ends.
"""

from __future__ import annotations

# Allow running directly as a script as well as through pytest.
if __package__ in (None, ""):
    import os
    import sys

    sys.path.insert(
        0,
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    )

import asyncio
import contextlib
import socket

import websockets
from kubernetes.client import configuration as k8s_client_config_lib
from starlette import websockets as starlette_websockets

from cloud_pipelines_backend.tangent import kubernetes_proxy_utils
from cloud_pipelines_backend.tangent.kubernetes_proxy_utils import (
    KubernetesApiServerInfo,
)
from cloud_pipelines_backend.tangent.kubernetes_proxy_utils import (
    proxy_websocket_via_port_forward as port_forward_websocket,
)

_TIMEOUT = 10
_DATA_CHANNEL = kubernetes_proxy_utils._PORTFORWARD_DATA_CHANNEL
_ERROR_CHANNEL = kubernetes_proxy_utils._PORTFORWARD_ERROR_CHANNEL


# --------------------------------------------------------------------------- #
# Fake pod agents (real WebSocket servers reached through the tunnel).
# --------------------------------------------------------------------------- #
async def _agent_echo(connection: websockets.ServerConnection) -> None:
    """Announce the request path (to prove the query string survived), then echo."""
    await connection.send("PATH:" + connection.request.path)
    async for message in connection:
        await connection.send(message)


async def _agent_close_after_one(
    connection: websockets.ServerConnection,
) -> None:
    """Echo nothing; close from the agent side after a single client message."""
    await connection.send("PATH:" + connection.request.path)
    await connection.recv()
    await connection.close()


# --------------------------------------------------------------------------- #
# Fake API-server `portforward` subresource: speaks the v4.channel.k8s.io
# channel protocol and relays the data channel to/from a raw TCP connection to
# the agent -- exactly what the real kubelet does.
# --------------------------------------------------------------------------- #
def _make_portforward_handler(agent_port: int):
    async def handler(connection: websockets.ServerConnection) -> None:
        # Open both channels with their port-confirmation frames.
        port_bytes = agent_port.to_bytes(2, "little")
        await connection.send(bytes([_DATA_CHANNEL]) + port_bytes)
        await connection.send(bytes([_ERROR_CHANNEL]) + port_bytes)

        reader, writer = await asyncio.open_connection("127.0.0.1", agent_port)

        async def channel_to_tcp() -> None:
            try:
                async for message in connection:
                    if (
                        isinstance(message, (bytes, bytearray))
                        and message
                        and message[0] == _DATA_CHANNEL
                        and message[1:]
                    ):
                        writer.write(bytes(message[1:]))
                        await writer.drain()
            finally:
                with contextlib.suppress(Exception):
                    writer.close()

        async def tcp_to_channel() -> None:
            try:
                while True:
                    data = await reader.read(65536)
                    if not data:
                        break
                    await connection.send(bytes([_DATA_CHANNEL]) + data)
            finally:
                with contextlib.suppress(Exception):
                    await connection.close()

        await asyncio.gather(channel_to_tcp(), tcp_to_channel())

    return handler


# --------------------------------------------------------------------------- #
# Minimal stand-in for the Starlette WebSocket on the browser side.
# --------------------------------------------------------------------------- #
class _FakeBrowserWebSocket:
    """Mirrors the ASGI contract the proxy relies on.

    In particular, a server-initiated ``close()`` makes a subsequently-awaited
    ``receive()`` return a ``websocket.disconnect`` (uvicorn pushes that event
    after the application closes the connection); the bidirectional pipe depends
    on this to unblock its client-reading task.
    """

    def __init__(self, subprotocols: list[str] | None = None) -> None:
        self.scope = {"subprotocols": subprotocols or []}
        self.application_state = starlette_websockets.WebSocketState.CONNECTING
        self.accepted = False
        self.accepted_subprotocol: str | None = None
        self.close_code: int | None = None
        self._incoming: asyncio.Queue = asyncio.Queue()
        self.outgoing: asyncio.Queue = asyncio.Queue()

    async def accept(self, subprotocol: str | None = None) -> None:
        self.application_state = starlette_websockets.WebSocketState.CONNECTED
        self.accepted = True
        self.accepted_subprotocol = subprotocol

    async def receive(self) -> dict:
        return await self._incoming.get()

    async def send_bytes(self, data: bytes) -> None:
        await self.outgoing.put(("bytes", data))

    async def send_text(self, data: str) -> None:
        await self.outgoing.put(("text", data))

    async def close(self, code: int = 1000) -> None:
        if self.application_state != starlette_websockets.WebSocketState.DISCONNECTED:
            self.application_state = starlette_websockets.WebSocketState.DISCONNECTED
            self.close_code = code
            await self._incoming.put({"type": "websocket.disconnect", "code": code})

    # --- drivers used by the tests to act as the browser ---
    async def client_send_text(self, text: str) -> None:
        await self._incoming.put({"type": "websocket.receive", "text": text})

    async def client_disconnect(self) -> None:
        await self._incoming.put({"type": "websocket.disconnect"})


def _make_server_info(api_port: int) -> KubernetesApiServerInfo:
    config = k8s_client_config_lib.Configuration()
    config.host = f"http://127.0.0.1:{api_port}"
    config.verify_ssl = False
    config.api_key = {"authorization": "fake-token"}
    config.api_key_prefix = {"authorization": "Bearer"}
    return KubernetesApiServerInfo(config)


@contextlib.asynccontextmanager
async def _running_cluster(agent_handler=_agent_echo):
    """Yield ``(server_info, agent_port)`` with the agent + API servers running."""
    agent_server = await websockets.serve(agent_handler, "127.0.0.1", 0)
    agent_port = agent_server.sockets[0].getsockname()[1]
    api_server = await websockets.serve(
        _make_portforward_handler(agent_port),
        "127.0.0.1",
        0,
        subprotocols=[kubernetes_proxy_utils._PORTFORWARD_SUBPROTOCOL],
    )
    api_port = api_server.sockets[0].getsockname()[1]
    try:
        yield _make_server_info(api_port), agent_port
    finally:
        agent_server.close()
        with contextlib.suppress(Exception):
            await agent_server.wait_closed()
        api_server.close()
        with contextlib.suppress(Exception):
            await api_server.wait_closed()


# --------------------------------------------------------------------------- #
# Tests.
# --------------------------------------------------------------------------- #
def test_portforward_ws_url() -> None:
    url = kubernetes_proxy_utils._make_kubernetes_pod_portforward_ws_url(
        api_server_host="https://10.0.0.1:6443",
        namespace="ns",
        pod="pod-0",
        port=8000,
    )
    assert url == (
        "wss://10.0.0.1:6443/api/v1/namespaces/ns/pods/pod-0/portforward?ports=8000"
    ), url

    http_url = kubernetes_proxy_utils._make_kubernetes_pod_portforward_ws_url(
        api_server_host="http://localhost:8080",
        namespace="ns",
        pod="pod-0",
        port=80,
    )
    assert http_url == (
        "ws://localhost:8080/api/v1/namespaces/ns/pods/pod-0/portforward?ports=80"
    ), http_url


async def _query_string_preserved_and_bidirectional() -> None:
    async with _running_cluster() as (server_info, agent_port):
        browser = _FakeBrowserWebSocket()
        task = asyncio.create_task(
            port_forward_websocket(
                websocket=browser,
                kubernetes_server_info=server_info,
                namespace="default",
                pod="tangent-abc-0",
                port=agent_port,
                # socket.io carries essential params in the query string; the
                # whole point of the portforward path is that these survive.
                path_and_query_string="socket.io/?EIO=4&transport=websocket&sid=XYZ",
            )
        )

        kind, path_message = await asyncio.wait_for(browser.outgoing.get(), _TIMEOUT)
        assert kind == "text", kind
        assert path_message == (
            "PATH:/socket.io/?EIO=4&transport=websocket&sid=XYZ"
        ), path_message
        assert browser.accepted

        await browser.client_send_text("hello-tangent")
        kind, echoed = await asyncio.wait_for(browser.outgoing.get(), _TIMEOUT)
        assert (kind, echoed) == ("text", "hello-tangent"), (kind, echoed)

        await browser.client_disconnect()
        await asyncio.wait_for(task, _TIMEOUT)


def test_query_string_preserved_and_bidirectional() -> None:
    asyncio.run(_query_string_preserved_and_bidirectional())


async def _agent_initiated_close_tears_down() -> None:
    async with _running_cluster(_agent_close_after_one) as (
        server_info,
        agent_port,
    ):
        browser = _FakeBrowserWebSocket()
        task = asyncio.create_task(
            port_forward_websocket(
                websocket=browser,
                kubernetes_server_info=server_info,
                namespace="default",
                pod="tangent-abc-0",
                port=agent_port,
                path_and_query_string="ws",
            )
        )

        # Consume the path announcement so we know the tunnel is up.
        await asyncio.wait_for(browser.outgoing.get(), _TIMEOUT)
        await browser.client_send_text("trigger-close")

        # The agent closes; the proxy must close the browser and the task must
        # finish (no thread/pump leak left blocking the event loop).
        await asyncio.wait_for(task, _TIMEOUT)
        assert (
            browser.application_state
            == starlette_websockets.WebSocketState.DISCONNECTED
        )


def test_agent_initiated_close_tears_down() -> None:
    asyncio.run(_agent_initiated_close_tears_down())


async def _unreachable_tunnel_rejected_with_1011() -> None:
    # A port with nothing listening: the portforward tunnel cannot be opened.
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    dead_port = probe.getsockname()[1]
    probe.close()

    browser = _FakeBrowserWebSocket()
    await asyncio.wait_for(
        port_forward_websocket(
            websocket=browser,
            kubernetes_server_info=_make_server_info(dead_port),
            namespace="default",
            pod="tangent-abc-0",
            port=12345,
            path_and_query_string="ws",
        ),
        _TIMEOUT,
    )
    assert browser.close_code == 1011, browser.close_code
    assert not browser.accepted


def test_unreachable_tunnel_rejected_with_1011() -> None:
    asyncio.run(_unreachable_tunnel_rejected_with_1011())


def _main() -> None:
    import traceback

    tests = [
        value
        for name, value in sorted(globals().items())
        if name.startswith("test_") and callable(value)
    ]
    failures = 0
    for test in tests:
        try:
            test()
        except Exception as exc:  # noqa: BLE001 - report and continue
            failures += 1
            traceback.print_exc()
            print(f"FAIL {test.__name__}: {exc!r}")
        else:
            print(f"PASS {test.__name__}")
    print(f"\n{len(tests) - failures}/{len(tests)} passed")
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    _main()
