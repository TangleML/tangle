"""Runtime helpers for Tangle multi-node component containers."""

from __future__ import annotations

import dataclasses
import json
import os
import socket
import time
from collections.abc import Iterable, Mapping
from typing import Any, Final

NUMBER_OF_NODES_ENV_VAR_NAME: Final = "TANGLE_MULTI_NODE_NUMBER_OF_NODES"
NODE_INDEX_ENV_VAR_NAME: Final = "TANGLE_MULTI_NODE_NODE_INDEX"
NODE_0_ADDRESS_ENV_VAR_NAME: Final = "TANGLE_MULTI_NODE_NODE_0_ADDRESS"
ALL_NODE_ADDRESSES_ENV_VAR_NAME: Final = "TANGLE_MULTI_NODE_ALL_NODE_ADDRESSES"
BARRIER_TOKEN_ENV_VAR_NAME: Final = "TANGLE_MULTI_NODE_BARRIER_TOKEN"
BARRIER_PORT_ENV_VAR_NAME: Final = "TANGLE_MULTI_NODE_BARRIER_PORT"

DEFAULT_BARRIER_PORT: Final = 34855
DEFAULT_BARRIER_TIMEOUT_SECONDS: Final = 600.0
DEFAULT_CONNECT_RETRY_INTERVAL_SECONDS: Final = 1.0

_MAX_MESSAGE_BYTES: Final = 64 * 1024
_PROTOCOL_VERSION: Final = 1
_REQUEST_READ_TIMEOUT_SECONDS: Final = 5.0
_LISTENER_POLL_SECONDS: Final = 0.2

__all__ = [
    "ALL_NODE_ADDRESSES_ENV_VAR_NAME",
    "BARRIER_PORT_ENV_VAR_NAME",
    "BARRIER_TOKEN_ENV_VAR_NAME",
    "DEFAULT_BARRIER_PORT",
    "DEFAULT_BARRIER_TIMEOUT_SECONDS",
    "DEFAULT_CONNECT_RETRY_INTERVAL_SECONDS",
    "MultiNodeBarrierError",
    "MultiNodeBarrierTimeoutError",
    "MultiNodeConfig",
    "MultiNodeConfigurationError",
    "NODE_0_ADDRESS_ENV_VAR_NAME",
    "NODE_INDEX_ENV_VAR_NAME",
    "NUMBER_OF_NODES_ENV_VAR_NAME",
    "barrier",
]


class MultiNodeConfigurationError(ValueError):
    """Raised when multi-node runtime configuration is invalid."""


class MultiNodeBarrierError(RuntimeError):
    """Raised when a multi-node barrier cannot complete."""


class MultiNodeBarrierTimeoutError(MultiNodeBarrierError):
    """Raised when a multi-node barrier does not complete before its deadline."""


class _BarrierRejectedError(MultiNodeBarrierError):
    pass


@dataclasses.dataclass(frozen=True)
class MultiNodeConfig:
    """Configuration injected into a Tangle multi-node component container."""

    number_of_nodes: int
    node_index: int
    node_0_address: str
    all_node_addresses: tuple[str, ...]
    barrier_token: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "all_node_addresses", tuple(self.all_node_addresses))
        self._validate()

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "MultiNodeConfig":
        env = os.environ if environ is None else environ
        node_0_address = env.get(NODE_0_ADDRESS_ENV_VAR_NAME, "localhost")
        all_node_addresses = tuple(
            address.strip()
            for address in env.get(
                ALL_NODE_ADDRESSES_ENV_VAR_NAME,
                node_0_address,
            ).split(",")
        )
        return cls(
            number_of_nodes=_parse_int_env(
                env,
                NUMBER_OF_NODES_ENV_VAR_NAME,
                default=1,
            ),
            node_index=_parse_int_env(
                env,
                NODE_INDEX_ENV_VAR_NAME,
                default=0,
            ),
            node_0_address=node_0_address,
            all_node_addresses=all_node_addresses,
            barrier_token=env.get(BARRIER_TOKEN_ENV_VAR_NAME, ""),
        )

    @property
    def is_multi_node(self) -> bool:
        return self.number_of_nodes > 1

    def _validate(self) -> None:
        if not _is_int(self.number_of_nodes) or self.number_of_nodes < 1:
            raise MultiNodeConfigurationError(
                f"{NUMBER_OF_NODES_ENV_VAR_NAME} must be a positive integer; got {self.number_of_nodes!r}."
            )
        if not _is_int(self.node_index):
            raise MultiNodeConfigurationError(
                f"{NODE_INDEX_ENV_VAR_NAME} must be an integer; got {self.node_index!r}."
            )
        if not 0 <= self.node_index < self.number_of_nodes:
            raise MultiNodeConfigurationError(
                f"{NODE_INDEX_ENV_VAR_NAME} must be between 0 and {self.number_of_nodes - 1}; got {self.node_index}."
            )
        if len(self.all_node_addresses) != self.number_of_nodes:
            raise MultiNodeConfigurationError(
                f"{ALL_NODE_ADDRESSES_ENV_VAR_NAME} must contain {self.number_of_nodes} addresses; got {len(self.all_node_addresses)}."
            )
        if not self.node_0_address:
            raise MultiNodeConfigurationError(
                f"{NODE_0_ADDRESS_ENV_VAR_NAME} must not be empty."
            )
        if any(not address for address in self.all_node_addresses):
            raise MultiNodeConfigurationError(
                f"{ALL_NODE_ADDRESSES_ENV_VAR_NAME} must not contain empty addresses."
            )
        if self.is_multi_node and not self.barrier_token:
            raise MultiNodeConfigurationError(
                f"{BARRIER_TOKEN_ENV_VAR_NAME} is required for multi-node barriers."
            )


def barrier(
    name: str = "default",
    *,
    timeout_seconds: float = DEFAULT_BARRIER_TIMEOUT_SECONDS,
    port: int | None = None,
    config: MultiNodeConfig | None = None,
    connect_retry_interval_seconds: float = DEFAULT_CONNECT_RETRY_INTERVAL_SECONDS,
) -> None:
    """Block until every node in the current Tangle multi-node task reaches the barrier."""

    if not name:
        raise ValueError("Barrier name must not be empty.")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive.")
    if connect_retry_interval_seconds <= 0:
        raise ValueError("connect_retry_interval_seconds must be positive.")
    if port is not None and not 0 < port < 65536:
        raise ValueError("port must be between 1 and 65535.")

    config = config or MultiNodeConfig.from_env()
    if not config.is_multi_node:
        return

    if port is None:
        port = _parse_int_env(
            os.environ,
            BARRIER_PORT_ENV_VAR_NAME,
            default=DEFAULT_BARRIER_PORT,
        )
    if not 0 < port < 65536:
        raise ValueError("port must be between 1 and 65535.")

    deadline = time.monotonic() + timeout_seconds
    if config.node_index == 0:
        _coordinate_barrier(
            config=config,
            name=name,
            port=port,
            deadline=deadline,
            timeout_seconds=timeout_seconds,
        )
    else:
        _join_barrier(
            config=config,
            name=name,
            port=port,
            deadline=deadline,
            timeout_seconds=timeout_seconds,
            connect_retry_interval_seconds=connect_retry_interval_seconds,
        )


def _coordinate_barrier(
    *,
    config: MultiNodeConfig,
    name: str,
    port: int,
    deadline: float,
    timeout_seconds: float,
) -> None:
    pending_node_indexes = set(range(1, config.number_of_nodes))
    connections_by_node_index: dict[int, socket.socket] = {}

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        try:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("", port))
            listener.listen(max(1, config.number_of_nodes - 1))
        except OSError as ex:
            raise MultiNodeBarrierError(
                f"Failed to listen for multi-node barrier {name!r} on port {port}."
            ) from ex

        try:
            while pending_node_indexes:
                remaining_seconds = _remaining_seconds(deadline)
                if remaining_seconds <= 0:
                    break
                listener.settimeout(min(_LISTENER_POLL_SECONDS, remaining_seconds))
                try:
                    connection, _ = listener.accept()
                except socket.timeout:
                    continue

                node_index = _accept_barrier_request(
                    connection=connection,
                    config=config,
                    name=name,
                    deadline=min(
                        deadline,
                        time.monotonic() + _REQUEST_READ_TIMEOUT_SECONDS,
                    ),
                )
                if node_index is None:
                    continue
                if node_index not in pending_node_indexes:
                    _reject_connection(
                        connection,
                        name=name,
                        error=(
                            f"Unexpected node index {node_index} for barrier {name!r}; "
                            f"still waiting for {sorted(pending_node_indexes)}."
                        ),
                    )
                    continue

                connections_by_node_index[node_index] = connection
                pending_node_indexes.remove(node_index)

            if pending_node_indexes:
                message = _timeout_message(
                    name=name,
                    timeout_seconds=timeout_seconds,
                    missing_node_indexes=sorted(pending_node_indexes),
                )
                _send_error_to_connections(
                    connections_by_node_index.values(),
                    name=name,
                    error=message,
                )
                raise MultiNodeBarrierTimeoutError(message)

            _release_connections(connections_by_node_index, name=name)
        finally:
            for connection in connections_by_node_index.values():
                connection.close()


def _join_barrier(
    *,
    config: MultiNodeConfig,
    name: str,
    port: int,
    deadline: float,
    timeout_seconds: float,
    connect_retry_interval_seconds: float,
) -> None:
    request_payload = _request_payload(config=config, name=name)
    last_error: BaseException | None = None

    while _remaining_seconds(deadline) > 0:
        try:
            timeout = min(connect_retry_interval_seconds, _remaining_seconds(deadline))
            with socket.create_connection(
                (config.node_0_address, port),
                timeout=timeout,
            ) as connection:
                _send_json_line(connection, request_payload)
                response_payload = _recv_json_line(connection, deadline=deadline)
                _validate_response(response_payload, name=name)
                return
        except _BarrierRejectedError:
            raise
        except (OSError, MultiNodeBarrierError) as ex:
            last_error = ex
            sleep_seconds = min(
                connect_retry_interval_seconds,
                max(0.0, _remaining_seconds(deadline)),
            )
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

    message = _timeout_message(
        name=name,
        timeout_seconds=timeout_seconds,
        missing_node_indexes=[config.node_index],
    )
    if last_error:
        message = f"{message} Last error while contacting node 0 at {config.node_0_address}:{port}: {last_error}"
    raise MultiNodeBarrierTimeoutError(message)


def _accept_barrier_request(
    *,
    connection: socket.socket,
    config: MultiNodeConfig,
    name: str,
    deadline: float,
) -> int | None:
    try:
        payload = _recv_json_line(connection, deadline=deadline)
        return _validate_request(payload, config=config, name=name)
    except MultiNodeBarrierError as ex:
        _reject_connection(connection, name=name, error=str(ex))
        return None


def _validate_request(
    payload: Any,
    *,
    config: MultiNodeConfig,
    name: str,
) -> int:
    if not isinstance(payload, dict):
        raise MultiNodeBarrierError("Barrier request payload must be a JSON object.")
    if payload.get("version") != _PROTOCOL_VERSION:
        raise MultiNodeBarrierError("Barrier request protocol version does not match.")
    if payload.get("type") != "barrier":
        raise MultiNodeBarrierError("Barrier request type does not match.")
    if payload.get("name") != name:
        raise MultiNodeBarrierError(
            f"Barrier request name {payload.get('name')!r} does not match {name!r}."
        )
    if payload.get("token") != config.barrier_token:
        raise MultiNodeBarrierError("Barrier request token does not match.")
    if payload.get("number_of_nodes") != config.number_of_nodes:
        raise MultiNodeBarrierError(
            "Barrier request number_of_nodes does not match the coordinator."
        )

    node_index = payload.get("node_index")
    if not _is_int(node_index):
        raise MultiNodeBarrierError("Barrier request node_index must be an integer.")
    if not 0 <= node_index < config.number_of_nodes:
        raise MultiNodeBarrierError(
            f"Barrier request node_index must be between 0 and {config.number_of_nodes - 1}; got {node_index}."
        )
    return node_index


def _validate_response(payload: Any, *, name: str) -> None:
    if not isinstance(payload, dict):
        raise _BarrierRejectedError("Barrier response payload must be a JSON object.")
    if payload.get("version") != _PROTOCOL_VERSION:
        raise _BarrierRejectedError("Barrier response protocol version does not match.")
    if payload.get("name") != name:
        raise _BarrierRejectedError(
            f"Barrier response name {payload.get('name')!r} does not match {name!r}."
        )
    status = payload.get("status")
    if status == "ok":
        return
    if status == "error":
        raise _BarrierRejectedError(str(payload.get("error") or "Barrier failed."))
    raise _BarrierRejectedError(f"Barrier response status {status!r} is not supported.")


def _request_payload(*, config: MultiNodeConfig, name: str) -> dict[str, Any]:
    return {
        "version": _PROTOCOL_VERSION,
        "type": "barrier",
        "name": name,
        "token": config.barrier_token,
        "number_of_nodes": config.number_of_nodes,
        "node_index": config.node_index,
    }


def _response_payload(
    *,
    name: str,
    status: str,
    error: str | None = None,
) -> dict[str, Any]:
    payload = {
        "version": _PROTOCOL_VERSION,
        "name": name,
        "status": status,
    }
    if error:
        payload["error"] = error
    return payload


def _reject_connection(connection: socket.socket, *, name: str, error: str) -> None:
    try:
        _send_json_line(
            connection,
            _response_payload(name=name, status="error", error=error),
        )
    except OSError:
        pass
    finally:
        try:
            connection.close()
        except OSError:
            pass


def _release_connections(
    connections_by_node_index: Mapping[int, socket.socket],
    *,
    name: str,
) -> None:
    release_errors: list[str] = []
    for node_index, connection in connections_by_node_index.items():
        try:
            _send_json_line(connection, _response_payload(name=name, status="ok"))
        except OSError as ex:
            release_errors.append(f"node {node_index}: {ex}")
    if release_errors:
        raise MultiNodeBarrierError(
            "Failed to release every node from multi-node barrier "
            f"{name!r}: {', '.join(release_errors)}"
        )


def _send_error_to_connections(
    connections: Iterable[socket.socket],
    *,
    name: str,
    error: str,
) -> None:
    for connection in connections:
        try:
            _send_json_line(
                connection,
                _response_payload(name=name, status="error", error=error),
            )
        except OSError:
            pass


def _send_json_line(connection: socket.socket, payload: dict[str, Any]) -> None:
    connection.sendall(
        json.dumps(payload, separators=(",", ":")).encode("utf-8") + b"\n"
    )


def _recv_json_line(connection: socket.socket, *, deadline: float) -> Any:
    chunks: list[bytes] = []
    total_bytes = 0

    while True:
        remaining_seconds = _remaining_seconds(deadline)
        if remaining_seconds <= 0:
            raise MultiNodeBarrierTimeoutError("Timed out waiting for barrier message.")
        connection.settimeout(remaining_seconds)
        try:
            chunk = connection.recv(4096)
        except socket.timeout as ex:
            raise MultiNodeBarrierTimeoutError(
                "Timed out waiting for barrier message."
            ) from ex
        except OSError as ex:
            raise MultiNodeBarrierError(
                "Error while reading barrier message from connection."
            ) from ex
        if not chunk:
            raise MultiNodeBarrierError(
                "Connection closed before a barrier message arrived."
            )

        total_bytes += len(chunk)
        if total_bytes > _MAX_MESSAGE_BYTES:
            raise MultiNodeBarrierError("Barrier message exceeded maximum size.")
        chunks.append(chunk)

        if b"\n" in chunk:
            break

    message = b"".join(chunks).split(b"\n", 1)[0]
    try:
        return json.loads(message.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as ex:
        raise MultiNodeBarrierError("Barrier message is not valid JSON.") from ex


def _parse_int_env(env: Mapping[str, str], name: str, *, default: int) -> int:
    raw_value = env.get(name)
    if raw_value is None or raw_value == "":
        return default
    try:
        return int(raw_value)
    except ValueError as ex:
        raise MultiNodeConfigurationError(
            f"{name} must be an integer; got {raw_value!r}."
        ) from ex


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _remaining_seconds(deadline: float) -> float:
    return deadline - time.monotonic()


def _timeout_message(
    *,
    name: str,
    timeout_seconds: float,
    missing_node_indexes: list[int],
) -> str:
    return (
        f"Timed out after {timeout_seconds:g}s waiting for multi-node barrier {name!r}; "
        f"missing node indexes: {missing_node_indexes}."
    )
