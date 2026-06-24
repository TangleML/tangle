import socket
import struct
import threading
import time

import pytest

from cloud_pipelines_backend.runtime import multi_node


def _unused_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _connect_and_reset(port: int) -> None:
    deadline = time.monotonic() + 1
    while True:
        try:
            connection = socket.create_connection(("127.0.0.1", port), timeout=0.05)
            connection.setsockopt(
                socket.SOL_SOCKET,
                socket.SO_LINGER,
                struct.pack("ii", 1, 0),
            )
            connection.close()
            return
        except OSError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.01)


def _config_for_node(
    node_index: int, number_of_nodes: int = 3
) -> multi_node.MultiNodeConfig:
    return multi_node.MultiNodeConfig(
        number_of_nodes=number_of_nodes,
        node_index=node_index,
        node_0_address="127.0.0.1",
        all_node_addresses=tuple(
            f"node-{idx}.example" for idx in range(number_of_nodes)
        ),
        barrier_token="test-token",
    )


def test_multi_node_config_from_env_parses_launcher_values():
    config = multi_node.MultiNodeConfig.from_env(
        {
            multi_node.NUMBER_OF_NODES_ENV_VAR_NAME: "3",
            multi_node.NODE_INDEX_ENV_VAR_NAME: "2",
            multi_node.NODE_0_ADDRESS_ENV_VAR_NAME: "job-0.service",
            multi_node.ALL_NODE_ADDRESSES_ENV_VAR_NAME: "job-0.service,job-1.service,job-2.service",
            multi_node.BARRIER_TOKEN_ENV_VAR_NAME: "token",
        }
    )

    assert config.number_of_nodes == 3
    assert config.node_index == 2
    assert config.node_0_address == "job-0.service"
    assert config.all_node_addresses == (
        "job-0.service",
        "job-1.service",
        "job-2.service",
    )
    assert config.barrier_token == "token"


def test_multi_node_config_defaults_to_single_node():
    config = multi_node.MultiNodeConfig.from_env({})

    assert config == multi_node.MultiNodeConfig(
        number_of_nodes=1,
        node_index=0,
        node_0_address="localhost",
        all_node_addresses=("localhost",),
    )
    multi_node.barrier(config=config, timeout_seconds=0.01, port=1)


def test_multi_node_config_requires_barrier_token_for_multi_node():
    with pytest.raises(
        multi_node.MultiNodeConfigurationError,
        match=multi_node.BARRIER_TOKEN_ENV_VAR_NAME,
    ):
        multi_node.MultiNodeConfig(
            number_of_nodes=2,
            node_index=1,
            node_0_address="node-0",
            all_node_addresses=("node-0", "node-1"),
        )


def test_multi_node_config_rejects_empty_address_entries_from_env():
    with pytest.raises(
        multi_node.MultiNodeConfigurationError,
        match=multi_node.ALL_NODE_ADDRESSES_ENV_VAR_NAME,
    ):
        multi_node.MultiNodeConfig.from_env(
            {
                multi_node.NUMBER_OF_NODES_ENV_VAR_NAME: "2",
                multi_node.NODE_INDEX_ENV_VAR_NAME: "0",
                multi_node.NODE_0_ADDRESS_ENV_VAR_NAME: "node-0",
                multi_node.ALL_NODE_ADDRESSES_ENV_VAR_NAME: "node-0,",
                multi_node.BARRIER_TOKEN_ENV_VAR_NAME: "token",
            }
        )


def test_barrier_releases_all_nodes_after_every_node_arrives():
    port = _unused_tcp_port()
    completed_node_indexes: list[int] = []
    errors: list[BaseException] = []

    def wait_at_barrier(node_index: int) -> None:
        try:
            multi_node.barrier(
                "finished",
                config=_config_for_node(node_index),
                port=port,
                timeout_seconds=3,
                connect_retry_interval_seconds=0.01,
            )
            completed_node_indexes.append(node_index)
        except BaseException as ex:
            errors.append(ex)

    threads = [
        threading.Thread(target=wait_at_barrier, args=(node_index,))
        for node_index in [1, 2]
    ]
    for thread in threads:
        thread.start()
    time.sleep(0.05)

    coordinator_thread = threading.Thread(target=wait_at_barrier, args=(0,))
    coordinator_thread.start()
    threads.append(coordinator_thread)

    for thread in threads:
        thread.join(timeout=5)

    assert all(not thread.is_alive() for thread in threads)
    assert errors == []
    assert sorted(completed_node_indexes) == [0, 1, 2]


def test_barrier_ignores_reset_connection_before_valid_worker_arrives():
    port = _unused_tcp_port()
    completed_node_indexes: list[int] = []
    errors: list[BaseException] = []

    def wait_at_barrier(node_index: int) -> None:
        try:
            multi_node.barrier(
                "finished",
                config=_config_for_node(node_index, number_of_nodes=2),
                port=port,
                timeout_seconds=2,
                connect_retry_interval_seconds=0.01,
            )
            completed_node_indexes.append(node_index)
        except BaseException as ex:
            errors.append(ex)

    coordinator_thread = threading.Thread(target=wait_at_barrier, args=(0,))
    coordinator_thread.start()
    _connect_and_reset(port)

    worker_thread = threading.Thread(target=wait_at_barrier, args=(1,))
    worker_thread.start()

    coordinator_thread.join(timeout=5)
    worker_thread.join(timeout=5)

    assert not coordinator_thread.is_alive()
    assert not worker_thread.is_alive()
    assert errors == []
    assert sorted(completed_node_indexes) == [0, 1]


def test_barrier_wraps_coordinator_bind_failure():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("", 0))
        listener.listen()
        port = int(listener.getsockname()[1])

        with pytest.raises(
            multi_node.MultiNodeBarrierError,
            match="Failed to listen",
        ):
            multi_node.barrier(
                "finished",
                config=_config_for_node(0, number_of_nodes=2),
                port=port,
                timeout_seconds=0.1,
                connect_retry_interval_seconds=0.01,
            )


def test_barrier_times_out_with_missing_node_indexes():
    port = _unused_tcp_port()

    with pytest.raises(
        multi_node.MultiNodeBarrierTimeoutError,
        match=r"missing node indexes: \[1\]",
    ):
        multi_node.barrier(
            "finished",
            config=_config_for_node(0, number_of_nodes=2),
            port=port,
            timeout_seconds=0.1,
            connect_retry_interval_seconds=0.01,
        )
