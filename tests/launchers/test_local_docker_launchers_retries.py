"""Tests for the retries annotation handling in the local Docker launcher.

The Docker client is faked so that the tests run without Docker.
"""

from __future__ import annotations

import logging

import pytest

pytest.importorskip("docker")

from cloud_pipelines_backend import component_structures as structures
from cloud_pipelines_backend.launchers import common_annotations
from cloud_pipelines_backend.launchers import interfaces
from cloud_pipelines_backend.launchers import local_docker_launchers

_RETRIES_KEY = common_annotations.RETRIES_MAX_RETRIES_ANNOTATION_KEY


class _ContainerRunCalled(Exception):
    pass


class _FakeContainers:
    def __init__(self):
        self.run_calls: list[dict] = []

    def run(self, **kwargs):
        self.run_calls.append(kwargs)
        # Stopping the launch here. The rest of the launch flow is not relevant for these tests.
        raise _ContainerRunCalled()


class _FakeDockerClient:
    def __init__(self):
        self.containers = _FakeContainers()


def _launch(launcher, annotations, tmp_path):
    return launcher.launch_container_task(
        component_spec=structures.ComponentSpec(
            name="test",
            implementation=structures.ContainerImplementation(
                container=structures.ContainerSpec(
                    image="alpine",
                    command=["echo", "hello"],
                )
            ),
        ),
        input_arguments={},
        output_uris={},
        log_uri=str(tmp_path / "log.txt"),
        annotations=annotations,
    )


def test_docker_launcher_warns_and_ignores_retries(tmp_path, caplog):
    client = _FakeDockerClient()
    launcher = local_docker_launchers.DockerContainerLauncher(client=client)
    with caplog.at_level(logging.WARNING, logger=local_docker_launchers.__name__):
        with pytest.raises(_ContainerRunCalled):
            _launch(launcher, {_RETRIES_KEY: "3"}, tmp_path)
    # The container is still launched (without retries).
    assert len(client.containers.run_calls) == 1
    assert any(
        _RETRIES_KEY in record.getMessage() and "ignored" in record.getMessage()
        for record in caplog.records
    )


def test_docker_launcher_without_retries_does_not_warn(tmp_path, caplog):
    client = _FakeDockerClient()
    launcher = local_docker_launchers.DockerContainerLauncher(client=client)
    with caplog.at_level(logging.WARNING, logger=local_docker_launchers.__name__):
        with pytest.raises(_ContainerRunCalled):
            _launch(launcher, {_RETRIES_KEY: "0"}, tmp_path)
    assert not any(_RETRIES_KEY in record.getMessage() for record in caplog.records)


@pytest.mark.parametrize("value", ["-1", "6", "abc"])
def test_docker_launcher_invalid_retries_fails_closed(tmp_path, value):
    client = _FakeDockerClient()
    launcher = local_docker_launchers.DockerContainerLauncher(client=client)
    with pytest.raises(interfaces.LauncherError):
        _launch(launcher, {_RETRIES_KEY: value}, tmp_path)
    assert client.containers.run_calls == []
