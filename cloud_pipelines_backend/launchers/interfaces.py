# Based on cloud_pipelines/orchestration/launchers/interfaces.py

import abc
import dataclasses
import datetime
import enum

import typing
from typing import Any

# typing.Self is only added in Python 3.11
import typing_extensions

from .. import component_structures as structures

_T = typing.TypeVar("_T")
_TLaunchedContainer = typing.TypeVar(
    "_TLaunchedContainer", bound="LaunchedContainer", covariant=True
)


__all__ = [
    "ContainerTaskLauncher",
    "InputArgument",
]


class LauncherError(RuntimeError):
    pass


@dataclasses.dataclass(kw_only=True)
class InputArgument:
    total_size: int
    is_dir: bool
    value: str | None = None
    uri: str | None = None
    staging_uri: str
    is_secret: bool = False


class ContainerTaskLauncher(typing.Generic[_TLaunchedContainer], abc.ABC):
    @abc.abstractmethod
    def launch_container_task(
        self,
        *,
        component_spec: structures.ComponentSpec,
        input_arguments: dict[str, InputArgument],
        output_uris: dict[str, str],
        log_uri: str,
        annotations: dict[str, str] | None = None,
    ) -> _TLaunchedContainer:
        raise NotImplementedError()

    def deserialize_launched_container_from_dict(
        self, launched_container_dict: dict[str, Any]
    ) -> _TLaunchedContainer:
        raise NotImplementedError()

    def get_refreshed_launched_container_from_dict(
        self, launched_container_dict: dict[str, Any]
    ) -> _TLaunchedContainer:
        raise NotImplementedError()


class ContainerStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    ERROR = "ERROR"


class LaunchedContainer(abc.ABC):

    # @classmethod
    # def get(cls: typing.Type[_TLaunchedContainer]) -> _TLaunchedContainer:
    #     raise NotImplementedError()

    # @property
    # def id(self) -> str:
    #     raise NotImplementedError()

    @property
    def status(self) -> ContainerStatus:
        raise NotImplementedError()

    @property
    def exit_code(self) -> int | None:
        raise NotImplementedError()

    @property
    def has_ended(self) -> bool:
        raise NotImplementedError()

    @property
    def has_succeeded(self) -> bool:
        raise NotImplementedError()

    @property
    def has_failed(self) -> bool:
        raise NotImplementedError()

    @property
    def started_at(self) -> datetime.datetime | None:
        raise NotImplementedError()

    @property
    def ended_at(self) -> datetime.datetime | None:
        raise NotImplementedError()

    @property
    def launcher_error_message(self) -> str | None:
        raise NotImplementedError()

    def to_dict(self) -> dict[str, Any]:
        raise NotImplementedError()

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> typing_extensions.Self:
        raise NotImplementedError()

    def get_refreshed(
        self,
    ) -> typing_extensions.Self:
        raise NotImplementedError()

    def get_log(self) -> str:
        raise NotImplementedError()

    def upload_log(self) -> None:
        raise NotImplementedError()

    def stream_log_lines(self) -> typing.Iterator[str]:
        raise NotImplementedError()

    def terminate(self) -> None:
        raise NotImplementedError()


# @dataclasses.dataclass
# class ContainerExecutionResult:
#     start_time: datetime.datetime
#     end_time: datetime.datetime
#     exit_code: int
#     # TODO: Replace with logs_artifact
#     ### log: "ProcessLog"
