"""The resolved input argument must not print its value.

A secret consumed as a value is held in `InputArgument.value`. Launcher error messages
interpolate the whole argument -- `f"... {input_argument=}"` -- and the orchestrator logs
that message, reports it, stores it on the execution node and serves it through the
execution-log API, so a printed value reaches several durable places at once.
"""

import dataclasses
import json
import pathlib
import traceback
from typing import Final

from cloud_pipelines_backend.launchers import interfaces

_SECRET_VALUE: Final[str] = "secret-value-authenticates-nothing"

_LAUNCHER_MODULES: Final[tuple[str, ...]] = (
    "kubernetes_launchers.py",
    "local_docker_launchers.py",
    "huggingface_launchers.py",
)


def _input_argument() -> interfaces.InputArgument:
    return interfaces.InputArgument(
        total_size=len(_SECRET_VALUE),
        is_dir=False,
        value=_SECRET_VALUE,
        uri=None,
        staging_uri="memory://staging/inputs/token/data",
    )


class TestInputArgumentDoesNotPrintItsValue:
    def test_every_way_of_stringifying_it_omits_the_value(self) -> None:
        argument = _input_argument()
        for rendered in (
            repr(argument),
            str(argument),
            f"{argument}",
            f"{argument!r}",
            f"{argument=}",
            "{}".format(argument),
            "%s" % (argument,),
        ):
            assert _SECRET_VALUE not in rendered, rendered

    def test_the_other_fields_still_print(self) -> None:
        """The messages are written to identify an input, so they must keep doing that."""
        rendered = repr(_input_argument())
        assert "total_size=" in rendered
        assert "is_dir=" in rendered
        assert "staging_uri=" in rendered

    def test_the_value_is_still_readable_and_still_a_field(self) -> None:
        """Hidden from printing, not removed: the launcher passes it to the container."""
        argument = _input_argument()
        assert argument.value == _SECRET_VALUE
        assert "value" in {field.name for field in dataclasses.fields(argument)}

    def test_a_traceback_built_the_way_the_launchers_build_one_is_clean(self) -> None:
        """The message shape used at every raise site that reports an input argument."""
        argument = _input_argument()
        input_name = "token"
        try:
            raise interfaces.LauncherError(
                f"Artifact is too big to consume as value. Consume it as file instead."
                f" {input_name=}, {argument=}"
            )
        except interfaces.LauncherError:
            captured = traceback.format_exc()
        assert _SECRET_VALUE not in captured
        assert "input_name='token'" in captured


class TestNoLauncherMessageReachesAroundTheRepr:
    """`repr=False` is defeated by a message that interpolates the field directly.

    This guards the fix rather than the symptom: a new `{input_argument=}` is safe, while
    an `{input_argument.value}` would print the secret again.
    """

    def test_no_launcher_formats_an_argument_value_into_a_string(self) -> None:
        launchers_dir = pathlib.Path(interfaces.__file__).resolve().parent
        offenders: list[str] = []
        for module_name in _LAUNCHER_MODULES:
            source = (launchers_dir / module_name).read_text()
            for forbidden in (
                "{input_argument.value",
                "{artifact_data.value",
                "{ia.value",
            ):
                if forbidden in source:
                    offenders.append(f"{module_name}: {forbidden}")
        assert not offenders, offenders

    def test_the_value_is_not_serialized_into_a_dict_anywhere(self) -> None:
        """`dataclasses.asdict` ignores `repr=False`, so no launcher may use it here."""
        launchers_dir = pathlib.Path(interfaces.__file__).resolve().parent
        for module_name in _LAUNCHER_MODULES:
            source = (launchers_dir / module_name).read_text()
            assert "asdict(input_argument" not in source, module_name

    def test_asdict_would_have_leaked_it(self) -> None:
        """Pins why the test above exists, rather than asserting a bare convention."""
        assert _SECRET_VALUE in json.dumps(dataclasses.asdict(_input_argument()))
