"""Structural validation of task-output references in saved pipeline definitions.

The bug these tests pin down: a saved/hydrated `TaskSpec` whose `taskOutput`
points at an existing task but a nonexistent output name reached the backend's
unguarded `task_output_artifact_nodes[task_id][output_name]` lookups and came
back as a catch-all 500 `{"error_message": "'wait_for_output'"}`.

Every reference site the backend resolves that way is covered here: task
arguments, `is_enabled`, and graph `outputValues`; both a missing task id and a
missing output name; and nested graphs.
"""

from typing import Any

import fastapi.testclient
import pytest
import sqlalchemy
from cloud_pipelines_backend import api_server_sql, component_structures
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.user_pipelines import (
    db_models,
    pipeline_structure_validation,
    services,
)
from cloud_pipelines_backend.user_pipelines.errors import (
    PipelineValidationError,
)
from sqlalchemy import orm

from tests.user_pipelines.conftest import pipeline_task

MISSING_OUTPUT = "wait_for_output"


def _producer(*, outputs: list[str]) -> dict[str, Any]:
    """A container task exposing `outputs`."""
    return {
        "componentRef": {
            "spec": {
                "name": "producer",
                "outputs": [{"name": name} for name in outputs],
                "implementation": {"container": {"image": "alpine"}},
            }
        }
    }


def _consumer(
    *,
    arguments: dict[str, Any] | None = None,
    is_enabled: Any = None,
    inputs: list[str] | None = None,
) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "name": "consumer",
        "implementation": {"container": {"image": "alpine"}},
    }
    if inputs:
        spec["inputs"] = [{"name": name} for name in inputs]
    task: dict[str, Any] = {"componentRef": {"spec": spec}}
    if arguments is not None:
        task["arguments"] = arguments
    if is_enabled is not None:
        task["isEnabled"] = is_enabled
    return task


def _task_output(*, task_id: str = "producer", output_name: str) -> dict[str, Any]:
    return {"taskOutput": {"taskId": task_id, "outputName": output_name}}


def _root(
    *,
    tasks: dict[str, Any],
    output_values: dict[str, Any] | None = None,
    name: str = "pipeline",
) -> dict[str, Any]:
    graph: dict[str, Any] = {"tasks": tasks}
    if output_values is not None:
        graph["outputValues"] = output_values
    return {
        "componentRef": {
            "spec": {
                "name": name,
                "implementation": {"graph": graph},
            }
        }
    }


def _validate(root_json: dict[str, Any]) -> None:
    pipeline_structure_validation.validate_task_output_references(
        component_structures.TaskSpec.from_json_dict(root_json)
    )


class TestValidatorRejectsDanglingReferences:
    def test_task_argument_missing_output_name(self) -> None:
        root = _root(
            tasks={
                "producer": _producer(outputs=["model", "metrics"]),
                "consumer": _consumer(
                    inputs=["x"],
                    arguments={"x": _task_output(output_name=MISSING_OUTPUT)},
                ),
            }
        )

        with pytest.raises(PipelineValidationError) as exc_info:
            _validate(root)

        assert str(exc_info.value) == (
            "Pipeline graph root: task 'consumer' argument 'x' references output "
            "'wait_for_output' of task 'producer', but that task produces no such "
            "output. Available outputs: 'metrics', 'model'."
        )

    def test_task_argument_missing_task_id(self) -> None:
        root = _root(
            tasks={
                "producer": _producer(outputs=["model"]),
                "consumer": _consumer(
                    inputs=["x"],
                    arguments={"x": _task_output(task_id="ghost", output_name="model")},
                ),
            }
        )

        with pytest.raises(PipelineValidationError) as exc_info:
            _validate(root)

        assert str(exc_info.value) == (
            "Pipeline graph root: task 'consumer' argument 'x' references output "
            "'model' of task 'ghost', but no such task exists in this graph. "
            "Known tasks: 'consumer', 'producer'."
        )

    def test_is_enabled_missing_output_name(self) -> None:
        root = _root(
            tasks={
                "producer": _producer(outputs=["model"]),
                "consumer": _consumer(
                    is_enabled=_task_output(output_name=MISSING_OUTPUT)
                ),
            }
        )

        with pytest.raises(PipelineValidationError) as exc_info:
            _validate(root)

        assert str(exc_info.value) == (
            "Pipeline graph root: task 'consumer' is_enabled references output "
            "'wait_for_output' of task 'producer', but that task produces no such "
            "output. Available outputs: 'model'."
        )

    def test_graph_output_values_missing_output_name(self) -> None:
        root = _root(
            tasks={"producer": _producer(outputs=["model"])},
            output_values={"result": _task_output(output_name=MISSING_OUTPUT)},
        )

        with pytest.raises(PipelineValidationError) as exc_info:
            _validate(root)

        assert str(exc_info.value) == (
            "Pipeline graph root: graph output 'result' references output "
            "'wait_for_output' of task 'producer', but that task produces no such "
            "output. Available outputs: 'model'."
        )

    def test_graph_output_values_missing_task_id(self) -> None:
        """`outputValues` task ids are not covered by the backend's toposort check."""
        root = _root(
            tasks={"producer": _producer(outputs=["model"])},
            output_values={
                "result": _task_output(task_id="ghost", output_name="model")
            },
        )

        with pytest.raises(PipelineValidationError) as exc_info:
            _validate(root)

        assert str(exc_info.value) == (
            "Pipeline graph root: graph output 'result' references output 'model' "
            "of task 'ghost', but no such task exists in this graph. "
            "Known tasks: 'producer'."
        )

    def test_nested_graph_is_validated_and_path_identifies_it(self) -> None:
        inner = _root(
            tasks={
                "producer": _producer(outputs=["model"]),
                "consumer": _consumer(
                    inputs=["x"],
                    arguments={"x": _task_output(output_name=MISSING_OUTPUT)},
                ),
            },
            name="inner",
        )
        root = _root(tasks={"sub": inner})

        with pytest.raises(PipelineValidationError) as exc_info:
            _validate(root)

        assert str(exc_info.value) == (
            "Pipeline graph root.tasks['sub']: task 'consumer' argument 'x' "
            "references output 'wait_for_output' of task 'producer', but that task "
            "produces no such output. Available outputs: 'model'."
        )

    def test_graph_producer_exposes_only_mapped_output_values(self) -> None:
        """A graph task's outputs are the ones its graph maps, not its declared ones.

        The backend builds a graph task's output artifact links from
        `outputValues`, so a declared-but-unmapped output is still a KeyError.
        """
        graph_producer = _root(
            tasks={"leaf": _producer(outputs=["model"])},
            output_values={"mapped": _task_output(task_id="leaf", output_name="model")},
            name="graph-producer",
        )
        graph_producer["componentRef"]["spec"]["outputs"] = [
            {"name": "mapped"},
            {"name": "declared_only"},
        ]
        root = _root(
            tasks={
                "producer": graph_producer,
                "consumer": _consumer(
                    inputs=["x"],
                    arguments={"x": _task_output(output_name="declared_only")},
                ),
            }
        )

        with pytest.raises(PipelineValidationError) as exc_info:
            _validate(root)

        assert "Available outputs: 'mapped'." in str(exc_info.value)


class TestValidatorAcceptsWhatTheBackendCanResolve:
    def test_valid_references_across_every_site(self) -> None:
        root = _root(
            tasks={
                "producer": _producer(outputs=["model", "flag"]),
                "consumer": _consumer(
                    inputs=["x"],
                    arguments={"x": _task_output(output_name="model")},
                    is_enabled=_task_output(output_name="flag"),
                ),
            },
            output_values={"result": _task_output(output_name="model")},
        )

        _validate(root)  # does not raise

    def test_non_task_output_arguments_are_ignored(self) -> None:
        root = _root(
            tasks={
                "consumer": _consumer(
                    inputs=["constant", "from_graph", "secret"],
                    arguments={
                        "constant": "literal",
                        "from_graph": {"graphInput": {"inputName": "anything"}},
                        "secret": {"dynamicData": {"secret": {"name": "API_TOKEN"}}},
                    },
                    is_enabled="true",
                )
            }
        )

        _validate(root)  # does not raise

    def test_non_inlined_producer_component_is_skipped(self) -> None:
        """Only `componentRef.spec` tells us the interface; a url/name ref does not."""
        root = _root(
            tasks={
                "producer": {"componentRef": {"url": "https://example.test/comp.yaml"}},
                "consumer": _consumer(
                    inputs=["x"],
                    arguments={"x": _task_output(output_name=MISSING_OUTPUT)},
                ),
            }
        )

        _validate(root)  # unknown interface: skipped, not guessed

    def test_producer_without_implementation_is_skipped(self) -> None:
        root = _root(
            tasks={
                "producer": {
                    "componentRef": {
                        "spec": {
                            "name": "producer",
                            "outputs": [{"name": "model"}],
                        }
                    }
                },
                "consumer": _consumer(
                    inputs=["x"],
                    arguments={"x": _task_output(output_name=MISSING_OUTPUT)},
                ),
            }
        )

        _validate(root)  # does not raise

    def test_undeclared_argument_key_with_bad_output_name_is_accepted(
        self,
    ) -> None:
        """The backend never reads it, so refusing it would break a green pipeline.

        The backend resolves arguments by iterating the consumer's DECLARED
        inputs, so `stale` below is never looked up and its dangling output name
        is inert. Probed on the pre-fix backend: this shape saves 200 and runs
        200 (PR #661 review thread M1).
        """
        root = _root(
            tasks={
                "producer": _producer(outputs=["model"]),
                "consumer": _consumer(
                    inputs=["x"],
                    arguments={
                        "x": _task_output(output_name="model"),
                        "stale": _task_output(output_name=MISSING_OUTPUT),
                    },
                ),
            }
        )

        _validate(root)  # does not raise

    def test_undeclared_argument_key_with_bad_task_id_is_still_rejected(
        self,
    ) -> None:
        """Narrowing stops at output names: `_toposort_tasks` checks every task id.

        That case already fails today as an unmapped `TypeError` -> 500, so
        turning it into a 422 is an improvement, not a regression.
        """
        root = _root(
            tasks={
                "producer": _producer(outputs=["model"]),
                "consumer": _consumer(
                    inputs=["x"],
                    arguments={
                        "x": _task_output(output_name="model"),
                        "stale": _task_output(task_id="ghost", output_name="model"),
                    },
                ),
            }
        )

        with pytest.raises(PipelineValidationError) as exc_info:
            _validate(root)

        assert "no such task exists in this graph" in str(exc_info.value)

    def test_consumer_with_no_declared_inputs_reads_no_arguments(self) -> None:
        root = _root(
            tasks={
                "producer": _producer(outputs=["model"]),
                "consumer": _consumer(
                    arguments={"x": _task_output(output_name=MISSING_OUTPUT)}
                ),
            }
        )

        _validate(root)  # does not raise

    def test_unavailable_consumer_interface_keeps_validating(self) -> None:
        """Unknown declared inputs must not silently narrow the check away."""
        root = _root(
            tasks={
                "producer": _producer(outputs=["model"]),
                "consumer": {
                    "componentRef": {"url": "https://example.test/consumer.yaml"},
                    "arguments": {"x": _task_output(output_name=MISSING_OUTPUT)},
                },
            }
        )

        with pytest.raises(PipelineValidationError) as exc_info:
            _validate(root)

        assert "produces no such output" in str(exc_info.value)

    def test_is_enabled_is_always_checked_regardless_of_declared_inputs(
        self,
    ) -> None:
        """`is_enabled` is not an input port; the backend resolves it directly."""
        root = _root(
            tasks={
                "producer": _producer(outputs=["model"]),
                "consumer": _consumer(
                    is_enabled=_task_output(output_name=MISSING_OUTPUT)
                ),
            }
        )

        with pytest.raises(PipelineValidationError):
            _validate(root)

    def test_message_contains_no_argument_values_or_annotations(self) -> None:
        root = _root(
            tasks={
                "producer": _producer(outputs=["model"]),
                "consumer": _consumer(
                    inputs=["x", "secret"],
                    arguments={
                        "x": _task_output(output_name=MISSING_OUTPUT),
                        "secret": "super-secret-value",
                    },
                ),
            }
        )
        root["annotations"] = {"private": "do-not-leak"}

        with pytest.raises(PipelineValidationError) as exc_info:
            _validate(root)

        message = str(exc_info.value)
        assert "super-secret-value" not in message
        assert "do-not-leak" not in message


class TestMessageSize:
    def test_known_tasks_list_is_capped(self) -> None:
        """A large graph's full task inventory does not land in the 422 body."""
        tasks: dict[str, Any] = {
            f"task_{index:02d}": _producer(outputs=["model"]) for index in range(25)
        }
        tasks["consumer"] = _consumer(
            inputs=["x"],
            arguments={"x": _task_output(task_id="ghost", output_name="model")},
        )
        root = _root(tasks=tasks)

        with pytest.raises(PipelineValidationError) as exc_info:
            _validate(root)

        message = str(exc_info.value)
        assert "'task_00', 'task_01'" in message
        assert "... (+16 more)." in message
        assert "'task_20'" not in message

    def test_available_outputs_list_is_capped(self) -> None:
        root = _root(
            tasks={
                "producer": _producer(
                    outputs=[f"out_{index:02d}" for index in range(15)]
                ),
                "consumer": _consumer(
                    inputs=["x"],
                    arguments={"x": _task_output(output_name=MISSING_OUTPUT)},
                ),
            }
        )

        with pytest.raises(PipelineValidationError) as exc_info:
            _validate(root)

        assert "... (+5 more)." in str(exc_info.value)


class TestParseFailureSanitization:
    """The parse that runs just before validation must not echo submitted values."""

    def test_pydantic_input_values_do_not_reach_the_message(self) -> None:
        with pytest.raises(PipelineValidationError) as exc_info:
            services.prepare_pipeline_content(
                root_pipeline_task={
                    "componentRef": {
                        "spec": {
                            "name": "saved",
                            "implementation": {"graph": {"tasks": {}}},
                        }
                    },
                    "isEnabled": {"secretToken": "super-secret-value"},
                },
                pipeline_run_annotations=None,
            )

        message = str(exc_info.value)
        assert "super-secret-value" not in message
        assert "root_pipeline_task is not a valid TaskSpec" in message
        assert "isEnabled" in message  # the location is still actionable

    def test_non_pydantic_failures_report_only_the_type(self) -> None:
        assert (
            pipeline_structure_validation.describe_parse_failure(
                ValueError("super-secret-value")
            )
            == "ValueError"
        )


def _bad_pipeline_json() -> dict[str, Any]:
    return _root(
        tasks={
            "producer": _producer(outputs=["model"]),
            "consumer": _consumer(
                inputs=["x"],
                arguments={"x": _task_output(output_name=MISSING_OUTPUT)},
            ),
        },
        name="saved",
    )


class TestSaveBoundary:
    def test_saving_a_dangling_reference_is_rejected_with_422(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        response = client.put(
            "/api/users/me/pipelines",
            params={"file_path": "pipelines/bad.yaml"},
            json={
                "root_pipeline_task": _bad_pipeline_json(),
                "pipeline_run_annotations": None,
            },
        )

        assert response.status_code == 422, response.json()
        assert "wait_for_output" in response.json()["detail"]
        assert "produces no such output" in response.json()["detail"]

    def test_valid_pipeline_still_saves(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        good = _root(
            tasks={
                "producer": _producer(outputs=["model"]),
                "consumer": _consumer(
                    inputs=["x"],
                    arguments={"x": _task_output(output_name="model")},
                ),
            },
            name="saved",
        )
        response = client.put(
            "/api/users/me/pipelines",
            params={"file_path": "pipelines/good.yaml"},
            json={"root_pipeline_task": good, "pipeline_run_annotations": None},
        )

        assert response.status_code == 200, response.json()

    def test_nothing_is_persisted_when_validation_fails(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        client.put(
            "/api/users/me/pipelines",
            params={"file_path": "pipelines/bad.yaml"},
            json={
                "root_pipeline_task": _bad_pipeline_json(),
                "pipeline_run_annotations": None,
            },
        )

        with orm.Session(db_engine) as session:
            assert (
                session.scalars(sqlalchemy.select(db_models.UserPipeline)).all() == []
            )


def _store_bad_snapshot(
    db_engine: sqlalchemy.Engine,
    *,
    pipeline_id: str,
) -> None:
    """Rewrite the current version's snapshot to one the save boundary would reject.

    Models a row written before this validation existed, or inserted directly.
    """
    with orm.Session(db_engine) as session, session.begin():
        pipeline = session.get(db_models.UserPipeline, pipeline_id)
        assert pipeline is not None
        version = session.get(
            db_models.UserPipelineVersion,
            (pipeline.id, pipeline.current_version_key),
        )
        assert version is not None
        version.root_pipeline_task = _bad_pipeline_json()


class TestRunBoundary:
    """Runs from an already-stored bad snapshot: 422, not a catch-all 500."""

    def _save_then_corrupt(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> str:
        saved = client.put(
            "/api/users/me/pipelines",
            params={"file_path": "pipelines/legacy.yaml"},
            json={
                "root_pipeline_task": pipeline_task(name="saved"),
                "pipeline_run_annotations": None,
            },
        )
        assert saved.status_code == 200, saved.json()
        pipeline_id = saved.json()["id"]
        _store_bad_snapshot(db_engine, pipeline_id=pipeline_id)
        return pipeline_id

    def test_stored_bad_snapshot_fails_as_422_not_500(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        pipeline_id = self._save_then_corrupt(client, db_engine)

        response = client.post(
            f"/api/pipeline_runs/from_pipeline/{pipeline_id}",
            json={},
        )

        assert response.status_code == 422, response.json()
        detail = response.json()["detail"]
        assert "wait_for_output" in detail
        assert "produces no such output" in detail
        # The pre-fix symptom: a bare KeyError rendered as the output name alone.
        assert detail != "'wait_for_output'"

    def test_no_run_rows_are_written_for_a_rejected_snapshot(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        pipeline_id = self._save_then_corrupt(client, db_engine)

        client.post(f"/api/pipeline_runs/from_pipeline/{pipeline_id}", json={})

        with orm.Session(db_engine) as session:
            assert session.scalars(sqlalchemy.select(bts.PipelineRun)).all() == []

    def test_validation_runs_before_the_backend_is_reached(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Mutation guard: the rejection is ours, not the backend's.

        A fake `_create_in_transaction` cannot reproduce this protection -- it is
        never called. Tests that stub run creation out (as several in
        `test_saved_pipeline_execution.py` do) therefore prove nothing about this
        failure mode, which is why the tests above drive the real path.
        """
        pipeline_id = self._save_then_corrupt(client, db_engine)
        calls: list[Any] = []

        def fake_create_in_transaction(self, **kwargs):
            calls.append(kwargs)
            raise AssertionError(
                "run construction must not be reached for an invalid snapshot"
            )

        monkeypatch.setattr(
            api_server_sql.PipelineRunsApiService_Sql,
            "_create_in_transaction",
            fake_create_in_transaction,
        )

        response = client.post(
            f"/api/pipeline_runs/from_pipeline/{pipeline_id}",
            json={},
        )

        assert response.status_code == 422, response.json()
        assert calls == []


class TestUndeclaredKeyEndToEnd:
    """The M1 regression, exercised through the real save and run endpoints."""

    def test_undeclared_stale_reference_saves_and_runs(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        pipeline = _root(
            tasks={
                "producer": _producer(outputs=["model"]),
                "consumer": _consumer(
                    inputs=["x"],
                    arguments={
                        "x": _task_output(output_name="model"),
                        "stale": _task_output(output_name=MISSING_OUTPUT),
                    },
                ),
            },
            name="saved",
        )
        saved = client.put(
            "/api/users/me/pipelines",
            params={"file_path": "pipelines/stale-key.yaml"},
            json={
                "root_pipeline_task": pipeline,
                "pipeline_run_annotations": None,
            },
        )
        assert saved.status_code == 200, saved.json()

        run = client.post(
            f"/api/pipeline_runs/from_pipeline/{saved.json()['id']}",
            json={},
        )

        assert run.status_code == 200, run.json()


class TestServiceLevelReuse:
    def test_prepare_pipeline_content_rejects_before_digesting(self) -> None:
        with pytest.raises(PipelineValidationError) as exc_info:
            services.prepare_pipeline_content(
                root_pipeline_task=_bad_pipeline_json(),
                pipeline_run_annotations=None,
            )

        assert "wait_for_output" in str(exc_info.value)
