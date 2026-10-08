import json
from typing import Any

import fastapi.testclient
import sqlalchemy
from cloud_pipelines_backend import (
    api_router,
    api_server_sql,
    component_structures,
)
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.user_pipelines import (
    api_routes,
    db_models,
    errors,
    pipeline_run_annotations,
    services,
)
from sqlalchemy import orm

from tests.user_pipelines.conftest import (
    DEFAULT_USER,
    OTHER_USER,
    pipeline_task,
)


def _save_pipeline(
    client: fastapi.testclient.TestClient,
    *,
    file_path: str = "pipelines/saved.yaml",
    name: str = "saved",
    arguments: dict[str, Any] | None = None,
    annotations: dict[str, str] | None = None,
    versioning_mode: str | None = None,
    inputs: list[str] | None = None,
    graph_tasks: dict[str, Any] | None = None,
) -> dict[str, Any]:
    task = pipeline_task(name=name)
    if graph_tasks is not None:
        task["componentRef"]["spec"]["implementation"]["graph"]["tasks"] = graph_tasks
    if inputs is not None:
        # Declared because run creation validates arguments against the component's inputs.
        # Only tests that pass arguments need them; the default spec takes none.
        task["componentRef"]["spec"]["inputs"] = [{"name": name} for name in inputs]
    if arguments is not None:
        task["arguments"] = arguments
    request: dict[str, Any] = {
        "root_pipeline_task": task,
        "pipeline_run_annotations": annotations,
    }
    if versioning_mode is not None:
        request["versioning_mode"] = versioning_mode
    response = client.put(
        "/api/users/me/pipelines",
        params={"file_path": file_path},
        json=request,
    )
    assert response.status_code == 200
    return response.json()


def _spy_on_run_creation(monkeypatch) -> list[dict[str, Any]]:
    """Record every run `create_from_pipeline_no_commit` builds, and let it build them.

    A spy rather than a stub. The old shape here replaced `PipelineRunsApiService_Sql.create`
    outright, which worked only because the creator committed on its own; now that it writes
    into the caller's transaction, a stub would have to hand back a `PipelineRun` real enough
    to survive the wrapper's `commit()` and `refresh()`. Delegating instead is both less
    fixture and more coverage: these tests assert on what was passed *and* the real run gets
    written.

    `in_transaction` is captured because it is the whole point of the split — see
    `test_execution_adapter_merges_copies_and_delegates_in_the_callers_transaction`.
    """
    calls: list[dict[str, Any]] = []
    real = api_server_sql.PipelineRunsApiService_Sql._create_in_transaction

    def spy(
        self,
        session: orm.Session,
        root_task: component_structures.TaskSpec,
        components=None,
        annotations=None,
        created_by=None,
    ) -> bts.PipelineRun:
        calls.append(
            {
                "root_task": root_task,
                "annotations": annotations,
                "created_by": created_by,
                "in_transaction": session.in_transaction(),
            }
        )
        return real(
            self,
            session=session,
            root_task=root_task,
            components=components,
            annotations=annotations,
            created_by=created_by,
        )

    monkeypatch.setattr(
        api_server_sql.PipelineRunsApiService_Sql, "_create_in_transaction", spy
    )
    return calls


def _overwrite_current_annotations(
    db_engine: sqlalchemy.Engine,
    *,
    pipeline_id: str,
    annotations: dict[str, str],
) -> None:
    with orm.Session(db_engine) as session, session.begin():
        pipeline = session.get(db_models.UserPipeline, pipeline_id)
        assert pipeline is not None
        version = session.get(
            db_models.UserPipelineVersion,
            (pipeline.id, pipeline.current_version_key),
        )
        assert version is not None
        version.pipeline_run_annotations = annotations


def _mirrored_project_ids(
    db_engine: sqlalchemy.Engine,
    *,
    run_id: str,
) -> list[str]:
    """The run's project memberships as the run *filter* sees them, not as the run stores them.

    From `pipeline_run_annotation` rather than `PipelineRun.annotations` -- two different
    writes, and the mirror that makes a key filterable skips some keys.

    Read off the keys, because that is where the project id lives; the value is a marker. A
    list rather than one value so a run carrying two memberships shows up as two here instead
    of as the first one found -- the storage takes them even though the API does not.
    """
    with orm.Session(db_engine) as session:
        keys = session.scalars(
            sqlalchemy.select(bts.PipelineRunAnnotation.key).where(
                bts.PipelineRunAnnotation.pipeline_run_id == run_id,
                bts.PipelineRunAnnotation.key.startswith(
                    pipeline_run_annotations.PROJECT_ANNOTATION_PREFIX
                ),
            )
        )
        return [
            key.removeprefix(pipeline_run_annotations.PROJECT_ANNOTATION_PREFIX)
            for key in keys
        ]


def test_execution_adapter_merges_copies_and_delegates_in_the_callers_transaction(
    client: fastapi.testclient.TestClient,
    monkeypatch,
) -> None:
    """The merge is unchanged; the transaction assertion is the opposite of what it was.

    This test used to be called `..._with_clean_transaction` and assert
    `session.in_transaction() is False`, because `create_from_pipeline` called
    `session.rollback()` before delegating. That rollback is gone, and this asserts `True`.

    Why the reversal is safe rather than a regression. The rollback existed for exactly one
    reason: `PipelineRunsApiService_Sql.create` opens `session.begin()`, which raises if a
    transaction is already active, and the pipeline lookup above it starts an implicit read
    transaction. It was a precondition fix, never a correctness boundary — and a destructive
    one, since `rollback()` discards *any* pending write the caller had, not just the read.
    `create_from_pipeline` now calls `_create_in_transaction`, which never calls `begin()`, so
    the precondition is gone with it. The commit moved into the wrapper, so this route's
    behaviour is unchanged; what changed is that a caller can now write the run atomically
    with rows of its own, which is what the trigger fence needs.

    Guarded from the other side by `test_a_callers_pending_write_survives_the_run_creation`.
    """
    saved = _save_pipeline(
        client,
        arguments={"kept": "stored", "replaced": "stored"},
        annotations={
            "kept": "stored",
            "replaced": "stored",
        },
        inputs=["kept", "replaced", "added", "credential"],
    )
    calls = _spy_on_run_creation(monkeypatch)
    response = client.post(
        f"/api/pipeline_runs/from_pipeline/{saved['id']}",
        json={
            "run_arguments": {
                "replaced": "request",
                "added": "request",
                "credential": {"dynamicData": {"secret": {"name": "API_TOKEN"}}},
            },
            "pipeline_run_annotations": {
                "replaced": "request",
                "added": "request",
            },
        },
    )

    assert response.status_code == 200, response.json()
    assert response.json()["id"]
    assert "pipeline_run" not in response.json()
    (captured,) = calls
    # The inversion. The run is built while the caller's transaction is still open.
    assert captured["in_transaction"] is True
    effective_arguments = dict(captured["root_task"].arguments)
    assert effective_arguments.pop(
        "credential"
    ) == component_structures.DynamicDataArgument(
        dynamic_data={"secret": {"name": "API_TOKEN"}}
    )
    assert effective_arguments == {
        "kept": "stored",
        "replaced": "request",
        "added": "request",
    }
    assert captured["annotations"] == {
        "kept": "stored",
        "replaced": "request",
        "added": "request",
        "tangleml.com/source/user-pipeline": "true",
        "tangleml.com/user-pipeline/pipeline-id": saved["id"],
        "tangleml.com/user-pipeline/version": saved["version"],
        "tangleml.com/user-pipeline/owner": DEFAULT_USER,
        "tangleml.com/user-pipeline/file-path": "pipelines/saved.yaml",
    }
    assert captured["created_by"] == DEFAULT_USER

    stored = client.get(
        "/api/users/me/pipelines",
        params={"file_path": "pipelines/saved.yaml"},
    ).json()
    assert stored["root_pipeline_task"]["arguments"] == {
        "kept": "stored",
        "replaced": "stored",
    }
    assert stored["pipeline_run_annotations"] == {
        "kept": "stored",
        "replaced": "stored",
    }


def test_legacy_stored_provenance_is_overwritten_by_server_values(
    client: fastapi.testclient.TestClient,
    db_engine: sqlalchemy.Engine,
    monkeypatch,
) -> None:
    saved = _save_pipeline(
        client,
        file_path="pipelines/legacy-provenance.yaml",
        annotations={"kept": "stored"},
    )
    _overwrite_current_annotations(
        db_engine,
        pipeline_id=saved["id"],
        annotations={
            "kept": "stored",
            pipeline_run_annotations.SOURCE_ANNOTATION: "false",
            pipeline_run_annotations.PIPELINE_ID_ANNOTATION: "spoofed-id",
            pipeline_run_annotations.VERSION_ANNOTATION: "spoofed-version",
            pipeline_run_annotations.OWNER_ANNOTATION: "spoofed-owner",
            pipeline_run_annotations.FILE_PATH_ANNOTATION: "spoofed/path.yaml",
        },
    )
    calls = _spy_on_run_creation(monkeypatch)
    response = client.post(
        f"/api/pipeline_runs/from_pipeline/{saved['id']}",
        json={},
    )

    assert response.status_code == 200
    (captured,) = calls
    assert captured["created_by"] == DEFAULT_USER
    assert captured["annotations"] == {
        "kept": "stored",
        pipeline_run_annotations.SOURCE_ANNOTATION: "true",
        pipeline_run_annotations.PIPELINE_ID_ANNOTATION: saved["id"],
        pipeline_run_annotations.VERSION_ANNOTATION: saved["version"],
        pipeline_run_annotations.OWNER_ANNOTATION: DEFAULT_USER,
        pipeline_run_annotations.FILE_PATH_ANNOTATION: "pipelines/legacy-provenance.yaml",
    }


def test_current_and_historical_version_selection(
    client: fastapi.testclient.TestClient,
    monkeypatch,
) -> None:
    first = _save_pipeline(client, name="first", versioning_mode="full")
    second = _save_pipeline(client, name="second", versioning_mode="full")
    calls = _spy_on_run_creation(monkeypatch)
    current_response = client.post(
        f"/api/pipeline_runs/from_pipeline/{first['id']}", json={}
    )
    historical_response = client.post(
        f"/api/pipeline_runs/from_pipeline/{first['id']}",
        params={"version": first["version"]},
        json={},
    )

    assert current_response.status_code == 200
    assert historical_response.status_code == 200
    assert [
        (
            call["root_task"].component_ref.spec.name,
            call["annotations"]["tangleml.com/user-pipeline/version"],
        )
        for call in calls
    ] == [
        ("second", second["version"]),
        ("first", first["version"]),
    ]


def test_full_to_disabled_current_digest_executes_pointed_content(
    client: fastapi.testclient.TestClient,
    monkeypatch,
) -> None:
    saved = _save_pipeline(
        client,
        name="disabled-pointed",
        versioning_mode="full",
    )
    patched = client.patch(
        f"/api/users/me/pipelines/{saved['id']}/properties",
        json={"versioning_mode": "disabled"},
    )
    assert patched.status_code == 200
    calls = _spy_on_run_creation(monkeypatch)
    response = client.post(
        f"/api/pipeline_runs/from_pipeline/{saved['id']}",
        params={"version": saved["version"]},
        json={},
    )

    assert response.status_code == 200
    (captured,) = calls
    assert captured["root_task"].component_ref.spec.name == "disabled-pointed"
    assert (
        captured["annotations"]["tangleml.com/user-pipeline/version"]
        == saved["version"]
    )


def test_execution_rejects_soft_deleted_pipeline(
    client: fastapi.testclient.TestClient,
    db_engine: sqlalchemy.Engine,
) -> None:
    saved = _save_pipeline(client, name="deleted")
    deleted = client.delete(
        "/api/users/me/pipelines",
        params={"file_path": "pipelines/saved.yaml"},
    )
    assert deleted.status_code == 204

    response = client.post(
        f"/api/pipeline_runs/from_pipeline/{saved['id']}",
        json={},
    )

    assert response.status_code == 404
    with orm.Session(db_engine) as session:
        assert (
            session.scalar(sqlalchemy.select(sqlalchemy.func.count(bts.PipelineRun.id)))
            == 0
        )


def test_alternate_key_allows_authorized_cross_user_execution(
    other_user_client: fastapi.testclient.TestClient,
    client: fastapi.testclient.TestClient,
    monkeypatch,
) -> None:
    saved = _save_pipeline(other_user_client, file_path="team/pipeline.yaml")
    calls = _spy_on_run_creation(monkeypatch)
    response = client.post(
        "/api/pipeline_runs/from_pipeline",
        params={"user_id": OTHER_USER, "file_path": "team/pipeline.yaml"},
        json={},
    )

    assert response.status_code == 200
    (captured,) = calls
    assert captured["created_by"] == DEFAULT_USER
    assert (
        captured["annotations"]["tangleml.com/user-pipeline/pipeline-id"] == saved["id"]
    )
    assert captured["annotations"]["tangleml.com/user-pipeline/owner"] == OTHER_USER
    assert (
        captured["annotations"]["tangleml.com/user-pipeline/file-path"]
        == "team/pipeline.yaml"
    )


def test_execution_requires_authentication_and_read_write_permissions(
    client: fastapi.testclient.TestClient,
) -> None:
    saved = _save_pipeline(client)
    path = f"/api/pipeline_runs/from_pipeline/{saved['id']}"

    assert client.post(path, headers={"x-user": ""}, json={}).status_code == 401
    assert client.post(path, headers={"x-read": "false"}, json={}).status_code == 403
    assert client.post(path, headers={"x-write": "false"}, json={}).status_code == 403


def test_execution_rejects_reserved_annotations_and_invalid_lookups(
    client: fastapi.testclient.TestClient,
) -> None:
    saved = _save_pipeline(client)
    path = f"/api/pipeline_runs/from_pipeline/{saved['id']}"

    assert (
        client.post(
            path,
            json={"pipeline_run_annotations": {"system/created_by": "spoof"}},
        ).status_code
        == 422
    )
    assert (
        client.post(
            path,
            json={
                "pipeline_run_annotations": {
                    "tangleml.com/user-pipeline/version": "spoof"
                }
            },
        ).status_code
        == 422
    )
    assert (
        client.post(
            path,
            json={
                "pipeline_run_annotations": {
                    "tangleml.com/user-pipeline/file-path": "spoofed/path.yaml"
                }
            },
        ).status_code
        == 422
    )
    assert client.post(path, json={"unexpected": "field"}).status_code == 422
    assert (
        client.post(
            path,
            params={"version": "0" * 64},
            json={},
        ).status_code
        == 404
    )
    assert (
        client.post("/api/pipeline_runs/from_pipeline/not-a-uuid", json={}).status_code
        == 422
    )
    assert (
        client.post(
            "/api/pipeline_runs/from_pipeline",
            params={"user_id": DEFAULT_USER},
            json={},
        ).status_code
        == 422
    )


def test_execution_rejects_stored_system_annotations_without_persistence(
    client: fastapi.testclient.TestClient,
    db_engine: sqlalchemy.Engine,
) -> None:
    saved = _save_pipeline(
        client,
        file_path="pipelines/reserved-system-annotation.yaml",
        annotations={"valid": "stored"},
    )
    _overwrite_current_annotations(
        db_engine,
        pipeline_id=saved["id"],
        annotations={"system/pipeline_run.created_by": "spoofed-owner"},
    )

    response = client.post(f"/api/pipeline_runs/from_pipeline/{saved['id']}", json={})

    assert response.status_code == 422
    assert "reserved for system use" in response.json()["detail"]
    with orm.Session(db_engine) as session:
        assert (
            session.scalar(sqlalchemy.select(sqlalchemy.func.count(bts.PipelineRun.id)))
            == 0
        )
        assert (
            session.scalar(
                sqlalchemy.select(sqlalchemy.func.count(bts.PipelineRunAnnotation.key))
            )
            == 0
        )


def test_pipeline_run_validation_failure_rolls_back_partial_graph(
    client: fastapi.testclient.TestClient,
    db_engine: sqlalchemy.Engine,
) -> None:
    task = pipeline_task(name="invalid-at-run-time")
    task["componentRef"]["spec"]["inputs"] = [{"name": "required", "type": "String"}]
    saved_response = client.put(
        "/api/users/me/pipelines",
        params={"file_path": "pipelines/missing-input.yaml"},
        json={"root_pipeline_task": task},
    )
    assert saved_response.status_code == 200

    response = client.post(
        f"/api/pipeline_runs/from_pipeline/{saved_response.json()['id']}",
        json={},
    )

    assert response.status_code == 422
    with orm.Session(db_engine) as session:
        assert (
            session.scalar(sqlalchemy.select(sqlalchemy.func.count(bts.PipelineRun.id)))
            == 0
        )
        assert (
            session.scalar(
                sqlalchemy.select(sqlalchemy.func.count(bts.ExecutionNode.id))
            )
            == 0
        )
        assert (
            session.scalar(
                sqlalchemy.select(sqlalchemy.func.count(bts.ArtifactNode.id))
            )
            == 0
        )


class TestWhoOwnsTheCommit:
    """The split behind the fence: one creator commits, the other leaves it to the caller.

    `create_from_pipeline` is the self-contained entry point and still commits, so the HTTP
    route never changed. `create_from_pipeline_no_commit` is the one the trigger needs, and
    these tests pin the two halves of its contract: it writes, and it does not settle.
    """

    def _saved_pipeline_id(self, client: fastapi.testclient.TestClient) -> str:
        return _save_pipeline(client, name="commit-ownership")["id"]

    def _create(
        self,
        session: orm.Session,
        *,
        pipeline_id: str,
        commit: bool,
    ) -> Any:
        service = services.UserPipelineService()
        arguments: dict[str, Any] = {
            "session": session,
            "pipeline_id": pipeline_id,
            "user_id": None,
            "file_path": None,
            "version": None,
            "run_arguments": None,
            "pipeline_run_annotations": None,
            "created_by": DEFAULT_USER,
        }
        if commit:
            return service.create_from_pipeline(**arguments)
        return service.create_from_pipeline_no_commit(**arguments)

    def test_create_from_pipeline_commits(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        pipeline_id = self._saved_pipeline_id(client)
        with orm.Session(db_engine) as session:
            self._create(session, pipeline_id=pipeline_id, commit=True)

        # A second session sees it, which is the only definition of committed that matters.
        with orm.Session(db_engine) as other:
            assert other.scalar(_count_of(bts.PipelineRun)) == 1

    def test_no_commit_leaves_the_run_at_the_callers_mercy(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        pipeline_id = self._saved_pipeline_id(client)
        with orm.Session(db_engine) as session:
            run = self._create(session, pipeline_id=pipeline_id, commit=False)
            # Flushed, so it has an id the caller can link to before anything is durable.
            assert run.id is not None
            session.rollback()

        with orm.Session(db_engine) as other:
            assert other.scalar(_count_of(bts.PipelineRun)) == 0
            assert other.scalar(_count_of(bts.ExecutionNode)) == 0
            assert other.scalar(_count_of(bts.ArtifactNode)) == 0

    def test_no_commit_is_durable_once_the_caller_commits(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        pipeline_id = self._saved_pipeline_id(client)
        with orm.Session(db_engine) as session:
            run = self._create(session, pipeline_id=pipeline_id, commit=False)
            run_id = run.id
            session.commit()

        with orm.Session(db_engine) as other:
            assert other.get(bts.PipelineRun, run_id) is not None

    def test_a_callers_pending_write_survives_the_run_creation(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The regression guard for deleting `session.rollback()`.

        That rollback discarded every pending write the caller had, not just the implicit read
        transaction it was aimed at. The trigger's fence row is written *before* the run and
        would have been the first casualty — a cycle claimed against a run that then appeared
        without it. Here a marker row stands in for the fence.
        """
        pipeline_id = self._saved_pipeline_id(client)
        marker_path = "pipelines/written-before-the-run.yaml"
        with orm.Session(db_engine) as session:
            session.add(
                db_models.UserPipeline(user_id=DEFAULT_USER, file_path=marker_path)
            )
            session.flush()
            self._create(session, pipeline_id=pipeline_id, commit=False)
            session.commit()

        with orm.Session(db_engine) as other:
            survived = other.scalar(
                sqlalchemy.select(db_models.UserPipeline).where(
                    db_models.UserPipeline.file_path == marker_path
                )
            )
            assert survived is not None
            assert other.scalar(_count_of(bts.PipelineRun)) == 1


def _count_of(model: Any):
    return sqlalchemy.select(sqlalchemy.func.count()).select_from(model)


def test_openapi_exposes_only_approved_saved_pipeline_execution_routes(
    client: fastapi.testclient.TestClient,
) -> None:
    schema = client.get("/openapi.json").json()
    by_id = schema["paths"]["/api/pipeline_runs/from_pipeline/{pipeline_id}"]["post"]
    by_key = schema["paths"]["/api/pipeline_runs/from_pipeline"]["post"]

    assert by_id["tags"] == ["pipelineRuns"]
    assert by_key["tags"] == ["pipelineRuns"]
    assert "stored file path" in by_id["description"]
    assert "stored file path" in by_key["description"]
    request_schema = schema["components"]["schemas"]["SavedPipelineRunRequest"]
    # No `project_id`. The project travels in `pipeline_run_annotations`, because that is
    # the only field `POST /api/pipeline_runs/` also has -- a typed field here would be a
    # mechanism one submit route has and the other does not.
    assert set(request_schema["properties"]) == {
        "run_arguments",
        "pipeline_run_annotations",
    }
    assert not any("from_saved_pipeline" in path for path in schema["paths"])


def test_identical_execution_requests_create_distinct_runs_with_persisted_provenance(
    client: fastapi.testclient.TestClient,
    db_engine: sqlalchemy.Engine,
) -> None:
    saved = _save_pipeline(client, name="non-idempotent")
    path = f"/api/pipeline_runs/from_pipeline/{saved['id']}"

    first = client.post(path, json={})
    second = client.post(path, json={})

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    assert first.json()["id"] != second.json()["id"]
    expected_provenance = {
        "tangleml.com/source/user-pipeline": "true",
        "tangleml.com/user-pipeline/pipeline-id": saved["id"],
        "tangleml.com/user-pipeline/version": saved["version"],
        "tangleml.com/user-pipeline/owner": DEFAULT_USER,
        "tangleml.com/user-pipeline/file-path": "pipelines/saved.yaml",
    }
    run_ids = {first.json()["id"], second.json()["id"]}
    for response in (first, second):
        assert response.json()["created_by"] == DEFAULT_USER
        assert response.json()["annotations"] == expected_provenance

    with orm.Session(db_engine) as session:
        persisted_runs = list(
            session.scalars(
                sqlalchemy.select(bts.PipelineRun).where(
                    bts.PipelineRun.id.in_(run_ids)
                )
            )
        )
        assert len(persisted_runs) == 2
        assert all(run.annotations == expected_provenance for run in persisted_runs)

        mirrored_annotations = set(
            session.execute(
                sqlalchemy.select(
                    bts.PipelineRunAnnotation.pipeline_run_id,
                    bts.PipelineRunAnnotation.key,
                    bts.PipelineRunAnnotation.value,
                ).where(
                    bts.PipelineRunAnnotation.pipeline_run_id.in_(run_ids),
                    bts.PipelineRunAnnotation.key.in_(expected_provenance),
                )
            ).tuples()
        )
        assert mirrored_annotations == {
            (run_id, key, value)
            for run_id in run_ids
            for key, value in expected_provenance.items()
        }


class TestProjectAttribution:
    """The run annotation that files a run under a project.

    The link is an annotation and nothing else: no table, no foreign key, no row written in
    the projects subsystem. That is what lets a run reach a project from the UI, the CLI, an
    agent, a trigger or the scheduler without any of them knowing that subsystem exists.

    Client-supplied rather than server-derived, because the project is the submitter's own
    context and nothing here can derive or check it. So unlike owner and file path it is not
    reserved, and both submit routes accept it in the same field.
    """

    PROJECT_ID = "3f2b0c111111111111a1"

    def test_a_submitted_project_annotation_is_accepted_and_kept(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """The key is not in `PROVENANCE_ANNOTATIONS`, so the annotations map takes it.

        Asserted alongside the server's own provenance because the merge has to do both:
        reserving the key would be a 422, dropping it would leave the project feed empty.
        """
        saved = _save_pipeline(client, name="attributed")
        response = client.post(
            f"/api/pipeline_runs/from_pipeline/{saved['id']}",
            json={
                "pipeline_run_annotations": {
                    pipeline_run_annotations.project_run_key(
                        self.PROJECT_ID
                    ): pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE
                }
            },
        )
        assert response.status_code == 200, response.text
        annotations = response.json()["annotations"]
        assert (
            annotations[pipeline_run_annotations.project_run_key(self.PROJECT_ID)]
            == pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE
        )
        assert (
            annotations[pipeline_run_annotations.PIPELINE_ID_ANNOTATION] == saved["id"]
        )

    def test_it_reaches_the_table_the_project_feed_filters_on(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The assertion the whole key choice rests on.

        `_mirror_single_pipeline_run_annotation` skips `system/`-prefixed keys, so the
        originally designed `system/pipeline_run.project_id` would have been stored on the run
        and never copied here -- a feed empty forever with nothing raising.
        """
        saved = _save_pipeline(client, name="attributed")
        response = client.post(
            f"/api/pipeline_runs/from_pipeline/{saved['id']}",
            json={
                "pipeline_run_annotations": {
                    pipeline_run_annotations.project_run_key(
                        self.PROJECT_ID
                    ): pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE
                }
            },
        )
        run_id = response.json()["id"]

        assert _mirrored_project_ids(db_engine, run_id=run_id) == [self.PROJECT_ID]

    def test_both_submit_routes_land_the_same_annotation(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The UI submits through `POST /api/pipeline_runs/`, the CLI and the scheduler
        through `from_pipeline`. Same annotation, same field, same mirrored row.

        The generic route is exercised at the service layer because it *is* the service layer:
        `api_router` wires it straight to `PipelineRunsApiService_Sql.create` with no
        validation of its own, and that route is not mounted on this package's test app.
        """
        saved = _save_pipeline(client, name="two-routes")
        from_pipeline_run_id = client.post(
            f"/api/pipeline_runs/from_pipeline/{saved['id']}",
            json={
                "pipeline_run_annotations": {
                    pipeline_run_annotations.project_run_key(
                        self.PROJECT_ID
                    ): pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE
                }
            },
        ).json()["id"]

        with orm.Session(db_engine) as session:
            direct = api_server_sql.PipelineRunsApiService_Sql().create(
                session=session,
                root_task=component_structures.TaskSpec.from_json_dict(
                    pipeline_task(name="two-routes")
                ),
                annotations={
                    pipeline_run_annotations.project_run_key(
                        self.PROJECT_ID
                    ): pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE
                },
                created_by=DEFAULT_USER,
            )
            # `create` commits and then refreshes, which autobegins a fresh transaction on
            # the way out; ending it here keeps the later read off a stale one.
            session.rollback()

        assert direct.annotations is not None
        assert (
            direct.annotations[
                pipeline_run_annotations.project_run_key(self.PROJECT_ID)
            ]
            == pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE
        )
        assert _mirrored_project_ids(db_engine, run_id=direct.id) == [self.PROJECT_ID]
        assert _mirrored_project_ids(db_engine, run_id=from_pipeline_run_id) == [
            self.PROJECT_ID
        ]

    def test_the_project_never_touches_a_spec(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Not in the saved pipeline's spec, and not in the run's copy of it.

        Asserted by searching the serialised JSON rather than by naming fields, because
        `TaskSpec` carries `annotations` of its own at both the task and the input level -- a
        future merge could put it somewhere this test does not know to look, and a spec that
        differs per project is a spec that digests differently per project.
        """
        saved = _save_pipeline(client, name="untouched-spec")
        response = client.post(
            f"/api/pipeline_runs/from_pipeline/{saved['id']}",
            json={
                "pipeline_run_annotations": {
                    pipeline_run_annotations.project_run_key(
                        self.PROJECT_ID
                    ): pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE
                }
            },
        )
        run_id = response.json()["id"]

        with orm.Session(db_engine) as session:
            stored_spec = session.scalar(
                sqlalchemy.select(
                    db_models.UserPipelineVersion.root_pipeline_task
                ).where(
                    db_models.UserPipelineVersion.pipeline_id == saved["id"],
                )
            )
            run = session.get(bts.PipelineRun, run_id)
            assert run is not None
            submitted_spec = run.root_execution.task_spec

        assert self.PROJECT_ID not in json.dumps(stored_spec)
        assert self.PROJECT_ID not in json.dumps(submitted_spec)
        # And the run does carry it, so the two assertions above are not passing because the
        # annotation went missing altogether.
        assert (
            run.annotations[pipeline_run_annotations.project_run_key(self.PROJECT_ID)]
            == pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE
        )

    def test_an_unattributed_run_carries_no_key_at_all(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """No key, rather than a key holding nothing.

        The presence of the row *is* the membership, so a key with an empty value would be a
        membership in a project spelled by the key anyway -- there is no "unattributed" value
        to write. Asserted over the whole prefix rather than one key, because the question is
        whether the run is in any project at all.
        """
        saved = _save_pipeline(client, name="unattributed")
        response = client.post(
            f"/api/pipeline_runs/from_pipeline/{saved['id']}", json={}
        )
        assert response.status_code == 200, response.text
        assert (
            pipeline_run_annotations.project_run_keys(response.json()["annotations"])
            == []
        )

    def test_the_server_owned_provenance_keys_are_still_refused(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Unreserving the project key must not have unreserved the five beside it.

        Owner and file path are refused because the server derives them and a client value
        could only disagree. Nothing on the server knows the project, which is the split.
        """
        saved = _save_pipeline(client, name="spoofer")
        response = client.post(
            f"/api/pipeline_runs/from_pipeline/{saved['id']}",
            json={
                "pipeline_run_annotations": {
                    pipeline_run_annotations.OWNER_ANNOTATION: "someone-else"
                }
            },
        )
        assert response.status_code == 422, response.text
        assert "reserved" in response.text

    def test_a_key_stored_on_a_pipeline_never_reaches_a_run(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Parked on the stored pipeline by an older deploy, and dropped on the way into every
        run built from it.

        The write path now refuses the key outright, so such a row can only predate that rule
        or have been inserted outside the CRUD boundary. Left in place it would file every
        future run under a project the submitter never named, with no way to submit outside it.
        """
        saved = _save_pipeline(client, name="smuggler")
        _overwrite_current_annotations(
            db_engine,
            pipeline_id=saved["id"],
            annotations={
                "kept": "stored",
                pipeline_run_annotations.project_run_key(
                    "not-a-real-project"
                ): pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE,
            },
        )

        response = client.post(
            f"/api/pipeline_runs/from_pipeline/{saved['id']}", json={}
        )

        assert response.status_code == 200, response.text
        annotations = response.json()["annotations"]
        # `kept` proves the stored map was merged at all, so the absence below is the pop
        # and not a merge that silently stopped happening.
        assert annotations["kept"] == "stored"
        assert pipeline_run_annotations.project_run_keys(annotations) == []

    def test_a_stored_key_spelled_in_capitals_is_dropped_and_not_fatal(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The only row the new refusal could have stranded, so the drop has to reach it.

        A differently spelled key was storable until the write path started refusing it, and
        the stored map is validated on the way out as well -- so a row carrying one would 422
        on every submission, with no request to correct and no way for its owner to clear it.
        The pop runs first and includes near misses, which both unstrands the pipeline and
        stops MySQL filing its runs under a project nobody named.
        """
        saved = _save_pipeline(client, name="legacy-upper")
        stale = pipeline_run_annotations.project_run_key("not-a-real-project").upper()
        _overwrite_current_annotations(
            db_engine,
            pipeline_id=saved["id"],
            annotations={"kept": "stored", stale: "true"},
        )

        response = client.post(
            f"/api/pipeline_runs/from_pipeline/{saved['id']}", json={}
        )

        assert response.status_code == 200, response.text
        annotations = response.json()["annotations"]
        assert annotations["kept"] == "stored"
        assert stale not in annotations

    def test_a_request_annotation_wins_over_a_stored_one(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """A stale stored project, with the submitter naming one of their own.

        Separate from the test above because the two fail differently, and this one fails
        worse. The stored key and the submitted key are *different keys* now that the id is in
        the key, so a merge cannot resolve them by overwriting: without the pop the run would
        carry both and be filed under a project nobody named, which no ordering of the merge
        fixes. Exactly one project key survives, and it is the submitter's.
        """
        saved = _save_pipeline(client, name="legacy")
        _overwrite_current_annotations(
            db_engine,
            pipeline_id=saved["id"],
            annotations={
                pipeline_run_annotations.project_run_key(
                    "not-a-real-project"
                ): pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE
            },
        )
        response = client.post(
            f"/api/pipeline_runs/from_pipeline/{saved['id']}",
            json={
                "pipeline_run_annotations": {
                    pipeline_run_annotations.project_run_key(
                        self.PROJECT_ID
                    ): pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE
                }
            },
        )
        assert response.status_code == 200, response.text
        assert pipeline_run_annotations.project_run_keys(
            response.json()["annotations"]
        ) == [pipeline_run_annotations.project_run_key(self.PROJECT_ID)]


class TestOneProjectPerRunIsAPolicy:
    """`_reject_more_than_one_project`, and what it is guarding.

    The storage takes any number of memberships -- that is the point of putting the id in the
    key, and `tests/projects/test_project_run_api_routes.py` asserts the feed already reads
    them. What holds the product at one project is this rule and nothing else, so it is worth
    having tests that say so: the refusal is a decision, not a limit, and the day it is removed
    these are the cases that should change.
    """

    SECOND_PROJECT_ID = "4a3c1d222222222222a2"

    def test_two_projects_on_one_submission_is_422(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        saved = _save_pipeline(client, name="two-projects")
        response = client.post(
            f"/api/pipeline_runs/from_pipeline/{saved['id']}",
            json={
                "pipeline_run_annotations": {
                    pipeline_run_annotations.project_run_key(
                        TestProjectAttribution.PROJECT_ID
                    ): pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE,
                    pipeline_run_annotations.project_run_key(
                        self.SECOND_PROJECT_ID
                    ): pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE,
                }
            },
        )
        assert response.status_code == 422, response.text
        assert "at most one project" in response.json()["detail"]

    def test_the_refused_submission_creates_no_run(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Validated before the pipeline is even looked up, so there is no half-written run to
        find and no annotation rows mirrored from one."""
        saved = _save_pipeline(client, name="two-projects")
        client.post(
            f"/api/pipeline_runs/from_pipeline/{saved['id']}",
            json={
                "pipeline_run_annotations": {
                    pipeline_run_annotations.project_run_key(
                        TestProjectAttribution.PROJECT_ID
                    ): pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE,
                    pipeline_run_annotations.project_run_key(
                        self.SECOND_PROJECT_ID
                    ): pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE,
                }
            },
        )
        with orm.Session(db_engine) as session:
            assert (
                session.scalar(
                    sqlalchemy.select(sqlalchemy.func.count(bts.PipelineRun.id))
                )
                == 0
            )

    def test_one_project_is_still_fine(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The guard counts rather than forbids, so it must not have made the ordinary case a
        422 -- which is the way a "reject duplicates" rule usually breaks."""
        saved = _save_pipeline(client, name="one-project")
        response = client.post(
            f"/api/pipeline_runs/from_pipeline/{saved['id']}",
            json={
                "pipeline_run_annotations": {
                    pipeline_run_annotations.project_run_key(
                        TestProjectAttribution.PROJECT_ID
                    ): pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE
                }
            },
        )
        assert response.status_code == 200, response.text

    def test_the_generic_submit_route_is_not_covered_by_it(
        self,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Named so the gap is a known one rather than a surprise.

        `POST /api/pipeline_runs/` hands its annotations map straight to
        `PipelineRunsApiService_Sql.create` and runs none of this validation -- the same gap the
        reserved-key rules have had all along. Asserted at the service layer because that route
        is not mounted on this package's test app.

        Two memberships therefore land, and the feed honours both. That is the storage doing
        what it was chosen to do; holding the *product* to one project is the UI's job and this
        guard's, on the route that has a request body to guard.
        """
        with orm.Session(db_engine) as session:
            run = api_server_sql.PipelineRunsApiService_Sql().create(
                session=session,
                root_task=component_structures.TaskSpec.from_json_dict(
                    pipeline_task(name="unguarded")
                ),
                annotations={
                    pipeline_run_annotations.project_run_key(
                        TestProjectAttribution.PROJECT_ID
                    ): pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE,
                    pipeline_run_annotations.project_run_key(
                        self.SECOND_PROJECT_ID
                    ): pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE,
                },
                created_by=DEFAULT_USER,
            )
            session.rollback()

        assert sorted(_mirrored_project_ids(db_engine, run_id=run.id)) == sorted(
            [TestProjectAttribution.PROJECT_ID, self.SECOND_PROJECT_ID]
        )


class TestProjectKeyCanonicalization:
    """Project keys are matched exactly; only the value is rewritten.

    `pipeline_run_annotation.key` is vendored and declares no collation, so MySQL compares it
    case-insensitively while SQLite is byte-exact. A key spelled any other way is therefore not
    folded onto the canonical one -- it is refused, the same posture `triggers` takes for a
    subscription key.

    Refused rather than stored, because storing it is the harmful half: MySQL matches it
    against the canonical key and files the run into that project's feed, while the rules in
    `pipeline_run_annotations` see no project key and let it past. These cases run on SQLite,
    where that mismatch is invisible, so they assert the refusal and not the query.
    """

    PROJECT_ID = TestProjectAttribution.PROJECT_ID

    def _submit(
        self,
        client: fastapi.testclient.TestClient,
        *,
        name: str,
        keys: dict[str, str],
    ):
        saved = _save_pipeline(client, name=name)
        return client.post(
            f"/api/pipeline_runs/from_pipeline/{saved['id']}",
            json={"pipeline_run_annotations": keys},
        )

    def test_a_differently_spelled_prefix_is_refused(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Neither stored as sent nor folded onto the canonical key."""
        key = pipeline_run_annotations.project_run_key(self.PROJECT_ID).upper()
        response = self._submit(
            client,
            name="upper-case-prefix",
            keys={key: "false"},
        )
        assert response.status_code == 422, response.text
        assert "only in case" in response.json()["detail"]

    def test_a_mixed_case_prefix_is_refused_too(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """`.upper()` is the obvious spelling, not the only reachable one."""
        response = self._submit(
            client,
            name="mixed-case-prefix",
            keys={
                f"Tangleml.com/Project/Id/{self.PROJECT_ID}": (
                    pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE
                )
            },
        )
        assert response.status_code == 422, response.text
        assert "only in case" in response.json()["detail"]

    def test_the_one_project_guard_cannot_be_walked_past_with_capitals(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """The reason the refusal is unconditional rather than scoped to the write path.

        A canonical key plus a differently spelled one used to count as a single project: the
        guard reads the second as an ordinary annotation, while MySQL files the run into both
        feeds. So the pair has to fail on the spelling, before any counting happens.
        """
        response = self._submit(
            client,
            name="canonical-plus-upper",
            keys={
                pipeline_run_annotations.project_run_key(self.PROJECT_ID): (
                    pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE
                ),
                pipeline_run_annotations.project_run_key(
                    TestOneProjectPerRunIsAPolicy.SECOND_PROJECT_ID
                ).upper(): pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE,
            },
        )
        assert response.status_code == 422, response.text
        assert "only in case" in response.json()["detail"]

    def test_an_unrelated_annotation_is_still_ordinary(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """The refusal is the prefix, not the word: a key that merely looks similar is fine."""
        key = "tangleml.com/project-notes/id/anything"
        response = self._submit(
            client,
            name="near-but-not-the-prefix",
            keys={key: "kept"},
        )
        assert response.status_code == 200, response.text
        assert response.json()["annotations"][key] == "kept"

    def test_two_projects_stay_refused_whatever_the_spelling(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        response = self._submit(
            client,
            name="still-two",
            keys={
                pipeline_run_annotations.project_run_key(
                    self.PROJECT_ID.upper()
                ): pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE,
                pipeline_run_annotations.project_run_key(
                    TestOneProjectPerRunIsAPolicy.SECOND_PROJECT_ID
                ): pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE,
            },
        )
        assert response.status_code == 422, response.text
        assert "at most one project" in response.json()["detail"]

    def test_the_membership_value_is_normalized_to_the_marker(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """The value is rewritten for the same reason the key's suffix is.

        The feed predicate is `key_exists`, so the value is not read: sending the key *is* the
        membership whatever the value says, and `"false"` filed a run into its project exactly
        like `"true"` did while both submit routes told clients `"true"` was required. Storing
        the marker keeps the documented contract true of what is actually in the column, which
        is what a later tightening to `value_equals(PROJECT_MEMBERSHIP_VALUE)` needs -- under a
        pass-through, that change silently drops every historically mismarked run out of its
        project. A client that means "not in this project" omits the key.
        """
        response = self._submit(
            client,
            name="false-marker",
            keys={pipeline_run_annotations.project_run_key(self.PROJECT_ID): "false"},
        )
        assert response.status_code == 200, response.text
        annotations = response.json()["annotations"]
        key = pipeline_run_annotations.project_run_key(self.PROJECT_ID)
        assert annotations[key] == pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE

    def test_an_empty_suffix_is_a_422(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """A key with no id after the prefix could never match a feed: accepting it would file
        the run under nothing, with nothing raising."""
        response = self._submit(
            client,
            name="no-suffix",
            keys={
                pipeline_run_annotations.PROJECT_ANNOTATION_PREFIX: (
                    pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE
                )
            },
        )
        assert response.status_code == 422, response.text
        assert "must end in a project id" in response.json()["detail"]

    def test_the_generic_submit_route_stores_the_value_it_was_sent(
        self,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Named so the gap is a known one, like the one-project guard's above.

        `POST /api/pipeline_runs/` hands its annotations straight to
        `PipelineRunsApiService_Sql.create` in the core API, with no seam of ours to
        canonicalize on -- closing it starts with an upstream change forwarding
        `pipeline_run_creation_hook` through `setup_routes`. Until then a run submitted there
        keeps whatever value it was sent, where the saved route would have rewritten it to the
        membership marker.
        """
        key = pipeline_run_annotations.project_run_key(self.PROJECT_ID)
        with orm.Session(db_engine) as session:
            run = api_server_sql.PipelineRunsApiService_Sql().create(
                session=session,
                root_task=component_structures.TaskSpec.from_json_dict(
                    pipeline_task(name="verbatim")
                ),
                annotations={key: "false"},
                created_by=DEFAULT_USER,
            )
            session.rollback()

        with orm.Session(db_engine) as session:
            stored = session.scalar(
                sqlalchemy.select(bts.PipelineRunAnnotation.value).where(
                    bts.PipelineRunAnnotation.pipeline_run_id == run.id,
                    bts.PipelineRunAnnotation.key == key,
                )
            )
        assert stored == "false"


def test_run_hooks_prepare_a_copy_and_record_inside_the_transaction(
    client: fastapi.testclient.TestClient,
    db_engine: sqlalchemy.Engine,
) -> None:
    saved = _save_pipeline(client, name="original")
    context = {"target": "prepared"}
    seen = []

    class Hooks:
        def prepare_saved_run(self, task_json):
            task_json["componentRef"]["spec"]["name"] = "prepared"
            seen.append("prepare")
            return context

        def run_created(self, run, received_context):
            assert received_context is context
            session = orm.object_session(run)
            assert session is not None and session.in_transaction()
            assert run.id is not None
            run.extra_data = {
                **run.extra_data,
                "prepared_target": context["target"],
            }
            seen.append("created")

    service = services.UserPipelineService(hooks=Hooks())
    with orm.Session(db_engine) as session:
        response = service.create_from_pipeline(
            session=session,
            pipeline_id=saved["id"],
            user_id=None,
            file_path=None,
            version=None,
            run_arguments=None,
            pipeline_run_annotations=None,
            created_by=DEFAULT_USER,
        )
    with orm.Session(db_engine) as session:
        run = session.get(bts.PipelineRun, response.id)
        assert run.extra_data["prepared_target"] == "prepared"
        assert run.extra_data["pipeline_name"] == "prepared"
    stored = client.get(
        "/api/users/me/pipelines", params={"file_path": "pipelines/saved.yaml"}
    ).json()
    assert stored["root_pipeline_task"]["componentRef"]["spec"]["name"] == "original"
    assert stored["version"] == saved["version"]
    assert seen == ["prepare", "created"]


def test_injected_run_hook_preserves_structured_rejection(
    client: fastapi.testclient.TestClient,
    db_engine: sqlalchemy.Engine,
) -> None:
    saved = _save_pipeline(client)

    class Hooks:
        def prepare_saved_run(self, task_json):
            raise errors.RunTargetUnavailableError(
                "Target has retired",
                code="target_retired",
                successor={"target": "next"},
            )

        def run_created(self, run, context):
            raise AssertionError("Run creation must not happen after rejection")

    app = fastapi.FastAPI()

    def get_session():
        with orm.Session(db_engine) as session:
            yield session

    def get_user_details():
        return api_router.UserDetails(
            name=DEFAULT_USER,
            permissions=api_router.Permissions(read=True, write=True, admin=False),
        )

    api_routes.setup_user_pipeline_routes(
        app=app,
        get_session=get_session,
        user_details_getter=get_user_details,
        service=services.UserPipelineService(hooks=Hooks()),
    )
    response = fastapi.testclient.TestClient(app).post(
        f"/api/pipeline_runs/from_pipeline/{saved['id']}", json={}
    )
    assert response.status_code == 422
    assert response.json() == {
        "detail": "Target has retired",
        "code": "target_retired",
        "successor": {"target": "next"},
    }
    with orm.Session(db_engine) as session:
        assert (
            session.scalar(sqlalchemy.select(sqlalchemy.func.count(bts.PipelineRun.id)))
            == 0
        )
