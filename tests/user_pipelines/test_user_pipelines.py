import collections.abc
import json

import fastapi
import fastapi.testclient
import pytest
import sqlalchemy
from sqlalchemy import event, orm

from cloud_pipelines_backend import api_router, api_server_sql, database_ops
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.user_pipelines import (
    api_routes,
    errors,
    pipeline_run_annotations,
    services,
)

DEFAULT_USER = "alice@example.com"
OTHER_USER = "bob@example.com"
MY_PIPELINES = "/api/users/me/pipelines"
MISSING_ID = "00000000-0000-4000-8000-000000000000"
SECRET = {"dynamicData": {"secret": {"name": "API_TOKEN"}}}


def pipeline_task(*, name: str, tasks: dict | None = None, **spec) -> dict:
    """A graph pipeline task with the given name, child tasks and extra spec fields."""
    return {
        "componentRef": {
            "spec": {
                "name": name,
                **spec,
                "implementation": {"graph": {"tasks": tasks or {}}},
            }
        }
    }


@pytest.fixture()
def db_engine() -> sqlalchemy.Engine:
    engine = database_ops.create_db_engine(database_uri="sqlite://")

    @event.listens_for(engine, "connect")
    def _enable_foreign_keys(dbapi_connection, connection_record) -> None:
        del connection_record
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    bts._TableBase.metadata.create_all(engine)
    return engine


@pytest.fixture()
def app(db_engine: sqlalchemy.Engine) -> fastapi.FastAPI:
    test_app = fastapi.FastAPI()

    def get_session() -> collections.abc.Iterator[orm.Session]:
        with orm.Session(bind=db_engine) as session:
            yield session

    def get_user_details(request: fastapi.Request) -> api_router.UserDetails:
        return api_router.UserDetails(
            name=request.headers.get("x-user", DEFAULT_USER),
            permissions=api_router.Permissions(
                read=request.headers.get("x-read", "true") == "true",
                write=request.headers.get("x-write", "true") == "true",
                admin=request.headers.get("x-admin", "false") == "true",
            ),
        )

    api_routes.setup_user_pipeline_routes(
        app=test_app,
        get_session=get_session,
        user_details_getter=get_user_details,
    )
    return test_app


@pytest.fixture()
def client(app: fastapi.FastAPI) -> fastapi.testclient.TestClient:
    return fastapi.testclient.TestClient(app)


@pytest.fixture()
def other_user_client(app: fastapi.FastAPI) -> fastapi.testclient.TestClient:
    return fastapi.testclient.TestClient(
        app,
        headers={"x-user": OTHER_USER},
    )


def _save(client, file_path="p.yaml", task=None, status_code=200, **body):
    """Saves a pipeline for the client's user and returns the response body."""
    response = client.put(
        MY_PIPELINES,
        params={"file_path": file_path},
        json={"root_pipeline_task": task or pipeline_task(name="P"), **body},
    )
    assert response.status_code == status_code, response.text
    return response.json()


def _search(client, filter_query=None, **body):
    """Runs a search and returns the response body."""
    if filter_query is not None:
        body["filter_query"] = json.dumps(filter_query)
    response = client.post("/api/pipelines/search", json=body)
    assert response.status_code == 200, response.text
    return response.json()


def _secret_task(*, x: int = 0, command: str = "echo") -> dict:
    """A pipeline whose child task reads a root secret argument."""
    task = pipeline_task(
        name="S",
        inputs=[{"name": "token"}],
        tasks={
            "use": {
                "componentRef": {"spec": {"name": command}},
                "arguments": {"token": {"graphInput": {"inputName": "token"}}},
                "annotations": {"editor.position": json.dumps({"x": x, "y": 0})},
            }
        },
    )
    task["arguments"] = {"token": SECRET}
    return task


def test_saving_the_same_content_twice_changes_nothing(client) -> None:
    first = _save(client)
    second = _save(client)

    assert first["updated"] is True
    assert second["updated"] is False
    assert second["id"] == first["id"]
    assert second["version"] == first["version"]


def test_disabled_versioning_overwrites_the_current_version(client) -> None:
    first = _save(client)
    second = _save(client, task=pipeline_task(name="Q"))

    assert second["versioning_mode"] == "disabled"
    assert second["pipeline_name"] == "Q"
    old = client.get(
        f"/api/pipelines/{first['id']}", params={"version": first["version"]}
    )
    assert old.status_code == 404


def test_full_versioning_keeps_earlier_versions(client) -> None:
    first = _save(client, versioning_mode="full")
    second = _save(client, task=pipeline_task(name="Q"))

    old = client.get(
        f"/api/pipelines/{first['id']}", params={"version": first["version"]}
    )

    assert old.status_code == 200
    assert old.json()["pipeline_name"] == "P"
    assert old.json()["current_version"] == second["version"]


def test_switching_versioning_mode_keeps_the_content(client) -> None:
    saved = _save(client)
    url = f"{MY_PIPELINES}/{saved['id']}/properties"

    full = client.patch(url, json={"versioning_mode": "full"}).json()
    disabled = client.patch(url, json={"versioning_mode": "disabled"}).json()

    assert (full["versioning_mode"], disabled["versioning_mode"]) == (
        "full",
        "disabled",
    )
    assert saved["version"] == full["version"] == disabled["version"]
    assert (
        client.patch(url, json={"versioning_mode": "disabled"}).json()["updated"]
        is False
    )


def test_a_deleted_pipeline_is_hidden_until_saved_again(client) -> None:
    saved = _save(client)

    params = {"file_path": "p.yaml"}
    deleted = client.delete(MY_PIPELINES, params=params)
    by_path = client.get(MY_PIPELINES, params=params)
    by_id = client.get(f"/api/pipelines/{saved['id']}")
    deleted_again = client.delete(MY_PIPELINES, params=params)
    revived = _save(client)

    assert deleted.status_code == 204
    assert (by_path.status_code, by_id.status_code) == (404, 404)
    assert deleted_again.status_code == 404
    assert revived["id"] == saved["id"]


@pytest.mark.parametrize(
    "body",
    [
        {"root_pipeline_task": {"componentRef": "not a ref"}},
        {"root_pipeline_task": pipeline_task(name="x" * 1025)},
        {
            "root_pipeline_task": pipeline_task(name="P"),
            "pipeline_run_annotations": {"system/pipeline_run.created_by": "x"},
        },
        {
            "root_pipeline_task": pipeline_task(name="P"),
            "pipeline_run_annotations": {
                pipeline_run_annotations.OWNER_ANNOTATION: "x"
            },
        },
    ],
)
def test_invalid_pipelines_are_rejected(client, body) -> None:
    response = client.put(MY_PIPELINES, params={"file_path": "p.yaml"}, json=body)

    assert response.status_code == 422


def test_writes_need_an_authenticated_writer(client) -> None:
    body = {"root_pipeline_task": pipeline_task(name="P")}

    anonymous = client.put(
        MY_PIPELINES, params={"file_path": "p.yaml"}, json=body, headers={"x-user": ""}
    )
    reader = client.put(
        MY_PIPELINES,
        params={"file_path": "p.yaml"},
        json=body,
        headers={"x-write": "false"},
    )

    assert anonymous.status_code == 401
    assert reader.status_code == 403


def test_listing_pages_newest_first_and_filters_by_prefix(client) -> None:
    for path in ["a/1.yaml", "a/2.yaml", "b/3.yaml"]:
        _save(client, file_path=path)

    first = client.get(f"{MY_PIPELINES}/all", params={"page_size": 2}).json()
    second = client.get(
        f"{MY_PIPELINES}/all", params={"page_token": first["next_page_token"]}
    ).json()
    prefixed = client.get(f"{MY_PIPELINES}/all", params={"file_path": "a/"}).json()

    assert [p["file_path"] for p in first["pipelines"]] == ["b/3.yaml", "a/2.yaml"]
    assert [p["file_path"] for p in second["pipelines"]] == ["a/1.yaml"]
    assert second["next_page_token"] is None
    assert (first["total_count"], prefixed["total_count"]) == (3, 2)


def test_other_users_pipelines_can_be_listed_and_read(
    client, other_user_client
) -> None:
    saved = _save(client)

    listed = other_user_client.get(f"/api/users/{DEFAULT_USER}/pipelines/all").json()
    read = other_user_client.get(
        "/api/pipelines", params={"user_id": DEFAULT_USER, "file_path": "p.yaml"}
    ).json()

    assert [p["id"] for p in listed["pipelines"]] == [saved["id"]]
    assert read["root_pipeline_task"] == saved["root_pipeline_task"]


def test_a_writer_can_save_into_and_delete_another_users_pipeline(
    client, other_user_client
) -> None:
    saved = _save(client)

    updated = other_user_client.put(
        f"/api/pipelines/{saved['id']}",
        json={"root_pipeline_task": pipeline_task(name="Q")},
    )
    deleted = other_user_client.delete(f"/api/pipelines/{saved['id']}")
    after_delete = client.get(f"/api/pipelines/{saved['id']}")

    assert updated.status_code == 200
    assert (updated.json()["user_id"], updated.json()["pipeline_name"]) == (
        DEFAULT_USER,
        "Q",
    )
    assert deleted.status_code == 204
    assert after_delete.status_code == 404


def test_by_id_writes_to_a_missing_pipeline_are_404(client) -> None:
    put = client.put(
        f"/api/pipelines/{MISSING_ID}",
        json={"root_pipeline_task": pipeline_task(name="P")},
    )
    delete = client.delete(f"/api/pipelines/{MISSING_ID}")

    assert (put.status_code, delete.status_code) == (404, 404)


@pytest.mark.parametrize(
    ("task", "headers", "status_code"),
    [
        (_secret_task(x=50), {}, 200),
        (pipeline_task(name="S"), {}, 403),
        (_secret_task(command="curl"), {}, 403),
        (_secret_task(command="curl"), {"x-admin": "true"}, 200),
        (_secret_task(command="curl"), {"x-user": DEFAULT_USER}, 200),
    ],
)
def test_only_the_owner_or_an_admin_can_change_secrets(
    client, other_user_client, task, headers, status_code
) -> None:
    saved = _save(client, task=_secret_task())

    response = other_user_client.put(
        f"/api/pipelines/{saved['id']}",
        json={"root_pipeline_task": task},
        headers=headers,
    )

    assert response.status_code == status_code, response.text


def test_search_filters_by_owner_name_and_annotation(client, other_user_client) -> None:
    task = pipeline_task(name="Training run", metadata={"annotations": {"team": "ml"}})
    mine = _save(client, task=task)
    other = _save(other_user_client, task=pipeline_task(name="Training other"))

    def ids(*predicates):
        return [
            p["id"] for p in _search(client, {"and": list(predicates)})["pipelines"]
        ]

    owner = {"value_equals": {"key": "system/pipeline.user_id", "value": "me"}}
    name = {
        "value_contains": {"key": "system/pipeline.name", "value_substring": "train"}
    }
    team = {"value_equals": {"key": "team", "value": "ml"}}
    assert ids(owner) == [mine["id"]]
    assert len(ids(name)) == 2
    assert ids(team) == [mine["id"]]
    assert ids({"not": team}) == [other["id"]]


def test_search_pages_with_the_same_sort(client) -> None:
    for name in ["b", "C", "a"]:
        _save(client, file_path=f"{name}.yaml", task=pipeline_task(name=name))

    first = _search(client, page_size=2, sort_field="name", sort_direction="asc")
    second = _search(client, page_token=first["next_page_token"])

    names = [p["pipeline_name"] for p in first["pipelines"] + second["pipelines"]]
    assert names == ["a", "b", "C"]
    assert second["next_page_token"] is None


def test_search_rejects_unknown_fields(client) -> None:
    query = {"and": [{"key_exists": {"key": "system/pipeline.secret"}}]}

    response = client.post(
        "/api/pipelines/search", json={"filter_query": json.dumps(query)}
    )

    assert response.status_code == 422


def test_a_run_records_provenance_and_merges_arguments(client) -> None:
    task = pipeline_task(
        name="P",
        inputs=[{"name": "a", "default": "1"}, {"name": "b", "default": "2"}],
    )
    saved = _save(client, task=task, pipeline_run_annotations={"team": "ml"})

    response = client.post(
        f"/api/pipeline_runs/from_pipeline/{saved['id']}",
        json={"run_arguments": {"b": "3"}, "pipeline_run_annotations": {"note": "x"}},
    )

    assert response.status_code == 200, response.text
    annotations = response.json()["annotations"]
    assert annotations["team"] == "ml"
    assert annotations["note"] == "x"
    assert annotations[pipeline_run_annotations.PIPELINE_ID_ANNOTATION] == saved["id"]
    assert annotations[pipeline_run_annotations.VERSION_ANNOTATION] == saved["version"]
    assert annotations[pipeline_run_annotations.OWNER_ANNOTATION] == DEFAULT_USER


def test_a_run_of_an_earlier_version_records_that_version(client) -> None:
    first = _save(client, versioning_mode="full")
    _save(client, task=pipeline_task(name="Q"))

    response = client.post(
        f"/api/pipeline_runs/from_pipeline/{first['id']}",
        params={"version": first["version"]},
        json={},
    )

    annotations = response.json()["annotations"]
    assert annotations[pipeline_run_annotations.VERSION_ANNOTATION] == first["version"]


def test_a_run_cannot_override_provenance(client) -> None:
    saved = _save(client)

    response = client.post(
        f"/api/pipeline_runs/from_pipeline/{saved['id']}",
        json={
            "pipeline_run_annotations": {pipeline_run_annotations.OWNER_ANNOTATION: "x"}
        },
    )

    assert response.status_code == 422


def test_reference_helpers(client, db_engine: sqlalchemy.Engine) -> None:
    disabled = _save(client, file_path="d.yaml")
    full = _save(client, file_path="f.yaml", versioning_mode="full")
    client.delete(MY_PIPELINES, params={"file_path": "d.yaml"})

    with orm.Session(db_engine) as session:
        live = services.live_pipeline_ids(
            session=session, pipeline_ids=[disabled["id"], full["id"], "not-a-uuid"]
        )
        pipeline = services.get_live_owned_pipeline(
            session=session, pipeline_id=full["id"], user_id=DEFAULT_USER
        )
        key = services.resolve_pinnable_version_key(
            session=session, pipeline=pipeline, content_digest=full["version"]
        )
        with pytest.raises(errors.PipelineNotFoundError):
            services.get_live_owned_pipeline(
                session=session, pipeline_id=full["id"], user_id=OTHER_USER
            )
        with pytest.raises(errors.PipelineValidationError):
            services.resolve_pinnable_version_key(
                session=session, pipeline=pipeline, content_digest="0" * 64
            )

    assert live == {full["id"]}
    assert key == full["version"]


def test_scheduler_runs_can_require_a_pinnable_version(
    client, db_engine: sqlalchemy.Engine
) -> None:
    disabled = _save(client, file_path="d.yaml")
    full = _save(client, file_path="f.yaml", versioning_mode="full")
    service = services.UserPipelineService()

    def run(saved: dict) -> api_server_sql.PipelineRunResponse:
        with orm.Session(db_engine) as session:
            return service.create_from_pipeline(
                session=session,
                pipeline_id=saved["id"],
                user_id=DEFAULT_USER,
                file_path=None,
                version=saved["version"],
                run_arguments=None,
                pipeline_run_annotations=None,
                created_by=DEFAULT_USER,
                require_pinnable_versioning=True,
                end_lookup_transaction=True,
            )

    pinned = run(full)
    with pytest.raises(errors.PipelineValidationError):
        run(disabled)

    assert (
        pinned.annotations[pipeline_run_annotations.VERSION_ANNOTATION]
        == full["version"]
    )


def test_validation_only_lookups_skip_the_pipeline_content(
    client, db_engine: sqlalchemy.Engine
) -> None:
    saved = _save(client, versioning_mode="full")

    with orm.Session(db_engine) as session:
        pipeline, selected, _ = services.UserPipelineService().get_pipeline_and_version(
            session=session,
            pipeline_id=saved["id"],
            user_id=DEFAULT_USER,
            file_path=None,
            version=saved["version"],
            require_pinnable=True,
            validation_only=True,
        )
        pipeline_id = pipeline.id
        with pytest.raises(sqlalchemy.exc.InvalidRequestError):
            selected.root_pipeline_task

    assert pipeline_id == saved["id"]
