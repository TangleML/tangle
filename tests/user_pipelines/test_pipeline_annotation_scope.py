import json

import pytest

from tests.user_pipelines.conftest import pipeline_task

_USER_HEADERS = {"x-user": "alice@example.com"}


def _save(client, task, *, run_annotations=None):
    response = client.put(
        "/api/users/me/pipelines",
        headers=_USER_HEADERS,
        params={"file_path": "scope.yaml"},
        json={
            "root_pipeline_task": task,
            "pipeline_run_annotations": run_annotations,
        },
    )
    assert response.status_code == 200, response.text
    return response.json()


def _assert_matches(client, predicate, expected_ids):
    response = client.get(
        "/api/pipelines/search",
        headers=_USER_HEADERS,
        params={"filter_query": json.dumps({"and": [predicate]})},
    )
    assert response.status_code == 200, response.text
    page = response.json()
    assert [pipeline["id"] for pipeline in page["pipelines"]] == expected_ids
    assert page["total_count"] == len(expected_ids)


def test_search_uses_pipeline_metadata_not_task_or_run_annotations(client):
    task = pipeline_task(name="scope")
    spec = task["componentRef"]["spec"]
    spec["metadata"] = {"annotations": {"team": "pipeline"}}
    task["annotations"] = {"team": "root-task"}
    child = pipeline_task(name="child")
    child["componentRef"]["spec"]["metadata"] = {
        "annotations": {"team": "child-component"}
    }
    spec["implementation"]["graph"]["tasks"]["child"] = child
    saved = _save(client, task, run_annotations={"team": "run-default"})

    run = client.post(
        f"/api/pipeline_runs/from_pipeline/{saved['id']}",
        headers=_USER_HEADERS,
        json={
            "pipeline_run_annotations": {
                "team": "execution",
                "execution-only": "present",
            }
        },
    )
    assert run.status_code == 200, run.text
    assert run.json()["annotations"]["team"] == "execution"
    assert run.json()["annotations"]["execution-only"] == "present"

    for value in (
        "pipeline",
        "run-default",
        "root-task",
        "child-component",
        "execution",
    ):
        _assert_matches(
            client,
            {"value_equals": {"key": "team", "value": value}},
            [saved["id"]] if value == "pipeline" else [],
        )
    _assert_matches(client, {"key_exists": {"key": "execution-only"}}, [])


@pytest.mark.parametrize(
    "metadata_fields",
    [
        {},
        {"metadata": None},
        {"metadata": {}},
        {"metadata": {"annotations": None}},
        {"metadata": {"annotations": {}}},
    ],
    ids=[
        "missing-metadata",
        "null-metadata",
        "empty-metadata",
        "null-annotations",
        "empty-annotations",
    ],
)
def test_absent_pipeline_annotations_do_not_fall_back_to_other_scopes(
    client, metadata_fields
):
    task = pipeline_task(name="unannotated")
    task["componentRef"]["spec"].update(metadata_fields)
    task["annotations"] = {"team": "root-task"}
    saved = _save(client, task, run_annotations={"team": "run-default"})

    predicate = {"key_exists": {"key": "team"}}
    _assert_matches(client, predicate, [])
    _assert_matches(client, {"not": predicate}, [saved["id"]])
