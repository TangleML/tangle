import base64
import datetime
import json
from typing import Any

import pytest
import sqlalchemy
from cloud_pipelines_backend.user_pipelines import db_models
from sqlalchemy import orm

from tests.user_pipelines.conftest import pipeline_task

SEARCH_PATH = "/api/pipelines/search"
ALICE = "alice@example.com"
BOB = "bob@example.com"


def _put(
    client,
    *,
    file_path: str,
    name: str | None = "training",
    user_id: str = ALICE,
    versioning_mode: str = "full",
    annotations: dict[str, str] | None = None,
) -> dict[str, Any]:
    task = pipeline_task(name="unused" if name is None else name)
    if name is None:
        del task["componentRef"]["spec"]["name"]
    task["componentRef"]["spec"]["metadata"] = {
        "annotations": {"team": "research"} if annotations is None else annotations
    }
    response = client.put(
        "/api/users/me/pipelines",
        headers={"x-user": user_id},
        params={"file_path": file_path},
        json={
            "root_pipeline_task": task,
            "versioning_mode": versioning_mode,
        },
    )
    assert response.status_code == 200, response.text
    return response.json()


def _search(
    client,
    *,
    filter_query: dict[str, Any] | None = None,
    user_id: str = ALICE,
    method: str = "GET",
    **params,
) -> dict[str, Any]:
    if filter_query is not None:
        params["filter_query"] = json.dumps(filter_query)
    response = _search_response(
        client,
        method=method,
        headers={"x-user": user_id},
        **params,
    )
    assert response.status_code == 200, response.text
    return response.json()


def _search_response(client, *, method: str = "GET", headers=None, **params):
    return client.request(
        method,
        SEARCH_PATH,
        headers=headers,
        **{"params" if method == "GET" else "json": params},
    )


def _equals(field: str, value: str) -> dict[str, Any]:
    return {"value_equals": {"key": field, "value": value}}


def _filter(operator: str, field: str, **arguments: Any) -> dict[str, Any]:
    return {"and": [{operator: {"key": field, **arguments}}]}


def _ids(page: dict[str, Any]) -> list[str]:
    return [pipeline["id"] for pipeline in page["pipelines"]]


def _set_times(
    db_engine: sqlalchemy.Engine,
    pipeline_times: dict[str, datetime.datetime],
    *,
    field: str = "updated_at",
) -> None:
    with orm.Session(db_engine) as session:
        for pipeline_id, timestamp in pipeline_times.items():
            pipeline = session.get(db_models.UserPipeline, pipeline_id)
            assert pipeline is not None
            setattr(pipeline, field, timestamp)
        session.commit()


@pytest.mark.parametrize("sort_field", ["updated_at", "name"])
def test_search_returns_current_summaries_for_all_owners_without_loading_tasks(
    client, db_engine: sqlalchemy.Engine, sort_field: str
) -> None:
    _put(client, file_path="alice.yaml", name="historical")
    own = _put(client, file_path="alice.yaml", name="current")
    other = _put(
        client,
        file_path="bob.yaml",
        name="mutable",
        user_id=BOB,
        versioning_mode="disabled",
    )
    statements: list[str] = []

    def capture_statement(
        connection, cursor, statement, parameters, context, executemany
    ) -> None:
        statements.append(statement)

    sqlalchemy.event.listen(db_engine, "before_cursor_execute", capture_statement)
    try:
        result = _search(client, sort_field=sort_field)
    finally:
        sqlalchemy.event.remove(db_engine, "before_cursor_execute", capture_statement)

    assert result["total_count"] == 2
    assert result["next_page_token"] is None
    summaries = {item["id"]: item for item in result["pipelines"]}
    assert set(summaries) == {own["id"], other["id"]}
    expected_fields = {
        "id",
        "user_id",
        "file_path",
        "pipeline_name",
        "created_at",
        "updated_at",
        "current_version",
        "versioning_mode",
    }
    for saved in (own, other):
        assert summaries[saved["id"]] == {
            field: saved[field] for field in expected_fields
        }
    # Saved definitions remain searchable without ever having been run. A list
    # must also avoid fetching each version's potentially large task JSON.
    assert statements
    assert all("root_pipeline_task" not in sql for sql in statements)
    assert all("pipeline_run_annotations" not in sql for sql in statements)


@pytest.mark.parametrize(
    "versioning_mode, restore_history",
    [("full", False), ("disabled", False), ("full", True)],
)
def test_name_filter_only_matches_selected_current_version(
    client, versioning_mode: str, restore_history: bool
) -> None:
    _put(client, file_path="versions.yaml", name="original")
    current = _put(
        client,
        file_path="versions.yaml",
        name="newer",
        versioning_mode=versioning_mode,
    )
    if restore_history:
        current = _put(client, file_path="versions.yaml", name="original")
    historical_name = "newer" if restore_history else "original"
    middle = _put(client, file_path="middle.yaml", name="newt")

    historical = _search(
        client,
        filter_query=_filter(
            "value_equals", "system/pipeline.name", value=historical_name
        ),
    )
    selected = _search(
        client,
        filter_query=_filter(
            "value_equals",
            "system/pipeline.name",
            value=current["pipeline_name"],
        ),
    )

    assert historical["total_count"] == 0
    assert historical["pipelines"] == []
    assert _ids(selected) == [current["id"]]
    assert selected["total_count"] == 1
    assert selected["pipelines"][0]["current_version"] == current["version"]
    ordered = _search(client, sort_field="name", sort_direction="asc")
    assert _ids(ordered) == (
        [middle["id"], current["id"]]
        if restore_history
        else [current["id"], middle["id"]]
    )


@pytest.mark.parametrize(
    "versioning_mode, restore_history",
    [("full", False), ("disabled", False), ("full", True)],
)
def test_annotation_filters_only_match_selected_current_version(
    client, versioning_mode: str, restore_history: bool
) -> None:
    original_annotations = {"team": "original", "removed": "value"}
    original = _put(client, file_path="versions.yaml", annotations=original_annotations)
    current = _put(
        client,
        file_path="versions.yaml",
        annotations={"team": "newer"},
        versioning_mode=versioning_mode,
    )
    if restore_history:
        current = _put(
            client, file_path="versions.yaml", annotations=original_annotations
        )
        assert current["version"] == original["version"]

    selected_value = "original" if restore_history else "newer"
    historical_value = "newer" if restore_history else "original"
    selected = _search(
        client,
        filter_query=_filter("value_equals", "team", value=selected_value),
    )
    historical = _search(
        client,
        filter_query=_filter("value_equals", "team", value=historical_value),
    )
    removed_key = _search(client, filter_query=_filter("key_exists", "removed"))

    assert _ids(selected) == [current["id"]]
    assert selected["total_count"] == 1
    assert selected["pipelines"][0]["current_version"] == current["version"]
    assert historical["pipelines"] == []
    assert historical["total_count"] == 0
    assert _ids(removed_key) == ([current["id"]] if restore_history else [])


@pytest.mark.parametrize("versioning_mode", ["full", "disabled"])
def test_search_excludes_deleted_pipelines_and_restores_reactivated_identity(
    client, versioning_mode: str
) -> None:
    saved = _put(client, file_path="reactivated.yaml", versioning_mode=versioning_mode)
    deleted = client.delete(
        "/api/users/me/pipelines",
        headers={"x-user": ALICE},
        params={"file_path": saved["file_path"]},
    )
    assert deleted.status_code == 204
    assert _search(client) == {
        "pipelines": [],
        "total_count": 0,
        "next_page_token": None,
    }

    reactivated = _put(
        client,
        file_path=saved["file_path"],
        versioning_mode=versioning_mode,
    )

    result = _search(client)
    assert reactivated["id"] == saved["id"]
    assert _ids(result) == [saved["id"]]
    assert result["total_count"] == 1


@pytest.mark.parametrize("operator", ["value_equals", "value_in"])
@pytest.mark.parametrize(
    "field, response_field",
    [
        ("system/pipeline.id", "id"),
        ("system/pipeline.user_id", "user_id"),
        ("system/pipeline.name", "pipeline_name"),
        ("system/pipeline.file_path", "file_path"),
        ("system/pipeline.versioning_mode", "versioning_mode"),
    ],
)
def test_string_fields_support_equality_and_membership(
    client, operator: str, field: str, response_field: str
) -> None:
    match = _put(client, file_path="selected.yaml", name="selected")
    _put(
        client,
        file_path="excluded.yaml",
        name="excluded",
        user_id=BOB,
        versioning_mode="disabled",
    )
    if operator == "value_equals":
        arguments = {"value": match[response_field]}
    else:
        arguments = {"values": [match[response_field]]}

    result = _search(client, filter_query=_filter(operator, field, **arguments))

    assert _ids(result) == [match["id"]]
    assert result["total_count"] == 1


def test_nested_boolean_filters_and_me_are_resolved_for_requesting_reader(
    client,
) -> None:
    alice = _put(client, file_path="alice.yaml", name="training")
    bob = _put(client, file_path="bob.yaml", name="training", user_id=BOB)
    _put(client, file_path="excluded.yaml", name="evaluation", user_id=BOB)
    query = {
        "and": [
            {
                "or": [
                    _equals("system/pipeline.user_id", "me"),
                    {
                        "value_in": {
                            "key": "system/pipeline.user_id",
                            "values": ["me"],
                        }
                    },
                ]
            },
            {"not": _equals("system/pipeline.name", "evaluation")},
        ]
    }

    assert _ids(_search(client, filter_query=query)) == [alice["id"]]
    assert _ids(_search(client, filter_query=query, user_id=BOB)) == [bob["id"]]
    union = _search(
        client,
        filter_query={
            "and": [
                {
                    "value_in": {
                        "key": "system/pipeline.user_id",
                        "values": ["me", BOB],
                    }
                },
                _equals("system/pipeline.name", "training"),
            ]
        },
    )
    assert set(_ids(union)) == {alice["id"], bob["id"]}


@pytest.mark.parametrize(
    "field", ["system/pipeline.name", "system/pipeline.file_path", "team"]
)
@pytest.mark.parametrize("literal", ["%", "_", "\\"])
def test_contains_is_case_insensitive_and_treats_wildcards_literally(
    client, field: str, literal: str
) -> None:
    match = _put(
        client,
        file_path=f"folder/TRAIN{literal}ING.yaml",
        name=f"TRAIN{literal}ING",
        annotations={"team": f"TRAIN{literal}ING"},
    )
    _put(
        client,
        file_path="folder/trainXing.yaml",
        name="trainXing",
        annotations={"team": "trainXing"},
    )
    _put(
        client,
        file_path="folder/training.yaml",
        name="training",
        annotations={"team": "training"},
    )

    result = _search(
        client,
        filter_query=_filter(
            "value_contains", field, value_substring=f"train{literal}ing"
        ),
    )

    assert _ids(result) == [match["id"]]


def test_contains_matches_identical_non_ascii_name(client) -> None:
    saved = _put(client, file_path="unicode.yaml", name="École")

    result = _search(
        client,
        filter_query=_filter(
            "value_contains", "system/pipeline.name", value_substring="École"
        ),
    )

    assert _ids(result) == [saved["id"]]


@pytest.mark.parametrize(
    "predicate",
    [
        _equals("system/pipeline.name", "training"),
        {
            "value_in": {
                "key": "system/pipeline.name",
                "values": ["training"],
            }
        },
        {
            "value_contains": {
                "key": "system/pipeline.name",
                "value_substring": "train",
            }
        },
        {"key_exists": {"key": "system/pipeline.name"}},
    ],
)
def test_missing_names_fail_positive_filters_and_match_negation(
    client, predicate: dict[str, Any]
) -> None:
    named = _put(client, file_path="named.yaml", name="training")
    unnamed = _put(client, file_path="unnamed.yaml", name=None)

    positive = _search(client, filter_query={"and": [predicate]})
    negative = _search(client, filter_query={"and": [{"not": predicate}]})

    assert _ids(positive) == [named["id"]]
    assert _ids(negative) == [unnamed["id"]]
    assert negative["pipelines"][0]["pipeline_name"] is None


@pytest.mark.parametrize(
    "operator, arguments, expected_labels",
    [
        ("key_exists", {}, {"populated", "empty"}),
        ("value_equals", {"value": "research"}, {"populated"}),
        ("value_in", {"values": ["other", "research"]}, {"populated"}),
        ("value_contains", {"value_substring": "SEA"}, {"populated"}),
        ("value_equals", {"value": ""}, {"empty"}),
    ],
)
def test_annotation_operators_distinguish_missing_and_empty_values(
    client,
    operator: str,
    arguments: dict[str, Any],
    expected_labels: set[str],
) -> None:
    saved = {
        label: _put(client, file_path=f"{label}.yaml", annotations=annotations)
        for label, annotations in {
            "populated": {"team": "research"},
            "empty": {"team": ""},
            "missing": {},
        }.items()
    }
    query = _filter(operator, "team", **arguments)
    positive = _search(client, filter_query=query)
    negative = _search(client, filter_query={"and": [{"not": query["and"][0]}]})

    expected_ids = {saved[label]["id"] for label in expected_labels}
    assert set(_ids(positive)) == expected_ids
    assert positive["total_count"] == len(expected_ids)
    assert set(_ids(negative)) == {item["id"] for item in saved.values()} - expected_ids
    assert negative["total_count"] == len(saved) - len(expected_ids)


@pytest.mark.parametrize(
    "operator, arguments",
    [
        ("key_exists", {}),
        ("value_equals", {"value": "me"}),
        ("value_in", {"values": ["me"]}),
        ("value_contains", {"value_substring": "me"}),
    ],
)
@pytest.mark.parametrize(
    "key",
    [
        "annotation/team",
        "example.com/team.name",
        "team name",
        "équipe",
        "team[0]",
        r"team\name",
        'team"name',
        'x"."secret',
        "team\nname",
    ],
)
def test_annotation_keys_and_me_values_are_literal(
    client, key: str, operator: str, arguments: dict[str, Any]
) -> None:
    saved = _put(client, file_path="literal.yaml", annotations={key: "me"})
    other = _put(
        client,
        file_path="other.yaml",
        annotations={key: ALICE, "team": "me"},
    )
    missing = _put(client, file_path="missing.yaml", annotations={"team": "me"})

    query = _filter(operator, key, **arguments)
    result = _search(client, filter_query=query)
    negative = _search(client, filter_query={"and": [{"not": query["and"][0]}]})

    expected_ids = {saved["id"]}
    if operator == "key_exists":
        expected_ids.add(other["id"])
    assert set(_ids(result)) == expected_ids
    assert result["total_count"] == len(expected_ids)
    assert (
        set(_ids(negative)) == {saved["id"], other["id"], missing["id"]} - expected_ids
    )
    assert negative["total_count"] == 3 - len(expected_ids)


@pytest.mark.parametrize("operator", ["value_equals", "value_in", "value_contains"])
def test_annotation_values_are_not_truncated(client, operator: str) -> None:
    prefix = "x" * 300
    value = prefix + "selected"
    saved = _put(client, file_path="long.yaml", annotations={"description": value})
    _put(
        client,
        file_path="other.yaml",
        annotations={"description": prefix + "other"},
    )
    arguments = {
        "value_equals": {"value": value},
        "value_in": {"values": [value]},
        "value_contains": {"value_substring": "selected"},
    }[operator]

    result = _search(
        client,
        filter_query=_filter(operator, "description", **arguments),
    )

    assert _ids(result) == [saved["id"]]


@pytest.mark.parametrize("field", ["created_at", "updated_at"])
@pytest.mark.parametrize(
    "bounds, expected_indices",
    [
        (
            {
                "start_time": "2026-01-01T02:00:00+02:00",
                "end_time": "2026-01-02T02:00:00+02:00",
            },
            {1, 2},
        ),
        ({"start_time": "2026-01-01T00:00:00Z"}, {1, 2, 3}),
        ({"end_time": "2026-01-02T00:00:00Z"}, {0, 1, 2}),
    ],
)
def test_date_ranges_use_utc_and_inclusive_start_exclusive_end(
    client,
    db_engine: sqlalchemy.Engine,
    field: str,
    bounds: dict[str, str],
    expected_indices: set[int],
) -> None:
    start = datetime.datetime(2026, 1, 1)
    times = [
        start - datetime.timedelta(microseconds=1),
        start,
        start + datetime.timedelta(hours=12),
        start + datetime.timedelta(days=1),
    ]
    saved = [_put(client, file_path=f"date-{index}.yaml") for index in range(4)]
    _set_times(
        db_engine,
        {pipeline["id"]: timestamp for pipeline, timestamp in zip(saved, times)},
        field=field,
    )

    result = _search(
        client,
        filter_query=_filter("time_range", f"system/pipeline.date.{field}", **bounds),
    )

    assert set(_ids(result)) == {saved[index]["id"] for index in expected_indices}
    assert result["total_count"] == len(expected_indices)


def test_pagination_orders_timestamp_ties_without_gaps_or_duplicate_rows(
    client, db_engine: sqlalchemy.Engine
) -> None:
    saved = [_put(client, file_path=f"page-{index}.yaml") for index in range(6)]
    same_time = datetime.datetime(2026, 1, 1)
    _set_times(db_engine, {item["id"]: same_time for item in saved})
    expected = sorted((item["id"] for item in saved), reverse=True)

    first = _search(client, page_size=2)
    second = _search(client, page_size=2, page_token=first["next_page_token"])
    third = _search(client, page_size=2, page_token=second["next_page_token"])

    assert _ids(first) + _ids(second) + _ids(third) == expected
    assert all(page["total_count"] == 6 for page in (first, second, third))
    assert first["next_page_token"] is not None
    assert second["next_page_token"] is not None
    # An exactly full final page must not advertise a phantom empty next page.
    assert third["next_page_token"] is None


@pytest.mark.parametrize("sort_field", ["name", "updated_at"])
@pytest.mark.parametrize("sort_direction", ["asc", "desc"])
def test_sorting_paginates_the_complete_filtered_set_with_ties_and_missing_names(
    client,
    db_engine: sqlalchemy.Engine,
    sort_field: str,
    sort_direction: str,
) -> None:
    definitions = [
        ("keep/one.yaml", "lima"),
        ("keep/two.yaml", "alpha"),
        ("keep/three.yaml", "ALPHA"),
        ("keep/four.yaml", "Alpha"),
        ("keep/Zulu.yaml", None),
        ("keep/Delta.yaml", ""),
    ]
    saved = [
        _put(client, file_path=file_path, name=name) for file_path, name in definitions
    ]
    # Writes normalize empty names to absent metadata. Also cover older rows
    # containing an explicit empty name in the selected version's metadata.
    with orm.Session(db_engine) as session:
        empty_version = session.get(
            db_models.UserPipelineVersion,
            (saved[-1]["id"], saved[-1]["version"]),
        )
        assert empty_version is not None
        empty_version.extra_data = {"pipeline_name": ""}
        session.commit()
    _put(client, file_path="keep/other-owner.yaml", user_id=BOB, name="alpha")
    _put(client, file_path="excluded.yaml", name="alpha")
    start = datetime.datetime(2026, 1, 1)
    timestamps = {
        item["id"]: start + datetime.timedelta(minutes=offset)
        for item, offset in zip(saved, [2, 0, 1, 0, 2, 0])
    }
    _set_times(db_engine, timestamps)
    # Full paths place the unnamed Zulu row before lima; a basename fallback
    # would incorrectly place it after lima.
    names = dict(
        zip(
            (item["id"] for item in saved),
            [
                "lima",
                "alpha",
                "alpha",
                "alpha",
                "keep/zulu.yaml",
                "keep/delta.yaml",
            ],
        )
    )
    values = names if sort_field == "name" else timestamps
    expected = sorted(
        (item["id"] for item in saved),
        key=lambda pipeline_id: (values[pipeline_id], pipeline_id),
        reverse=sort_direction == "desc",
    )
    query = {
        "and": [
            _equals("system/pipeline.user_id", "me"),
            {
                "value_contains": {
                    "key": "system/pipeline.file_path",
                    "value_substring": "keep/",
                }
            },
        ]
    }
    first = _search(
        client,
        filter_query=query,
        page_size=2,
        sort_field=sort_field,
        sort_direction=sort_direction,
    )
    second = _search(client, page_size=2, page_token=first["next_page_token"])
    third = _search(client, page_size=2, page_token=second["next_page_token"])

    assert _ids(first) + _ids(second) + _ids(third) == expected
    assert all(page["total_count"] == 6 for page in (first, second, third))
    assert all(len(page["pipelines"]) == 2 for page in (first, second, third))
    assert first["next_page_token"] is not None
    assert second["next_page_token"] is not None
    assert third["next_page_token"] is None
    if sort_field == "updated_at" and sort_direction == "desc":
        assert _search(client, filter_query=query, page_size=2) == first


@pytest.mark.parametrize("method", ["GET", "POST"])
@pytest.mark.parametrize("sort_field", ["name", "updated_at"])
@pytest.mark.parametrize("sort_direction", ["asc", "desc"])
def test_cursor_inherits_each_omitted_sort_parameter_and_rejects_conflicts(
    client, method: str, sort_field: str, sort_direction: str
) -> None:
    for index, name in enumerate(["bravo", "alpha", "charlie"]):
        _put(client, file_path=f"sorting-{index}.yaml", name=name)
    sorting = {"sort_field": sort_field, "sort_direction": sort_direction}
    expected = _ids(_search(client, method=method, **sorting))
    first = _search(client, method=method, page_size=1, **sorting)
    token = first["next_page_token"]
    assert token is not None
    cursor = json.loads(base64.urlsafe_b64decode(token))
    assert cursor["sort_field"] == sort_field
    assert cursor["sort_direction"] == sort_direction
    assert _ids(first) == expected[:1]
    if sort_direction == "desc":
        assert (
            _search(client, method=method, page_size=1, sort_field=sort_field) == first
        )
    if sort_field == "updated_at":
        assert (
            _search(
                client,
                method=method,
                page_size=1,
                sort_direction=sort_direction,
            )
            == first
        )

    for explicit_sort in (
        {},
        {"sort_field": sort_field},
        {"sort_direction": sort_direction},
        sorting,
    ):
        remaining = _search(
            client,
            method=method,
            page_size=2,
            page_token=token,
            **explicit_sort,
        )
        assert _ids(remaining) == expected[1:]
        assert remaining["total_count"] == 3
        assert remaining["next_page_token"] is None

    for conflicting_sort in (
        {"sort_field": "updated_at" if sort_field == "name" else "name"},
        {"sort_direction": "desc" if sort_direction == "asc" else "asc"},
    ):
        response = _search_response(
            client,
            method=method,
            headers={"x-user": ALICE},
            page_token=token,
            **conflicting_sort,
        )
        assert response.status_code == 422


def test_name_cursor_keeps_its_sort_value_when_the_last_result_is_renamed(
    client,
) -> None:
    saved = [
        _put(client, file_path=f"rename-{index}.yaml", name=name)
        for index, name in enumerate(["alpha", "Bravo", "charlie", "delta"])
    ]
    first = _search(client, page_size=2, sort_field="name", sort_direction="asc")
    token = first["next_page_token"]
    assert token is not None
    cursor = json.loads(base64.urlsafe_b64decode(token))
    assert cursor["sort_name"] == "bravo"
    _put(client, file_path=saved[1]["file_path"], name="aardvark")

    last = _search(client, page_size=2, page_token=token)

    assert _ids(first) == [item["id"] for item in saved[:2]]
    assert _ids(last) == [item["id"] for item in saved[2:]]
    assert last["total_count"] == 4
    assert last["next_page_token"] is None


@pytest.mark.parametrize("initial_method", ["GET", "POST"])
@pytest.mark.parametrize("versioning_mode", ["full", "disabled"])
@pytest.mark.parametrize("boundary_change", ["unchanged", "renamed", "deleted"])
def test_name_sort_continues_over_http_after_a_large_pipeline_name(
    client, initial_method: str, versioning_mode: str, boundary_change: str
) -> None:
    # The names differ only beyond the shared prefix: truncating the cursor
    # would repeat earlier results, and looking up its row would lose the
    # original boundary after a rename or deletion.
    prefix = "A" * 100_000
    saved = [
        _put(
            client,
            file_path=f"large-name-{suffix}.yaml",
            name=prefix + suffix,
            versioning_mode=versioning_mode,
        )
        for suffix in ("a", "b", "c")
    ]
    first = _search(
        client,
        method=initial_method,
        page_size=2,
        sort_field="name",
        sort_direction="asc",
    )
    token = first["next_page_token"]
    assert token is not None
    boundary = saved[1]
    if boundary_change == "renamed":
        _put(
            client,
            file_path=boundary["file_path"],
            name="A",
            versioning_mode=versioning_mode,
        )
    elif boundary_change == "deleted":
        deleted = client.delete(
            "/api/users/me/pipelines",
            headers={"x-user": ALICE},
            params={"file_path": boundary["file_path"]},
        )
        assert deleted.status_code == 204

    last = _search(client, method="POST", page_size=2, page_token=token)

    assert _ids(first) == [item["id"] for item in saved[:2]]
    assert _ids(last) == [saved[2]["id"]]
    assert first["total_count"] == 3
    assert last["total_count"] == (2 if boundary_change == "deleted" else 3)
    assert last["next_page_token"] is None


@pytest.mark.parametrize("method", ["GET", "POST"])
def test_default_page_size_is_25_and_page_size_can_change_during_pagination(
    client, method: str
) -> None:
    saved_ids = {
        _put(client, file_path=f"default-{index}.yaml")["id"] for index in range(26)
    }

    first = _search(client, method=method)
    last = _search(
        client,
        method=method,
        page_size=100,
        page_token=first["next_page_token"],
    )

    assert len(first["pipelines"]) == 25
    assert first["next_page_token"] is not None
    assert len(last["pipelines"]) == 1
    assert last["next_page_token"] is None
    assert first["total_count"] == last["total_count"] == 26
    assert set(_ids(first) + _ids(last)) == saved_ids


def test_insert_before_cursor_does_not_repeat_an_earlier_page(
    client, db_engine: sqlalchemy.Engine
) -> None:
    saved = [_put(client, file_path=f"existing-{index}.yaml") for index in range(4)]
    start = datetime.datetime(2026, 1, 1)
    _set_times(
        db_engine,
        {
            item["id"]: start + datetime.timedelta(minutes=index)
            for index, item in enumerate(saved)
        },
    )
    first = _search(client, page_size=2)
    inserted = _put(client, file_path="inserted.yaml")
    _set_times(db_engine, {inserted["id"]: start + datetime.timedelta(days=1)})

    last = _search(client, page_size=2, page_token=first["next_page_token"])

    assert _ids(first) + _ids(last) == [item["id"] for item in reversed(saved)]
    assert last["total_count"] == 5
    assert last["next_page_token"] is None


@pytest.mark.parametrize("method", ["GET", "POST"])
def test_cursor_carries_filter_and_rejects_different_filter_or_caller(
    client, method: str
) -> None:
    matching_ids = {
        _put(client, file_path=f"mine-{index}.yaml")["id"] for index in range(3)
    }
    _put(client, file_path="other.yaml", user_id=BOB)
    _put(client, file_path="different-team.yaml", annotations={"team": "other"})
    _put(
        client,
        file_path="archived.yaml",
        annotations={"team": "research", "status": "archived"},
    )
    query = {
        "and": [
            _equals("system/pipeline.user_id", "me"),
            {
                "or": [
                    _equals("team", "research"),
                    _equals("team", "operations"),
                ]
            },
            {"not": _equals("status", "archived")},
        ]
    }
    first = _search(
        client,
        method=method,
        filter_query=query,
        page_size=1,
        sort_field="name",
        sort_direction="asc",
    )
    token = first["next_page_token"]
    assert token is not None

    # The next request need not resubmit filters or use the original page size.
    remaining = _search(client, method=method, page_token=token, page_size=2)
    repeated_filter = _search(
        client, method=method, filter_query=query, page_token=token, page_size=2
    )
    conflicting_filter = _search_response(
        client,
        method=method,
        headers={"x-user": ALICE},
        page_token=token,
        filter_query=json.dumps({"and": [_equals("system/pipeline.user_id", BOB)]}),
    )
    different_caller = _search_response(
        client, method=method, headers={"x-user": BOB}, page_token=token
    )

    assert first["total_count"] == remaining["total_count"] == 3
    assert set(_ids(first) + _ids(remaining)) == matching_ids
    assert remaining["next_page_token"] is None
    assert repeated_filter == remaining
    assert conflicting_filter.status_code == 422
    assert different_caller.status_code == 422


def test_maximum_length_filter_continues_without_datetime_expansion(
    client,
) -> None:
    saved_ids = {
        _put(client, file_path=f"maximum-filter-{index}.yaml")["id"]
        for index in range(2)
    }
    query = {
        "or": [
            {"value_equals": {"value": "", "key": "system/pipeline.name"}},
            {
                "time_range": {
                    "start_time": 0,
                    "key": "system/pipeline.date.created_at",
                }
            },
        ]
    }
    compact = json.dumps(query, separators=(",", ":"))
    query["or"][0]["value_equals"]["value"] = "x" * (16_384 - len(compact))
    compact = json.dumps(query, separators=(",", ":"))
    assert len(compact) == 16_384
    # Serializing the parsed epoch timestamp to an ISO string would make this
    # otherwise valid filter too long to parse again from the next-page token.
    first_response = client.get(
        SEARCH_PATH,
        headers={"x-user": ALICE},
        params={"filter_query": compact, "page_size": 1},
    )
    assert first_response.status_code == 200, first_response.text
    first = first_response.json()
    token = first["next_page_token"]
    assert token is not None

    last = _search(client, page_token=token, page_size=1)
    reordered = json.dumps(query, separators=(",", ":"), sort_keys=True)
    assert reordered != compact
    repeated_response = client.get(
        SEARCH_PATH,
        headers={"x-user": ALICE},
        params={"filter_query": reordered, "page_token": token, "page_size": 1},
    )

    assert first["total_count"] == last["total_count"] == 2
    assert len(first["pipelines"]) == len(last["pipelines"]) == 1
    assert set(_ids(first) + _ids(last)) == saved_ids
    assert last["next_page_token"] is None
    assert repeated_response.status_code == 200, repeated_response.text
    assert repeated_response.json() == last


@pytest.mark.parametrize("method", ["GET", "POST"])
@pytest.mark.parametrize("page_size", [0, -1, 101, "invalid", 1.5])
def test_search_rejects_invalid_page_size(client, method: str, page_size) -> None:
    response = _search_response(client, method=method, page_size=page_size)

    assert response.status_code == 422


@pytest.mark.parametrize("method", ["GET", "POST"])
@pytest.mark.parametrize(
    "sorting",
    [
        {"sort_field": "pipeline_name"},
        {"sort_field": "created_at"},
        {"sort_field": ""},
        {"sort_direction": "ASC"},
        {"sort_direction": "descending"},
        {"sort_direction": ""},
    ],
)
def test_search_rejects_invalid_sorting(
    client, method: str, sorting: dict[str, str]
) -> None:
    response = _search_response(client, method=method, **sorting)

    assert response.status_code == 422


@pytest.mark.parametrize("method", ["GET", "POST"])
@pytest.mark.parametrize(
    "page_token",
    [
        "!not-base64!",
        "bad-token",
        "2026-01-01T00:00:00Z~00000000-0000-0000-0000-000000000000",
        base64.urlsafe_b64encode(b"null").decode(),
        base64.urlsafe_b64encode(b"[]").decode(),
        base64.urlsafe_b64encode(b"{}").decode(),
    ],
)
def test_search_rejects_malformed_page_tokens(
    client, method: str, page_token: str
) -> None:
    response = _search_response(client, method=method, page_token=page_token)

    assert response.status_code == 422


@pytest.mark.parametrize(
    "sort_field, removed_field, changes",
    [
        ("updated_at", "sort_field", {}),
        ("updated_at", "sort_direction", {}),
        ("name", "sort_name", {}),
        ("name", None, {"sort_name": None}),
        ("updated_at", None, {"sort_name": "alpha"}),
        ("updated_at", None, {"sort_field": "created_at"}),
        ("updated_at", None, {"sort_direction": "ascending"}),
    ],
)
def test_search_rejects_malformed_cursor_sorting(
    client,
    sort_field: str,
    removed_field: str | None,
    changes: dict[str, Any],
) -> None:
    _put(client, file_path="cursor-first.yaml", name="alpha")
    _put(client, file_path="cursor-second.yaml", name="bravo")
    first = _search(client, page_size=1, sort_field=sort_field)
    cursor = json.loads(base64.urlsafe_b64decode(first["next_page_token"]))
    if removed_field is not None:
        del cursor[removed_field]
    cursor.update(changes)
    token = base64.urlsafe_b64encode(json.dumps(cursor).encode()).decode()

    response = client.get(
        SEARCH_PATH,
        headers={"x-user": ALICE},
        params={"page_token": token},
    )

    assert response.status_code == 422


@pytest.mark.parametrize(
    "updated_at",
    [
        "2026-01-01T00:00:00",
        "0001-01-01T00:00:00+12:00",
        "9999-12-31T23:00:00-12:00",
    ],
)
def test_cursor_rejects_missing_timezone_and_utc_overflow(
    client, updated_at: str
) -> None:
    _put(client, file_path="cursor-first.yaml")
    _put(client, file_path="cursor-second.yaml")
    first = _search(client, page_size=1)
    cursor = json.loads(base64.urlsafe_b64decode(first["next_page_token"]))
    cursor["updated_at"] = updated_at
    token = base64.urlsafe_b64encode(json.dumps(cursor).encode()).decode()

    response = client.get(
        SEARCH_PATH,
        headers={"x-user": ALICE},
        params={"page_token": token},
    )

    assert response.status_code == 422


@pytest.mark.parametrize("method", ["GET", "POST"])
@pytest.mark.parametrize(
    "filter_query",
    [
        "{",
        json.dumps({"and": []}),
        json.dumps(_filter("time_range", "team", start_time="2026-01-01T00:00:00Z")),
        json.dumps(_filter("value_in", "team", values=[""])),
        json.dumps(_filter("value_contains", "team", value_substring="")),
        json.dumps(_filter("value_equals", "system/pipeline_run.user_id", value=ALICE)),
        json.dumps(_filter("value_equals", "system/pipeline.unknown", value="value")),
        json.dumps(
            _filter(
                "value_equals",
                "system/pipeline.date.created_at",
                value="2026-01-01",
            )
        ),
        json.dumps(_filter("key_exists", "system/pipeline.date.updated_at")),
        json.dumps(
            _filter(
                "value_contains",
                "system/pipeline.user_id",
                value_substring="alice",
            )
        ),
        json.dumps(
            _filter(
                "time_range",
                "system/pipeline.name",
                start_time="2026-01-01T00:00:00Z",
            )
        ),
        json.dumps(_filter("value_equals", "system/pipeline.name", value="x" * 17000)),
    ],
)
def test_search_rejects_invalid_filters(client, method: str, filter_query: str) -> None:
    response = _search_response(client, method=method, filter_query=filter_query)

    assert response.status_code == 422


@pytest.mark.parametrize(
    "body",
    [
        None,
        [],
        {"unexpected": True},
        {"filter_query": {}},
        {"page_token": []},
        {"page_size": None},
    ],
)
def test_post_search_rejects_invalid_request_bodies(client, body) -> None:
    response = client.post(SEARCH_PATH, json=body)

    assert response.status_code == 422


@pytest.mark.parametrize("method", ["GET", "POST"])
@pytest.mark.parametrize(
    "headers, expected_status",
    [({"x-user": ""}, 401), ({"x-user": ALICE, "x-read": "false"}, 403)],
)
def test_search_requires_authenticated_read_permission(
    client, method: str, headers: dict[str, str], expected_status: int
) -> None:
    _put(client, file_path="private-read-check.yaml")

    response = _search_response(client, method=method, headers=headers)

    assert response.status_code == expected_status


@pytest.mark.parametrize("method", ["GET", "POST"])
def test_search_does_not_require_write_permission(client, method: str) -> None:
    saved = _put(client, file_path="read-only.yaml", user_id=BOB)

    response = _search_response(
        client,
        method=method,
        headers={"x-user": ALICE, "x-write": "false"},
    )

    assert response.status_code == 200
    assert _ids(response.json()) == [saved["id"]]
