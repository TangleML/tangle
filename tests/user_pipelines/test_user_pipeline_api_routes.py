import pytest
import sqlalchemy
from cloud_pipelines_backend.user_pipelines import (
    db_models,
    pipeline_run_annotations,
)
from sqlalchemy import orm

from tests.user_pipelines.conftest import DEFAULT_USER, pipeline_task

PIPELINE_PATH = "projects/example/pipeline.yaml"
_RESERVED_ANNOTATION_KEYS = (
    "system/pipeline_run.created_by",
    *sorted(pipeline_run_annotations.PROVENANCE_ANNOTATIONS),
)


def _put(
    client,
    *,
    name: str,
    annotations: dict[str, str] | None = None,
    file_path: str = PIPELINE_PATH,
    versioning_mode: str = "full",
):
    return client.put(
        "/api/users/me/pipelines",
        params={"file_path": file_path},
        json={
            "root_pipeline_task": pipeline_task(name=name),
            "pipeline_run_annotations": annotations,
            "versioning_mode": versioning_mode,
        },
    )


def _patch_mode(client, pipeline_id: str, versioning_mode: str):
    return client.patch(
        f"/api/users/me/pipelines/{pipeline_id}/properties",
        json={"versioning_mode": versioning_mode},
    )


def test_update_creates_new_version(client, db_engine: sqlalchemy.Engine) -> None:
    first = _put(client, name="first")
    second = _put(client, name="second")

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["id"] == second.json()["id"]
    assert first.json()["version"] != second.json()["version"]
    assert second.json()["updated"] is True
    assert second.json()["reused_version"] is False

    with orm.Session(db_engine) as session:
        assert (
            session.scalar(
                sqlalchemy.select(sqlalchemy.func.count(db_models.UserPipeline.id))
            )
            == 1
        )
        assert (
            session.scalar(
                sqlalchemy.select(
                    sqlalchemy.func.count(db_models.UserPipelineVersion.version_key)
                )
            )
            == 2
        )


def test_update_reuses_historical_version(client, db_engine: sqlalchemy.Engine) -> None:
    first = _put(client, name="first").json()
    _put(client, name="second")

    restored_response = _put(client, name="first")

    assert restored_response.status_code == 200
    restored = restored_response.json()
    assert restored["id"] == first["id"]
    assert restored["version"] == first["version"]
    assert restored["reused_version"] is True
    assert restored["updated"] is True
    assert "restoring an existing version" in restored["message"]

    with orm.Session(db_engine) as session:
        assert (
            session.scalar(
                sqlalchemy.select(
                    sqlalchemy.func.count(db_models.UserPipelineVersion.version_key)
                )
            )
            == 2
        )


def test_full_disabled_full_transition_preserves_and_reuses_immutable_history(
    client,
    db_engine: sqlalchemy.Engine,
) -> None:
    historical = _put(client, name="historical").json()
    latest = _put(client, name="latest").json()
    disabled = _put(
        client,
        name="mutable",
        versioning_mode="disabled",
    ).json()

    assert disabled["versioning_mode"] == "disabled"
    assert disabled["version"] not in {historical["version"], latest["version"]}
    history_while_disabled = client.get(
        f"/api/pipelines/{historical['id']}/versions"
    ).json()
    assert {item["version"] for item in history_while_disabled["versions"]} == {
        historical["version"],
        latest["version"],
    }
    assert all(not item["is_current"] for item in history_while_disabled["versions"])

    restored = _put(client, name="historical", versioning_mode="full").json()

    assert restored["versioning_mode"] == "full"
    assert restored["version"] == historical["version"]
    assert restored["updated"] is True
    assert restored["reused_version"] is True
    history_while_full = client.get(
        f"/api/pipelines/{historical['id']}/versions"
    ).json()
    assert history_while_full["total_count"] == 2
    assert [
        item["version"] for item in history_while_full["versions"] if item["is_current"]
    ] == [historical["version"]]

    with orm.Session(db_engine) as session:
        rows = session.scalars(sqlalchemy.select(db_models.UserPipelineVersion)).all()
        assert len(rows) == 2
        assert db_models.CURRENT_VERSION_KEY not in {row.version_key for row in rows}


def test_disabled_to_full_with_new_content_replaces_sentinel_atomically(
    client,
    db_engine: sqlalchemy.Engine,
) -> None:
    disabled = client.put(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
        json={"root_pipeline_task": pipeline_task(name="mutable")},
    ).json()

    enabled = _put(client, name="new-immutable", versioning_mode="full").json()

    assert enabled["id"] == disabled["id"]
    assert enabled["versioning_mode"] == "full"
    assert enabled["version"] != disabled["version"]
    assert enabled["updated"] is True
    assert enabled["reused_version"] is False
    with orm.Session(db_engine) as session:
        pipeline = session.get(db_models.UserPipeline, disabled["id"])
        assert pipeline is not None
        assert pipeline.current_version_key == enabled["version"]
        rows = session.scalars(sqlalchemy.select(db_models.UserPipelineVersion)).all()
        assert [row.version_key for row in rows] == [enabled["version"]]
        assert rows[0].content_digest == enabled["version"]


def test_patch_mode_transitions_preserve_content_and_reuse_history(
    client,
    db_engine: sqlalchemy.Engine,
) -> None:
    created = client.put(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
        json={
            "root_pipeline_task": pipeline_task(name="patch-mode"),
            "pipeline_run_annotations": {"source": "preserved"},
        },
    ).json()

    enabled = _patch_mode(client, created["id"], "full").json()
    disabled = _patch_mode(client, created["id"], "disabled").json()
    restored = _patch_mode(client, created["id"], "full").json()

    for response in (enabled, disabled, restored):
        assert response["id"] == created["id"]
        assert response["version"] == created["version"]
        assert response["current_version"] == created["current_version"]
        assert response["root_pipeline_task"] == created["root_pipeline_task"]
        assert response["pipeline_run_annotations"] == {"source": "preserved"}
        assert response["updated"] is True
    assert enabled["versioning_mode"] == "full"
    assert enabled["reused_version"] is False
    assert disabled["versioning_mode"] == "disabled"
    assert disabled["reused_version"] is False
    assert restored["versioning_mode"] == "full"
    assert restored["reused_version"] is True

    history = client.get(f"/api/pipelines/{created['id']}/versions").json()
    assert history["total_count"] == 1
    assert history["versions"][0]["version"] == created["version"]
    with orm.Session(db_engine) as session:
        pipeline = session.get(db_models.UserPipeline, created["id"])
        assert pipeline is not None
        assert pipeline.current_version_key == created["version"]
        rows = session.scalars(sqlalchemy.select(db_models.UserPipelineVersion)).all()
        assert [row.version_key for row in rows] == [created["version"]]


def test_every_stored_version_keys_itself_by_its_digest(
    client,
    db_engine: sqlalchemy.Engine,
) -> None:
    """The invariant the pin resolver rests on: an immutable row's key is its content digest.

    `resolve_pinnable_version_key` looks a pin up by `version_key` -- the primary key -- rather
    than by the `content_digest` column, which makes it a single-row seek instead of a walk
    over a pipeline's whole history. A writer that ever keyed a row by anything else would make
    that pin silently unresolvable, so every write path is exercised here.
    """
    first = _put(client, name="digest-keyed-v1", versioning_mode="full").json()
    second = _put(client, name="digest-keyed-v2", versioning_mode="full").json()
    _patch_mode(client, first["id"], "disabled")
    _patch_mode(client, first["id"], "full")
    client.put(
        "/api/users/me/pipelines",
        params={"file_path": "projects/example/mutable.yaml"},
        json={"root_pipeline_task": pipeline_task(name="mutable-head")},
    )

    with orm.Session(db_engine) as session:
        rows = session.scalars(sqlalchemy.select(db_models.UserPipelineVersion)).all()

    immutable = [
        row for row in rows if row.version_key != db_models.CURRENT_VERSION_KEY
    ]
    assert {row.version_key for row in immutable} >= {
        first["version"],
        second["version"],
    }
    assert [row for row in immutable if row.version_key != row.content_digest] == []
    # The sentinel is the one row allowed to differ, and the resolver excludes it by name.
    assert [
        row.version_key
        for row in rows
        if row.version_key == db_models.CURRENT_VERSION_KEY
    ]


def test_patch_same_mode_is_noop(client) -> None:
    created = _put(client, name="patch-noop", versioning_mode="full").json()

    response = _patch_mode(client, created["id"], "full")

    assert response.status_code == 200
    assert response.json()["updated"] is False
    assert response.json()["reused_version"] is True
    assert response.json()["updated_at"] == created["updated_at"]
    assert response.json()["version"] == created["version"]
    assert response.json()["root_pipeline_task"] == created["root_pipeline_task"]


def test_omitted_mode_preserves_existing_full_mode(client) -> None:
    enabled = _put(client, name="enabled", versioning_mode="full").json()
    updated = client.put(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
        json={"root_pipeline_task": pipeline_task(name="still-full")},
    ).json()

    assert enabled["versioning_mode"] == "full"
    assert updated["versioning_mode"] == "full"
    assert updated["version"] != enabled["version"]
    history = client.get(f"/api/pipelines/{enabled['id']}/versions").json()
    assert history["total_count"] == 2


def test_current_version_update_is_successful_noop(client) -> None:
    created = _put(client, name="same").json()

    response = _put(client, name="same")

    assert response.status_code == 200
    assert response.json()["version"] == created["version"]
    assert response.json()["updated_at"] == created["updated_at"]
    assert response.json()["updated"] is False
    assert response.json()["reused_version"] is True
    assert "no changes were made" in response.json()["message"]


def test_default_disabled_mode_mutates_bounded_sentinel_and_hides_it_from_history(
    client,
    db_engine: sqlalchemy.Engine,
) -> None:
    first = client.put(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
        json={"root_pipeline_task": pipeline_task(name="disabled-first")},
    ).json()
    second = client.put(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
        json={"root_pipeline_task": pipeline_task(name="disabled-second")},
    ).json()
    no_op = client.put(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
        json={"root_pipeline_task": pipeline_task(name="disabled-second")},
    ).json()

    assert first["versioning_mode"] == "disabled"
    assert second["versioning_mode"] == "disabled"
    assert first["version"] != second["version"]
    assert second["version"] == second["current_version"]
    assert len(second["version"]) == db_models.DIGEST_LENGTH
    assert no_op["version"] == second["version"]
    assert no_op["updated"] is False
    assert no_op["reused_version"] is True

    history = client.get(f"/api/pipelines/{first['id']}/versions").json()
    sentinel_read = client.get(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH, "version": "current"},
    )
    assert history["versioning_mode"] == "disabled"
    assert history["current_version"] == second["version"]
    assert history["versions"] == []
    assert history["total_count"] == 0
    assert sentinel_read.status_code == 404

    with orm.Session(db_engine) as session:
        pipeline = session.get(db_models.UserPipeline, first["id"])
        assert pipeline is not None
        assert pipeline.versioning_mode is db_models.PipelineVersioningMode.DISABLED
        assert pipeline.current_version_key == db_models.CURRENT_VERSION_KEY
        rows = session.scalars(sqlalchemy.select(db_models.UserPipelineVersion)).all()
        assert len(rows) == 1
        assert rows[0].version_key == db_models.CURRENT_VERSION_KEY
        assert rows[0].content_digest == second["version"]


def test_patch_requires_owned_active_pipeline(
    client,
    other_user_client,
) -> None:
    created = _put(client, name="patch-owner").json()

    foreign = _patch_mode(other_user_client, created["id"], "disabled")
    missing = _patch_mode(
        client,
        "00000000-0000-0000-0000-000000000000",
        "disabled",
    )
    denied = client.patch(
        f"/api/users/me/pipelines/{created['id']}/properties",
        headers={"x-write": "false"},
        json={"versioning_mode": "disabled"},
    )
    unchanged = client.get(f"/api/pipelines/{created['id']}").json()
    client.delete(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
    )
    deleted = _patch_mode(client, created["id"], "disabled")

    assert foreign.status_code == 404
    assert missing.status_code == 404
    assert denied.status_code == 403
    assert unchanged["versioning_mode"] == "full"
    assert deleted.status_code == 404


def test_patch_rejects_empty_body_and_invalid_mode(client) -> None:
    created = _put(client, name="patch-validation").json()

    empty = client.patch(
        f"/api/users/me/pipelines/{created['id']}/properties",
        json={},
    )
    invalid = _patch_mode(client, created["id"], "invalid")
    unsupported_property = client.patch(
        f"/api/users/me/pipelines/{created['id']}/properties",
        json={"versioning_mode": "full", "extra_data": {}},
    )

    assert empty.status_code == 422
    assert invalid.status_code == 422
    assert unsupported_property.status_code == 422


def test_invalid_versioning_mode_is_rejected(client) -> None:
    response = client.put(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
        json={
            "root_pipeline_task": pipeline_task(name="invalid-mode"),
            "versioning_mode": "invalid",
        },
    )

    assert response.status_code == 422


@pytest.mark.parametrize("versioning_mode", ["disabled", "full"])
@pytest.mark.parametrize("reserved_key", _RESERVED_ANNOTATION_KEYS)
def test_write_rejects_reserved_annotations_before_create(
    client,
    db_engine: sqlalchemy.Engine,
    versioning_mode: str,
    reserved_key: str,
) -> None:
    response = client.put(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
        json={
            "root_pipeline_task": pipeline_task(name="reserved-create"),
            "pipeline_run_annotations": {reserved_key: "spoofed"},
            "versioning_mode": versioning_mode,
        },
    )

    assert response.status_code == 422
    assert "reserved" in response.json()["detail"]
    with orm.Session(db_engine) as session:
        assert (
            session.scalar(
                sqlalchemy.select(sqlalchemy.func.count(db_models.UserPipeline.id))
            )
            == 0
        )
        assert (
            session.scalar(
                sqlalchemy.select(
                    sqlalchemy.func.count(db_models.UserPipelineVersion.version_key)
                )
            )
            == 0
        )


def test_write_rejects_the_project_annotation(
    client, db_engine: sqlalchemy.Engine
) -> None:
    """Refused for a different reason than the keys above, hence the separate case.

    Those are refused because the server derives them. A project is refused because it is not
    a property of a pipeline -- the same pipeline submitted from two projects is one pipeline --
    and because storing it would fold it into the content digest, making "the same pipeline in a
    different project" a different *version* of it.

    Asserted explicitly because `_RESERVED_ANNOTATION_KEYS` above is derived from
    `PROVENANCE_ANNOTATIONS`, which the project key deliberately stays out of so it remains
    legal on the run-submit routes. The parametrised list therefore stopped covering it -- and
    could not cover it anyway, since the key is a prefix and a family of keys rather than one
    string to put in a list.
    """
    response = _put(
        client,
        name="parked-project",
        annotations={
            pipeline_run_annotations.project_run_key(
                "some-project-id"
            ): pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE
        },
    )

    assert response.status_code == 422
    assert "belongs to a run submission" in response.json()["detail"]
    with orm.Session(db_engine) as session:
        assert (
            session.scalar(
                sqlalchemy.select(sqlalchemy.func.count(db_models.UserPipeline.id))
            )
            == 0
        )


def test_write_rejects_a_project_annotation_spelled_in_capitals(
    client, db_engine: sqlalchemy.Engine
) -> None:
    """The same refusal, reached by the spelling rather than by the prefix match.

    Worth its own case because the two are enforced by different rules and only one of them
    used to fire. Matching is byte-exact, so capitals made the key above an ordinary annotation:
    the refusal never ran, and the id went into `calculate_pipeline_digest` -- part of the
    pipeline's identity -- while production's `utf8mb4_0900_ai_ci` matched it against the
    canonical key and filed every run from that pipeline into the project, undetachably.

    The message differs from the case above on purpose. "Spell it exactly" is the action; being
    told a project belongs on a run submission is confusing when you thought you had not sent
    one.
    """
    response = _put(
        client,
        name="parked-project-upper",
        annotations={
            pipeline_run_annotations.project_run_key(
                "some-project-id"
            ).upper(): pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE
        },
    )

    assert response.status_code == 422
    assert "only in case" in response.json()["detail"]
    with orm.Session(db_engine) as session:
        assert (
            session.scalar(
                sqlalchemy.select(sqlalchemy.func.count(db_models.UserPipeline.id))
            )
            == 0
        )


@pytest.mark.parametrize("versioning_mode", ["disabled", "full"])
@pytest.mark.parametrize(
    "reserved_key",
    [
        "system/pipeline_run.created_by",
        pipeline_run_annotations.VERSION_ANNOTATION,
    ],
)
def test_write_rejects_reserved_annotation_update_without_mutation(
    client,
    db_engine: sqlalchemy.Engine,
    versioning_mode: str,
    reserved_key: str,
) -> None:
    created = _put(
        client,
        name="before-invalid-update",
        annotations={"valid": "before"},
        versioning_mode=versioning_mode,
    ).json()

    response = client.put(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
        json={
            "root_pipeline_task": pipeline_task(name="invalid-update"),
            "pipeline_run_annotations": {reserved_key: "spoofed"},
            "versioning_mode": (
                "full" if versioning_mode == "disabled" else "disabled"
            ),
        },
    )

    assert response.status_code == 422
    current = client.get(f"/api/pipelines/{created['id']}").json()
    assert current["version"] == created["version"]
    assert current["versioning_mode"] == versioning_mode
    assert current["updated_at"] == created["updated_at"]
    assert current["pipeline_run_annotations"] == {"valid": "before"}
    assert current["pipeline_name"] == "before-invalid-update"
    with orm.Session(db_engine) as session:
        assert (
            session.scalar(
                sqlalchemy.select(
                    sqlalchemy.func.count(db_models.UserPipelineVersion.version_key)
                )
            )
            == 1
        )


@pytest.mark.parametrize("versioning_mode", ["disabled", "full"])
@pytest.mark.parametrize(
    "reserved_key",
    [
        "system/pipeline_run.created_by",
        pipeline_run_annotations.OWNER_ANNOTATION,
    ],
)
def test_write_rejects_reserved_annotation_reactivation_without_mutation(
    client,
    db_engine: sqlalchemy.Engine,
    versioning_mode: str,
    reserved_key: str,
) -> None:
    created = _put(
        client,
        name="deleted-valid",
        annotations={"valid": "retained"},
        versioning_mode=versioning_mode,
    ).json()
    delete_response = client.delete(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
    )
    assert delete_response.status_code == 204
    with orm.Session(db_engine) as session:
        pipeline = session.get(db_models.UserPipeline, created["id"])
        assert pipeline is not None
        before = (
            pipeline.deleted_at,
            pipeline.updated_at,
            pipeline.current_version_key,
        )

    response = client.put(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
        json={
            "root_pipeline_task": pipeline_task(name="invalid-reactivation"),
            "pipeline_run_annotations": {reserved_key: "spoofed"},
        },
    )

    assert response.status_code == 422
    assert client.get(f"/api/pipelines/{created['id']}").status_code == 404
    with orm.Session(db_engine) as session:
        pipeline = session.get(db_models.UserPipeline, created["id"])
        assert pipeline is not None
        assert (
            pipeline.deleted_at,
            pipeline.updated_at,
            pipeline.current_version_key,
        ) == before
        assert (
            session.scalar(
                sqlalchemy.select(
                    sqlalchemy.func.count(db_models.UserPipelineVersion.version_key)
                )
            )
            == 1
        )
        version = session.get(
            db_models.UserPipelineVersion,
            (pipeline.id, pipeline.current_version_key),
        )
        assert version is not None
        assert version.pipeline_run_annotations == {"valid": "retained"}


def test_omitted_null_and_empty_annotations_share_canonical_version(
    client,
    db_engine: sqlalchemy.Engine,
) -> None:
    omitted = client.put(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
        json={"root_pipeline_task": pipeline_task(name="annotations")},
    )
    explicit_null = client.put(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
        json={
            "root_pipeline_task": pipeline_task(name="annotations"),
            "pipeline_run_annotations": None,
        },
    )
    empty = client.put(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
        json={
            "root_pipeline_task": pipeline_task(name="annotations"),
            "pipeline_run_annotations": {},
        },
    )

    for response in (omitted, explicit_null, empty):
        assert response.status_code == 200
        assert response.json()["pipeline_run_annotations"] == {}
        assert response.json()["version"] == omitted.json()["version"]
    for response in (explicit_null, empty):
        assert response.json()["updated"] is False
        assert response.json()["reused_version"] is True
    with orm.Session(db_engine) as session:
        assert (
            session.scalar(
                sqlalchemy.select(
                    sqlalchemy.func.count(db_models.UserPipelineVersion.version_key)
                )
            )
            == 1
        )
        stored_annotations = session.scalar(
            sqlalchemy.select(db_models.UserPipelineVersion.pipeline_run_annotations)
        )
        assert stored_annotations == {}


def test_valid_annotations_are_digest_significant_and_reuse_full_versions(
    client,
    db_engine: sqlalchemy.Engine,
) -> None:
    first = _put(
        client,
        name="annotation-identity",
        annotations={"stage": "first"},
        versioning_mode="full",
    ).json()
    second = _put(
        client,
        name="annotation-identity",
        annotations={"stage": "second"},
        versioning_mode="full",
    ).json()
    restored = _put(
        client,
        name="annotation-identity",
        annotations={"stage": "first"},
        versioning_mode="full",
    ).json()

    assert second["version"] != first["version"]
    assert second["updated"] is True
    assert second["reused_version"] is False
    assert restored["version"] == first["version"]
    assert restored["pipeline_run_annotations"] == {"stage": "first"}
    assert restored["updated"] is True
    assert restored["reused_version"] is True
    with orm.Session(db_engine) as session:
        versions = session.scalars(
            sqlalchemy.select(db_models.UserPipelineVersion).order_by(
                db_models.UserPipelineVersion.content_digest
            )
        ).all()
        assert len(versions) == 2
        assert {version.pipeline_run_annotations["stage"] for version in versions} == {
            "first",
            "second",
        }


def test_explicit_defaults_and_omitted_defaults_share_canonical_version(
    client,
) -> None:
    task_with_explicit_default = pipeline_task(name="canonical")
    task_with_explicit_default["arguments"] = None
    created = client.put(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
        json={
            "root_pipeline_task": task_with_explicit_default,
            "versioning_mode": "full",
        },
    ).json()

    resubmitted = _put(client, name="canonical")

    assert resubmitted.status_code == 200
    assert resubmitted.json()["version"] == created["version"]
    assert resubmitted.json()["updated"] is False


def test_write_rejects_unknown_task_spec_fields(
    client, db_engine: sqlalchemy.Engine
) -> None:
    task_with_unknown_field = pipeline_task(name="unsupported")
    task_with_unknown_field["componentRef"]["spec"]["futureField"] = {"enabled": False}

    response = client.put(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
        json={"root_pipeline_task": task_with_unknown_field},
    )

    assert response.status_code == 422
    assert "futureField" in response.json()["detail"]
    assert "Unexpected keyword argument" in response.json()["detail"]
    with orm.Session(db_engine) as session:
        assert (
            session.scalar(
                sqlalchemy.select(sqlalchemy.func.count(db_models.UserPipeline.id))
            )
            == 0
        )


def test_get_defaults_to_current_and_accepts_version_filter(client) -> None:
    historical = _put(client, name="historical").json()
    current = _put(client, name="current").json()

    default_response = client.get(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
    )
    historical_response = client.get(
        "/api/users/me/pipelines",
        params={
            "file_path": PIPELINE_PATH,
            "version": historical["version"],
        },
    )

    assert default_response.status_code == 200
    assert default_response.json()["version"] == current["version"]
    assert default_response.json()["pipeline_name"] == "current"
    assert historical_response.status_code == 200
    assert historical_response.json()["pipeline_name"] == "historical"
    assert historical_response.json()["current_version"] == current["version"]


def test_disabled_current_digest_round_trips_but_reserved_key_does_not(
    client,
) -> None:
    created = client.put(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
        json={"root_pipeline_task": pipeline_task(name="disabled-alias")},
    ).json()

    digest_response = client.get(
        "/api/users/me/pipelines",
        params={
            "file_path": PIPELINE_PATH,
            "version": created["current_version"],
        },
    )
    reserved_response = client.get(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH, "version": "current"},
    )

    assert created["versioning_mode"] == "disabled"
    assert digest_response.status_code == 200
    assert digest_response.json()["version"] == created["version"]
    assert digest_response.json()["root_pipeline_task"] == created["root_pipeline_task"]
    assert reserved_response.status_code == 404


def test_changed_disabled_content_only_aliases_new_current_digest(
    client,
) -> None:
    first = client.put(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
        json={"root_pipeline_task": pipeline_task(name="disabled-old")},
    ).json()
    current = client.put(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
        json={"root_pipeline_task": pipeline_task(name="disabled-new")},
    ).json()

    old_response = client.get(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH, "version": first["version"]},
    )
    current_response = client.get(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH, "version": current["version"]},
    )

    assert first["version"] != current["version"]
    assert old_response.status_code == 404
    assert current_response.status_code == 200
    assert current_response.json()["pipeline_name"] == "disabled-new"


def test_full_to_disabled_duplicate_digest_aliases_pointed_current(
    client,
    db_engine: sqlalchemy.Engine,
) -> None:
    full = _put(client, name="duplicate-digest", versioning_mode="full").json()
    disabled = _patch_mode(client, full["id"], "disabled").json()

    alias_response = client.get(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH, "version": full["version"]},
    )
    history = client.get(f"/api/pipelines/{full['id']}/versions").json()

    assert alias_response.status_code == 200
    assert alias_response.json()["versioning_mode"] == "disabled"
    assert alias_response.json()["version_created_at"] == disabled["version_created_at"]
    assert history["total_count"] == 1
    assert history["versions"][0]["version"] == full["version"]
    with orm.Session(db_engine) as session:
        rows = session.scalars(sqlalchemy.select(db_models.UserPipelineVersion)).all()
        assert {row.version_key for row in rows} == {
            db_models.CURRENT_VERSION_KEY,
            full["version"],
        }
        assert {row.content_digest for row in rows} == {full["version"]}


def test_list_pipelines_and_versions(client) -> None:
    first = _put(client, name="first").json()
    second = _put(client, name="second").json()
    _put(client, name="another", annotations={"key": "value"})

    pipeline_list = client.get("/api/users/me/pipelines/all")
    version_list = client.get(
        f"/api/pipelines/{first['id']}/versions",
    )

    assert pipeline_list.status_code == 200
    assert len(pipeline_list.json()["pipelines"]) == 1
    assert pipeline_list.json()["pipelines"][0]["pipeline_name"] == "another"
    assert pipeline_list.json()["pipelines"][0]["versioning_mode"] == "full"

    assert version_list.status_code == 200
    assert version_list.json()["versioning_mode"] == "full"
    versions = version_list.json()["versions"]
    assert len(versions) == 3
    assert {version["version"] for version in versions} >= {
        first["version"],
        second["version"],
    }
    assert sum(version["is_current"] for version in versions) == 1


def test_version_history_is_paginated_without_gaps_or_duplicates(
    client,
) -> None:
    created = [_put(client, name=f"version-{index}").json() for index in range(12)]

    versions_path = f"/api/pipelines/{created[0]['id']}/versions"
    first = client.get(
        versions_path,
        params={"page_size": 5},
    ).json()
    second = client.get(
        versions_path,
        params={
            "page_size": 5,
            "page_token": first["next_page_token"],
        },
    ).json()
    third = client.get(
        versions_path,
        params={
            "page_size": 5,
            "page_token": second["next_page_token"],
        },
    ).json()

    pages = [first, second, third]
    listed_versions = [
        version["version"] for page in pages for version in page["versions"]
    ]
    assert [len(page["versions"]) for page in pages] == [5, 5, 2]
    assert all(page["total_count"] == 12 for page in pages)
    assert first["next_page_token"] is not None
    assert second["next_page_token"] is not None
    assert third["next_page_token"] is None
    assert len(listed_versions) == len(set(listed_versions)) == 12
    assert set(listed_versions) == {item["version"] for item in created}
    assert (
        sum(version["is_current"] for page in pages for version in page["versions"])
        == 1
    )


def test_version_history_rejects_invalid_pagination(client) -> None:
    created = _put(client, name="version-pagination-validation").json()
    versions_path = f"/api/pipelines/{created['id']}/versions"

    invalid_size = client.get(
        versions_path,
        params={"page_size": 0},
    )
    invalid_token = client.get(
        versions_path,
        params={"page_token": "bad-token"},
    )

    assert invalid_size.status_code == 422
    assert invalid_token.status_code == 422
    assert "Unrecognized page_token format" in invalid_token.json()["detail"]


def test_self_list_uses_default_pagination(client) -> None:
    for index in range(12):
        _put(
            client,
            name=f"pipeline-{index}",
            file_path=f"pipelines/{index:02}.yaml",
        )

    response = client.get("/api/users/me/pipelines/all")

    assert response.status_code == 200
    data = response.json()
    assert len(data["pipelines"]) == 10
    assert data["total_count"] == 12
    assert data["next_page_token"] is not None


def test_self_list_paginates_without_gaps_or_duplicates(client) -> None:
    created_ids = {
        _put(
            client,
            name=f"pipeline-{index}",
            file_path=f"pages/{index}.yaml",
        ).json()["id"]
        for index in range(5)
    }

    first = client.get(
        "/api/users/me/pipelines/all",
        params={"page_size": 2},
    ).json()
    second = client.get(
        "/api/users/me/pipelines/all",
        params={"page_size": 2, "page_token": first["next_page_token"]},
    ).json()
    third = client.get(
        "/api/users/me/pipelines/all",
        params={"page_size": 2, "page_token": second["next_page_token"]},
    ).json()

    pages = [first, second, third]
    listed_ids = [pipeline["id"] for page in pages for pipeline in page["pipelines"]]
    assert len(first["pipelines"]) == 2
    assert len(second["pipelines"]) == 2
    assert len(third["pipelines"]) == 1
    assert first["next_page_token"] is not None
    assert second["next_page_token"] is not None
    assert third["next_page_token"] is None
    assert all(page["total_count"] == 5 for page in pages)
    assert len(listed_ids) == len(set(listed_ids)) == 5
    assert set(listed_ids) == created_ids


def test_self_list_exact_page_boundary_has_no_next_token(client) -> None:
    for index in range(2):
        _put(
            client,
            name=f"boundary-{index}",
            file_path=f"boundaries/{index}.yaml",
        )

    response = client.get(
        "/api/users/me/pipelines/all",
        params={"page_size": 2},
    )

    assert response.status_code == 200
    assert len(response.json()["pipelines"]) == 2
    assert response.json()["total_count"] == 2
    assert response.json()["next_page_token"] is None


def test_self_list_file_path_filter_uses_literal_prefix_semantics(
    client,
) -> None:
    matching_paths = {"teams/search/a.yaml", "teams/search/b.yaml"}
    for path in [*matching_paths, "teams/searchish/c.yaml", "other/d.yaml"]:
        _put(client, name=path, file_path=path)
    _put(client, name="literal-percent", file_path="literal%/one.yaml")
    _put(client, name="not-literal-percent", file_path="literalX/two.yaml")

    response = client.get(
        "/api/users/me/pipelines/all",
        params={"file_path": "teams/search/"},
    )
    literal_response = client.get(
        "/api/users/me/pipelines/all",
        params={"file_path": "literal%/"},
    )

    assert response.status_code == 200
    assert response.json()["total_count"] == 2
    assert {
        item["file_path"] for item in response.json()["pipelines"]
    } == matching_paths
    assert literal_response.status_code == 200
    assert literal_response.json()["total_count"] == 1
    assert literal_response.json()["pipelines"][0]["file_path"] == "literal%/one.yaml"


def test_self_list_prefix_filter_paginates_without_gaps_or_duplicates(
    client,
) -> None:
    matching_ids = {
        _put(
            client,
            name=f"filtered-{index}",
            file_path=f"filtered/{index}.yaml",
        ).json()["id"]
        for index in range(5)
    }
    _put(client, name="outside", file_path="outside/pipeline.yaml")

    page_token = None
    listed_ids: list[str] = []
    while True:
        params = {"file_path": "filtered/", "page_size": 2}
        if page_token is not None:
            params["page_token"] = page_token
        page = client.get("/api/users/me/pipelines/all", params=params).json()
        assert page["total_count"] == 5
        listed_ids.extend(pipeline["id"] for pipeline in page["pipelines"])
        page_token = page["next_page_token"]
        if page_token is None:
            break

    assert len(listed_ids) == len(set(listed_ids)) == 5
    assert set(listed_ids) == matching_ids


def test_self_list_isolates_authenticated_owners(client, other_user_client) -> None:
    own = _put(client, name="own", file_path="owners/own.yaml").json()
    other = _put(
        other_user_client,
        name="other",
        file_path="owners/other.yaml",
    ).json()

    own_list = client.get("/api/users/me/pipelines/all").json()
    other_list = other_user_client.get("/api/users/me/pipelines/all").json()

    assert own_list["total_count"] == 1
    assert [pipeline["id"] for pipeline in own_list["pipelines"]] == [own["id"]]
    assert other_list["total_count"] == 1
    assert [pipeline["id"] for pipeline in other_list["pipelines"]] == [other["id"]]


def test_self_list_rejects_invalid_pagination_inputs(client) -> None:
    zero = client.get(
        "/api/users/me/pipelines/all",
        params={"page_size": 0},
    )
    too_large = client.get(
        "/api/users/me/pipelines/all",
        params={"page_size": 101},
    )
    bad_token = client.get(
        "/api/users/me/pipelines/all",
        params={"page_token": "bad-token"},
    )

    assert zero.status_code == 422
    assert too_large.status_code == 422
    assert bad_token.status_code == 422
    assert "Unrecognized page_token format" in bad_token.json()["detail"]


def test_global_listing_route_is_not_available(client) -> None:
    response = client.get(
        "/api/pipelines/all",
        params={"user_id": DEFAULT_USER},
    )

    # With the UUID path route, "all" is treated as an invalid UUID rather than
    # as a global listing endpoint.
    assert response.status_code == 422


def test_self_query_uuid_filters_are_removed(client) -> None:
    created = _put(client, name="self-query-id-removed").json()

    response = client.get(
        "/api/users/me/pipelines",
        params={"pipeline_id": created["id"]},
    )
    versions_response = client.get(
        "/api/users/me/pipelines/versions",
        params={"pipeline_id": created["id"]},
    )

    assert response.status_code == 422
    assert versions_response.status_code == 404


def test_global_lookup_by_uuid_supports_cross_user_reads(
    client,
    other_user_client,
) -> None:
    historical = _put(client, name="shared-by-id-historical").json()
    current = _put(client, name="shared-by-id-current").json()

    response = other_user_client.get(
        f"/api/pipelines/{current['id']}",
        params={"version": historical["version"]},
    )
    versions_response = other_user_client.get(
        f"/api/pipelines/{current['id']}/versions",
        params={"page_size": 1},
    )
    next_versions_response = other_user_client.get(
        f"/api/pipelines/{current['id']}/versions",
        params={
            "page_size": 1,
            "page_token": versions_response.json()["next_page_token"],
        },
    )

    assert response.status_code == 200
    assert response.json()["id"] == current["id"]
    assert response.json()["version"] == historical["version"]
    assert response.json()["current_version"] == current["version"]
    assert response.json()["user_id"] == DEFAULT_USER
    assert versions_response.status_code == 200
    assert versions_response.json()["id"] == current["id"]
    assert versions_response.json()["total_count"] == 2
    assert len(versions_response.json()["versions"]) == 1
    assert versions_response.json()["next_page_token"] is not None
    assert next_versions_response.status_code == 200
    assert len(next_versions_response.json()["versions"]) == 1
    assert next_versions_response.json()["next_page_token"] is None


def test_disabled_current_digest_round_trips_for_cross_user_reader(
    client,
    other_user_client,
) -> None:
    created = client.put(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
        json={"root_pipeline_task": pipeline_task(name="shared-disabled")},
    ).json()

    response = other_user_client.get(
        f"/api/pipelines/{created['id']}",
        params={"version": created["current_version"]},
    )

    assert response.status_code == 200
    assert response.json()["id"] == created["id"]
    assert response.json()["user_id"] == DEFAULT_USER
    assert response.json()["version"] == created["current_version"]
    assert response.json()["pipeline_name"] == "shared-disabled"


def test_global_lookup_by_user_and_file_path_returns_uuid(
    client,
    other_user_client,
) -> None:
    created = _put(client, name="shared-by-key").json()

    response = other_user_client.get(
        "/api/pipelines",
        params={"user_id": DEFAULT_USER, "file_path": PIPELINE_PATH},
    )
    assert response.status_code == 200
    assert response.json()["id"] == created["id"]


def test_self_file_path_lookup_does_not_escape_self_scope(
    client,
    other_user_client,
) -> None:
    _put(client, name="private-to-self-route")

    response = other_user_client.get(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
    )

    assert response.status_code == 404


def test_property_patch_is_documented_in_openapi(client) -> None:
    openapi = client.get("/openapi.json").json()
    operation = openapi["paths"]["/api/users/me/pipelines/{pipeline_id}/properties"][
        "patch"
    ]

    assert operation["requestBody"]["required"] is True
    request_ref = operation["requestBody"]["content"]["application/json"]["schema"][
        "$ref"
    ]
    request_schema = openapi["components"]["schemas"][request_ref.rsplit("/", 1)[-1]]
    assert request_schema["required"] == ["versioning_mode"]
    mode_ref = request_schema["properties"]["versioning_mode"]["$ref"]
    mode_schema = openapi["components"]["schemas"][mode_ref.rsplit("/", 1)[-1]]
    assert mode_schema["enum"] == ["disabled", "full"]
    pipeline_id_parameter = next(
        parameter
        for parameter in operation["parameters"]
        if parameter["name"] == "pipeline_id"
    )
    assert pipeline_id_parameter["schema"]["format"] == "uuid"
    response_ref = operation["responses"]["200"]["content"]["application/json"][
        "schema"
    ]["$ref"]
    assert response_ref.endswith("/PipelineWriteResponse")


def test_public_read_policy_is_documented_in_openapi(client) -> None:
    paths = client.get("/openapi.json").json()["paths"]

    assert (
        "public to authenticated readers"
        in paths["/api/pipelines"]["get"]["description"]
    )
    assert (
        "public to authenticated readers"
        in paths["/api/pipelines/{pipeline_id}"]["get"]["description"]
    )
    assert "not a secret store" in paths["/api/pipelines"]["get"]["description"]

    alternate_parameters = {
        parameter["name"] for parameter in paths["/api/pipelines"]["get"]["parameters"]
    }
    self_parameters = {
        parameter["name"]
        for parameter in paths["/api/users/me/pipelines"]["get"]["parameters"]
    }
    assert alternate_parameters == {"user_id", "file_path", "version"}
    assert self_parameters == {"file_path", "version"}
    assert "/api/users/me/pipelines/versions" not in paths
    assert "/api/pipelines/versions" not in paths


def test_reads_require_authentication_and_read_permission(client) -> None:
    unauthenticated = client.get(
        "/api/users/me/pipelines/all",
        headers={"x-user": ""},
    )
    forbidden = client.get(
        "/api/users/me/pipelines/all",
        headers={"x-read": "false"},
    )

    assert unauthenticated.status_code == 401
    assert forbidden.status_code == 403


def test_write_requires_write_permission(client) -> None:
    response = client.put(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
        headers={"x-write": "false"},
        json={"root_pipeline_task": pipeline_task(name="forbidden")},
    )

    assert response.status_code == 403


def test_lookup_identifier_combinations_are_validated(client) -> None:
    created = _put(client, name="identifier-validation").json()

    self_missing = client.get("/api/users/me/pipelines")
    removed_self_uuid_query = client.get(
        "/api/users/me/pipelines",
        params={"pipeline_id": created["id"]},
    )
    global_missing = client.get("/api/pipelines")
    global_partial_key = client.get(
        "/api/pipelines",
        params={"user_id": DEFAULT_USER},
    )
    removed_uuid_query = client.get(
        "/api/pipelines",
        params={"pipeline_id": created["id"]},
    )
    invalid_uuid_path = client.get("/api/pipelines/not-a-uuid")

    assert self_missing.status_code == 422
    assert removed_self_uuid_query.status_code == 422
    assert global_missing.status_code == 422
    assert global_partial_key.status_code == 422
    assert removed_uuid_query.status_code == 422
    assert invalid_uuid_path.status_code == 422


def test_dynamic_user_path_routes_are_removed(client) -> None:
    response = client.get(
        f"/api/users/{DEFAULT_USER}/pipelines",
        params={"file_path": PIPELINE_PATH},
    )

    assert response.status_code == 404


def test_lookup_failures_and_validation(client) -> None:
    missing = client.get(
        "/api/users/me/pipelines",
        params={"file_path": "missing.yaml"},
    )
    invalid = client.put(
        "/api/users/me/pipelines",
        params={"file_path": "  "},
        json={"root_pipeline_task": pipeline_task(name="invalid")},
    )

    _put(client, name="exists")
    missing_version = client.get(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH, "version": "0" * 64},
    )

    assert missing.status_code == 404
    assert invalid.status_code == 422
    assert missing_version.status_code == 404


def test_delete_soft_deletes_idempotently_and_hides_pipeline(
    client,
    db_engine: sqlalchemy.Engine,
) -> None:
    created = _put(client, name="first").json()
    current = _put(client, name="second").json()

    first_delete = client.delete(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
    )
    with orm.Session(db_engine) as session:
        deleted_pipeline = session.get(db_models.UserPipeline, created["id"])
        assert deleted_pipeline is not None
        original_deleted_at = deleted_pipeline.deleted_at
        original_updated_at = deleted_pipeline.updated_at
        assert original_deleted_at is not None
        assert original_updated_at == original_deleted_at
        assert deleted_pipeline.current_version_key == current["version"]
        assert (
            session.scalar(
                sqlalchemy.select(
                    sqlalchemy.func.count(db_models.UserPipelineVersion.version_key)
                )
            )
            == 2
        )

    second_delete = client.delete(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
    )

    assert first_delete.status_code == 204
    assert second_delete.status_code == 204
    with orm.Session(db_engine) as session:
        deleted_pipeline = session.get(db_models.UserPipeline, created["id"])
        assert deleted_pipeline is not None
        assert deleted_pipeline.deleted_at == original_deleted_at
        assert deleted_pipeline.updated_at == original_updated_at
        assert (
            session.scalar(
                sqlalchemy.select(sqlalchemy.func.count(db_models.UserPipeline.id))
            )
            == 1
        )
        assert (
            session.scalar(
                sqlalchemy.select(
                    sqlalchemy.func.count(db_models.UserPipelineVersion.version_key)
                )
            )
            == 2
        )

    self_read = client.get(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
    )
    public_uuid_read = client.get(f"/api/pipelines/{created['id']}")
    public_key_read = client.get(
        "/api/pipelines",
        params={"user_id": DEFAULT_USER, "file_path": PIPELINE_PATH},
    )
    version_history = client.get(f"/api/pipelines/{created['id']}/versions")
    pipeline_list = client.get("/api/users/me/pipelines/all")

    assert self_read.status_code == 404
    assert public_uuid_read.status_code == 404
    assert public_key_read.status_code == 404
    assert version_history.status_code == 404
    assert pipeline_list.status_code == 200
    assert pipeline_list.json()["pipelines"] == []
    assert pipeline_list.json()["total_count"] == 0


def test_delete_never_existing_pipeline_returns_not_found(client) -> None:
    response = client.delete(
        "/api/users/me/pipelines",
        params={"file_path": "never-existed.yaml"},
    )

    assert response.status_code == 404


def test_put_reactivates_disabled_pipeline_and_preserves_mode_when_omitted(
    client,
    db_engine: sqlalchemy.Engine,
) -> None:
    created = client.put(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
        json={"root_pipeline_task": pipeline_task(name="disabled")},
    ).json()
    client.delete(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
    )

    reactivated = client.put(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
        json={"root_pipeline_task": pipeline_task(name="disabled")},
    ).json()

    assert reactivated["id"] == created["id"]
    assert reactivated["versioning_mode"] == "disabled"
    assert reactivated["updated"] is True
    assert reactivated["reused_version"] is True
    with orm.Session(db_engine) as session:
        pipeline = session.get(db_models.UserPipeline, created["id"])
        assert pipeline is not None
        assert pipeline.current_version_key == db_models.CURRENT_VERSION_KEY
        assert pipeline.deleted_at is None


def test_put_reactivates_current_version_with_same_identity(client) -> None:
    created = _put(client, name="current").json()
    client.delete(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
    )

    reactivated = _put(client, name="current")

    assert reactivated.status_code == 200
    assert reactivated.json()["id"] == created["id"]
    assert reactivated.json()["version"] == created["version"]
    assert reactivated.json()["current_version"] == created["version"]
    assert reactivated.json()["updated"] is True
    assert reactivated.json()["reused_version"] is True
    assert "reactivated" in reactivated.json()["message"]


def test_put_reactivates_historical_version_with_same_identity(
    client,
    db_engine: sqlalchemy.Engine,
) -> None:
    historical = _put(client, name="historical").json()
    _put(client, name="current")
    client.delete(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
    )

    reactivated = _put(client, name="historical")

    assert reactivated.status_code == 200
    assert reactivated.json()["id"] == historical["id"]
    assert reactivated.json()["version"] == historical["version"]
    assert reactivated.json()["current_version"] == historical["version"]
    assert reactivated.json()["updated"] is True
    assert reactivated.json()["reused_version"] is True
    with orm.Session(db_engine) as session:
        pipeline = session.get(db_models.UserPipeline, historical["id"])
        assert pipeline is not None
        assert pipeline.deleted_at is None
        assert (
            session.scalar(
                sqlalchemy.select(
                    sqlalchemy.func.count(db_models.UserPipelineVersion.version_key)
                )
            )
            == 2
        )


def test_put_reactivates_with_new_version_and_same_identity(
    client,
    db_engine: sqlalchemy.Engine,
) -> None:
    created = _put(client, name="before-delete").json()
    client.delete(
        "/api/users/me/pipelines",
        params={"file_path": PIPELINE_PATH},
    )

    reactivated = _put(client, name="after-delete")

    assert reactivated.status_code == 200
    assert reactivated.json()["id"] == created["id"]
    assert reactivated.json()["version"] != created["version"]
    assert reactivated.json()["updated"] is True
    assert reactivated.json()["reused_version"] is False
    with orm.Session(db_engine) as session:
        pipeline = session.get(db_models.UserPipeline, created["id"])
        assert pipeline is not None
        assert pipeline.deleted_at is None
        assert (
            session.scalar(
                sqlalchemy.select(
                    sqlalchemy.func.count(db_models.UserPipelineVersion.version_key)
                )
            )
            == 2
        )
