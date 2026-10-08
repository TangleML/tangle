"""Project resources: CRUD, duplicate protection, entity filtering, and the immutable pointer."""

import fastapi.testclient
import pytest
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend.projects import db_models, errors, services
from cloud_pipelines_backend.user_pipelines import (
    db_models as user_pipeline_db_models,
)
from tests import sql_capture
from tests.projects.conftest import (
    DEFAULT_USER,
    MISSING_WORKSPACE_ID,
    SANDBOX,
    add_pipeline,
    create_project,
    create_resource,
)

PIPELINE_ID = "aaaaaaaaaaaaaaaaaaaa"
OTHER_PIPELINE_ID = "bbbbbbbbbbbbbbbbbbbb"


@pytest.fixture()
def project(client: fastapi.testclient.TestClient) -> dict:
    return create_project(client)


class TestCreateResource:
    def test_a_document_carries_a_payload_and_no_entity_id(
        self,
        client: fastapi.testclient.TestClient,
        project: dict,
    ) -> None:
        resource = create_resource(
            client,
            project_id=project["id"],
            entity="document",
            name="Design notes",
            payload={"format": "markdown", "body": "# hello"},
        )
        assert resource["entity"] == "document"
        assert resource["entity_id"] is None
        assert resource["payload"] == {"format": "markdown", "body": "# hello"}
        assert resource["created_by"] == DEFAULT_USER

    def test_a_pipeline_carries_an_entity_id(
        self, client: fastapi.testclient.TestClient, project: dict
    ) -> None:
        resource = create_resource(
            client,
            project_id=project["id"],
            entity="pipeline",
            entity_id=PIPELINE_ID,
        )
        assert resource["entity_id"] == PIPELINE_ID

    def test_a_reference_may_carry_a_payload_alongside_its_id(
        self,
        client: fastapi.testclient.TestClient,
        project: dict,
    ) -> None:
        """Both columns populated, which the exclusive-or this replaced refused.

        Asserted on `pipeline` rather than `agent_session` because the allowance is a property
        of references, not a concession to one member.
        """
        resource = create_resource(
            client,
            project_id=project["id"],
            entity="pipeline",
            entity_id=PIPELINE_ID,
            payload={"branch": "main", "last_seen_revision": 7},
        )
        assert resource["entity_id"] == PIPELINE_ID
        assert resource["payload"] == {
            "branch": "main",
            "last_seen_revision": 7,
        }

    def test_a_session_is_a_reference_and_requires_its_id(
        self,
        client: fastapi.testclient.TestClient,
        project: dict,
    ) -> None:
        """`agent_session` is a reference: metadata is welcome, but not instead of the id.

        A payload alone would satisfy the CHECK at rest and then sit in the NULL group of the
        unique index, so this 422 is what duplicate protection rests on.
        """
        response = client.post(
            f"/api/projects/{project['id']}/resources/",
            json={"entity": "agent_session", "payload": {"model": "opus"}},
        )
        assert response.status_code == 422, response.text
        assert "entity_id" in response.text

        attached = create_resource(
            client,
            project_id=project["id"],
            entity="agent_session",
            entity_id=OTHER_PIPELINE_ID,
            payload={"model": "opus"},
        )
        assert attached["entity"] == "agent_session"
        assert attached["entity_id"] == OTHER_PIPELINE_ID

    def test_a_session_id_is_opaque_like_every_other_entity_id(
        self,
        client: fastapi.testclient.TestClient,
        project: dict,
    ) -> None:
        """`entity_id` holds ids from several spaces -- a 20-character pipeline id, a Tangent
        instance id, whatever a later entity brings -- so nothing here parses one."""
        instance_id = "0199a1b2c3d4e5f6a7b8"
        resource = create_resource(
            client,
            project_id=project["id"],
            entity="agent_session",
            entity_id=instance_id,
        )
        assert resource["entity_id"] == instance_id

    def test_a_document_still_cannot_name_an_entity_id(
        self,
        client: fastapi.testclient.TestClient,
        project: dict,
    ) -> None:
        """The half of the old rule that survives. A `document` is its own content, so there is
        no record for it to point at and an id on one would key a row against nothing.
        """
        response = client.post(
            f"/api/projects/{project['id']}/resources/",
            json={
                "entity": "document",
                "entity_id": PIPELINE_ID,
                "payload": {"body": "x"},
            },
        )
        assert response.status_code == 422, response.text
        assert "entity_id" in response.text

    def test_the_response_carries_the_project_and_not_a_workspace(
        self,
        client: fastapi.testclient.TestClient,
        project: dict,
    ) -> None:
        """A client wanting the workspace reads it from the project it already has."""
        resource = create_resource(client, project_id=project["id"])
        assert resource["project_id"] == project["id"]
        assert "workspace_id" not in resource
        assert (
            client.get(f"/api/projects/{project['id']}").json()["workspace_id"]
            == SANDBOX
        )

    def test_a_supplied_workspace_id_is_422(
        self, client: fastapi.testclient.TestClient, project: dict
    ) -> None:
        response = client.post(
            f"/api/projects/{project['id']}/resources/",
            json={"entity": "document", "workspace_id": SANDBOX},
        )
        assert response.status_code == 422, response.text

    def test_the_payload_is_stored_verbatim(
        self, client: fastapi.testclient.TestClient, project: dict
    ) -> None:
        """Opaque: no per-entity schema, no discriminator, no content rules. Any object round-trips."""
        payload = {
            "nested": {"deep": [1, 2, {"x": None}]},
            "unicode": "café",
            "": "empty key",
        }
        resource = create_resource(client, project_id=project["id"], payload=payload)
        assert resource["payload"] == payload
        fetched = client.get(
            f"/api/projects/{project['id']}/resources/{resource['id']}"
        ).json()
        assert fetched["payload"] == payload

    def test_a_non_object_payload_is_422(
        self, client: fastapi.testclient.TestClient, project: dict
    ) -> None:
        """ "A JSON object" is the whole of the validation, and it is still validation."""
        for payload in ("a string", 42, ["a", "list"]):
            response = client.post(
                f"/api/projects/{project['id']}/resources/",
                json={"entity": "document", "payload": payload},
            )
            assert response.status_code == 422, f"{payload!r} was accepted"

    def test_an_unknown_entity_is_422(
        self, client: fastapi.testclient.TestClient, project: dict
    ) -> None:
        """The enum does at the edge what the VARCHAR column deliberately does not do at rest."""
        response = client.post(
            f"/api/projects/{project['id']}/resources/",
            json={"entity": "run", "payload": {"body": "x"}},
        )
        assert response.status_code == 422, response.text

    def test_an_entity_differing_only_by_case_is_422(
        self,
        client: fastapi.testclient.TestClient,
        project: dict,
    ) -> None:
        """`entity` is half a unique key, so one legal spelling is a correctness rule: MySQL
        folds case here and SQLite does not."""
        response = client.post(
            f"/api/projects/{project['id']}/resources/",
            json={"entity": "Document"},
        )
        assert response.status_code == 422, response.text

    def test_an_entity_id_is_stored_verbatim(
        self,
        client: fastapi.testclient.TestClient,
        project: dict,
    ) -> None:
        """Whatever the client sent. Nothing here folds or reshapes an id."""
        resource = create_resource(
            client,
            project_id=project["id"],
            entity="pipeline",
            entity_id=PIPELINE_ID.upper(),
        )
        assert resource["entity_id"] == PIPELINE_ID.upper()

    def test_an_unknown_project_is_404(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        response = client.post(
            f"/api/projects/{MISSING_WORKSPACE_ID}/resources/",
            json={"entity": "document", "payload": {"body": "x"}},
        )
        assert response.status_code == 404, response.text

    def test_a_malformed_body_is_422_even_when_the_project_is_missing(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Shape before existence, the order FastAPI already validates a body in: the caller is
        told about the defect they can fix rather than the one behind it."""
        response = client.post(
            f"/api/projects/{MISSING_WORKSPACE_ID}/resources/",
            json={"entity": "document"},
        )
        assert response.status_code == 422, response.text


class TestTheShapeHoldsAtRestToo:
    """`ck_project_resource_reference_or_payload`, asserted on rows rather than on requests.

    The 422s above are the service layer, which a migration or a console session does not pass
    through. What survives under them is deliberately weaker: the real rule is entity-dependent
    -- only a `document` may omit `entity_id` -- and writing that into the DDL would spell an
    entity into a CHECK, which is the migration `entity` is a VARCHAR to avoid. So at rest the
    only refusal is of a row that is neither a reference nor content.

    The gap that leaves is asserted below, not just described: a reference stored without its
    `entity_id` lands in the unique index's NULL group, where every row is distinct, so
    duplicate protection for references rests on `services.validate_entity_shape` alone.
    """

    def test_a_row_with_neither_half_is_refused(self, session: orm.Session) -> None:
        with pytest.raises(sqlalchemy.exc.IntegrityError, match="reference_or_payload"):
            self._insert(session, entity_id=None, payload=None)

    def test_a_row_with_both_halves_is_accepted(self, session: orm.Session) -> None:
        """A reference carrying metadata is two populated columns, and this CHECK no longer
        has an opinion about that."""
        self._insert(session, entity_id=PIPELINE_ID, payload={"branch": "main"})

    def test_an_empty_object_payload_satisfies_it(self, session: orm.Session) -> None:
        """Why the constraint costs the document case nothing."""
        self._insert(session, entity_id=None, payload={})

    def test_the_check_does_not_catch_a_reference_missing_its_id(
        self, session: orm.Session
    ) -> None:
        """Pinning the gap, so nobody reads the CHECK as covering more than it does.

        Unreachable through the API -- `validate_entity_shape` 422s it -- so if the service ever
        stops refusing it, the failure surfaces here rather than as a silent second attach.
        """
        self._insert(session, entity_id=None, payload={"looks_like": "a document"})

    @staticmethod
    def _insert(
        session: orm.Session, *, entity_id: str | None, payload: dict | None
    ) -> None:
        project = db_models.Project(workspace_id=SANDBOX, name="Holder")
        session.add(project)
        session.flush()
        session.add(
            db_models.ProjectResource(
                project_id=project.id,
                entity=db_models.ProjectResourceEntity.DOCUMENT.value,
                entity_id=entity_id,
                payload=payload,
            )
        )
        session.flush()


class TestDuplicateProtection:
    def test_a_repeat_attach_never_reaches_the_insert(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
        project: dict,
    ) -> None:
        """The read in front of the insert, asserted where it can be seen.

        A duplicate INSERT that fails on the unique index still locks the record it collided
        with, so taking that path on every repeat attach is a lock amplifier rather than a
        wasted statement. The refusal is unchanged, so
        only the emitted SQL can tell the two apart: no INSERT, and the answer comes from a
        SELECT instead.

        The constraint is still the arbiter of a true race, which this cannot reach from one
        connection. That case stays covered by the index itself.
        """
        create_resource(
            client,
            project_id=project["id"],
            entity="pipeline",
            entity_id=PIPELINE_ID,
        )

        with sql_capture.capture_sql(db_engine) as statements:
            response = client.post(
                f"/api/projects/{project['id']}/resources/",
                json={"entity": "pipeline", "entity_id": PIPELINE_ID},
            )

        assert response.status_code == 409, response.text
        inserts = [
            statement
            for statement in statements
            if statement.lstrip().upper().startswith("INSERT")
            and "project_resource" in statement
        ]
        assert inserts == [], inserts
        assert sql_capture.selects_from(statements, table="project_resource")

    def test_a_duplicate_document_is_still_allowed(
        self,
        client: fastapi.testclient.TestClient,
        project: dict,
    ) -> None:
        """The read is skipped for documents, and must not start refusing them.

        `entity_id` is NULL on a document and UNIQUE treats NULLs as distinct, so two of them
        have always been legal. A pre-check that matched on NULL would have quietly turned
        that into a 409.
        """
        first = create_resource(
            client,
            project_id=project["id"],
            entity="document",
            payload={"body": "one"},
        )
        response = client.post(
            f"/api/projects/{project['id']}/resources/",
            json={"entity": "document", "payload": {"body": "two"}},
        )
        assert response.status_code == 201, response.text
        assert response.json()["id"] != first["id"]

    def test_the_same_pipeline_cannot_be_attached_twice(
        self,
        client: fastapi.testclient.TestClient,
        project: dict,
    ) -> None:
        create_resource(
            client,
            project_id=project["id"],
            entity="pipeline",
            entity_id=PIPELINE_ID,
        )
        response = client.post(
            f"/api/projects/{project['id']}/resources/",
            json={"entity": "pipeline", "entity_id": PIPELINE_ID},
        )
        assert response.status_code == 409, response.text
        assert PIPELINE_ID in response.json()["detail"]

    def test_the_409_names_the_resource_that_is_in_the_way(
        self,
        client: fastapi.testclient.TestClient,
        project: dict,
    ) -> None:
        """ "Delete the existing resource" has to be actionable.

        The unique index spans hidden rows while `list_resources` filters them out, so the
        blocking row can be one no listing will show -- attach a pipeline, soft-delete it in
        `user_pipelines`, attach it again. Naming the id makes the instruction followable
        through this API in that case too: `DELETE .../resources/{id}` skips `_hides` and takes
        it.
        """
        existing = create_resource(
            client,
            project_id=project["id"],
            entity="pipeline",
            entity_id=PIPELINE_ID,
        )
        response = client.post(
            f"/api/projects/{project['id']}/resources/",
            json={"entity": "pipeline", "entity_id": PIPELINE_ID},
        )
        assert response.status_code == 409, response.text
        assert existing["id"] in response.json()["detail"]

        # The id it handed back is one this API accepts, which is the whole point of naming it.
        detached = client.delete(
            f"/api/projects/{project['id']}/resources/{existing['id']}"
        )
        assert detached.status_code == 204, detached.text
        retried = client.post(
            f"/api/projects/{project['id']}/resources/",
            json={"entity": "pipeline", "entity_id": PIPELINE_ID},
        )
        assert retried.status_code == 201, retried.text

    def test_two_documents_may_be_identical(
        self, client: fastapi.testclient.TestClient, project: dict
    ) -> None:
        """Null `entity_id`, so the unique index does not apply. Two identical documents are two."""
        for _ in range(3):
            create_resource(
                client,
                project_id=project["id"],
                entity="document",
                payload={"body": "same"},
            )
        assert (
            client.get(f"/api/projects/{project['id']}/resources/").json()[
                "total_count"
            ]
            == 3
        )

    def test_the_slot_frees_the_moment_the_resource_is_deleted(
        self,
        client: fastapi.testclient.TestClient,
        project: dict,
    ) -> None:
        """No tombstone, so no revival question.

        A soft-deleted row would hold this slot while invisible to every read, and the next
        attach would have to choose between "duplicate" and "revive" -- the `user_pipelines`
        bug. Here the second attach is simply a new resource with a new id.
        """
        first = create_resource(
            client,
            project_id=project["id"],
            entity="pipeline",
            entity_id=PIPELINE_ID,
        )
        deleted = client.delete(
            f"/api/projects/{project['id']}/resources/{first['id']}"
        )
        assert deleted.status_code == 204
        second = create_resource(
            client,
            project_id=project["id"],
            entity="pipeline",
            entity_id=PIPELINE_ID,
        )
        assert second["id"] != first["id"]
        assert second["created_at"] >= first["created_at"]

    def test_the_same_pipeline_may_live_in_two_projects(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        for _ in range(2):
            other = create_project(client)
            create_resource(
                client,
                project_id=other["id"],
                entity="pipeline",
                entity_id=PIPELINE_ID,
            )


class TestListResources:
    @pytest.fixture()
    def seeded(self, client: fastapi.testclient.TestClient, project: dict) -> dict:
        return {
            "document": create_resource(
                client, project_id=project["id"], entity="document", name="Doc"
            ),
            "pipeline": create_resource(
                client,
                project_id=project["id"],
                entity="pipeline",
                entity_id=PIPELINE_ID,
            ),
            "other_pipeline": create_resource(
                client,
                project_id=project["id"],
                entity="pipeline",
                entity_id=OTHER_PIPELINE_ID,
            ),
        }

    def test_it_lists_them_newest_first(
        self,
        client: fastapi.testclient.TestClient,
        project: dict,
        seeded: dict,
    ) -> None:
        body = client.get(f"/api/projects/{project['id']}/resources/").json()
        assert body["total_count"] == 3
        assert body["resources"][0]["id"] == seeded["other_pipeline"]["id"]

    def test_entity_filters_it(
        self, client: fastapi.testclient.TestClient, project: dict, seeded: dict
    ) -> None:
        body = client.get(
            f"/api/projects/{project['id']}/resources/",
            params={"entity": "document"},
        ).json()
        assert [resource["id"] for resource in body["resources"]] == [
            seeded["document"]["id"]
        ]
        assert body["total_count"] == 1

    def test_entity_is_repeatable(
        self, client: fastapi.testclient.TestClient, project: dict, seeded: dict
    ) -> None:
        body = client.get(
            f"/api/projects/{project['id']}/resources/",
            params=[("entity", "document"), ("entity", "pipeline")],
        ).json()
        assert body["total_count"] == 3

    def test_an_unknown_entity_filter_is_422(
        self, client: fastapi.testclient.TestClient, project: dict
    ) -> None:
        assert (
            client.get(
                f"/api/projects/{project['id']}/resources/",
                params={"entity": "run"},
            ).status_code
            == 422
        )

    def test_it_pages_without_repeating_or_dropping_a_row(
        self,
        client: fastapi.testclient.TestClient,
        project: dict,
        seeded: dict,
    ) -> None:
        seen: list[str] = []
        page_token = None
        for _ in range(10):
            params = {"page_size": 1}
            if page_token:
                params["page_token"] = page_token
            body = client.get(
                f"/api/projects/{project['id']}/resources/", params=params
            ).json()
            seen.extend(resource["id"] for resource in body["resources"])
            page_token = body["next_page_token"]
            if not page_token:
                break
        assert len(seen) == len(set(seen)) == 3

    def test_an_unknown_project_is_404_not_an_empty_page(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Different answers, acted on differently by a client."""
        assert (
            client.get(f"/api/projects/{MISSING_WORKSPACE_ID}/resources/").status_code
            == 404
        )

    def test_a_project_with_nothing_attached_is_an_empty_page(
        self,
        client: fastapi.testclient.TestClient,
        project: dict,
    ) -> None:
        body = client.get(f"/api/projects/{project['id']}/resources/").json()
        assert body == {
            "resources": [],
            "total_count": 0,
            "next_page_token": None,
        }

    def test_an_empty_page_token_is_422_rather_than_page_one(
        self,
        client: fastapi.testclient.TestClient,
        project: dict,
    ) -> None:
        """Same gate as the projects list: `?page_token=` present and empty is a malformed
        token, and answering it as page 1 turns a client that echoes a null `next_page_token`
        as `""` into an endless loop."""
        response = client.get(
            f"/api/projects/{project['id']}/resources/",
            params={"page_token": ""},
        )
        assert response.status_code == 422, response.text

    def test_a_page_token_at_the_edge_of_the_calendar_is_422_not_500(
        self,
        client: fastapi.testclient.TestClient,
        project: dict,
    ) -> None:
        """The same `OverflowError` as the projects list, reached through the other route.

        Both share `_decode_cursor`, so this is here to keep a fix that is made for one
        list from being made only for that one.
        """
        response = client.get(
            f"/api/projects/{project['id']}/resources/",
            params={"page_token": f"0001-01-01T00:00:00+09:00~{PIPELINE_ID}"},
        )
        assert response.status_code == 422, response.text

    def test_an_explicitly_empty_entity_filter_matches_nothing(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
        project: dict,
    ) -> None:
        """Unreachable over HTTP -- FastAPI omits the parameter when no `?entity=` is sent -- so
        this goes through the service, where a computed filter list can legitimately come out
        empty. Read as "no filter", an empty list handed back a full unfiltered page and the
        full `total_count`: the opposite of what the caller asked for, and silently."""
        create_resource(
            client,
            project_id=project["id"],
            entity="document",
            payload={"body": "x"},
        )
        page = services.ProjectService().list_resources(
            session=session,
            project_id=project["id"],
            page_size=10,
            entities=[],
        )
        assert page.rows == []
        assert page.total_count == 0


class TestDeletedPipelinesAreHidden:
    """`user_pipelines` deletes softly, so a `pipeline` resource can outlive what it points at.

    The row is kept and filtered rather than deleted, because `user_pipelines` also *revives* --
    a `PUT` to a deleted path clears `deleted_at`. Deleting the membership would bring the
    pipeline back outside every project it had been in, with nothing telling the user to
    re-attach it.
    """

    def test_a_live_pipeline_is_listed(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        project = create_project(client)
        pipeline_id = add_pipeline(session, file_path="pipelines/live.yaml")
        create_resource(
            client,
            project_id=project["id"],
            entity="pipeline",
            entity_id=pipeline_id,
        )
        listed = client.get(f"/api/projects/{project['id']}/resources/").json()
        assert [resource["entity_id"] for resource in listed["resources"]] == [
            pipeline_id
        ]
        assert listed["total_count"] == 1

    def test_a_deleted_pipeline_is_not(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        project = create_project(client)
        pipeline_id = add_pipeline(
            session, file_path="pipelines/gone.yaml", deleted=True
        )
        create_resource(
            client,
            project_id=project["id"],
            entity="pipeline",
            entity_id=pipeline_id,
        )
        listed = client.get(f"/api/projects/{project['id']}/resources/").json()
        assert listed["resources"] == []
        assert listed["total_count"] == 0

    def test_re_attaching_one_409s_with_an_id_the_api_accepts(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """The case that made naming the blocking row necessary.

        The unique index spans hidden rows, so re-attaching a soft-deleted pipeline collides
        with a resource no listing shows. The 409 carries its id, and `delete_resource` skips
        `_hides` and takes it.
        """
        project = create_project(client)
        pipeline_id = add_pipeline(
            session, file_path="pipelines/hidden.yaml", deleted=True
        )
        hidden = create_resource(
            client,
            project_id=project["id"],
            entity="pipeline",
            entity_id=pipeline_id,
        )
        assert (
            client.get(f"/api/projects/{project['id']}/resources/").json()["resources"]
            == []
        )

        response = client.post(
            f"/api/projects/{project['id']}/resources/",
            json={"entity": "pipeline", "entity_id": pipeline_id},
        )
        assert response.status_code == 409, response.text
        assert hidden["id"] in response.json()["detail"]
        detached = client.delete(
            f"/api/projects/{project['id']}/resources/{hidden['id']}"
        )
        assert detached.status_code == 204, detached.text

    def test_the_row_survives_so_reviving_the_pipeline_brings_it_back(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """The whole reason this is a filter and not a cascade."""
        project = create_project(client)
        pipeline_id = add_pipeline(
            session, file_path="pipelines/revived.yaml", deleted=True
        )
        create_resource(
            client,
            project_id=project["id"],
            entity="pipeline",
            entity_id=pipeline_id,
        )
        assert (
            client.get(f"/api/projects/{project['id']}/resources/").json()[
                "total_count"
            ]
            == 0
        )

        with session.begin():
            pipeline = session.get(user_pipeline_db_models.UserPipeline, pipeline_id)
            assert pipeline is not None
            pipeline.deleted_at = None

        listed = client.get(f"/api/projects/{project['id']}/resources/").json()
        assert [resource["entity_id"] for resource in listed["resources"]] == [
            pipeline_id
        ]

    def test_the_counts_agree_with_the_list(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """A card reading "2 pipelines" above a list showing one is worse than either number
        being wrong on its own."""
        project = create_project(client)
        live = add_pipeline(session, file_path="pipelines/live.yaml")
        gone = add_pipeline(session, file_path="pipelines/gone.yaml", deleted=True)
        create_resource(
            client, project_id=project["id"], entity="pipeline", entity_id=live
        )
        create_resource(
            client, project_id=project["id"], entity="pipeline", entity_id=gone
        )
        create_resource(client, project_id=project["id"], entity="document")

        detail = client.get(f"/api/projects/{project['id']}").json()
        assert detail["resource_counts"] == {"pipeline": 1, "document": 1}

        summary = client.get("/api/projects/").json()["projects"][0]
        assert summary["resource_counts"] == detail["resource_counts"]

        listed = client.get(f"/api/projects/{project['id']}/resources/").json()
        assert listed["total_count"] == len(listed["resources"]) == 2

    def test_an_entity_filter_still_hides_it(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """`?entity=pipeline` is the one query that would most obviously expose the gap."""
        project = create_project(client)
        gone = add_pipeline(session, file_path="pipelines/gone.yaml", deleted=True)
        create_resource(
            client, project_id=project["id"], entity="pipeline", entity_id=gone
        )
        body = client.get(
            f"/api/projects/{project['id']}/resources/?entity=pipeline"
        ).json()
        assert body == {
            "resources": [],
            "total_count": 0,
            "next_page_token": None,
        }

    def test_only_pipelines_are_affected(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """A session sharing an id with a deleted pipeline is untouched: the filter names the
        entity as well as the id, because `entity_id` is opaque and two entities may
        legitimately use the same one."""
        project = create_project(client)
        gone = add_pipeline(session, file_path="pipelines/gone.yaml", deleted=True)
        create_resource(
            client, project_id=project["id"], entity="pipeline", entity_id=gone
        )
        create_resource(
            client,
            project_id=project["id"],
            entity="agent_session",
            entity_id=gone,
        )
        listed = client.get(f"/api/projects/{project['id']}/resources/").json()
        assert [resource["entity"] for resource in listed["resources"]] == [
            "agent_session"
        ]

    def test_hiding_costs_no_second_statement(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """A correlated `NOT EXISTS`, so the listing reads `pipeline` inside its own statement.

        The page and the count are one statement each however many pipelines the project
        references, which a probe that collected ids first could not promise.
        """
        project = create_project(client)
        for index in range(5):
            pipeline_id = add_pipeline(
                session,
                file_path=f"pipelines/p{index}.yaml",
                deleted=index % 2 == 0,
            )
            create_resource(
                client,
                project_id=project["id"],
                entity="pipeline",
                entity_id=pipeline_id,
            )

        with sql_capture.capture_sql(db_engine) as statements:
            listed = client.get(f"/api/projects/{project['id']}/resources/").json()
        assert listed["total_count"] == len(listed["resources"]) == 2
        assert len(sql_capture.selects_from(statements, table="pipeline")) == 2

    def test_an_entity_filter_that_admits_no_pipeline_drops_the_term(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """`?entity=document` cannot return a pipeline, so the term could only ever hide
        nothing -- a subquery per row of a page that contains none of them."""
        project = create_project(client)
        gone = add_pipeline(session, file_path="pipelines/gone.yaml", deleted=True)
        create_resource(
            client, project_id=project["id"], entity="pipeline", entity_id=gone
        )
        create_resource(
            client,
            project_id=project["id"],
            entity="document",
            payload={"body": "kept"},
        )

        with sql_capture.capture_sql(db_engine) as statements:
            listed = client.get(
                f"/api/projects/{project['id']}/resources/?entity=document"
            ).json()
        assert [resource["entity"] for resource in listed["resources"]] == ["document"]
        assert sql_capture.selects_from(statements, table="pipeline") == []

        # And naming `pipeline` alongside it brings the term back, since now one can appear.
        with sql_capture.capture_sql(db_engine) as statements:
            both = client.get(
                f"/api/projects/{project['id']}/resources/?entity=document&entity=pipeline"
            ).json()
        assert [resource["entity"] for resource in both["resources"]] == ["document"]
        assert sql_capture.selects_from(statements, table="pipeline")


class TestUnstorablePayload:
    def test_a_nan_payload_is_422_rather_than_a_dialect_dependent_write(
        self,
        client: fastapi.testclient.TestClient,
        project: dict,
    ) -> None:
        """`allow_nan=False` on the validator's serialise. MySQL's JSON column refuses NaN at
        flush -- a 500 -- while SQLite stores it, so it is refused up front.

        Sent as raw bytes because the server's `json.loads` reads a bare `NaN` token happily
        while httpx's own `json=` refuses to write one."""
        response = client.post(
            f"/api/projects/{project['id']}/resources/",
            content=b'{"entity": "document", "payload": {"value": NaN}}',
            headers={"Content-Type": "application/json"},
        )
        assert response.status_code == 422, response.text
        assert "JSON-serializable" in response.json()["detail"]
        assert (
            client.get(f"/api/projects/{project['id']}/resources/").json()[
                "total_count"
            ]
            == 0
        )


class TestPayloadMustBeAnObject:
    """The routes' Pydantic models already type `payload` as an object; the service must too.

    A direct caller that hands the service a list or a string would store it fine and then 500
    on every read, the response model typing `payload` as an object.
    """

    def test_a_list_payload_is_refused_at_create(
        self, session: orm.Session, project: dict
    ) -> None:
        with pytest.raises(errors.ProjectValidationError, match="JSON object.*list"):
            services.ProjectService().create_resource(
                session=session,
                project_id=project["id"],
                entity=db_models.ProjectResourceEntity.DOCUMENT,
                payload=["not", "an", "object"],  # type: ignore[arg-type]  # the point of the test
            )

    def test_a_string_payload_is_refused_at_patch(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
        project: dict,
    ) -> None:
        resource = create_resource(
            client, project_id=project["id"], payload={"body": "fine"}
        )
        with pytest.raises(errors.ProjectValidationError, match="JSON object.*str"):
            services.ProjectService().update_resource(
                session=session,
                project_id=project["id"],
                resource_id=resource["id"],
                updates={"payload": "just text"},
            )
        fetched = client.get(
            f"/api/projects/{project['id']}/resources/{resource['id']}"
        ).json()
        assert fetched["payload"] == {"body": "fine"}


class TestPayloadIsNotListed:
    def test_a_listed_resource_has_no_payload_field_at_all(
        self,
        client: fastapi.testclient.TestClient,
        project: dict,
    ) -> None:
        """Absent, not null: a null would say "this document has no content", which the
        reference/payload rule makes impossible."""
        create_resource(
            client, project_id=project["id"], payload={"body": "some content"}
        )
        listed = client.get(f"/api/projects/{project['id']}/resources/").json()[
            "resources"
        ][0]
        assert "payload" not in listed
        assert listed["entity"] == "document"

    def test_the_single_read_still_returns_it(
        self,
        client: fastapi.testclient.TestClient,
        project: dict,
    ) -> None:
        resource = create_resource(
            client, project_id=project["id"], payload={"body": "some content"}
        )
        fetched = client.get(
            f"/api/projects/{project['id']}/resources/{resource['id']}"
        ).json()
        assert fetched["payload"] == {"body": "some content"}

    def test_the_column_is_not_selected_rather_than_not_serialised(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
        project: dict,
    ) -> None:
        """The distinction that makes this worth doing: a page of 100 documents would otherwise
        read megabytes out of the database to serialise none of it. `raiseload=True` is the
        enforcement -- touching `.payload` on a listed row raises instead of querying per row,
        so reintroducing the field into the list response fails here, not in production.
        """
        create_resource(
            client, project_id=project["id"], payload={"body": "some content"}
        )
        page = services.ProjectService().list_resources(
            session=session,
            project_id=project["id"],
            page_size=10,
        )
        with pytest.raises(sqlalchemy.exc.InvalidRequestError) as caught:
            _ = page.rows[0].payload
        assert "payload" in str(caught.value)


class TestDataIsListed:
    """The counterpart to `TestPayloadIsNotListed`, and what makes that exclusion workable.

    Without `payload`, a page of `document` rows is a page of identical rows -- same entity, no
    `entity_id`, `name` optional -- so a client cannot categorise them.
    """

    def test_two_documents_are_distinguishable_without_their_payloads(
        self,
        client: fastapi.testclient.TestClient,
        project: dict,
    ) -> None:
        """The whole point of the field, as the thing that used to be impossible."""
        create_resource(
            client,
            project_id=project["id"],
            payload={"body": "a"},
            data={"document_type": "spec"},
        )
        create_resource(
            client,
            project_id=project["id"],
            payload={"body": "b"},
            data={"document_type": "note"},
        )
        listed = client.get(f"/api/projects/{project['id']}/resources/").json()[
            "resources"
        ]
        assert {resource["data"]["document_type"] for resource in listed} == {
            "spec",
            "note",
        }
        assert all("payload" not in resource for resource in listed)

    def test_it_survives_a_round_trip_unread(
        self, client: fastapi.testclient.TestClient, project: dict
    ) -> None:
        """Nothing in the service inspects the keys, so anything JSON-shaped comes back as sent."""
        data = {
            "a": [1, 2, {"b": None}],
            "c": {"d": True},
            "e": "",
            "f": {},
            "g": "\u00fcn\u00efcode",
        }
        resource = create_resource(client, project_id=project["id"], data=data)
        assert resource["data"] == data
        assert (
            client.get(
                f"/api/projects/{project['id']}/resources/{resource['id']}"
            ).json()["data"]
            == data
        )
        assert (
            client.get(f"/api/projects/{project['id']}/resources/").json()["resources"][
                0
            ]["data"]
            == data
        )

    def test_an_omitted_data_lists_as_null(
        self, client: fastapi.testclient.TestClient, project: dict
    ) -> None:
        """Null rather than absent, unlike `payload`: always part of the summary."""
        create_resource(client, project_id=project["id"])
        assert (
            client.get(f"/api/projects/{project['id']}/resources/").json()["resources"][
                0
            ]["data"]
            is None
        )

    def test_an_empty_object_is_not_folded_to_null(
        self, client: fastapi.testclient.TestClient, project: dict
    ) -> None:
        assert create_resource(client, project_id=project["id"], data={})["data"] == {}

    def test_a_reference_may_carry_it_too(
        self, client: fastapi.testclient.TestClient, project: dict
    ) -> None:
        """Not document-only: the column is on every row."""
        resource = create_resource(
            client,
            project_id=project["id"],
            entity="pipeline",
            entity_id=PIPELINE_ID,
            data={"pinned": True},
        )
        assert resource["data"] == {"pinned": True}

    def test_it_is_selected_alongside_the_deferred_payload(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
        project: dict,
    ) -> None:
        """`orm.defer` is per-column: `.data` must not raise the way `.payload` does."""
        create_resource(
            client,
            project_id=project["id"],
            data={"document_type": "spec"},
        )
        page = services.ProjectService().list_resources(
            session=session, project_id=project["id"], page_size=10
        )
        assert page.rows[0].data == {"document_type": "spec"}

    def test_a_non_object_is_refused_at_the_service_too(
        self,
        session: orm.Session,
        project: dict,
    ) -> None:
        """The response model types it as an object, so a stored list would 500 on every read."""
        with pytest.raises(
            errors.ProjectValidationError, match="data.*JSON object.*list"
        ):
            services.ProjectService().create_resource(
                session=session,
                project_id=project["id"],
                entity=db_models.ProjectResourceEntity.DOCUMENT,
                payload={"body": "fine"},
                data=["not", "an", "object"],  # type: ignore[arg-type]  # the point of the test
            )


class TestExtraDataIsNotOnTheWire:
    def test_it_is_refused_on_create_and_on_patch(
        self, client: fastapi.testclient.TestClient, project: dict
    ) -> None:
        assert (
            client.post(
                f"/api/projects/{project['id']}/resources/",
                json={
                    "entity": "document",
                    "payload": {"body": "x"},
                    "extra_data": {"pinned": True},
                },
            ).status_code
            == 422
        )
        resource = create_resource(client, project_id=project["id"])
        assert (
            client.patch(
                f"/api/projects/{project['id']}/resources/{resource['id']}",
                json={"extra_data": {"pinned": True}},
            ).status_code
            == 422
        )

    def test_it_appears_in_no_response(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
        project: dict,
    ) -> None:
        resource = create_resource(client, project_id=project["id"])
        session.execute(
            sqlalchemy.update(db_models.ProjectResource)
            .where(db_models.ProjectResource.id == resource["id"])
            .values(extra_data={"reason": "backfill"})
        )
        session.commit()
        for response in (
            client.get(f"/api/projects/{project['id']}/resources/"),
            client.get(f"/api/projects/{project['id']}/resources/{resource['id']}"),
        ):
            assert "extra_data" not in response.text, response.text

    def test_it_is_not_selected_by_the_list(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
        project: dict,
    ) -> None:
        """Deferred with `raiseload=True`, as `payload` is."""
        create_resource(client, project_id=project["id"])
        page = services.ProjectService().list_resources(
            session=session, project_id=project["id"], page_size=10
        )
        with pytest.raises(sqlalchemy.exc.InvalidRequestError) as caught:
            _ = page.rows[0].extra_data
        assert "extra_data" in str(caught.value)


class TestGetResource:
    def test_the_project_in_the_path_is_a_filter(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """A resource id belonging to another project is a 404 here, not a read across projects."""
        owner = create_project(client, name="Owner")
        stranger = create_project(client, name="Stranger")
        resource = create_resource(client, project_id=owner["id"])
        assert (
            client.get(
                f"/api/projects/{owner['id']}/resources/{resource['id']}"
            ).status_code
            == 200
        )
        assert (
            client.get(
                f"/api/projects/{stranger['id']}/resources/{resource['id']}"
            ).status_code
            == 404
        )


class TestPatchResource:
    def test_it_edits_name_and_payload(
        self, client: fastapi.testclient.TestClient, project: dict
    ) -> None:
        resource = create_resource(
            client, project_id=project["id"], name="Before", payload={"a": 1}
        )
        response = client.patch(
            f"/api/projects/{project['id']}/resources/{resource['id']}",
            json={"name": "After", "payload": {"b": 2}},
        )
        assert response.status_code == 200, response.text
        assert response.json()["name"] == "After"
        assert response.json()["payload"] == {"b": 2}

    def test_the_payload_is_replaced_not_merged(
        self, client: fastapi.testclient.TestClient, project: dict
    ) -> None:
        """The backend does not inspect the blob, so it has no basis for merging into it."""
        resource = create_resource(
            client, project_id=project["id"], payload={"keep": 1, "drop": 2}
        )
        body = client.patch(
            f"/api/projects/{project['id']}/resources/{resource['id']}",
            json={"payload": {"keep": 9}},
        ).json()
        assert body["payload"] == {"keep": 9}

    def test_it_edits_data(
        self, client: fastapi.testclient.TestClient, project: dict
    ) -> None:
        resource = create_resource(
            client,
            project_id=project["id"],
            data={"document_type": "note"},
        )
        body = client.patch(
            f"/api/projects/{project['id']}/resources/{resource['id']}",
            json={"data": {"document_type": "spec"}},
        ).json()
        assert body["data"] == {"document_type": "spec"}

    def test_data_is_replaced_not_merged(
        self, client: fastapi.testclient.TestClient, project: dict
    ) -> None:
        """Last-write-wins across the whole namespace, which is why the field wants a single
        writer: a client setting one key drops every key it did not send."""
        resource = create_resource(
            client, project_id=project["id"], data={"keep": 1, "drop": 2}
        )
        body = client.patch(
            f"/api/projects/{project['id']}/resources/{resource['id']}",
            json={"data": {"keep": 9}},
        ).json()
        assert body["data"] == {"keep": 9}

    def test_an_explicit_null_clears_data_and_an_omission_leaves_it(
        self,
        client: fastapi.testclient.TestClient,
        project: dict,
    ) -> None:
        resource = create_resource(
            client,
            project_id=project["id"],
            data={"document_type": "spec"},
        )
        kept = client.patch(
            f"/api/projects/{project['id']}/resources/{resource['id']}",
            json={"name": "Renamed"},
        ).json()
        assert kept["data"] == {"document_type": "spec"}
        cleared = client.patch(
            f"/api/projects/{project['id']}/resources/{resource['id']}",
            json={"data": None},
        ).json()
        assert cleared["data"] is None

    def test_the_two_json_fields_do_not_disturb_each_other(
        self,
        client: fastapi.testclient.TestClient,
        project: dict,
    ) -> None:
        """Separate branches, so editing content leaves the labels alone. Nor are they synced:
        the backend maintains neither."""
        resource = create_resource(
            client,
            project_id=project["id"],
            payload={"body": "before"},
            data={"document_type": "spec"},
        )
        body = client.patch(
            f"/api/projects/{project['id']}/resources/{resource['id']}",
            json={"payload": {"body": "after"}},
        ).json()
        assert body["payload"] == {"body": "after"}
        assert body["data"] == {"document_type": "spec"}

    def test_data_is_editable_on_an_entity_this_deploy_does_not_know(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
        project: dict,
    ) -> None:
        """The asymmetry with the payload edit below: validating a payload needs the entity cast
        back to the enum, labelling a row does not -- so a rollback can still recategorise.
        """
        resource = create_resource(
            client, project_id=project["id"], payload={"body": "before"}
        )
        with session.begin():
            session.execute(
                sqlalchemy.update(db_models.ProjectResource)
                .where(db_models.ProjectResource.id == resource["id"])
                .values(entity="benchmark")
            )
        response = client.patch(
            f"/api/projects/{project['id']}/resources/{resource['id']}",
            json={"data": {"document_type": "spec"}},
        )
        assert response.status_code == 200, response.text
        assert response.json()["data"] == {"document_type": "spec"}

    def test_a_reference_can_gain_and_then_change_its_metadata(
        self,
        client: fastapi.testclient.TestClient,
        project: dict,
    ) -> None:
        """The reason this route matters to `agent_session`: a session's metadata changes while
        the id it is keyed by does not. Both steps were 422s under the exclusive-or -- attaching
        with a payload, and adding one afterwards -- so this covers the reference that starts
        bare as well as the one that starts annotated."""
        resource = create_resource(
            client,
            project_id=project["id"],
            entity="agent_session",
            entity_id=PIPELINE_ID,
        )
        assert resource["payload"] is None

        annotated = client.patch(
            f"/api/projects/{project['id']}/resources/{resource['id']}",
            json={"payload": {"status": "running"}},
        )
        assert annotated.status_code == 200, annotated.text
        assert annotated.json()["payload"] == {"status": "running"}
        assert annotated.json()["entity_id"] == PIPELINE_ID

        finished = client.patch(
            f"/api/projects/{project['id']}/resources/{resource['id']}",
            json={"payload": {"status": "finished"}},
        )
        assert finished.json()["payload"] == {"status": "finished"}

    def test_a_reference_can_have_its_metadata_cleared_again(
        self,
        client: fastapi.testclient.TestClient,
        project: dict,
    ) -> None:
        """Null is a legal destination for a reference's payload, unlike a document's: the
        `entity_id` is still there, so the row is still something."""
        resource = create_resource(
            client,
            project_id=project["id"],
            entity="pipeline",
            entity_id=PIPELINE_ID,
            payload={"branch": "main"},
        )
        cleared = client.patch(
            f"/api/projects/{project['id']}/resources/{resource['id']}",
            json={"payload": None},
        )
        assert cleared.status_code == 200, cleared.text
        assert cleared.json()["payload"] is None

    def test_a_document_cannot_have_its_content_cleared(
        self,
        client: fastapi.testclient.TestClient,
        project: dict,
    ) -> None:
        """The surviving half of the old rule, at the one route that can still break it: a
        document with no payload and no `entity_id` is a row that is neither."""
        resource = create_resource(
            client, project_id=project["id"], payload={"body": "x"}
        )
        response = client.patch(
            f"/api/projects/{project['id']}/resources/{resource['id']}",
            json={"payload": None},
        )
        assert response.status_code == 422, response.text
        assert "payload" in response.json()["detail"]

    def test_the_pointer_cannot_move(
        self, client: fastapi.testclient.TestClient, project: dict
    ) -> None:
        """`entity`, `entity_id` and `project_id` are fixed: a resource pointing elsewhere is a
        different resource, and a movable pointer would slide the row out from under the unique
        index. `workspace_id` is refused for a different reason -- it is not a field here at all.
        """
        resource = create_resource(
            client,
            project_id=project["id"],
            entity="pipeline",
            entity_id=PIPELINE_ID,
        )
        for field, value in (
            ("entity", "document"),
            ("entity_id", OTHER_PIPELINE_ID),
            ("project_id", MISSING_WORKSPACE_ID),
            ("workspace_id", SANDBOX),
        ):
            response = client.patch(
                f"/api/projects/{project['id']}/resources/{resource['id']}",
                json={field: value},
            )
            assert response.status_code == 422, f"{field} was accepted: {response.text}"

        unchanged = client.get(
            f"/api/projects/{project['id']}/resources/{resource['id']}"
        ).json()
        assert unchanged["entity"] == "pipeline"
        assert unchanged["entity_id"] == PIPELINE_ID

    def test_an_entity_this_deploy_does_not_know_is_422_not_500(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
        project: dict,
    ) -> None:
        """`entity` is a CHECK-less VARCHAR by design, so after a rollback this deploy can be
        reading rows a newer one wrote. The payload edit is the one path that must cast the
        column back to the enum, and an unknown kind cannot say what shape its payload may
        take -- so it refuses, instead of letting the bare cast escape as a 500."""
        resource = create_resource(
            client, project_id=project["id"], payload={"body": "before"}
        )
        with session.begin():
            session.execute(
                sqlalchemy.update(db_models.ProjectResource)
                .where(db_models.ProjectResource.id == resource["id"])
                .values(entity="benchmark")
            )
        response = client.patch(
            f"/api/projects/{project['id']}/resources/{resource['id']}",
            json={"payload": {"body": "after"}},
        )
        assert response.status_code == 422, response.text
        assert "benchmark" in response.json()["detail"]

    def test_the_service_refuses_the_pointer_too(
        self,
        client: fastapi.testclient.TestClient,
        project: dict,
        session: orm.Session,
    ) -> None:
        """The same second guard the project routes have, on the resource pointer."""
        resource = create_resource(client, project_id=project["id"])
        with pytest.raises(errors.ProjectValidationError, match="entity_id"):
            services.ProjectService().update_resource(
                session=session,
                project_id=project["id"],
                resource_id=resource["id"],
                updates={"entity_id": PIPELINE_ID},
            )

    def test_a_resource_in_another_project_is_404(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        owner = create_project(client, name="Owner")
        stranger = create_project(client, name="Stranger")
        resource = create_resource(client, project_id=owner["id"])
        response = client.patch(
            f"/api/projects/{stranger['id']}/resources/{resource['id']}",
            json={"name": "x"},
        )
        assert response.status_code == 404, response.text


class TestDeleteResource:
    def test_it_returns_204_and_the_row_is_gone(
        self, client: fastapi.testclient.TestClient, project: dict
    ) -> None:
        resource = create_resource(client, project_id=project["id"])
        deleted = client.delete(
            f"/api/projects/{project['id']}/resources/{resource['id']}"
        )
        assert deleted.status_code == 204
        assert (
            client.get(
                f"/api/projects/{project['id']}/resources/{resource['id']}"
            ).status_code
            == 404
        )

    def test_deleting_it_twice_is_a_404(
        self, client: fastapi.testclient.TestClient, project: dict
    ) -> None:
        resource = create_resource(client, project_id=project["id"])
        client.delete(f"/api/projects/{project['id']}/resources/{resource['id']}")
        second_delete = client.delete(
            f"/api/projects/{project['id']}/resources/{resource['id']}"
        )
        assert second_delete.status_code == 404

    def test_the_project_survives(
        self, client: fastapi.testclient.TestClient, project: dict
    ) -> None:
        resource = create_resource(client, project_id=project["id"])
        client.delete(f"/api/projects/{project['id']}/resources/{resource['id']}")
        assert (
            client.get(f"/api/projects/{project['id']}").json()["resource_counts"] == {}
        )


class TestWriteGuards:
    def test_writes_are_refused_in_read_only_mode(
        self,
        read_only_client: fastapi.testclient.TestClient,
        client: fastapi.testclient.TestClient,
        project: dict,
    ) -> None:
        response = read_only_client.post(
            f"/api/projects/{project['id']}/resources/",
            json={"entity": "document"},
        )
        assert response.status_code == 503, response.text

    def test_a_caller_without_write_permission_is_403(
        self,
        no_write_client: fastapi.testclient.TestClient,
        client: fastapi.testclient.TestClient,
        project: dict,
    ) -> None:
        response = no_write_client.post(
            f"/api/projects/{project['id']}/resources/",
            json={"entity": "document"},
        )
        assert response.status_code == 403, response.text
