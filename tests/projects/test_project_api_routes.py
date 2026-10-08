"""Projects CRUD, listing, filters, immutability, and the delete that takes resources with it."""

import datetime
import json

import fastapi.testclient
import pytest
import sqlalchemy
from cloud_pipelines_backend.projects import (
    api_routes,
    db_models,
    errors,
    services,
)
from sqlalchemy import orm

from tests import sql_capture
from tests.projects.conftest import (
    DEFAULT_USER,
    INTERNAL,
    MISSING_WORKSPACE_ID,
    OTHER_USER,
    SANDBOX,
    create_project,
    create_resource,
)

# The id half of a page token, for cases that are only about the timestamp half.
_ANY_ID = "11111111111111111111"


class TestCreateProject:
    def test_it_stamps_the_caller_and_the_workspace(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        project = create_project(
            client, name="Retention modelling", description="Q3 churn work"
        )
        assert project["workspace_id"] == SANDBOX
        assert project["created_by"] == DEFAULT_USER
        assert project["description"] == "Q3 churn work"
        assert project["data"] is None
        assert project["resource_counts"] == {}

    def test_data_is_stored_raw(self, client: fastapi.testclient.TestClient) -> None:
        """No rendering, no sanitising, no parsing -- the backend hands back what it was given,
        edges and all. Notes live in here, so a trailing newline and a leading indent are the
        author's, unlike `description`, where they are a typo."""
        notes = "# heading\n<script>alert(1)</script>\n\n  indented\n\n"
        project = create_project(
            client, data={"notes": notes}, description="  Padded  "
        )
        assert project["data"] == {"notes": notes}
        assert project["description"] == "Padded"
        assert client.get(f"/api/projects/{project['id']}").json()["data"] == {
            "notes": notes
        }

    def test_an_explicit_null_clears_data(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        project = create_project(client, data={"notes": "scratch"})
        cleared = client.patch(f"/api/projects/{project['id']}", json={"data": None})
        assert cleared.status_code == 200, cleared.text
        assert cleared.json()["data"] is None

    def test_created_by_cannot_be_supplied(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Attribution comes from the authenticated caller, never from the body."""
        response = client.post(
            "/api/projects/",
            json={
                "workspace_id": SANDBOX,
                "name": "Mine",
                "created_by": OTHER_USER,
            },
        )
        assert response.status_code == 422, response.text

    def test_an_unknown_workspace_is_404(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Resolved before insert, so this is a 404 naming the workspace and not a 500 from the FK."""
        response = client.post(
            "/api/projects/",
            json={"workspace_id": MISSING_WORKSPACE_ID, "name": "Mine"},
        )
        assert response.status_code == 404, response.text

    def test_an_unparseable_workspace_id_is_404_like_any_other_miss(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Ids are opaque, so there is no shape to fail: a workspace id that could not have been
        minted here is simply one that names nothing."""
        response = client.post(
            "/api/projects/", json={"workspace_id": "sandbox", "name": "Mine"}
        )
        assert response.status_code == 404, response.text

    def test_a_blank_name_is_422(self, client: fastapi.testclient.TestClient) -> None:
        assert (
            client.post(
                "/api/projects/", json={"workspace_id": SANDBOX, "name": "   "}
            ).status_code
            == 422
        )

    def test_a_padded_name_is_stripped(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        assert create_project(client, name="  Retention  ")["name"] == "Retention"


class TestListProjects:
    @pytest.fixture()
    def seeded(
        self,
        client: fastapi.testclient.TestClient,
        other_user_client: fastapi.testclient.TestClient,
    ) -> dict[str, dict]:
        """Four projects across two workspaces and two users, so every filter has a subset."""
        return {
            "alice_sandbox": create_project(
                client, name="Alice sandbox", workspace_id=SANDBOX
            ),
            "alice_internal": create_project(
                client, name="Alice internal", workspace_id=INTERNAL
            ),
            "bob_sandbox": create_project(
                other_user_client, name="Bob sandbox", workspace_id=SANDBOX
            ),
            "bob_internal": create_project(
                other_user_client, name="Bob internal", workspace_id=INTERNAL
            ),
        }

    def test_it_returns_every_project_by_default(
        self,
        client: fastapi.testclient.TestClient,
        seeded: dict[str, dict],
    ) -> None:
        """No implicit created-by-me scope. Alice sees Bob's projects and vice versa."""
        body = client.get("/api/projects/").json()
        assert body["total_count"] == 4
        assert {project["id"] for project in body["projects"]} == {
            p["id"] for p in seeded.values()
        }

    def test_the_other_caller_sees_the_same_four(
        self,
        other_user_client: fastapi.testclient.TestClient,
        seeded: dict[str, dict],
    ) -> None:
        assert other_user_client.get("/api/projects/").json()["total_count"] == 4

    def test_created_by_narrows_it(
        self, client: fastapi.testclient.TestClient, seeded: dict[str, dict]
    ) -> None:
        body = client.get("/api/projects/", params={"created_by": OTHER_USER}).json()
        assert {project["id"] for project in body["projects"]} == {
            seeded["bob_sandbox"]["id"],
            seeded["bob_internal"]["id"],
        }

    def test_workspace_id_narrows_it(
        self, client: fastapi.testclient.TestClient, seeded: dict[str, dict]
    ) -> None:
        body = client.get("/api/projects/", params={"workspace_id": INTERNAL}).json()
        assert {project["id"] for project in body["projects"]} == {
            seeded["alice_internal"]["id"],
            seeded["bob_internal"]["id"],
        }

    def test_the_two_filters_compose_with_and(
        self,
        client: fastapi.testclient.TestClient,
        seeded: dict[str, dict],
    ) -> None:
        body = client.get(
            "/api/projects/",
            params={"created_by": OTHER_USER, "workspace_id": SANDBOX},
        ).json()
        assert [project["id"] for project in body["projects"]] == [
            seeded["bob_sandbox"]["id"]
        ]
        assert body["total_count"] == 1

    def test_it_sorts_by_most_recently_updated(
        self,
        client: fastapi.testclient.TestClient,
        seeded: dict[str, dict],
    ) -> None:
        client.patch(
            f"/api/projects/{seeded['alice_sandbox']['id']}",
            json={"name": "Touched"},
        )
        body = client.get("/api/projects/").json()
        assert body["projects"][0]["id"] == seeded["alice_sandbox"]["id"]

    def test_it_pages_without_repeating_or_dropping_a_row(
        self,
        client: fastapi.testclient.TestClient,
        seeded: dict[str, dict],
    ) -> None:
        seen: list[str] = []
        page_token = None
        for _ in range(10):
            params = {"page_size": 2}
            if page_token:
                params["page_token"] = page_token
            body = client.get("/api/projects/", params=params).json()
            seen.extend(project["id"] for project in body["projects"])
            page_token = body["next_page_token"]
            if not page_token:
                break
        assert len(seen) == len(set(seen)) == 4

    def test_a_final_full_page_ends_the_walk(
        self,
        client: fastapi.testclient.TestClient,
        seeded: dict[str, dict],
    ) -> None:
        """The `page_size + 1` probe: four rows at `page_size=4` is one page, not two."""
        body = client.get("/api/projects/", params={"page_size": 4}).json()
        assert len(body["projects"]) == 4
        assert body["next_page_token"] is None

    def test_a_malformed_page_token_is_422(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        assert (
            client.get("/api/projects/", params={"page_token": "nonsense"}).status_code
            == 422
        )

    def test_an_empty_page_token_is_422_rather_than_page_one(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """`?page_token=` present and empty is a malformed token, not "no token".

        Gated on truthiness it skipped the decode and was answered as an unpaginated first page
        with a 200, while every other bad token got this 422. A client echoing a null
        `next_page_token` as `""` then re-fetches page 1 forever, and a paging loop keyed on
        "the request succeeded and I sent a token" never terminates.
        """
        assert (
            client.get("/api/projects/", params={"page_token": ""}).status_code == 422
        )

    @pytest.mark.parametrize(
        "page_token",
        [
            f"0001-01-01T00:00:00+09:00~{_ANY_ID}",
            f"9999-12-31T23:59:59-09:00~{_ANY_ID}",
        ],
        ids=["underflow", "overflow"],
    )
    def test_a_page_token_at_the_edge_of_the_calendar_is_422_not_500(
        self, client: fastapi.testclient.TestClient, page_token: str
    ) -> None:
        """`OverflowError` is not a `ValueError`, so these two answered 500.

        Normalising to UTC subtracts the offset, which walks a boundary date out of the
        representable range. Any offset inside a day reaches it -- Go's zero `time.Time`
        is `0001-01-01T00:00:00Z`, and a client east of UTC re-emits the first case.
        """
        response = client.get("/api/projects/", params={"page_token": page_token})
        assert response.status_code == 422, response.text
        assert "page_token" in response.text

    def test_the_list_carries_data(self, client: fastapi.testclient.TestClient) -> None:
        """The client's metadata is what a card is drawn from, so it travels with the page."""
        create_project(client, data={"notes": "scratch"})
        assert client.get("/api/projects/").json()["projects"][0]["data"] == {
            "notes": "scratch"
        }

    def test_the_list_does_not_carry_extra_data(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Nor does anything else: `extra_data` is off the contract in both directions."""
        listed = client.get("/api/projects/").json()
        create_project(client)
        assert "extra_data" not in json.dumps(listed)

    def test_extra_data_is_not_selected_rather_than_not_serialised(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """`raiseload=True`: reintroducing it into the list fails here rather than becoming a
        query per row in production."""
        create_project(client)
        page = services.ProjectService().list_projects(session=session, page_size=10)
        with pytest.raises(sqlalchemy.exc.InvalidRequestError) as caught:
            _ = page.rows[0].extra_data
        assert "extra_data" in str(caught.value)

    def test_a_naive_token_timestamp_is_read_as_utc(self) -> None:
        """Mirrors `_encode_cursor`'s stamping: the decoded half is aware UTC however the token
        spells it, so a hand-built token means the same instant a minted one does instead of a
        naive value whose meaning depends on what it is later compared against."""
        row_id = _ANY_ID
        naive, _ = api_routes._decode_cursor(
            page_token=f"2026-01-01T00:00:00~{row_id}",
            timestamp_field="updated_at",
        )
        aware, _ = api_routes._decode_cursor(
            page_token=f"2026-01-01T01:00:00+01:00~{row_id}",
            timestamp_field="updated_at",
        )
        assert (
            naive
            == aware
            == datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        )


class TestResourceCountsAreNotNPlusOne:
    def test_the_query_count_does_not_grow_with_the_page(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Read off the SQL rather than asserted by eye.

        Counted per row, the six-project page would read `project_resource` six times; here both
        pages cost the same fixed number of statements. Fixed rather than one, because the
        deleted-pipeline filter adds a second read that also takes the whole page's ids. The
        equality is the invariant -- a third statement would be fine, a growing count would not.
        """

        def resource_reads() -> list[str]:
            with sql_capture.capture_sql(db_engine) as statements:
                assert (
                    client.get("/api/projects/", params={"page_size": 100}).status_code
                    == 200
                )
            return sql_capture.selects_from(statements, table="project_resource")

        for _ in range(2):
            project = create_project(client)
            create_resource(client, project_id=project["id"], entity="document")
        with_two = resource_reads()

        for _ in range(4):
            project = create_project(client)
            create_resource(client, project_id=project["id"], entity="document")
        with_six = resource_reads()

        assert len(with_six) == len(with_two), (
            f"a page of 6 read project_resource {len(with_six)} times and a page of 2 read it "
            f"{len(with_two)} times:\n" + "\n".join(with_six)
        )
        assert sum("GROUP BY" in statement.upper() for statement in with_six) == 1


class TestOrigin:
    """Who made a project: attribution about the *creator's nature*, not about the creator.

    `created_by` answers "which identity"; this answers "was there a person behind it".
    Declared by the caller, because an agent reaches this API with a person's credentials.
    """

    def test_it_defaults_to_user(self, client: fastapi.testclient.TestClient) -> None:
        """So an older client keeps working, and the default is true of every project that
        predates the field."""
        assert create_project(client)["origin"] == "user"

    def test_an_agent_can_declare_itself(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        assert create_project(client, origin="agent")["origin"] == "agent"

    def test_an_unknown_origin_is_422(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        response = client.post(
            "/api/projects/",
            json={"workspace_id": SANDBOX, "name": "P", "origin": "robot"},
        )
        assert response.status_code == 422, response.text

    def test_it_cannot_be_changed_afterwards(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Evidence about how the row came to exist, which a later edit cannot revise -- and
        the field a human-facing list filters on, which a flippable one could not be."""
        project = create_project(client, origin="agent")
        response = client.patch(
            f"/api/projects/{project['id']}", json={"origin": "user"}
        )
        assert response.status_code == 422, response.text
        assert client.get(f"/api/projects/{project['id']}").json()["origin"] == "agent"

    def test_the_list_carries_it(self, client: fastapi.testclient.TestClient) -> None:
        """On the summary, not only the detail: filtering agent-created projects out of a human
        list is why the column exists, and that happens on the list."""
        create_project(client, name="By hand")
        create_project(client, name="By an agent", origin="agent")
        listed = client.get("/api/projects/").json()["projects"]
        assert {project["name"]: project["origin"] for project in listed} == {
            "By hand": "user",
            "By an agent": "agent",
        }


class TestGetProject:
    def test_it_carries_data_and_counts_grouped_by_entity(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        project = create_project(client, data={"notes": "scratch"})
        create_resource(client, project_id=project["id"], entity="document")
        create_resource(client, project_id=project["id"], entity="document")
        create_resource(
            client,
            project_id=project["id"],
            entity="pipeline",
            entity_id=_ANY_ID,
        )
        body = client.get(f"/api/projects/{project['id']}").json()
        assert body["data"] == {"notes": "scratch"}
        assert body["resource_counts"] == {"document": 2, "pipeline": 1}

    def test_a_missing_project_is_404(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        assert client.get(f"/api/projects/{MISSING_WORKSPACE_ID}").status_code == 404


class TestPatchProject:
    def test_it_edits_name_description_and_data(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        project = create_project(client, description="before", data={"notes": "before"})
        response = client.patch(
            f"/api/projects/{project['id']}",
            json={
                "name": "After",
                "description": "after",
                "data": {"notes": "after"},
            },
        )
        assert response.status_code == 200, response.text
        assert response.json()["name"] == "After"
        assert response.json()["data"] == {"notes": "after"}

    def test_an_omitted_field_is_left_alone(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        project = create_project(client, description="keep me")
        body = client.patch(
            f"/api/projects/{project['id']}", json={"name": "Renamed"}
        ).json()
        assert body["description"] == "keep me"

    def test_data_is_replaced_not_merged(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """As a workspace's and a resource's are: nothing inspects the object, so nothing can
        merge into it."""
        project = create_project(client, data={"keep": 1, "drop": 2})
        body = client.patch(
            f"/api/projects/{project['id']}", json={"data": {"keep": 9}}
        ).json()
        assert body["data"] == {"keep": 9}

    def test_an_omitted_data_is_left_alone(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        project = create_project(client, data={"colour": "blue"})
        kept = client.patch(
            f"/api/projects/{project['id']}", json={"name": "Renamed"}
        ).json()
        assert kept["data"] == {"colour": "blue"}

    def test_an_explicit_null_clears_a_field(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Omitted and null are different requests, which is why the route reads `model_fields_set`."""
        project = create_project(client, description="clear me")
        assert (
            client.patch(
                f"/api/projects/{project['id']}", json={"description": None}
            ).json()["description"]
            is None
        )

    def test_a_null_name_is_422_not_500(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The one editable field that is NOT NULL, so "null clears it" cannot apply to it."""
        project = create_project(client, name="Keep me")
        response = client.patch(f"/api/projects/{project['id']}", json={"name": None})
        assert response.status_code == 422, response.text
        assert "name" in response.json()["detail"]
        assert client.get(f"/api/projects/{project['id']}").json()["name"] == "Keep me"

    def test_an_empty_body_is_a_legal_no_op(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        project = create_project(client, name="Unchanged")
        assert (
            client.patch(f"/api/projects/{project['id']}", json={}).json()["name"]
            == "Unchanged"
        )

    def test_it_refuses_to_move_a_project_between_workspaces(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """A project belongs to exactly one workspace, decided at creation and never after."""
        project = create_project(client, workspace_id=SANDBOX)
        response = client.patch(
            f"/api/projects/{project['id']}", json={"workspace_id": INTERNAL}
        )
        assert response.status_code == 422, response.text
        assert (
            client.get(f"/api/projects/{project['id']}").json()["workspace_id"]
            == SANDBOX
        )

    def test_the_service_refuses_it_too(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """The second guard, bypassing the request model as a non-Pydantic caller would.

        `extra="forbid"` protects the HTTP edge, this protects the method, and they fail
        differently -- which is why both exist.
        """
        project = create_project(client)
        with pytest.raises(errors.ProjectValidationError, match="workspace_id"):
            services.ProjectService().update_project(
                session=session,
                project_id=project["id"],
                updates={"workspace_id": INTERNAL},
            )

    def test_id_created_by_and_timestamps_are_refused(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        project = create_project(client)
        for field, value in (
            ("id", MISSING_WORKSPACE_ID),
            ("created_by", OTHER_USER),
            ("created_at", "2020-01-01T00:00:00Z"),
            ("updated_at", "2020-01-01T00:00:00Z"),
        ):
            response = client.patch(
                f"/api/projects/{project['id']}", json={field: value}
            )
            assert response.status_code == 422, f"{field} was accepted: {response.text}"

    def test_anyone_may_edit_anyone_else_s_project(
        self,
        client: fastapi.testclient.TestClient,
        other_user_client: fastapi.testclient.TestClient,
    ) -> None:
        """`created_by` is attribution, never access control -- so this is a 200 by design."""
        project = create_project(client)
        assert (
            other_user_client.patch(
                f"/api/projects/{project['id']}", json={"name": "Bob was here"}
            ).status_code
            == 200
        )


class TestProjectData:
    """The client's metadata field. No rules beyond "a JSON object that can be stored"."""

    def test_it_survives_a_round_trip_unread(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        data = {
            "a": [1, 2, {"b": None}],
            "c": {"d": True},
            "e": "",
            "f": {},
            "g": "\u00fcn\u00efcode",
            "notes": "# heading\n\n- item",
        }
        project = create_project(client, data=data)
        assert project["data"] == data
        assert client.get(f"/api/projects/{project['id']}").json()["data"] == data

    def test_an_omitted_data_is_null(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        assert create_project(client)["data"] is None

    def test_an_empty_object_is_not_folded_to_null(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        assert create_project(client, data={})["data"] == {}

    def test_a_non_object_is_refused_at_the_service_too(
        self, session: orm.Session
    ) -> None:
        """The routes' Pydantic model guarantees this; a direct caller does not."""
        with pytest.raises(
            errors.ProjectValidationError, match="data.*JSON object.*list"
        ):
            services.ProjectService().create_project(
                session=session,
                workspace_id=SANDBOX,
                name="Wrong shape",
                data=["not", "an", "object"],  # type: ignore[arg-type]  # the point of the test
            )


class TestExtraDataIsNotOnTheWire:
    """A backend-only column. A client cannot set it and never sees it."""

    def test_it_is_refused_on_create_and_on_patch(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        assert (
            client.post(
                "/api/projects/",
                json={
                    "workspace_id": SANDBOX,
                    "name": "Mine",
                    "extra_data": {"pinned": True},
                },
            ).status_code
            == 422
        )
        project = create_project(client)
        assert (
            client.patch(
                f"/api/projects/{project['id']}",
                json={"extra_data": {"pinned": True}},
            ).status_code
            == 422
        )

    def test_it_appears_in_no_response(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """Even with a value in the column, put there the only way there is."""
        project = create_project(client)
        session.execute(
            sqlalchemy.update(db_models.Project)
            .where(db_models.Project.id == project["id"])
            .values(extra_data={"reason": "backfill"})
        )
        session.commit()
        for response in (
            client.get("/api/projects/"),
            client.get(f"/api/projects/{project['id']}"),
            client.patch(f"/api/projects/{project['id']}", json={}),
        ):
            assert "extra_data" not in response.text, response.text


class TestDeleteProject:
    def test_it_reports_what_it_removed_grouped_by_entity(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        project = create_project(client)
        create_resource(client, project_id=project["id"], entity="document")
        create_resource(client, project_id=project["id"], entity="document")
        create_resource(
            client,
            project_id=project["id"],
            entity="pipeline",
            entity_id="22222222-2222-4222-8222-222222222222",
        )
        response = client.delete(f"/api/projects/{project['id']}")
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["deleted_resource_counts"] == {"document": 2, "pipeline": 1}
        assert body["deleted_resource_total"] == 3

    def test_the_project_and_its_resources_are_gone(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        project = create_project(client)
        resource = create_resource(client, project_id=project["id"])
        client.delete(f"/api/projects/{project['id']}")
        assert client.get(f"/api/projects/{project['id']}").status_code == 404
        assert (
            client.get(
                f"/api/projects/{project['id']}/resources/{resource['id']}"
            ).status_code
            == 404
        )

    def test_it_is_gone_on_the_foreign_key_engine_too(
        self, fk_client: fastapi.testclient.TestClient
    ) -> None:
        """Same outcome whether the cascade fires or the explicit delete does the work."""
        project = create_project(fk_client)
        create_resource(fk_client, project_id=project["id"])
        deleted = fk_client.delete(f"/api/projects/{project['id']}")
        assert deleted.json()["deleted_resource_total"] == 1
        assert fk_client.get(f"/api/projects/{project['id']}").status_code == 404

    def test_deleting_it_twice_is_a_404(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Hard delete, so there is no tombstone left to find and nothing to revive."""
        project = create_project(client)
        first_delete = client.delete(f"/api/projects/{project['id']}")
        assert first_delete.status_code == 200
        second_delete = client.delete(f"/api/projects/{project['id']}")
        assert second_delete.status_code == 404

    def test_the_other_project_is_untouched(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        keep = create_project(client, name="Keep")
        keep_resource = create_resource(client, project_id=keep["id"])
        drop = create_project(client, name="Drop")
        create_resource(client, project_id=drop["id"])
        client.delete(f"/api/projects/{drop['id']}")
        assert (
            client.get(
                f"/api/projects/{keep['id']}/resources/{keep_resource['id']}"
            ).status_code
            == 200
        )


class TestWriteGuards:
    def test_writes_are_refused_in_read_only_mode(
        self, read_only_client: fastapi.testclient.TestClient
    ) -> None:
        assert (
            read_only_client.post(
                "/api/projects/", json={"workspace_id": SANDBOX, "name": "x"}
            ).status_code
            == 503
        )

    def test_reads_still_work_in_read_only_mode(
        self, read_only_client: fastapi.testclient.TestClient
    ) -> None:
        assert read_only_client.get("/api/projects/").status_code == 200

    def test_a_caller_without_write_permission_is_403(
        self, no_write_client: fastapi.testclient.TestClient
    ) -> None:
        assert (
            no_write_client.post(
                "/api/projects/", json={"workspace_id": SANDBOX, "name": "x"}
            ).status_code
            == 403
        )

    def test_they_can_still_read(
        self, no_write_client: fastapi.testclient.TestClient
    ) -> None:
        assert no_write_client.get("/api/projects/").status_code == 200
