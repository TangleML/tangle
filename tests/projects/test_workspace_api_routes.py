"""Workspaces: the reads every caller gets, and the three writes only an admin gets.

The fixture workspaces are inserted by `conftest._add_workspaces` rather than by anything in
the application -- there is no seeder. So the test that matters most here is the one proving the
create route is a usable replacement for one: able to set, in the request that makes the row,
everything a seeder could.
"""

import fastapi.testclient
import pytest
import sqlalchemy
from cloud_pipelines_backend.projects import db_models, errors, services
from cloud_pipelines_backend.utils import db as db_utils
from sqlalchemy import orm

from tests.projects.conftest import (
    ADMIN_USER,
    INTERNAL,
    MISSING_WORKSPACE_ID,
    OTHER_USER,
    SANDBOX,
    create_project,
    create_resource,
)

# A well-formed id nothing in the fixtures uses and nothing can create, for the tests that
# prove an id in a request body is refused rather than honoured.
UNUSED_ID = "11111111222233334444"


def create_workspace(
    client: fastapi.testclient.TestClient,
    *,
    name: str = "Research",
    **extra,
) -> dict:
    response = client.post("/api/workspaces/", json={"name": name, **extra})
    assert response.status_code == 201, response.text
    return response.json()


class TestListWorkspaces:
    def test_it_returns_them_by_name(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        response = client.get("/api/workspaces/")
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["total_count"] == 3
        assert [workspace["name"] for workspace in body["workspaces"]] == [
            "Internal",
            "Public",
            "Sandbox",
        ]

    def test_a_created_workspace_appears_in_it(
        self,
        admin_client: fastapi.testclient.TestClient,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Created by the admin, listed for everyone -- reads are not admin-gated."""
        create_workspace(admin_client, name="Aardvark")
        body = client.get("/api/workspaces/").json()
        assert body["total_count"] == 4
        assert body["workspaces"][0]["name"] == "Aardvark"


class TestGetWorkspace:
    def test_it_returns_one(self, client: fastapi.testclient.TestClient) -> None:
        response = client.get(f"/api/workspaces/{SANDBOX}")
        assert response.status_code == 200, response.text
        assert response.json()["name"] == "Sandbox"

    def test_it_carries_description_is_active_and_data(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """The columns an administrator fills in. A row created without them leaves two empty;
        `is_active` has behaviour behind it and has to default true -- a workspace that arrived
        inactive would refuse every project with a 422 and nothing would say why."""
        body = client.get(f"/api/workspaces/{SANDBOX}").json()
        assert body["is_active"] is True
        assert body["description"] is None
        assert body["data"] is None
        # Inserted straight into the table by the fixture, so nothing stamped a creator.
        assert body["created_by"] is None

    def test_the_list_carries_created_by_and_both_timestamps(
        self,
        admin_client: fastapi.testclient.TestClient,
    ) -> None:
        """Present on the summary as well as the get -- there is one workspace model."""
        created = create_workspace(admin_client, name="Listed")
        listed = next(
            workspace
            for workspace in admin_client.get("/api/workspaces/").json()["workspaces"]
            if workspace["id"] == created["id"]
        )
        assert listed["created_by"] == ADMIN_USER
        assert listed["created_at"] == created["created_at"]
        assert listed["updated_at"] == created["updated_at"]

    def test_a_missing_workspace_is_404(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        assert client.get(f"/api/workspaces/{MISSING_WORKSPACE_ID}").status_code == 404

    def test_an_unparseable_id_is_404_too(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Ids are opaque: there is no shape to fail, only a row to miss."""
        assert client.get("/api/workspaces/not-an-id").status_code == 404


class TestCreateWorkspace:
    def test_an_admin_creates_one(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        workspace = create_workspace(admin_client, name="Research")
        assert workspace["name"] == "Research"
        assert workspace["is_active"] is True
        assert workspace["description"] is None
        assert workspace["data"] is None
        assert (
            admin_client.get(f"/api/workspaces/{workspace['id']}").json() == workspace
        )

    def test_a_minted_id_matches_the_rest_of_the_repo(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        """`bts.generate_unique_id`, as pipelines, runs and triggers use."""
        workspace_id = create_workspace(admin_client)["id"]
        assert len(workspace_id) == db_utils.ID_LENGTH
        assert set(workspace_id) <= set("0123456789abcdef")

    def test_an_id_cannot_be_chosen(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        """422 from `extra="forbid"`, not a silently ignored field. A workspace id is minted for
        the same reason a project's and a resource's are, and a caller that thinks it set one
        would otherwise go on to look the workspace up at an id that names nothing."""
        response = admin_client.post(
            "/api/workspaces/", json={"name": "Research", "id": UNUSED_ID}
        )
        assert response.status_code == 422, response.text
        assert admin_client.get(f"/api/workspaces/{UNUSED_ID}").status_code == 404

    def test_two_creates_with_one_name_make_two_workspaces(
        self,
        admin_client: fastapi.testclient.TestClient,
    ) -> None:
        """The cost of minting, recorded rather than defended against: without a caller-chosen
        id there is nothing for a second create to collide with, so `POST` is not idempotent and
        `name` is not unique. A provisioner that must not double up lists first."""
        first = create_workspace(admin_client, name="Sandbox")
        second = create_workspace(admin_client, name="Sandbox")
        assert first["id"] != second["id"]
        assert admin_client.get("/api/workspaces/").json()["total_count"] == 5

    def test_it_stores_description_is_active_and_data(
        self,
        admin_client: fastapi.testclient.TestClient,
    ) -> None:
        """All three at creation. The seeder this replaced could only write a name."""
        workspace = create_workspace(
            admin_client,
            name="Staging",
            description="Runs against the staging cluster",
            is_active=False,
            data={"instance_url": "https://staging.example.com", "tier": 2},
        )
        assert workspace["description"] == "Runs against the staging cluster"
        assert workspace["is_active"] is False
        assert workspace["data"] == {
            "instance_url": "https://staging.example.com",
            "tier": 2,
        }

    def test_data_survives_a_round_trip_unread(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        """Opaque: nested, mixed and empty values come back exactly as sent."""
        data = {"a": [1, 2, {"b": None}], "c": {"d": True}, "e": "", "f": {}}
        workspace = create_workspace(admin_client, data=data)
        assert (
            admin_client.get(f"/api/workspaces/{workspace['id']}").json()["data"]
            == data
        )

    def test_an_empty_data_object_is_not_folded_to_null(
        self,
        admin_client: fastapi.testclient.TestClient,
    ) -> None:
        """`{}` is a deployment saying it has no extra detail, which is a different statement
        from never having been asked. A client that writes one reads one back."""
        assert create_workspace(admin_client, data={})["data"] == {}

    def test_data_must_be_an_object(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        for value in ([1, 2], "text", 3):
            response = admin_client.post(
                "/api/workspaces/",
                json={"name": "Wrong shape", "data": value},
            )
            assert (
                response.status_code == 422
            ), f"{value!r} was accepted: {response.text}"

    def test_the_service_refuses_a_non_object_too(self, session: orm.Session) -> None:
        """The second guard, bypassing the request model as a non-Pydantic caller would. A list
        would store and then 500 on every read, the response model typing it as an object.
        """
        with pytest.raises(errors.ProjectValidationError, match="data"):
            services.ProjectService().create_workspace(
                session=session,
                name="Wrong shape",
                data=["not", "an", "object"],  # type: ignore[arg-type]
            )

    def test_extra_data_is_refused(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        """A backend-only column: `extra="forbid"` keeps it off the request, and no response
        carries it."""
        assert (
            admin_client.post(
                "/api/workspaces/",
                json={"name": "R", "extra_data": {"tier": 2}},
            ).status_code
            == 422
        )
        assert "extra_data" not in admin_client.get("/api/workspaces/").text

    def test_a_blank_name_is_422(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        assert (
            admin_client.post("/api/workspaces/", json={"name": "   "}).status_code
            == 422
        )

    def test_a_missing_name_is_422(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        assert admin_client.post("/api/workspaces/", json={}).status_code == 422

    def test_a_name_is_stripped(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        assert create_workspace(admin_client, name="  Research  ")["name"] == "Research"

    def test_an_unknown_field_is_422(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        """`extra="forbid"`: an unsupported field is refused rather than silently ignored."""
        assert (
            admin_client.post(
                "/api/workspaces/",
                json={"name": "R", "created_at": "2020-01-01"},
            ).status_code
            == 422
        )

    def test_created_at_is_stamped(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        assert create_workspace(admin_client)["created_at"] is not None

    def test_created_by_is_stamped_from_the_caller(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        assert create_workspace(admin_client)["created_by"] == ADMIN_USER

    def test_created_by_cannot_be_chosen(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        """Attribution, so the caller names themselves by being the caller."""
        response = admin_client.post(
            "/api/workspaces/",
            json={"name": "R", "created_by": OTHER_USER},
        )
        assert response.status_code == 422, response.text

    def test_a_new_workspace_carries_one_timestamp_under_two_names(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        """`updated_at == created_at` exactly, so "has this ever been edited?" is answerable."""
        workspace = create_workspace(admin_client)
        assert workspace["updated_at"] == workspace["created_at"]

    def test_projects_can_be_created_in_a_new_workspace(
        self,
        admin_client: fastapi.testclient.TestClient,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """The whole point of the endpoint, end to end: provision a workspace through the API
        and an ordinary caller can put projects in it, with no seeder and no manual INSERT.
        """
        workspace = create_workspace(admin_client, name="Provisioned")
        project = create_project(
            client, workspace_id=workspace["id"], name="First project"
        )
        assert project["workspace_id"] == workspace["id"]
        assert (
            client.get(f"/api/projects/?workspace_id={workspace['id']}").json()[
                "total_count"
            ]
            == 1
        )

    def test_a_workspace_created_inactive_refuses_projects(
        self,
        admin_client: fastapi.testclient.TestClient,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Provisioned ahead of the deploy that opens it."""
        workspace = create_workspace(admin_client, name="Not yet", is_active=False)
        response = client.post(
            "/api/projects/",
            json={"workspace_id": workspace["id"], "name": "Too early"},
        )
        assert response.status_code == 422, response.text
        assert "no longer accepting" in response.json()["detail"]


class TestUpdateWorkspace:
    def test_it_edits_the_name(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        response = admin_client.patch(
            f"/api/workspaces/{SANDBOX}", json={"name": "Playground"}
        )
        assert response.status_code == 200, response.text
        assert response.json()["name"] == "Playground"
        assert (
            admin_client.get(f"/api/workspaces/{SANDBOX}").json()["name"]
            == "Playground"
        )

    def test_it_edits_data(self, admin_client: fastapi.testclient.TestClient) -> None:
        """The field this endpoint mainly exists for: the instance URL and whatever else a
        deployment has to say, which nothing else can set."""
        body = admin_client.patch(
            f"/api/workspaces/{SANDBOX}",
            json={"data": {"instance_url": "https://sandbox.example.com"}},
        ).json()
        assert body["data"] == {"instance_url": "https://sandbox.example.com"}

    def test_data_is_replaced_not_merged(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        """The backend never inspects this object, so it has no basis for deciding what merging
        would mean. A client changing one key sends back the object it read."""
        admin_client.patch(
            f"/api/workspaces/{SANDBOX}", json={"data": {"a": 1, "b": 2}}
        )
        body = admin_client.patch(
            f"/api/workspaces/{SANDBOX}", json={"data": {"a": 9}}
        ).json()
        assert body["data"] == {"a": 9}

    def test_an_explicit_null_clears_data_and_description(
        self,
        admin_client: fastapi.testclient.TestClient,
    ) -> None:
        admin_client.patch(
            f"/api/workspaces/{SANDBOX}",
            json={"data": {"a": 1}, "description": "Notes"},
        )
        body = admin_client.patch(
            f"/api/workspaces/{SANDBOX}",
            json={"data": None, "description": None},
        ).json()
        assert body["data"] is None
        assert body["description"] is None

    def test_an_omitted_field_is_left_alone(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        """The distinction the whole PATCH convention rests on: omitted is not null."""
        admin_client.patch(
            f"/api/workspaces/{SANDBOX}",
            json={"data": {"a": 1}, "description": "Keep me"},
        )
        body = admin_client.patch(
            f"/api/workspaces/{SANDBOX}", json={"name": "Renamed"}
        ).json()
        assert body["data"] == {"a": 1}
        assert body["description"] == "Keep me"

    def test_an_empty_body_is_a_legal_no_op(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        assert (
            admin_client.patch(f"/api/workspaces/{SANDBOX}", json={}).json()["name"]
            == "Sandbox"
        )

    def test_a_null_name_is_422(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        """NOT NULL, so "explicit null clears it" cannot apply -- and the 422 has to be raised
        before the normaliser meets the None as an AttributeError and a 500."""
        response = admin_client.patch(f"/api/workspaces/{SANDBOX}", json={"name": None})
        assert response.status_code == 422, response.text
        assert "name" in response.json()["detail"]
        assert (
            admin_client.get(f"/api/workspaces/{SANDBOX}").json()["name"] == "Sandbox"
        )

    def test_a_null_is_active_is_422(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        response = admin_client.patch(
            f"/api/workspaces/{SANDBOX}", json={"is_active": None}
        )
        assert response.status_code == 422, response.text
        assert "is_active" in response.json()["detail"]
        assert (
            admin_client.get(f"/api/workspaces/{SANDBOX}").json()["is_active"] is True
        )

    @pytest.mark.parametrize("not_a_bool", ["false", "no", 0, 1, [], {}], ids=repr)
    def test_a_non_boolean_is_active_is_refused_rather_than_coerced(
        self, session: orm.Session, not_a_bool
    ) -> None:
        """Through the service, because that is the only caller that can reach this.

        The route's Pydantic model already settles the question for HTTP -- `"false"` arrives
        as `False`. A script or a test calling `update_workspace` directly does not, and
        `bool("false")` is `True`: the coercing version retired nothing when asked to retire,
        and retired a workspace when asked to activate it with `"no"`. Truthiness is the wrong
        question for a field whose two values are both meaningful.
        """
        with pytest.raises(errors.ProjectValidationError, match="is_active"):
            services.ProjectService().update_workspace(
                session=session,
                workspace_id=SANDBOX,
                updates={"is_active": not_a_bool},
            )

    def test_it_retires_and_reopens_a_workspace(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        assert (
            admin_client.patch(
                f"/api/workspaces/{SANDBOX}", json={"is_active": False}
            ).json()["is_active"]
            is False
        )
        assert (
            admin_client.patch(
                f"/api/workspaces/{SANDBOX}", json={"is_active": True}
            ).json()["is_active"]
            is True
        )

    def test_the_id_cannot_be_changed(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        """Every project in the workspace and every config file that names it point at this id,
        so moving it would rename the row's identity rather than edit it."""
        response = admin_client.patch(
            f"/api/workspaces/{SANDBOX}", json={"id": UNUSED_ID}
        )
        assert response.status_code == 422, response.text
        assert admin_client.get(f"/api/workspaces/{SANDBOX}").status_code == 200

    def test_created_at_cannot_be_changed(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        response = admin_client.patch(
            f"/api/workspaces/{SANDBOX}",
            json={"created_at": "2020-01-01T00:00:00Z"},
        )
        assert response.status_code == 422, response.text

    def test_created_by_cannot_be_changed(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        """Immutable for the reason a project's is: it records who made the row, not who owns it."""
        response = admin_client.patch(
            f"/api/workspaces/{SANDBOX}", json={"created_by": OTHER_USER}
        )
        assert response.status_code == 422, response.text

    def test_updated_at_cannot_be_changed(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        response = admin_client.patch(
            f"/api/workspaces/{SANDBOX}",
            json={"updated_at": "2020-01-01T00:00:00Z"},
        )
        assert response.status_code == 422, response.text

    def test_an_edit_moves_only_updated_at(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        workspace = create_workspace(admin_client)
        edited = admin_client.patch(
            f"/api/workspaces/{workspace['id']}", json={"name": "Renamed"}
        )
        assert edited.status_code == 200, edited.text
        edited = edited.json()
        assert edited["created_at"] == workspace["created_at"]
        assert edited["updated_at"] > workspace["updated_at"]

    def test_an_empty_patch_does_not_move_updated_at(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        """`{}` is a legal no-op, so it must not look like an edit afterwards."""
        workspace = create_workspace(admin_client)
        unchanged = admin_client.patch(
            f"/api/workspaces/{workspace['id']}", json={}
        ).json()
        assert unchanged["updated_at"] == workspace["updated_at"]

    def test_the_service_refuses_an_immutable_field_too(
        self, session: orm.Session
    ) -> None:
        """`extra="forbid"` protects the HTTP edge, `_reject_immutable_fields` protects the
        method, and they fail differently -- which is why both exist."""
        with pytest.raises(errors.ProjectValidationError, match="id"):
            services.ProjectService().update_workspace(
                session=session,
                workspace_id=SANDBOX,
                updates={"id": UNUSED_ID},
            )

    def test_a_missing_workspace_is_404(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        response = admin_client.patch(
            f"/api/workspaces/{MISSING_WORKSPACE_ID}", json={"name": "Ghost"}
        )
        assert response.status_code == 404, response.text


class TestDeleteWorkspace:
    def test_it_deletes_an_empty_workspace(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        workspace = create_workspace(admin_client, name="Short lived")
        response = admin_client.delete(f"/api/workspaces/{workspace['id']}")
        assert response.status_code == 204, response.text
        assert not response.content
        assert admin_client.get(f"/api/workspaces/{workspace['id']}").status_code == 404

    def test_it_refuses_a_workspace_holding_projects(
        self,
        admin_client: fastapi.testclient.TestClient,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Never a cascade. Taking the projects -- and through them every resource on them --
        would make this the largest irreversible operation in the API, behind a request with no
        body at all."""
        project = create_project(client, workspace_id=SANDBOX, name="In the way")
        response = admin_client.delete(f"/api/workspaces/{SANDBOX}")
        assert response.status_code == 409, response.text
        detail = response.json()["detail"]
        assert "1 project" in detail
        assert "is_active" in detail
        assert admin_client.get(f"/api/workspaces/{SANDBOX}").status_code == 200
        assert client.get(f"/api/projects/{project['id']}").status_code == 200

    def test_the_count_it_reports_is_the_real_one(
        self,
        admin_client: fastapi.testclient.TestClient,
        client: fastapi.testclient.TestClient,
    ) -> None:
        create_project(client, workspace_id=SANDBOX, name="One")
        create_project(client, workspace_id=SANDBOX, name="Two")
        create_project(client, workspace_id=INTERNAL, name="Elsewhere")
        refused = admin_client.delete(f"/api/workspaces/{SANDBOX}")
        assert "2 project" in refused.json()["detail"]

    def test_it_succeeds_once_the_projects_are_gone(
        self,
        admin_client: fastapi.testclient.TestClient,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """409 is about the state of the system, not the shape of the request: the same DELETE
        works afterwards."""
        project = create_project(client, workspace_id=SANDBOX)
        create_resource(client, project_id=project["id"])
        refused = admin_client.delete(f"/api/workspaces/{SANDBOX}")
        assert refused.status_code == 409
        project_deleted = client.delete(f"/api/projects/{project['id']}")
        assert project_deleted.status_code == 200
        retried = admin_client.delete(f"/api/workspaces/{SANDBOX}")
        assert retried.status_code == 204

    def test_it_leaves_the_other_workspaces_alone(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        workspace = create_workspace(admin_client, name="Short lived")
        admin_client.delete(f"/api/workspaces/{workspace['id']}")
        assert admin_client.get("/api/workspaces/").json()["total_count"] == 3

    def test_the_row_is_actually_gone(
        self,
        admin_client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """Hard, not soft: this subsystem has no `deleted_at` anywhere, and a workspace is only
        deletable while nothing points at it, so there is no dangling reference for a tombstone
        to keep honest."""
        workspace = create_workspace(admin_client)
        admin_client.delete(f"/api/workspaces/{workspace['id']}")
        assert session.get(db_models.Workspace, workspace["id"]) is None
        assert (
            session.scalar(
                sqlalchemy.select(sqlalchemy.func.count()).select_from(
                    db_models.Workspace
                )
            )
            == 3
        )

    def test_a_missing_workspace_is_404(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        response = admin_client.delete(f"/api/workspaces/{MISSING_WORKSPACE_ID}")
        assert response.status_code == 404


class TestWorkspaceWriteGuards:
    """Who may write a workspace: an admin, on a deployment that is not read-only.

    The ordinary `client` has `write` permission and is not an admin, which is the case these
    routes exist to close -- a workspace is a shared axis of the deployment, not something an
    individual writer changes.
    """

    def test_a_writer_without_admin_cannot_create_one(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        response = client.post("/api/workspaces/", json={"name": "Mine"})
        assert response.status_code == 403, response.text
        assert "admin" in response.json()["detail"]
        assert client.get("/api/workspaces/").json()["total_count"] == 3

    def test_a_writer_without_admin_cannot_edit_one(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        assert (
            client.patch(
                f"/api/workspaces/{SANDBOX}", json={"name": "Mine"}
            ).status_code
            == 403
        )
        assert client.get(f"/api/workspaces/{SANDBOX}").json()["name"] == "Sandbox"

    def test_a_writer_without_admin_cannot_delete_one(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        refused = client.delete(f"/api/workspaces/{SANDBOX}")
        assert refused.status_code == 403
        assert client.get(f"/api/workspaces/{SANDBOX}").status_code == 200

    def test_a_non_admin_can_still_read_them(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Reads are not gated: a workspace picker has to work for everyone."""
        assert client.get("/api/workspaces/").status_code == 200
        assert client.get(f"/api/workspaces/{SANDBOX}").status_code == 200

    def test_a_caller_without_write_permission_is_403(
        self, no_write_client: fastapi.testclient.TestClient
    ) -> None:
        assert (
            no_write_client.post("/api/workspaces/", json={"name": "Mine"}).status_code
            == 403
        )

    def test_admin_writes_are_refused_in_read_only_mode(
        self,
        read_only_admin_client: fastapi.testclient.TestClient,
    ) -> None:
        """503 rather than 403: being an admin is not the thing standing in the way."""
        assert (
            read_only_admin_client.post(
                "/api/workspaces/", json={"name": "Mine"}
            ).status_code
            == 503
        )
        assert (
            read_only_admin_client.patch(
                f"/api/workspaces/{SANDBOX}", json={"name": "M"}
            ).status_code
            == 503
        )
        deleted = read_only_admin_client.delete(f"/api/workspaces/{SANDBOX}")
        assert deleted.status_code == 503

    def test_reads_still_work_in_read_only_mode(
        self,
        read_only_admin_client: fastapi.testclient.TestClient,
    ) -> None:
        assert read_only_admin_client.get("/api/workspaces/").status_code == 200


class TestInactiveWorkspaces:
    """Retired, not removed. It still lists and still resolves; it just takes no new projects.

    The one way to take a workspace out of use while it still holds projects, `DELETE` refusing
    those.
    """

    @staticmethod
    def _retire(client: fastapi.testclient.TestClient, workspace_id: str) -> None:
        response = client.patch(
            f"/api/workspaces/{workspace_id}", json={"is_active": False}
        )
        assert response.status_code == 200, response.text

    def test_an_inactive_workspace_is_still_listed(
        self,
        client: fastapi.testclient.TestClient,
        admin_client: fastapi.testclient.TestClient,
    ) -> None:
        """Hidden from a picker is the UI's call. Absent from the API is not, because the
        projects already inside it are reachable and their workspace has to resolve."""
        self._retire(admin_client, SANDBOX)
        body = client.get("/api/workspaces/").json()
        assert body["total_count"] == 3
        sandbox = next(
            workspace for workspace in body["workspaces"] if workspace["id"] == SANDBOX
        )
        assert sandbox["is_active"] is False
        assert client.get(f"/api/workspaces/{SANDBOX}").status_code == 200

    def test_it_refuses_a_new_project_with_422_not_404(
        self,
        client: fastapi.testclient.TestClient,
        admin_client: fastapi.testclient.TestClient,
    ) -> None:
        """404 would be a lie: it exists. 422 says the body named a workspace that cannot take
        this, which the caller fixes by picking another."""
        self._retire(admin_client, SANDBOX)
        response = client.post(
            "/api/projects/", json={"workspace_id": SANDBOX, "name": "Too late"}
        )
        assert response.status_code == 422, response.text
        assert "no longer accepting" in response.json()["detail"]

    def test_projects_already_in_it_keep_working(
        self,
        client: fastapi.testclient.TestClient,
        admin_client: fastapi.testclient.TestClient,
    ) -> None:
        created = client.post(
            "/api/projects/",
            json={"workspace_id": SANDBOX, "name": "Grandfathered"},
        ).json()
        self._retire(admin_client, SANDBOX)
        assert client.get(f"/api/projects/{created['id']}").status_code == 200
        assert (
            client.patch(
                f"/api/projects/{created['id']}", json={"name": "Renamed"}
            ).status_code
            == 200
        )
        assert (
            client.get(f"/api/projects/?workspace_id={SANDBOX}").json()["total_count"]
            == 1
        )
