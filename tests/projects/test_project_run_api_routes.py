"""A project's run feed: `GET /api/projects/{project_id}/runs`.

Runs are the one thing a project holds that is not a `project_resource` row: nothing is
attached, and the feed comes entirely from an annotation the submitter puts on the run. So a run
reaches a project from the UI, the CLI, an agent, a trigger or the scheduler without any of them
knowing this subsystem exists.

The design called for a `system/pipeline_run.project_id` key, which would not have worked:
`api_server_sql._mirror_single_pipeline_run_annotation` silently skips that prefix, so the
annotation would be stored on the run and never copied to `pipeline_run_annotation`, the table
the filter reads -- an empty feed forever with nothing raising.
`test_a_system_prefixed_key_would_not_have_worked` pins that down so it is not "simplified" back.

The key carries the project id (`.../project/id/<id>`) and the value is a marker, which is not
how a one-project-per-run feature would naturally be spelled. `TestTheKeyShapeLeavesRoomForManyToMany`
is why: the key is a client contract on two submit routes, and this shape is the one that does
not have to change if runs are ever allowed in several projects.
"""

import base64
import datetime
import json
import re

import fastapi.testclient
import pytest
import sqlalchemy
from cloud_pipelines_backend import api_server_sql, filter_query_sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.projects import services
from cloud_pipelines_backend.user_pipelines import pipeline_run_annotations
from cloud_pipelines_backend.utils import db as db_utils
from sqlalchemy import orm

from tests import sql_capture
from tests.projects.conftest import (
    DEFAULT_USER,
    MISSING_WORKSPACE_ID,
    add_run,
    create_project,
)


class TestListProjectRuns:
    def test_it_returns_the_runs_annotated_with_this_project(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        project = create_project(client)
        other = create_project(client, name="Somewhere else")
        mine = add_run(session, name="mine", project_id=project["id"])
        add_run(session, name="theirs", project_id=other["id"])
        add_run(session, name="unattributed")

        body = client.get(f"/api/projects/{project['id']}/runs").json()
        assert [run["id"] for run in body["runs"]] == [mine]
        assert body["runs"][0]["created_by"] == DEFAULT_USER
        assert body["runs"][0]["pipeline_name"] == "mine"

    def test_a_project_with_no_runs_is_an_empty_feed(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        project = create_project(client)
        assert client.get(f"/api/projects/{project['id']}/runs").json() == {
            "runs": [],
            "next_page_token": None,
        }

    def test_an_unknown_project_is_404_not_an_empty_feed(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Different answers, acted on differently. An empty feed says "nothing has run yet"."""
        assert (
            client.get(f"/api/projects/{MISSING_WORKSPACE_ID}/runs").status_code == 404
        )

    def test_an_unparseable_project_id_is_404_too(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Ids are opaque, so there is no shape to fail -- only a project to miss."""
        assert client.get("/api/projects/not-an-id/runs").status_code == 404

    def test_newest_first(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        project = create_project(client)
        ids = [
            add_run(session, name=f"run-{index}", project_id=project["id"])
            for index in range(3)
        ]
        body = client.get(f"/api/projects/{project['id']}/runs").json()
        assert [run["id"] for run in body["runs"]] == list(reversed(ids))

    def test_it_pages_at_the_run_services_size_and_the_token_walks_the_rest(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """No `page_size` parameter: the page is `PipelineRunsApiService_Sql._DEFAULT_PAGE_SIZE`
        and the token is that service's own OFFSET cursor, passed back unchanged. Walking the
        whole feed is the assertion that matters -- the token has to carry the project filter
        forward, which it does by storing the compiled `filter_query` inside itself."""
        project = create_project(client)
        created = {
            add_run(session, name=f"run-{index}", project_id=project["id"])
            for index in range(12)
        }

        seen: list[str] = []
        page_token: str | None = None
        for _ in range(5):
            params = {"page_token": page_token} if page_token else {}
            body = client.get(
                f"/api/projects/{project['id']}/runs", params=params
            ).json()
            seen.extend(run["id"] for run in body["runs"])
            page_token = body["next_page_token"]
            if not page_token:
                break

        assert len(seen) == len(set(seen)) == 12
        assert set(seen) == created

    def test_the_project_annotation_is_on_the_run_itself(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """Returned in `annotations`, so a client rendering a run can show which project it
        was for without a second lookup.

        The id is read off the key, not the value, so "which projects is this run in" is a
        prefix scan of the map -- the same answer for one project as for several.
        """
        project = create_project(client)
        add_run(session, name="mine", project_id=project["id"])
        run = client.get(f"/api/projects/{project['id']}/runs").json()["runs"][0]
        assert run["annotations"][
            pipeline_run_annotations.project_run_key(project["id"])
        ] == (pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE)

    def test_deleting_the_project_does_not_delete_its_runs(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """A run is a record of something that happened and outlives the grouping it was filed
        under. The annotation is left pointing at an id that no longer resolves, which is the
        honest state: it is still true that the run was submitted for that project."""
        project = create_project(client)
        run_id = add_run(session, name="mine", project_id=project["id"])
        deleted = client.delete(f"/api/projects/{project['id']}")
        assert deleted.status_code == 200
        assert session.get(bts.PipelineRun, run_id) is not None


class TestPageTokensAreBoundToTheProject:
    """The vendored run list prefers a token's embedded filter over anything the caller sends
    (`filter_query_sql._resolve_filter_value`), so on pages after the first the token *is* the
    query -- and it is unauthenticated base64 JSON anyone can mint. The route therefore refuses
    a token whose embedded filter does not name the project in the URL. The legitimate walk is
    `test_it_pages_at_the_run_services_size_and_the_token_walks_the_rest` above.
    """

    def test_a_token_from_another_projects_feed_is_refused(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """Otherwise project A's URL would happily serve project B's runs from page 2 on."""
        mine = create_project(client, name="Mine")
        other = create_project(client, name="Other")
        for index in range(12):
            add_run(session, name=f"other-run-{index}", project_id=other["id"])
        token = client.get(f"/api/projects/{other['id']}/runs").json()[
            "next_page_token"
        ]
        assert token, "the setup needs enough runs to mint a second page"

        response = client.get(
            f"/api/projects/{mine['id']}/runs", params={"page_token": token}
        )
        assert response.status_code == 422, response.text
        assert "page_token" in response.json()["detail"]

    def test_a_token_with_no_filter_at_all_is_refused(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """The shape the unfiltered `GET /api/pipeline_runs/` list mints: `filter_query` null.
        Passed through, it would serve every run on the deployment under this project's URL.
        """
        project = create_project(client)
        add_run(session, name="not-in-any-project", project_id=None)
        token = base64.b64encode(
            json.dumps({"offset": 0, "filter_query": None}).encode()
        ).decode()

        response = client.get(
            f"/api/projects/{project['id']}/runs", params={"page_token": token}
        )
        assert response.status_code == 422, response.text

    def test_garbage_is_422_not_500(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        project = create_project(client)
        response = client.get(
            f"/api/projects/{project['id']}/runs",
            params={"page_token": "not-a-token"},
        )
        assert response.status_code == 422, response.text

    def test_a_token_carrying_an_extra_predicate_is_refused(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """Containment was not enough. This token names the project *and* narrows by
        `created_by`, which the generic `GET /api/pipeline_runs/?filter_query=` will mint for
        anyone. Accepted, `list` prefers the token's copy and this URL serves a narrower feed --
        runs missing, `total_count` narrowed with them, and a 200 over the top of it."""
        project = create_project(client)
        for index in range(12):
            add_run(session, name=f"run-{index}", project_id=project["id"])
        smuggled = json.dumps(
            {
                "and": [
                    {
                        "key_exists": {
                            "key": pipeline_run_annotations.project_run_key(
                                project["id"]
                            )
                        }
                    },
                    {
                        "value_equals": {
                            "key": "system/pipeline_run.created_by",
                            "value": "someone-else",
                        }
                    },
                ]
            }
        )
        token = base64.b64encode(
            json.dumps({"offset": 0, "filter_query": smuggled}).encode()
        ).decode()

        response = client.get(
            f"/api/projects/{project['id']}/runs", params={"page_token": token}
        )
        assert response.status_code == 422, response.text
        assert "page_token" in response.json()["detail"]

    def test_a_token_naming_the_project_twice_is_refused(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Exactly one project predicate. Two is a shape this route cannot compile, so whatever
        minted it was not this feed."""
        project = create_project(client)
        key = pipeline_run_annotations.project_run_key(project["id"])
        doubled = json.dumps(
            {
                "and": [
                    {"key_exists": {"key": key}},
                    {"key_exists": {"key": key}},
                ]
            }
        )
        token = base64.b64encode(
            json.dumps({"offset": 0, "filter_query": doubled}).encode()
        ).decode()

        response = client.get(
            f"/api/projects/{project['id']}/runs", params={"page_token": token}
        )
        assert response.status_code == 422, response.text

    def test_a_token_with_no_time_range_is_refused(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """The window is mandatory, not merely usual -- the gap a containment check left open.

        `_run_window` always produces a lower bound, so a token this feed issued always carries
        exactly one `created_at` `time_range`. A hand-built token naming only the project is
        otherwise well-formed, and the vendored resolver prefers the token's filter, so it would
        run the feed across all time: cheap to mint and expensive to serve.
        """
        project = create_project(client)
        for index in range(12):
            add_run(session, name=f"run-{index}", project_id=project["id"])
        unbounded = json.dumps(
            {
                "and": [
                    {
                        "key_exists": {
                            "key": pipeline_run_annotations.project_run_key(
                                project["id"]
                            )
                        }
                    }
                ]
            }
        )
        token = base64.b64encode(
            json.dumps({"offset": 0, "filter_query": unbounded}).encode()
        ).decode()

        response = client.get(
            f"/api/projects/{project['id']}/runs", params={"page_token": token}
        )
        assert response.status_code == 422, response.text
        assert "page_token" in response.json()["detail"]

    def test_a_token_whose_window_has_no_lower_bound_is_refused(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """A `time_range` is not enough; it has to be a *lower* bound.

        The vendored `TimeRange` takes `start_time`, `end_time` or both, so an end-only range is
        a well-formed predicate that compiles to an upper bound and nothing else -- the walk to
        the beginning of time that `_DEFAULT_RUN_WINDOW` exists to prevent, reached by a token
        that carries a `created_at` `time_range` and so passes a key-only check.
        """
        project = create_project(client)
        add_run(
            session,
            name="old",
            project_id=project["id"],
            age=datetime.timedelta(days=400),
        )
        end_only = json.dumps(
            {
                "and": [
                    {
                        "key_exists": {
                            "key": pipeline_run_annotations.project_run_key(
                                project["id"]
                            )
                        }
                    },
                    {
                        "time_range": {
                            "key": services._RUN_CREATED_AT_KEY,
                            "end_time": "2027-01-01T00:00:00Z",
                        }
                    },
                ]
            }
        )
        token = base64.b64encode(
            json.dumps({"offset": 0, "filter_query": end_only}).encode()
        ).decode()

        response = client.get(
            f"/api/projects/{project['id']}/runs", params={"page_token": token}
        )
        assert response.status_code == 422, response.text
        assert "page_token" in response.json()["detail"]

    def test_a_token_with_a_null_start_time_is_refused(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """The same gap spelled with an explicit null rather than an absent key."""
        project = create_project(client)
        nulled = json.dumps(
            {
                "and": [
                    {
                        "key_exists": {
                            "key": pipeline_run_annotations.project_run_key(
                                project["id"]
                            )
                        }
                    },
                    {
                        "time_range": {
                            "key": services._RUN_CREATED_AT_KEY,
                            "start_time": None,
                            "end_time": "2027-01-01T00:00:00Z",
                        }
                    },
                ]
            }
        )
        token = base64.b64encode(
            json.dumps({"offset": 0, "filter_query": nulled}).encode()
        ).decode()

        response = client.get(
            f"/api/projects/{project['id']}/runs", params={"page_token": token}
        )
        assert response.status_code == 422, response.text

    @pytest.mark.parametrize(
        "start_time", ["banana", "2026-01-01T00:00:00", {"nested": 1}, []]
    )
    def test_a_token_whose_lower_bound_is_not_an_aware_datetime_is_refused(
        self,
        client: fastapi.testclient.TestClient,
        start_time: object,
    ) -> None:
        """Non-null is not enough; it has to be a bound the compiler can compile.

        The guard parses the body with the vendored `TimeRange`, so a wrong *type* is a foreign
        token here. Duck-typed on presence, each of these reached
        `FilterQuery.model_validate_json` inside the vendored list and raised a
        `ValidationError` there -- a 500 echoing pydantic, off an unauthenticated token, where
        this route documents a 422. The naive datetime is the same defect without the garbage:
        `TimeRange` takes `AwareDatetime` only, and `_run_window` refuses naive input at the
        edge for the same reason.
        """
        project = create_project(client)
        mistyped = json.dumps(
            {
                "and": [
                    {
                        "key_exists": {
                            "key": pipeline_run_annotations.project_run_key(
                                project["id"]
                            )
                        }
                    },
                    {
                        "time_range": {
                            "key": services._RUN_CREATED_AT_KEY,
                            "start_time": start_time,
                        }
                    },
                ]
            }
        )
        token = base64.b64encode(
            json.dumps({"offset": 0, "filter_query": mistyped}).encode()
        ).decode()

        response = client.get(
            f"/api/projects/{project['id']}/runs", params={"page_token": token}
        )
        assert response.status_code == 422, response.text
        assert "page_token" in response.json()["detail"]

    @pytest.mark.parametrize("offset", ["banana", None, -1, 1.5])
    def test_a_token_whose_offset_is_not_a_count_is_refused(
        self,
        client: fastapi.testclient.TestClient,
        offset: object,
    ) -> None:
        """The token's other slot, unguarded for the same reason and checked for it.

        The vendored pager does `offset + page_size` on whatever it finds, so a string or a
        null raised a `TypeError` two layers down -- again a 500 off an unauthenticated token.
        A negative offset is worse than an error: SQLite accepts it and MySQL refuses it, so it
        is the dialect split rather than the crash that a test here can see.
        """
        project = create_project(client)
        legitimate = json.dumps(
            {
                "and": [
                    {
                        "key_exists": {
                            "key": pipeline_run_annotations.project_run_key(
                                project["id"]
                            )
                        }
                    },
                    {
                        "time_range": {
                            "key": services._RUN_CREATED_AT_KEY,
                            "start_time": "2026-01-01T00:00:00Z",
                        }
                    },
                ]
            }
        )
        token = base64.b64encode(
            json.dumps({"offset": offset, "filter_query": legitimate}).encode()
        ).decode()

        response = client.get(
            f"/api/projects/{project['id']}/runs", params={"page_token": token}
        )
        assert response.status_code == 422, response.text
        assert "page_token" in response.json()["detail"]

    def test_a_token_whose_offset_is_larger_than_any_real_walk_is_refused(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """A whole non-negative count is not enough; it has to be one a page could sit at.

        `10**100` is a valid Python `int` and passes every type check, so it reached the driver
        as an `OFFSET` bind and raised `OverflowError: Python int too large to convert to SQLite
        INTEGER` -- a 500 off an unauthenticated token. Below the driver's ceiling the same
        token buys an arbitrarily deep scan, which is why the bound is
        `_MAX_PAGE_TOKEN_OFFSET` rather than the largest integer the driver survives.
        """
        project = create_project(client)
        legitimate = json.dumps(
            {
                "and": [
                    {
                        "key_exists": {
                            "key": pipeline_run_annotations.project_run_key(
                                project["id"]
                            )
                        }
                    },
                    {
                        "time_range": {
                            "key": services._RUN_CREATED_AT_KEY,
                            "start_time": "2026-01-01T00:00:00Z",
                        }
                    },
                ]
            }
        )
        token = base64.b64encode(
            json.dumps({"offset": 10**100, "filter_query": legitimate}).encode()
        ).decode()

        response = client.get(
            f"/api/projects/{project['id']}/runs", params={"page_token": token}
        )
        assert response.status_code == 422, response.text
        assert "page_token" in response.json()["detail"]

    def test_a_token_with_two_time_ranges_is_refused(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Two windows would let a token widen one bound while keeping the other, and the
        vendored compiler ANDs them rather than refusing. One is the only legal count.
        """
        project = create_project(client)
        key = pipeline_run_annotations.project_run_key(project["id"])
        created_at = services._RUN_CREATED_AT_KEY
        doubled = json.dumps(
            {
                "and": [
                    {"key_exists": {"key": key}},
                    {
                        "time_range": {
                            "key": created_at,
                            "start": "2026-01-01T00:00:00Z",
                        }
                    },
                    {
                        "time_range": {
                            "key": created_at,
                            "start": "2020-01-01T00:00:00Z",
                        }
                    },
                ]
            }
        )
        token = base64.b64encode(
            json.dumps({"offset": 0, "filter_query": doubled}).encode()
        ).decode()

        response = client.get(
            f"/api/projects/{project['id']}/runs", params={"page_token": token}
        )
        assert response.status_code == 422, response.text

    def test_a_real_token_from_this_feed_round_trips(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """The tripwire for core API drift.

        `_reject_foreign_run_page_token` reads a private vendored format: base64 JSON, the
        filter under `_PAGE_TOKEN_FILTER_QUERY_KEY`, its predicates a JSON-encoded
        `{"and": [...]}`. If any of that changes, page 1 keeps working -- it decodes no token --
        and every continuation 422s, so a test that never paginates stays green. This one mints
        a real token and walks it, so the drift surfaces at bump time.
        """
        project = create_project(client)
        for index in range(12):
            add_run(session, name=f"run-{index}", project_id=project["id"])

        first = client.get(f"/api/projects/{project['id']}/runs").json()
        token = first["next_page_token"]
        assert token, "the setup needs enough runs to mint a second page"

        # The guard's own assumptions, asserted directly: a failure here names the drift instead
        # of surfacing as a puzzling 422 from the walk below.
        decoded = json.loads(base64.b64decode(token))
        assert services._PAGE_TOKEN_FILTER_QUERY_KEY in decoded, decoded
        predicates = json.loads(decoded[services._PAGE_TOKEN_FILTER_QUERY_KEY])["and"]
        assert services._is_project_run_filter(
            predicates=predicates,
            expected_key=pipeline_run_annotations.project_run_key(project["id"]),
        ), predicates

        second = client.get(
            f"/api/projects/{project['id']}/runs", params={"page_token": token}
        )
        assert second.status_code == 200, second.text
        assert second.json()["runs"], "the second page should not be empty"

    def test_a_token_missing_the_filter_slot_reads_as_format_drift(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """A token with no `filter_query` key at all cannot have come from the vendored encoder,
        which always writes that slot. So the refusal points at the format rather than blaming
        the caller's token -- the message a reader needs when a core API change breaks paging.
        """
        project = create_project(client)
        token = base64.b64encode(json.dumps({"offset": 10}).encode()).decode()

        response = client.get(
            f"/api/projects/{project['id']}/runs", params={"page_token": token}
        )
        assert response.status_code == 422, response.text
        assert "format has changed" in response.json()["detail"]

    @pytest.mark.parametrize(
        "extra_slot",
        [
            filter_query_sql._PAGE_TOKEN_FILTER_KEY,
            "not_a_slot_we_issue",
        ],
        ids=["legacy-filter", "unknown"],
    )
    def test_a_token_carrying_a_slot_this_feed_never_issues_is_refused(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
        extra_slot: str,
    ) -> None:
        """Checking the filter is not enough: the reader honours slots this module never writes.

        Every other test in this class attacks `filter_query`, which the guard inspects
        predicate by predicate. This one leaves a genuine token for *this* project untouched and
        adds a second slot beside it -- so the filter check passes and only the allow-list can
        refuse it. The legacy `filter` slot is the one that matters: the vendored reader lets it
        *replace* the filter query outright, which is the whole guard bypassed by one extra key
        in a token anyone can mint.
        """
        project = create_project(client)
        for index in range(12):
            add_run(session, name=f"run-{index}", project_id=project["id"])

        first = client.get(f"/api/projects/{project['id']}/runs").json()
        token = first["next_page_token"]
        assert token, "the setup needs enough runs to mint a second page"

        decoded = json.loads(base64.b64decode(token))
        tampered = base64.b64encode(
            json.dumps({**decoded, extra_slot: "anything"}).encode()
        ).decode()

        response = client.get(
            f"/api/projects/{project['id']}/runs",
            params={"page_token": tampered},
        )
        assert response.status_code == 422, response.text
        assert "not issued by this project" in response.json()["detail"]

    def test_an_empty_page_token_is_422_not_page_one(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """`?page_token=` present and empty is a malformed token. Answered as page 1 it would
        turn a client that echoes a null `next_page_token` as `""` into an endless loop.
        """
        project = create_project(client)
        response = client.get(
            f"/api/projects/{project['id']}/runs", params={"page_token": ""}
        )
        assert response.status_code == 422, response.text


class TestWhyTheKeyIsNotASystemKey:
    def test_the_project_key_is_mirrored_into_the_filterable_table(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """The filter reads `pipeline_run_annotation`, not the run's own JSON blob. An
        annotation that never reaches that table is invisible to every query."""
        project = create_project(client)
        add_run(session, name="mine", project_id=project["id"])
        mirrored = session.scalars(
            bts.PipelineRunAnnotation.__table__.select().with_only_columns(
                bts.PipelineRunAnnotation.key
            )
        ).all()
        assert pipeline_run_annotations.project_run_key(project["id"]) in set(mirrored)

    def test_a_system_prefixed_key_would_not_have_worked(self) -> None:
        """`_mirror_single_pipeline_run_annotation` returns early on the `system/` prefix, so
        such an annotation would be stored on the run and never mirrored -- an empty feed with
        no error anywhere. Reads the core API's source rather than exercising it, the failure
        being an absence rather than a behaviour.
        """
        import inspect

        from cloud_pipelines_backend import (
            api_server_sql,
            filter_query_sql,
        )

        source = inspect.getsource(
            api_server_sql._mirror_single_pipeline_run_annotation
        )
        assert "SYSTEM_KEY_PREFIX" in source
        assert "return" in source
        assert not pipeline_run_annotations.PROJECT_ANNOTATION_PREFIX.startswith(
            filter_query_sql.SYSTEM_KEY_PREFIX
        )


class TestTheFeedIsNotAResourceListing:
    def test_a_run_does_not_appear_in_the_resource_list(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """No `run` entity, no row, no count. Runs are created continuously and a table that
        grew one per run would be a second copy of `pipeline_run` that nothing keeps in step.
        """
        project = create_project(client)
        add_run(session, name="mine", project_id=project["id"])
        assert (
            client.get(f"/api/projects/{project['id']}/resources/").json()[
                "total_count"
            ]
            == 0
        )
        assert (
            client.get(f"/api/projects/{project['id']}").json()["resource_counts"] == {}
        )

    def test_run_is_not_a_legal_entity(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        project = create_project(client)
        response = client.post(
            f"/api/projects/{project['id']}/resources/",
            json={"entity": "run", "entity_id": MISSING_WORKSPACE_ID},
        )
        assert response.status_code == 422, response.text


class TestTheFilterIsBuiltNotHandWritten:
    def test_it_delegates_to_the_run_service(
        self, session: orm.Session, client: fastapi.testclient.TestClient
    ) -> None:
        """A direct service call, pinning the return type this route wraps. If it stops being
        `ListPipelineJobsResponse`, the route is no longer borrowing the run feed."""
        project = create_project(client)
        page = services.ProjectService().list_project_runs(
            session=session, project_id=project["id"]
        )
        assert page.pipeline_runs == []
        assert page.next_page_token is None


class TestTheKeyShapeLeavesRoomForManyToMany:
    """Why the project id is in the key and not in the value.

    Many-to-many is not a feature here: the API holds a run to one project, and the UI to one
    project per run. What these assert is that the *storage* under it is not the thing standing
    in the way -- so choosing it now costs a predicate and buys not having to change a wire
    contract that clients will have shipped against.

    Kept as tests because the claim is cheap to break: a later "simplification" back to a
    constant key with the id in the value would pass every other test in this file. So each
    case carries a control that is in one project only -- under a constant key both feeds
    compile to the same predicate and every run reaches both.
    """

    @staticmethod
    def _add_membership(session: orm.Session, *, run_id: str, project_id: str) -> None:
        """A second project on an existing run, through the seam a membership route would use.

        `set_annotation` rather than a hand-built row, so this also asserts that no new write
        path has to be invented.
        """
        api_server_sql.PipelineRunsApiService_Sql().set_annotation(
            session=session,
            id=run_id,
            key=pipeline_run_annotations.project_run_key(project_id),
            value=pipeline_run_annotations.PROJECT_MEMBERSHIP_VALUE,
            user_name=DEFAULT_USER,
        )

    def test_two_memberships_are_two_rows_rather_than_one_overwritten(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """`pipeline_run_annotation` is keyed `(pipeline_run_id, key)`, so two project keys are
        two rows. Under a constant key the second write would `merge` onto the first and leave
        one row, which is the whole reason the id moved into the key -- so the count is the
        assertion, not the presence of the keys."""
        first = create_project(client, name="first")
        second = create_project(client, name="second")
        run_id = add_run(session, name="shared", project_id=first["id"])

        self._add_membership(session, run_id=run_id, project_id=second["id"])

        keys = sorted(
            session.scalars(
                sqlalchemy.select(bts.PipelineRunAnnotation.key).where(
                    bts.PipelineRunAnnotation.pipeline_run_id == run_id,
                    bts.PipelineRunAnnotation.key.startswith(
                        pipeline_run_annotations.PROJECT_ANNOTATION_PREFIX
                    ),
                )
            )
        )
        assert keys == sorted(
            [
                pipeline_run_annotations.project_run_key(first["id"]),
                pipeline_run_annotations.project_run_key(second["id"]),
            ]
        )

    def test_a_second_membership_widens_one_feed_without_widening_the_other(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """The feed predicate is already the many-to-many one: `key_exists` on a per-project
        key asks the same question for a run in one project as for a run in five, so what is
        left for many-to-many is the membership route, not the query or the schema.

        `only_first` is the control: a constant key would put both runs in both feeds and
        satisfy the shared-run half alone.
        """
        first = create_project(client, name="first")
        second = create_project(client, name="second")
        only_first = add_run(session, name="only-first", project_id=first["id"])
        shared = add_run(session, name="shared", project_id=first["id"])

        self._add_membership(session, run_id=shared, project_id=second["id"])

        def feed(project_id: str) -> set[str]:
            return {
                run["id"]
                for run in client.get(f"/api/projects/{project_id}/runs").json()["runs"]
            }

        assert feed(first["id"]) == {only_first, shared}
        assert feed(second["id"]) == {shared}

    def test_one_project_does_not_pick_up_a_run_filed_under_another(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """The plain negative, which is also what fails first if the key stops carrying the
        id -- a key that is the same string for every project shows up here."""
        first = create_project(client, name="first")
        second = create_project(client, name="second")
        add_run(session, name="only-first", project_id=first["id"])
        assert client.get(f"/api/projects/{second['id']}/runs").json()["runs"] == []


class TestTheCreatedAtWindow:
    """The bound that keeps the feed's cost independent of how much history exists.

    Nothing indexes `pipeline_run_annotation (key)` -- the one index leads with
    `pipeline_run_id` -- so the `EXISTS` this filter compiles to cannot be driven from the
    annotation. The planner walks `pipeline_run` newest-first and probes each row, worst case to
    the beginning of time to prove an empty page. `since` stops that, as a default not a cap.
    """

    def test_a_run_older_than_the_default_window_is_absent(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        project = create_project(client)
        recent = add_run(session, name="recent", project_id=project["id"])
        add_run(
            session,
            name="ancient",
            project_id=project["id"],
            age=datetime.timedelta(days=31),
        )

        body = client.get(f"/api/projects/{project['id']}/runs").json()
        assert [run["id"] for run in body["runs"]] == [recent]

    def test_a_run_inside_the_default_window_is_present(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """The other side of the boundary, so the test above cannot pass by filtering everything."""
        project = create_project(client)
        recent = add_run(
            session,
            name="recent",
            project_id=project["id"],
            age=datetime.timedelta(days=29),
        )
        assert [
            run["id"]
            for run in client.get(f"/api/projects/{project['id']}/runs").json()["runs"]
        ] == [recent]

    def test_an_explicit_since_reaches_the_full_history(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """The window is a default, not a cap: there is always a way to ask for everything."""
        project = create_project(client)
        recent = add_run(session, name="recent", project_id=project["id"])
        ancient = add_run(
            session,
            name="ancient",
            project_id=project["id"],
            age=datetime.timedelta(days=900),
        )

        body = client.get(
            f"/api/projects/{project['id']}/runs",
            params={"since": "2000-01-01T00:00:00Z"},
        ).json()
        assert [run["id"] for run in body["runs"]] == [recent, ancient]

    def test_until_closes_the_top_of_the_window(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        project = create_project(client)
        add_run(session, name="today", project_id=project["id"])
        older = add_run(
            session,
            name="last week",
            project_id=project["id"],
            age=datetime.timedelta(days=7),
        )

        cutoff = (db_utils.utc_now() - datetime.timedelta(days=1)).isoformat()
        body = client.get(
            f"/api/projects/{project['id']}/runs", params={"until": cutoff}
        ).json()
        assert [run["id"] for run in body["runs"]] == [older]

    def test_a_naive_since_is_422(self, client: fastapi.testclient.TestClient) -> None:
        """`pipeline_run.created_at` is UTC with no label, so a bound without an offset means
        whatever the caller's clock meant. Refused rather than guessed."""
        project = create_project(client)
        response = client.get(
            f"/api/projects/{project['id']}/runs",
            params={"since": "2026-01-01T00:00:00"},
        )
        assert response.status_code == 422, response.text
        assert "timezone" in response.json()["detail"]

    def test_a_naive_until_is_422(self, client: fastapi.testclient.TestClient) -> None:
        project = create_project(client)
        response = client.get(
            f"/api/projects/{project['id']}/runs",
            params={"until": "2026-01-01T00:00:00"},
        )
        assert response.status_code == 422, response.text

    def test_since_after_until_is_422(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """An empty window by arithmetic. Answered as the mistake it is rather than as no runs."""
        project = create_project(client)
        response = client.get(
            f"/api/projects/{project['id']}/runs",
            params={
                "since": "2026-06-01T00:00:00Z",
                "until": "2026-01-01T00:00:00Z",
            },
        )
        assert response.status_code == 422, response.text

    def test_since_equal_to_until_is_422(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """`until` is exclusive -- the vendored `_time_range_to_clause` compiles `created_at <
        end` -- so an equal pair is `>= X AND < X` and matches nothing. Refused rather than
        answered: the run below is inside every other reading of this window, so without the
        guard the caller gets an empty page indistinguishable from a project with no runs.
        """
        project = create_project(client)
        add_run(
            session,
            name="present under any sane window",
            project_id=project["id"],
        )
        bound = "2026-06-01T00:00:00Z"

        response = client.get(
            f"/api/projects/{project['id']}/runs",
            params={"since": bound, "until": bound},
        )
        assert response.status_code == 422, response.text
        assert "exclusive" in response.json()["detail"]

    def test_an_until_near_the_start_of_the_epoch_is_422_not_500(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """`?until=` alone anchors the default window by subtracting from it, and for any `until`
        within that window of `datetime.min` the subtraction has no representable answer.

        `OverflowError` is not a `ProjectError`, so this fell through to `app.py`'s catch-all as a
        500 and a logged exception. Reachable with nothing but `read` on a project, and reachable
        by accident: Go's zero `time.Time` marshals to exactly this string, as does an unset date
        picker in more than one UI. Every other bad `until` on this route is a 422.
        """
        project = create_project(client)
        response = client.get(
            f"/api/projects/{project['id']}/runs",
            params={"until": "0001-01-01T00:00:00Z"},
        )
        assert response.status_code == 422, response.text
        assert "until" in response.json()["detail"]

    def test_an_until_near_the_start_of_the_epoch_is_accepted_with_an_explicit_since(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """The refusal above is about the default window, not about the date. Named as a pair,
        year one needs no arithmetic and is an ordinary empty window."""
        response = client.get(
            f"/api/projects/{create_project(client)['id']}/runs",
            params={
                "since": "0001-01-01T00:00:00Z",
                "until": "0001-01-02T00:00:00Z",
            },
        )
        assert response.status_code == 200, response.text
        assert response.json()["runs"] == []

    def test_until_alone_anchors_the_default_window_rather_than_opening_it(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """`?until=` alone is "the window ending there", not "everything before there". An
        `until` older than the default `since` would otherwise invert the pair and 422, which
        would make looking at older history mean always sending both halves."""
        project = create_project(client)
        add_run(session, name="today", project_id=project["id"])
        inside = add_run(
            session,
            name="in window",
            project_id=project["id"],
            age=datetime.timedelta(days=50),
        )
        add_run(
            session,
            name="before it",
            project_id=project["id"],
            age=datetime.timedelta(days=200),
        )

        body = client.get(
            f"/api/projects/{project['id']}/runs",
            params={
                "until": (db_utils.utc_now() - datetime.timedelta(days=45)).isoformat()
            },
        ).json()
        assert [run["id"] for run in body["runs"]] == [inside]

    def test_the_page_token_walks_the_window_its_first_request_opened(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """The token carries the compiled filter, so the window is fixed by the request that
        started the walk. Widening `since` mid-walk does nothing -- reaching further back is a
        new request, which is what "paging moves the window rather than removing it" means.
        """
        project = create_project(client)
        for index in range(12):
            add_run(session, name=f"run-{index}", project_id=project["id"])
        ancient = add_run(
            session,
            name="ancient",
            project_id=project["id"],
            age=datetime.timedelta(days=400),
        )

        first = client.get(f"/api/projects/{project['id']}/runs").json()
        assert first["next_page_token"]
        rest = client.get(
            f"/api/projects/{project['id']}/runs",
            params={
                "page_token": first["next_page_token"],
                "since": "2000-01-01T00:00:00Z",
            },
        ).json()

        seen = [run["id"] for run in first["runs"]] + [
            run["id"] for run in rest["runs"]
        ]
        assert len(seen) == 12
        assert ancient not in seen

    def test_the_bound_is_a_created_at_predicate_not_another_exists(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The reason this works at all. `time_range` reads like an annotation predicate, but on
        the `created_at` system key the vendored compiler emits a comparison against
        `pipeline_run.created_at` -- the driving table's own ordering column, covered by
        `ix_pipeline_run_created_at_desc`. A second `EXISTS` would bound nothing."""
        project = create_project(client)
        add_run(session, name="mine", project_id=project["id"])

        with sql_capture.capture_sql(db_engine) as statements:
            assert client.get(f"/api/projects/{project['id']}/runs").status_code == 200

        feed = [
            statement
            for statement in sql_capture.selects_from(statements, table="pipeline_run")
            if "pipeline_run_annotation" in statement
        ]
        assert len(feed) == 1, statements
        assert re.search(r"pipeline_run\.created_at\s*>=", feed[0]), feed[0]
        assert feed[0].count("EXISTS") == 1, feed[0]
