"""Unit tests for scheduling.pipelines.api_routes."""

import ast
import collections.abc
import contextlib
import copy
import dataclasses
import datetime
import inspect
import logging
import textwrap
import typing
import uuid
from unittest import mock

import fastapi.testclient
import httpx
import pydantic
import pymysql.err
import pytest
import sqlalchemy
import sqlalchemy.orm
import starlette.routing
from starlette import status

from cloud_pipelines_backend import api_router, api_server_sql
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.scheduling.pipelines import (
    api_routes,
    database_migrations,
    db_models,
    executor,
    schedule_paths,
    schedule_queries,
    services,
)
from cloud_pipelines_backend.user_pipelines import db_models as user_pipeline_db_models
from cloud_pipelines_backend.user_pipelines import errors as user_pipeline_errors
from cloud_pipelines_backend.user_pipelines import services as user_pipeline_services
from tests import sql_capture
from tests.scheduling.pipelines.conftest import (
    BARE_COMPONENT_SPEC,
    DEFAULT_USER,
    OTHER_USER,
    SAMPLE_PIPELINE_TASK_SPEC,
    ClientFactory,
    closed_schema_report,
    insert_pathless_schedule_row,
    insert_pipeline_run,
    path_ready_reference_closed_schema_report,
)


class TestPipelineTaskSpecValidation:
    """The spec is parsed at write time, so a bad one fails the call that made it.

    Before this, `pipeline_task_spec` was stored as an unvalidated JSON blob and the
    only parse happened when the cron fired — on a schedule nobody was watching.
    """

    def test_create_rejects_bare_component_spec(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p1",
                "name": "Bare pipeline",
                "pipeline_task_spec": BARE_COMPONENT_SPEC,
                "cron_expression": "0 8 * * *",
            },
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert "componentRef" in resp.json()["detail"]

    @pytest.mark.parametrize(
        "bad_spec",
        [
            pytest.param({}, id="empty"),
            pytest.param({"componentRef": {}}, id="component-ref-with-no-locator"),
            pytest.param({"component_ref": {"name": "x"}}, id="snake-case-alias"),
        ],
    )
    def test_create_rejects_other_malformed_specs(
        self,
        client: fastapi.testclient.TestClient,
        bad_spec: dict,
    ) -> None:
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p2",
                "name": "Malformed",
                "pipeline_task_spec": bad_spec,
                "cron_expression": "0 8 * * *",
            },
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_update_rejects_bare_component_spec_and_leaves_the_spec_intact(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        created = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p3",
                "name": "Good then bad",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
            },
        )
        assert created.status_code == 201
        schedule_id = created.json()["id"]

        resp = client.patch(
            f"/api/schedules/pipelines/{schedule_id}",
            json={"pipeline_task_spec": BARE_COMPONENT_SPEC},
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

        after = client.get(
            f"/api/schedules/pipelines/{schedule_id}",
            params={"include_spec": True},
        )
        assert after.json()["pipeline_task_spec"] == SAMPLE_PIPELINE_TASK_SPEC

    def test_create_accepts_a_valid_root_task(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p4",
                "name": "Valid",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
            },
        )
        assert resp.status_code == 201


class TestPipelineScheduleAPI:
    def test_create_schedule(self, client: fastapi.testclient.TestClient) -> None:
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p5",
                "name": "API Test",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 9 * * *",
            },
        )
        assert resp.status_code == 201
        data = resp.json()
        assert len(data["id"]) == 20
        assert data["created_at"] is not None
        assert data["updated_at"] is not None
        assert data["next_run_at"] is not None
        assert data == {
            "id": data["id"],
            "name": "API Test",
            "cron_expression": "0 9 * * *",
            "timezone": "UTC",
            "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
            "paused": False,
            "created_by": "test@example.com",
            "created_at": data["created_at"],
            "updated_at": data["updated_at"],
            "last_run_at": None,
            "last_run_submission_result": None,
            "pipeline_task_spec_from_pipeline_run_id": None,
            "pipeline_task_spec_from_user_pipeline_id": None,
            "pipeline_task_spec_from_user_pipeline_version_key": None,
            "schedule_path": "sweep/p5",
            "next_run_at": data["next_run_at"],
            "pipeline_templates": {"arguments": {}},
        }

    def test_list_schedules(self, client: fastapi.testclient.TestClient) -> None:
        create_resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p6",
                "name": "List Test",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 9 * * *",
            },
        )
        created = create_resp.json()

        resp = client.get("/api/schedules/pipelines")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_count"] >= 1
        assert "next_page_token" in data
        schedule = next(s for s in data["schedules"] if s["id"] == created["id"])
        assert "pipeline_task_spec" not in schedule
        assert schedule == {
            "id": created["id"],
            "name": "List Test",
            "cron_expression": "0 9 * * *",
            "timezone": "UTC",
            "paused": False,
            "created_by": "test@example.com",
            "created_at": created["created_at"],
            "updated_at": created["updated_at"],
            "last_run_at": None,
            "last_run_submission_result": None,
            "pipeline_task_spec_from_pipeline_run_id": None,
            "pipeline_task_spec_from_user_pipeline_id": None,
            "pipeline_task_spec_from_user_pipeline_version_key": None,
            "schedule_path": "sweep/p6",
            "next_run_at": created["next_run_at"],
            "pipeline_templates": {"arguments": {}},
        }

    def test_get_schedule_without_spec(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        create_resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p7",
                "name": "Get Test",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 9 * * *",
            },
        )
        created = create_resp.json()

        resp = client.get(f"/api/schedules/pipelines/{created['id']}")
        assert resp.status_code == 200
        data = resp.json()
        assert "pipeline_task_spec" not in data
        assert data == {
            "id": created["id"],
            "name": "Get Test",
            "cron_expression": "0 9 * * *",
            "timezone": "UTC",
            "paused": False,
            "created_by": "test@example.com",
            "created_at": created["created_at"],
            "updated_at": created["updated_at"],
            "last_run_at": None,
            "last_run_submission_result": None,
            "pipeline_task_spec_from_pipeline_run_id": None,
            "pipeline_task_spec_from_user_pipeline_id": None,
            "pipeline_task_spec_from_user_pipeline_version_key": None,
            "schedule_path": "sweep/p7",
            "next_run_at": created["next_run_at"],
            "pipeline_templates": {"arguments": {}},
        }

    def test_get_schedule_with_spec(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        create_resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p8",
                "name": "Get Spec Test",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 9 * * *",
            },
        )
        created = create_resp.json()

        resp = client.get(f"/api/schedules/pipelines/{created['id']}?include_spec=true")
        assert resp.status_code == 200
        assert resp.json() == {
            "id": created["id"],
            "name": "Get Spec Test",
            "cron_expression": "0 9 * * *",
            "timezone": "UTC",
            "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
            "paused": False,
            "created_by": "test@example.com",
            "created_at": created["created_at"],
            "updated_at": created["updated_at"],
            "last_run_at": None,
            "last_run_submission_result": None,
            "pipeline_task_spec_from_pipeline_run_id": None,
            "pipeline_task_spec_from_user_pipeline_id": None,
            "pipeline_task_spec_from_user_pipeline_version_key": None,
            "schedule_path": "sweep/p8",
            "next_run_at": created["next_run_at"],
            "pipeline_templates": {"arguments": {}},
        }

    def test_update_schedule(self, client: fastapi.testclient.TestClient) -> None:
        create_resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p9",
                "name": "Update Test",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 9 * * *",
            },
        )
        created = create_resp.json()

        resp = client.patch(
            f"/api/schedules/pipelines/{created['id']}",
            json={"name": "Updated Name", "paused": True},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["updated_at"] != created["updated_at"]
        assert data == {
            "id": created["id"],
            "name": "Updated Name",
            "cron_expression": "0 9 * * *",
            "timezone": "UTC",
            "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
            "paused": True,
            "created_by": "test@example.com",
            "created_at": created["created_at"],
            "updated_at": data["updated_at"],
            "last_run_at": None,
            "last_run_submission_result": None,
            "pipeline_task_spec_from_pipeline_run_id": None,
            "pipeline_task_spec_from_user_pipeline_id": None,
            "pipeline_task_spec_from_user_pipeline_version_key": None,
            "schedule_path": "sweep/p9",
            "next_run_at": None,
            "pipeline_templates": {"arguments": {}},
        }

    def test_delete_schedule(self, client: fastapi.testclient.TestClient) -> None:
        create_resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p10",
                "name": "Delete Test",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 9 * * *",
            },
        )
        schedule_id = create_resp.json()["id"]

        resp = client.delete(f"/api/schedules/pipelines/{schedule_id}")
        assert resp.status_code == 204

        resp = client.get(f"/api/schedules/pipelines/{schedule_id}")
        assert resp.status_code == 404

    def test_get_nonexistent(self, client: fastapi.testclient.TestClient) -> None:
        resp = client.get("/api/schedules/pipelines/nonexistent")
        assert resp.status_code == 404

    def test_update_nonexistent(self, client: fastapi.testclient.TestClient) -> None:
        resp = client.patch(
            "/api/schedules/pipelines/nonexistent",
            json={"name": "nope"},
        )
        assert resp.status_code == 404

    def test_delete_nonexistent(self, client: fastapi.testclient.TestClient) -> None:
        resp = client.delete("/api/schedules/pipelines/nonexistent")
        assert resp.status_code == 404

    def test_invalid_cron(self, client: fastapi.testclient.TestClient) -> None:
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p11",
                "name": "Bad Cron",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "not a cron",
            },
        )
        assert resp.status_code == 422
        assert "Invalid cron expression 'not a cron'" in resp.json()["detail"]

    def test_invalid_timezone(self, client: fastapi.testclient.TestClient) -> None:
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p12",
                "name": "Bad TZ",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 9 * * *",
                "timezone": "Mars/Olympus",
            },
        )
        assert resp.status_code == 422
        assert (
            "Invalid cron expression '0 9 * * *' or timezone 'Mars/Olympus'"
            in resp.json()["detail"]
        )

    def test_trigger_paused_schedule_returns_409(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        create_resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p13",
                "name": "Paused Trigger Test",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 9 * * *",
            },
        )
        schedule_id = create_resp.json()["id"]

        client.patch(
            f"/api/schedules/pipelines/{schedule_id}",
            json={"paused": True},
        )

        resp = client.post(f"/api/schedules/pipelines/{schedule_id}/trigger")
        assert resp.status_code == 409
        assert resp.json() == {
            "detail": "Schedule is paused, unpause before triggering"
        }

    def test_six_field_cron(self, client: fastapi.testclient.TestClient) -> None:
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p14",
                "name": "6-field hourly",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 0 * * * *",
            },
        )
        assert resp.status_code == 201
        data = resp.json()
        assert len(data["id"]) == 20
        assert data["next_run_at"] is not None
        assert data == {
            "id": data["id"],
            "name": "6-field hourly",
            "cron_expression": "0 0 * * * *",
            "timezone": "UTC",
            "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
            "paused": False,
            "created_by": "test@example.com",
            "created_at": data["created_at"],
            "updated_at": data["updated_at"],
            "last_run_at": None,
            "last_run_submission_result": None,
            "pipeline_task_spec_from_pipeline_run_id": None,
            "pipeline_task_spec_from_user_pipeline_id": None,
            "pipeline_task_spec_from_user_pipeline_version_key": None,
            "schedule_path": "sweep/p14",
            "next_run_at": data["next_run_at"],
            "pipeline_templates": {"arguments": {}},
        }

    def test_cron_exceeds_daily_limit(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p15",
                "name": "Every minute",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "* * * * *",
            },
        )
        assert resp.status_code == 422
        assert resp.json() == {
            "detail": "Cron expression fires 51 times per day, exceeding the maximum of 50"
        }

    def test_cron_at_daily_limit(self, client: fastapi.testclient.TestClient) -> None:
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p16",
                "name": "Every 30 min",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "*/30 * * * *",
            },
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data == {
            "id": data["id"],
            "name": "Every 30 min",
            "cron_expression": "*/30 * * * *",
            "timezone": "UTC",
            "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
            "paused": False,
            "created_by": "test@example.com",
            "created_at": data["created_at"],
            "updated_at": data["updated_at"],
            "last_run_at": None,
            "last_run_submission_result": None,
            "pipeline_task_spec_from_pipeline_run_id": None,
            "pipeline_task_spec_from_user_pipeline_id": None,
            "pipeline_task_spec_from_user_pipeline_version_key": None,
            "schedule_path": "sweep/p16",
            "next_run_at": data["next_run_at"],
            "pipeline_templates": {"arguments": {}},
        }

    def test_create_removes_db_row_on_apscheduler_failure(
        self,
        test_app: fastapi.FastAPI,
        client: fastapi.testclient.TestClient,
        scheduler_svc: services.SchedulerService,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """If APScheduler add_schedule fails after DB commit, the orphan DB
        row is cleaned up."""
        no_raise_client = fastapi.testclient.TestClient(
            test_app, raise_server_exceptions=False
        )

        with caplog.at_level(
            logging.ERROR,
            logger="cloud_pipelines_backend.scheduling.pipelines.api_routes",
        ):
            with mock.patch.object(
                scheduler_svc,
                "add_schedule",
                side_effect=RuntimeError("APScheduler broke"),
            ):
                resp = no_raise_client.post(
                    "/api/schedules/pipelines",
                    json={
                        "schedule_path": "sweep/p17",
                        "name": "APScheduler Failure",
                        "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                        "cron_expression": "0 9 * * *",
                    },
                )

        assert resp.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert any(
            "APScheduler add_schedule failed" in record.message
            and "removing DB row" in record.message
            for record in caplog.records
        )

        list_resp = client.get("/api/schedules/pipelines")
        names = [s["name"] for s in list_resp.json()["schedules"]]
        assert "APScheduler Failure" not in names

    def test_create_empty_name_rejected(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p18",
                "name": "",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 9 * * *",
            },
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_create_empty_cron_rejected(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p19",
                "name": "Good Name",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "",
            },
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_create_name_exceeds_max_length(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p20",
                "name": "x" * 256,
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 9 * * *",
            },
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_create_cron_exceeds_max_length(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p21",
                "name": "Good Name",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "x" * 256,
            },
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_create_timezone_exceeds_max_length(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p22",
                "name": "Good Name",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 9 * * *",
                "timezone": "x" * 256,
            },
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_update_empty_name_rejected(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        create_resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p23",
                "name": "Valid Name",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 9 * * *",
            },
        )
        schedule_id = create_resp.json()["id"]

        resp = client.patch(
            f"/api/schedules/pipelines/{schedule_id}",
            json={"name": ""},
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_update_null_fields_accepted(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        create_resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p24",
                "name": "Null Update Test",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 9 * * *",
            },
        )
        created = create_resp.json()

        resp = client.patch(
            f"/api/schedules/pipelines/{created['id']}",
            json={"name": None, "cron_expression": None, "timezone": None},
        )
        assert resp.status_code == status.HTTP_200_OK
        data = resp.json()
        assert data["name"] == "Null Update Test"
        assert data["cron_expression"] == "0 9 * * *"
        assert data["timezone"] == "UTC"


class TestPaginationAPI:
    def test_default_page_size(self, client: fastapi.testclient.TestClient) -> None:
        for i in range(12):
            client.post(
                "/api/schedules/pipelines",
                json={
                    "schedule_path": f"sweep/p25-{i}",
                    "name": f"Page Test {i}",
                    "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                    "cron_expression": "0 9 * * *",
                },
            )

        resp = client.get("/api/schedules/pipelines")
        assert resp.status_code == status.HTTP_200_OK
        data = resp.json()
        assert len(data["schedules"]) == 10
        assert data["total_count"] == 12
        assert data["next_page_token"] is not None

    def test_custom_page_size(self, client: fastapi.testclient.TestClient) -> None:
        for i in range(5):
            client.post(
                "/api/schedules/pipelines",
                json={
                    "schedule_path": f"sweep/p26-{i}",
                    "name": f"Size Test {i}",
                    "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                    "cron_expression": "0 9 * * *",
                },
            )

        resp = client.get("/api/schedules/pipelines", params={"page_size": 3})
        assert resp.status_code == status.HTTP_200_OK
        data = resp.json()
        assert len(data["schedules"]) == 3
        assert data["total_count"] == 5
        assert data["next_page_token"] is not None

    def test_page_token_fetches_next_page(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        for i in range(5):
            client.post(
                "/api/schedules/pipelines",
                json={
                    "schedule_path": f"sweep/p27-{i}",
                    "name": f"Cursor Test {i}",
                    "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                    "cron_expression": "0 9 * * *",
                },
            )

        page1 = client.get("/api/schedules/pipelines", params={"page_size": 3}).json()
        assert len(page1["schedules"]) == 3
        assert page1["next_page_token"] is not None

        page2 = client.get(
            "/api/schedules/pipelines",
            params={"page_size": 3, "page_token": page1["next_page_token"]},
        ).json()
        assert len(page2["schedules"]) == 2
        assert page2["next_page_token"] is None

        page1_ids = {s["id"] for s in page1["schedules"]}
        page2_ids = {s["id"] for s in page2["schedules"]}
        assert page1_ids.isdisjoint(page2_ids)

    def test_last_page_has_no_token(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p28",
                "name": "Single",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 9 * * *",
            },
        )

        resp = client.get("/api/schedules/pipelines")
        data = resp.json()
        assert len(data["schedules"]) == 1
        assert data["next_page_token"] is None

    def test_invalid_page_token_rejected(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        resp = client.get(
            "/api/schedules/pipelines", params={"page_token": "bad-token"}
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert "Unrecognized page_token format" in resp.json()["detail"]

    def test_page_size_exceeds_max_rejected(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        resp = client.get("/api/schedules/pipelines", params={"page_size": 101})
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_page_size_zero_rejected(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        resp = client.get("/api/schedules/pipelines", params={"page_size": 0})
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_total_count_independent_of_pagination(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        for i in range(4):
            client.post(
                "/api/schedules/pipelines",
                json={
                    "schedule_path": f"sweep/p29-{i}",
                    "name": f"Count Test {i}",
                    "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                    "cron_expression": "0 9 * * *",
                },
            )

        page1 = client.get("/api/schedules/pipelines", params={"page_size": 2}).json()
        page2 = client.get(
            "/api/schedules/pipelines",
            params={"page_size": 2, "page_token": page1["next_page_token"]},
        ).json()

        assert page1["total_count"] == 4
        assert page2["total_count"] == 4


class TestCursorEncodeDecode:
    def test_round_trip_utc_aware(self) -> None:
        schedule = db_models.ScheduledPipelineRun(
            name="test",
            cron_expression="0 9 * * *",
            created_by="test@test.com",
        )
        schedule.id = "abc123"
        schedule.updated_at = datetime.datetime(
            2026, 6, 23, 9, 0, 0, tzinfo=datetime.timezone.utc
        )

        cursor = api_routes._encode_cursor(schedule=schedule)
        assert "~" in cursor
        assert "abc123" in cursor

        decoded_at, decoded_id = api_routes._decode_cursor(cursor=cursor)
        assert decoded_id == "abc123"
        assert decoded_at == datetime.datetime(
            2026, 6, 23, 9, 0, 0, tzinfo=datetime.timezone.utc
        )

    def test_round_trip_naive_datetime_treated_as_utc(self) -> None:
        schedule = db_models.ScheduledPipelineRun(
            name="test",
            cron_expression="0 9 * * *",
            created_by="test@test.com",
        )
        schedule.id = "naive001"
        schedule.updated_at = datetime.datetime(2026, 3, 15, 14, 30, 0)

        cursor = api_routes._encode_cursor(schedule=schedule)
        assert "+00:00" in cursor

        decoded_at, decoded_id = api_routes._decode_cursor(cursor=cursor)
        assert decoded_id == "naive001"
        assert decoded_at.tzinfo == datetime.timezone.utc
        assert decoded_at == datetime.datetime(
            2026, 3, 15, 14, 30, 0, tzinfo=datetime.timezone.utc
        )

    def test_decode_non_utc_timezone_converts_to_utc(self) -> None:
        cursor = "2026-06-23T04:00:00-05:00~tz_test_id"

        decoded_at, decoded_id = api_routes._decode_cursor(cursor=cursor)
        assert decoded_id == "tz_test_id"
        assert decoded_at.tzinfo == datetime.timezone.utc
        assert decoded_at == datetime.datetime(
            2026, 6, 23, 9, 0, 0, tzinfo=datetime.timezone.utc
        )

    def test_decode_dst_transition_day(self) -> None:
        """March 8, 2026: US spring-forward. 2:00 AM EST -> 3:00 AM EDT.
        Cursor with EDT offset (-04:00) should decode to correct UTC."""
        cursor = "2026-03-08T03:30:00-04:00~dst_spring"

        decoded_at, decoded_id = api_routes._decode_cursor(cursor=cursor)
        assert decoded_id == "dst_spring"
        assert decoded_at.tzinfo == datetime.timezone.utc
        assert decoded_at == datetime.datetime(
            2026, 3, 8, 7, 30, 0, tzinfo=datetime.timezone.utc
        )

    def test_decode_dst_fall_back(self) -> None:
        """Nov 1, 2026: US fall-back. Cursor with EST offset (-05:00)
        after the transition should decode to correct UTC."""
        cursor = "2026-11-01T01:30:00-05:00~dst_fall"

        decoded_at, decoded_id = api_routes._decode_cursor(cursor=cursor)
        assert decoded_id == "dst_fall"
        assert decoded_at.tzinfo == datetime.timezone.utc
        assert decoded_at == datetime.datetime(
            2026, 11, 1, 6, 30, 0, tzinfo=datetime.timezone.utc
        )

    def test_decode_missing_separator_raises(self) -> None:
        with pytest.raises(fastapi.HTTPException) as exc_info:
            api_routes._decode_cursor(cursor="no-separator-here")
        assert exc_info.value.status_code == 422

    def test_decode_id_containing_tilde(self) -> None:
        """ID with ~ in it — split on first ~ only."""
        cursor = "2026-06-23T09:00:00+00:00~id~with~tildes"

        decoded_at, decoded_id = api_routes._decode_cursor(cursor=cursor)
        assert decoded_id == "id~with~tildes"
        assert decoded_at == datetime.datetime(
            2026, 6, 23, 9, 0, 0, tzinfo=datetime.timezone.utc
        )

    def test_encode_produces_isoformat(self) -> None:
        schedule = db_models.ScheduledPipelineRun(
            name="test",
            cron_expression="0 9 * * *",
            created_by="test@test.com",
        )
        schedule.id = "fmt_test"
        schedule.updated_at = datetime.datetime(
            2026, 1, 15, 23, 59, 59, tzinfo=datetime.timezone.utc
        )

        cursor = api_routes._encode_cursor(schedule=schedule)
        assert cursor == "2026-01-15T23:59:59+00:00~fmt_test"


class TestOwnershipGuards:
    """Verify that non-admin users can only update/delete/trigger their own
    schedules, while admins can operate on any schedule."""

    def _create_schedule(
        self,
        client: fastapi.testclient.TestClient,
    ) -> str:
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p30",
                "name": "Ownership Test",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 9 * * *",
            },
        )
        assert resp.status_code == status.HTTP_201_CREATED
        return resp.json()["id"]

    def test_read_other_users_schedule_returns_403(
        self,
        client: fastapi.testclient.TestClient,
        other_user_client: fastapi.testclient.TestClient,
    ) -> None:
        """The read used to be the hole in this class .

        `GET /{id}` returned the whole row -- `pipeline_task_spec` included -- to
        any authenticated caller, while every mutating verb beside it refused the
        same person on the same id. `include_spec=true` is passed because the spec
        is the part worth protecting, and a scoping that stopped at the metadata
        would still hand it over.
        """
        schedule_id = self._create_schedule(client)

        resp = other_user_client.get(
            f"/api/schedules/pipelines/{schedule_id}",
            params={"include_spec": True},
        )

        assert resp.status_code == status.HTTP_403_FORBIDDEN
        assert "READ denied" in resp.json()["detail"]
        assert "test@example.com" in resp.json()["detail"]
        assert "other@example.com" in resp.json()["detail"]

    def test_read_own_schedule_succeeds(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        schedule_id = self._create_schedule(client)

        resp = client.get(
            f"/api/schedules/pipelines/{schedule_id}",
            params={"include_spec": True},
        )

        assert resp.status_code == status.HTTP_200_OK
        assert resp.json()["pipeline_task_spec"] == SAMPLE_PIPELINE_TASK_SPEC

    def test_admin_read_other_users_schedule_succeeds(
        self,
        client: fastapi.testclient.TestClient,
        admin_client: fastapi.testclient.TestClient,
    ) -> None:
        """Admin bypasses the read exactly as it bypasses the write.

        Deliberate: the read goes through `_check_ownership`, so there is one
        ownership rule for every id-addressed route and the answer cannot depend
        on the verb. Note the list is scoped differently -- see
        `test_an_admin_listing_sees_only_their_own`.
        """
        schedule_id = self._create_schedule(client)

        resp = admin_client.get(f"/api/schedules/pipelines/{schedule_id}")

        assert resp.status_code == status.HTTP_200_OK

    def test_reading_a_missing_schedule_is_404_not_403(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Absent stays distinguishable from forbidden on the id-addressed form.

        Unlike the path routes, which collapse both into 404 so a path cannot be
        probed for existence, an id is not a caller-chosen name -- so this keeps
        the sibling routes' statuses rather than inventing a third convention.
        """
        resp = client.get("/api/schedules/pipelines/no-such-schedule")

        assert resp.status_code == status.HTTP_404_NOT_FOUND

    def test_update_own_schedule_succeeds(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        schedule_id = self._create_schedule(client)
        resp = client.patch(
            f"/api/schedules/pipelines/{schedule_id}",
            json={"name": "Updated"},
        )
        assert resp.status_code == status.HTTP_200_OK
        assert resp.json()["name"] == "Updated"

    def test_update_other_users_schedule_returns_403(
        self,
        client: fastapi.testclient.TestClient,
        other_user_client: fastapi.testclient.TestClient,
    ) -> None:
        schedule_id = self._create_schedule(client)
        resp = other_user_client.patch(
            f"/api/schedules/pipelines/{schedule_id}",
            json={"name": "Hijacked"},
        )
        assert resp.status_code == status.HTTP_403_FORBIDDEN
        assert "UPDATE denied" in resp.json()["detail"]
        assert "test@example.com" in resp.json()["detail"]
        assert "other@example.com" in resp.json()["detail"]

    def test_admin_update_other_users_schedule_succeeds(
        self,
        client: fastapi.testclient.TestClient,
        admin_client: fastapi.testclient.TestClient,
    ) -> None:
        schedule_id = self._create_schedule(client)
        resp = admin_client.patch(
            f"/api/schedules/pipelines/{schedule_id}",
            json={"name": "Admin Override"},
        )
        assert resp.status_code == status.HTTP_200_OK
        assert resp.json()["name"] == "Admin Override"

    def test_delete_own_schedule_succeeds(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        schedule_id = self._create_schedule(client)
        resp = client.delete(f"/api/schedules/pipelines/{schedule_id}")
        assert resp.status_code == status.HTTP_204_NO_CONTENT

    def test_delete_other_users_schedule_returns_403(
        self,
        client: fastapi.testclient.TestClient,
        other_user_client: fastapi.testclient.TestClient,
    ) -> None:
        schedule_id = self._create_schedule(client)
        resp = other_user_client.delete(f"/api/schedules/pipelines/{schedule_id}")
        assert resp.status_code == status.HTTP_403_FORBIDDEN
        assert "DELETE denied" in resp.json()["detail"]
        assert "test@example.com" in resp.json()["detail"]
        assert "other@example.com" in resp.json()["detail"]

    def test_admin_delete_other_users_schedule_succeeds(
        self,
        client: fastapi.testclient.TestClient,
        admin_client: fastapi.testclient.TestClient,
    ) -> None:
        schedule_id = self._create_schedule(client)
        resp = admin_client.delete(f"/api/schedules/pipelines/{schedule_id}")
        assert resp.status_code == status.HTTP_204_NO_CONTENT

    def test_trigger_other_users_schedule_returns_403(
        self,
        client: fastapi.testclient.TestClient,
        other_user_client: fastapi.testclient.TestClient,
    ) -> None:
        schedule_id = self._create_schedule(client)
        resp = other_user_client.post(f"/api/schedules/pipelines/{schedule_id}/trigger")
        assert resp.status_code == status.HTTP_403_FORBIDDEN
        assert "TRIGGER denied" in resp.json()["detail"]
        assert "test@example.com" in resp.json()["detail"]
        assert "other@example.com" in resp.json()["detail"]

    def test_trigger_own_schedule_succeeds(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        schedule_id = self._create_schedule(client)
        fake_run = api_server_sql.PipelineRunResponse(
            id="run-123",
            root_execution_id="exec-123",
        )
        with mock.patch.object(
            executor,
            "execute_pipeline_schedule",
            return_value=fake_run,
        ):
            resp = client.post(f"/api/schedules/pipelines/{schedule_id}/trigger")
        assert resp.status_code == status.HTTP_200_OK

    def test_admin_trigger_other_users_schedule_succeeds(
        self,
        client: fastapi.testclient.TestClient,
        admin_client: fastapi.testclient.TestClient,
    ) -> None:
        schedule_id = self._create_schedule(client)
        fake_run = api_server_sql.PipelineRunResponse(
            id="run-456",
            root_execution_id="exec-456",
        )
        with mock.patch.object(
            executor,
            "execute_pipeline_schedule",
            return_value=fake_run,
        ):
            resp = admin_client.post(f"/api/schedules/pipelines/{schedule_id}/trigger")
        assert resp.status_code == status.HTTP_200_OK


class TestSchedulePathCanonicalization:
    """One normalizer for every write and every lookup.

    The point is cross-backend agreement: the stored value must be exactly what
    the caller supplied, and comparison must mean the same thing on SQLite's
    binary ``=`` and on the case-sensitive collation `db_models` gives the MySQL
    column. What canonicalization does NOT do any more is fold case -- see
    `TestSchedulePathIsCaseSensitive`.
    """

    def test_create_stores_canonical_form(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "name": "Nightly",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": "  Upi/Nightly  ",
            },
        )
        assert resp.status_code == status.HTTP_201_CREATED
        # Trimmed, and NOT folded: surrounding whitespace is transport noise,
        # case is the caller's chosen identity.
        assert resp.json()["schedule_path"] == "Upi/Nightly"

    def test_lookup_canonicalizes_the_query_too(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        client.post(
            "/api/schedules/pipelines",
            json={
                "name": "Nightly",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": "upi/nightly",
            },
        )
        # Same value, only surrounded by whitespace: the lookup trims exactly as
        # the write did. A DIFFERENT case is a different path and is covered by
        # `TestSchedulePathIsCaseSensitive`.
        resp = client.get(
            "/api/schedules/pipelines",
            params={"schedule_path": "  upi/nightly  "},
        )
        assert resp.status_code == status.HTTP_200_OK
        body = resp.json()
        assert [s["schedule_path"] for s in body["schedules"]] == ["upi/nightly"]
        assert body["total_count"] == 1

    @pytest.mark.parametrize(
        "bad_path",
        [
            pytest.param("", id="empty"),
            pytest.param("/leading", id="leading-slash"),
            pytest.param("trailing/", id="trailing-slash"),
            pytest.param("a//b", id="empty-segment"),
            pytest.param("..", id="parent-traversal"),
            pytest.param("a/../b", id="embedded-traversal"),
            pytest.param("-leading-dash", id="leading-dash"),
            pytest.param("_leading-underscore", id="leading-underscore"),
            pytest.param("a\\b", id="backslash"),
            pytest.param("café", id="non-ascii"),
            pytest.param("a" * 256, id="too-long"),
        ],
    )
    def test_create_rejects_invalid_paths(
        self,
        client: fastapi.testclient.TestClient,
        bad_path: str,
    ) -> None:
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "name": "Bad path",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": bad_path,
            },
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_embedded_dot_is_allowed(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """`..` is impossible but `v1.2` must stay legal."""
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "name": "Versioned",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": "rollup/v1.2",
            },
        )
        assert resp.status_code == status.HTTP_201_CREATED
        assert resp.json()["schedule_path"] == "rollup/v1.2"


class TestSchedulePathUniqueness:
    def test_duplicate_path_for_same_owner_conflicts(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        body = {
            "name": "First",
            "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
            "cron_expression": "0 8 * * *",
            "schedule_path": "upi/nightly",
        }
        assert (
            client.post("/api/schedules/pipelines", json=body).status_code
            == status.HTTP_201_CREATED
        )

        resp = client.post("/api/schedules/pipelines", json={**body, "name": "Second"})
        assert resp.status_code == status.HTTP_409_CONFLICT
        assert "already used" in resp.json()["detail"]

    def test_duplicate_is_detected_after_trimming(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """`Upi/Nightly` and `upi/nightly` are the same identity, not two."""
        client.post(
            "/api/schedules/pipelines",
            json={
                "name": "First",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": "upi/nightly",
            },
        )
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "name": "Second",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": "  upi/nightly  ",
            },
        )
        assert resp.status_code == status.HTTP_409_CONFLICT

    def test_losing_create_does_not_delete_the_winner(
        self,
        client: fastapi.testclient.TestClient,
        session,
    ) -> None:
        """The loser wrote no row, so it must not run the delete-my-row compensation.

        That compensation exists to undo a committed row when APScheduler refuses
        the job. A uniqueness loser never got that far, and running it would
        delete the *winner's* row -- which is why the commit sits outside the
        try/except rather than inside it.
        """
        body = {
            "name": "Winner",
            "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
            "cron_expression": "0 8 * * *",
            "schedule_path": "upi/nightly",
        }
        winner_id = client.post("/api/schedules/pipelines", json=body).json()["id"]

        assert (
            client.post(
                "/api/schedules/pipelines", json={**body, "name": "Loser"}
            ).status_code
            == status.HTTP_409_CONFLICT
        )

        survivor = session.get(db_models.ScheduledPipelineRun, winner_id)
        assert survivor is not None
        assert survivor.name == "Winner"
        assert survivor.schedule_path == "upi/nightly"

    def test_same_path_is_allowed_for_a_different_owner(
        self,
        client: fastapi.testclient.TestClient,
        other_user_client: fastapi.testclient.TestClient,
    ) -> None:
        """Uniqueness is per owner, so two users may each hold `upi/nightly`."""
        body = {
            "name": "Mine",
            "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
            "cron_expression": "0 8 * * *",
            "schedule_path": "upi/nightly",
        }
        assert (
            client.post("/api/schedules/pipelines", json=body).status_code
            == status.HTTP_201_CREATED
        )
        assert (
            other_user_client.post("/api/schedules/pipelines", json=body).status_code
            == status.HTTP_201_CREATED
        )


class TestSchedulePathLookupScoping:
    """A path is unique per owner, so an unscoped lookup is a cross-user leak."""

    def test_lookup_does_not_return_another_users_row(
        self,
        client: fastapi.testclient.TestClient,
        other_user_client: fastapi.testclient.TestClient,
    ) -> None:
        other_user_client.post(
            "/api/schedules/pipelines",
            json={
                "name": "Theirs",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": "upi/nightly",
            },
        )

        resp = client.get(
            "/api/schedules/pipelines",
            params={"schedule_path": "upi/nightly"},
        )
        assert resp.status_code == status.HTTP_200_OK
        assert resp.json() == {
            "schedules": [],
            "total_count": 0,
            "next_page_token": None,
        }

    def test_admin_lookup_stays_in_its_own_namespace(
        self,
        other_user_client: fastapi.testclient.TestClient,
        admin_client: fastapi.testclient.TestClient,
    ) -> None:
        """Admin is not a cross-user path resolver.

        A path names at most one row *per owner*, so resolving one globally as
        admin would be ambiguous rather than powerful.
        """
        other_user_client.post(
            "/api/schedules/pipelines",
            json={
                "name": "Theirs",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": "upi/nightly",
            },
        )

        resp = admin_client.get(
            "/api/schedules/pipelines",
            params={"schedule_path": "upi/nightly"},
        )
        assert resp.json()["schedules"] == []
        assert resp.json()["total_count"] == 0

    def test_miss_is_an_empty_collection_not_a_404(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        resp = client.get(
            "/api/schedules/pipelines",
            params={"schedule_path": "nope/absent"},
        )
        assert resp.status_code == status.HTTP_200_OK
        assert resp.json()["total_count"] == 0

    def test_total_count_reflects_the_filter(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """A filtered response whose total counted the whole table would mislead."""
        for i in range(3):
            client.post(
                "/api/schedules/pipelines",
                json={
                    "name": f"S{i}",
                    "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                    "cron_expression": "0 8 * * *",
                    "schedule_path": f"upi/s{i}",
                },
            )

        assert client.get("/api/schedules/pipelines").json()["total_count"] == 3
        filtered = client.get(
            "/api/schedules/pipelines",
            params={"schedule_path": "upi/s1"},
        ).json()
        assert filtered["total_count"] == 1
        assert len(filtered["schedules"]) == 1

    def test_the_unfiltered_list_is_owner_scoped_too(
        self,
        client: fastapi.testclient.TestClient,
        other_user_client: fastapi.testclient.TestClient,
    ) -> None:
        """Narrowed on purpose; this test used to assert the opposite.

        The unfiltered list returned every user's schedules, here and on `main`,
        while PATCH/DELETE/trigger on those same rows returned 403. Reviewed as an
        inconsistency  and resolved by scoping the reads rather
        than by widening the writes.

        `total_count` is asserted alongside the rows because the count is a second
        statement: a page can be scoped correctly while the count still describes
        the whole table, which would leak how many schedules exist that the caller
        may not see.
        """
        client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p31",
                "name": "Mine",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
            },
        )
        other_user_client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p32",
                "name": "Theirs",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
            },
        )

        body = client.get("/api/schedules/pipelines").json()
        assert [s["name"] for s in body["schedules"]] == ["Mine"]
        assert body["total_count"] == 1

        theirs = other_user_client.get("/api/schedules/pipelines").json()
        assert [s["name"] for s in theirs["schedules"]] == ["Theirs"]
        assert theirs["total_count"] == 1

    def test_pagination_never_walks_into_another_owner(
        self,
        client: fastapi.testclient.TestClient,
        other_user_client: fastapi.testclient.TestClient,
    ) -> None:
        """The cursor is derived from scoped rows, so pages cannot interleave.

        The failure this guards against is not a leak but a truncation: if the
        owner predicate were applied after `LIMIT`, a page filled by another
        owner's rows would come back short, and a `next_page_token` derived from
        the survivors would end the walk early -- silently hiding the caller's own
        later schedules. Interleaving the two owners' rows in time is what makes
        that reachable, so they are created alternately.
        """
        for index in range(3):
            client.post(
                "/api/schedules/pipelines",
                json={
                    "schedule_path": f"sweep/mine-{index}",
                    "name": f"Mine {index}",
                    "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                    "cron_expression": "0 8 * * *",
                },
            )
            other_user_client.post(
                "/api/schedules/pipelines",
                json={
                    "schedule_path": f"sweep/theirs-{index}",
                    "name": f"Theirs {index}",
                    "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                    "cron_expression": "0 8 * * *",
                },
            )

        seen: list[str] = []
        token: str | None = None
        for _ in range(5):
            params = {"page_size": 2}
            if token is not None:
                params["page_token"] = token
            body = client.get("/api/schedules/pipelines", params=params).json()
            assert body["total_count"] == 3
            seen.extend(s["name"] for s in body["schedules"])
            token = body["next_page_token"]
            if token is None:
                break

        assert sorted(seen) == ["Mine 0", "Mine 1", "Mine 2"]

    def test_an_admin_listing_sees_only_their_own(
        self,
        client: fastapi.testclient.TestClient,
        admin_client: fastapi.testclient.TestClient,
    ) -> None:
        """Recorded because it is the one place admin does NOT bypass.

        `_check_ownership` lets an admin read, update, delete and trigger any
        schedule by id. The list does not exempt them, and that is a choice rather
        than a limitation -- `_owned_by_caller` could return `true()` for an admin
        or drop the predicate entirely. It does not, because owner-only scoping is
        what was asked for and a fleet-wide admin list was not. Pinned here so
        granting one later is a visible edit.
        """
        client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p33",
                "name": "Mine",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
            },
        )

        body = admin_client.get("/api/schedules/pipelines").json()

        assert body["schedules"] == []
        assert body["total_count"] == 0


class TestTheListOwnerPredicateDelegatesToTheDatabase:
    """The owner rule the list applies, and the one it must not apply.

    This class asserted the opposite one commit ago. `_owned_by_caller` carried
    a byte-exact residual -- `CAST(CAST(created_by AS CHAR CHARACTER SET utf8mb4)
    AS BINARY)` on both sides -- so that a folding MySQL collation could not let
    'jose' answer for 'Jose'. The premise was that those are two principals.
    They are one, so the residual was removing the caller's own rows from the
    caller's own list on precisely the deployments it was written for.

    What replaces it is a plain equality. Owner identity is not case-sensitive,
    the comparison is the deployment's, and this layer states no rule of its
    own. The assertions below are therefore mostly negative -- naming the shape
    that must not come back -- because anyone re-deriving this from "the list
    applies LIMIT in the database" will arrive at the residual again.

    Still compiled rather than executed: the client suite runs on SQLite, and
    live MySQL coverage remains outstanding in SCHEDULER_DESIGN.md.
    """

    def _compiled(self, url: str) -> str:
        engine = sqlalchemy.create_engine(url)
        predicate = api_routes._owned_by_caller(
            user_details=api_router.UserDetails(
                name="jose@example.com",
                permissions=api_router.Permissions(read=True, write=True, admin=False),
            ),
        )
        return str(predicate.compile(engine, compile_kwargs={"literal_binds": True}))

    @pytest.mark.parametrize(
        "url", ["mysql+pymysql://user:pw@localhost/db", "sqlite://"]
    )
    def test_it_is_one_plain_equality_on_every_dialect(self, url: str) -> None:
        sql = self._compiled(url)

        assert "created_by" in sql
        assert sql.count("created_by") == 1
        # One conjunct. A second one is the residual coming back.
        assert " AND " not in sql

    @pytest.mark.parametrize(
        "url", ["mysql+pymysql://user:pw@localhost/db", "sqlite://"]
    )
    def test_no_cast_collation_or_byte_comparison_is_imposed(self, url: str) -> None:
        """Each rejected shape by name, so a regression reads as one.

        - `CAST(... AS CHAR CHARACTER SET utf8mb4)` plus `AS BINARY` was the
          shipped residual, and it excluded case variants of the caller.
        - `COLLATE utf8mb4_bin` was an earlier attempt at the same thing; it is
          additionally illegal unless the column's charset is already utf8mb4,
          and it is PAD SPACE.
        - `CAST(... AS BINARY)` alone compares each side's current bytes, so a
          latin1 column read over a utf8mb4 connection makes 'josé' `E9` on one
          side and `C3 A9` on the other.

        All three are wrong now for one reason that survives the details: this
        layer does not get to decide what makes two owner names equal.
        """
        sql = self._compiled(url).upper()

        assert "CAST" not in sql
        assert "COLLATE" not in sql
        assert "CHARACTER SET" not in sql
        assert "BINARY" not in sql
        assert "BLOB" not in sql

    def test_the_predicate_cannot_see_a_dialect_to_branch_on(self) -> None:
        """Structural, not documentary.

        The residual needed the bind to know whether to emit MySQL's charset
        conversion. `_owned_by_caller` no longer takes a `Session`, so a future
        edit cannot quietly reintroduce a dialect-specific comparison without
        changing the signature and every caller.
        """
        assert list(inspect.signature(api_routes._owned_by_caller).parameters) == [
            "user_details"
        ]


class TestOwnerCaseVariantsAddressOneNamespaceThroughTheApi:
    """The contract end to end, on a database that folds owners like MySQL.

    The client suite's SQLite compares byte-for-byte, so every ownership test in
    this file passes whether or not an exact-owner residual exists -- which is
    how one survived review. These build the folding column deliberately and
    drive the real routes through it.

    Caller identity and stored identity are parameterized independently
    (`client_for` versus `insert_schedule_row(created_by=...)`), because a
    fixture that varies them together cannot tell "the server matched" from
    "the two strings happened to be equal".
    """

    OWNER_STORED = "jose@example.com"
    OWNER_CALLING = "Jose@example.com"

    def test_the_harness_folds_owners_and_not_paths(
        self,
        folding_owner_db_engine: sqlalchemy.Engine,
    ) -> None:
        """Prove the fixture before trusting a single conclusion from it."""
        with sqlalchemy.orm.Session(folding_owner_db_engine) as session:
            insert = api_routes.db_models.ScheduledPipelineRun(
                name="n",
                cron_expression="0 8 * * *",
                timezone="UTC",
                pipeline_task_spec=SAMPLE_PIPELINE_TASK_SPEC,
                created_by=self.OWNER_STORED,
                schedule_path="team/nightly",
            )
            session.add(insert)
            session.commit()

            owner_folds = session.scalar(
                sqlalchemy.select(api_routes.db_models.ScheduledPipelineRun.id).where(
                    api_routes.db_models.ScheduledPipelineRun.created_by
                    == self.OWNER_CALLING
                )
            )
            path_is_exact = session.scalar(
                sqlalchemy.select(api_routes.db_models.ScheduledPipelineRun.id).where(
                    api_routes.db_models.ScheduledPipelineRun.schedule_path
                    == "Team/Nightly"
                )
            )

        assert owner_folds is not None
        assert path_is_exact is None

    def test_a_differently_cased_caller_lists_their_own_schedules(
        self,
        folding_owner_db_engine: sqlalchemy.Engine,
        client_for: ClientFactory,
    ) -> None:
        created = client_for(self.OWNER_STORED).post(
            "/api/schedules/pipelines",
            json={
                "name": "Nightly",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": "team/nightly",
            },
        )
        assert created.status_code == status.HTTP_201_CREATED

        listed = client_for(self.OWNER_CALLING).get("/api/schedules/pipelines")

        assert listed.status_code == status.HTTP_200_OK
        body = listed.json()
        assert [s["schedule_path"] for s in body["schedules"]] == ["team/nightly"]
        # The count is computed under the same predicate, so a residual applied
        # to one and not the other shows up here rather than in production.
        assert body["total_count"] == 1

    def test_a_differently_cased_caller_resolves_their_own_path(
        self,
        folding_owner_db_engine: sqlalchemy.Engine,
        client_for: ClientFactory,
    ) -> None:
        """The read that the removed residual turned into a 404."""
        client_for(self.OWNER_STORED).post(
            "/api/schedules/pipelines",
            json={
                "name": "Nightly",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": "team/nightly",
            },
        )

        found = client_for(self.OWNER_CALLING).get(
            "/api/schedules/pipelines",
            params={"schedule_path": "team/nightly"},
        )

        assert found.status_code == status.HTTP_200_OK
        assert found.json()["total_count"] == 1

    def test_a_differently_cased_caller_can_delete_their_own_schedule_by_path(
        self,
        folding_owner_db_engine: sqlalchemy.Engine,
        client_for: ClientFactory,
    ) -> None:
        """DELETE, because a 404 on a write is the half that cannot be worked around."""
        client_for(self.OWNER_STORED).post(
            "/api/schedules/pipelines",
            json={
                "name": "Nightly",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": "team/nightly",
            },
        )

        deleted = client_for(self.OWNER_CALLING).delete(
            "/api/schedules/pipelines",
            params={"schedule_path": "team/nightly"},
        )

        assert deleted.status_code == status.HTTP_204_NO_CONTENT

    def test_paths_stay_distinct_within_that_namespace(
        self,
        folding_owner_db_engine: sqlalchemy.Engine,
        client_for: ClientFactory,
    ) -> None:
        """The mixed key, driven through the API rather than the query layer.

        Two spellings of one owner create two differently-cased paths. Neither
        create may be refused as a duplicate, and each path must resolve only to
        itself -- the combination that a uniformly binary key could not express.
        """
        lower = client_for(self.OWNER_STORED).post(
            "/api/schedules/pipelines",
            json={
                "name": "Lower",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": "team/nightly",
            },
        )
        upper = client_for(self.OWNER_CALLING).post(
            "/api/schedules/pipelines",
            json={
                "name": "Upper",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": "Team/Nightly",
            },
        )

        assert lower.status_code == status.HTTP_201_CREATED
        assert upper.status_code == status.HTTP_201_CREATED
        assert lower.json()["id"] != upper.json()["id"]

        caller = client_for(self.OWNER_CALLING)

        def _resolve(path: str) -> str:
            body = caller.get(
                "/api/schedules/pipelines", params={"schedule_path": path}
            ).json()
            assert (
                body["total_count"] == 1
            ), f"{path} resolved {body['total_count']} schedules"
            return body["schedules"][0]["id"]

        assert _resolve("team/nightly") == lower.json()["id"]
        assert _resolve("Team/Nightly") == upper.json()["id"]

    def test_deleting_one_path_leaves_its_case_neighbour_alone(
        self,
        folding_owner_db_engine: sqlalchemy.Engine,
        client_for: ClientFactory,
    ) -> None:
        """The irreversible half of path distinctness.

        A read that resolves the wrong neighbour is recoverable; a DELETE is
        not. Asserted separately from the read because the delete path uses a
        different locator (`_path_stub_or_404`) with its own projection.
        """
        created = client_for(self.OWNER_STORED).post(
            "/api/schedules/pipelines",
            json={
                "name": "Survivor",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": "team/nightly",
            },
        )
        survivor = created.json()["id"]
        client_for(self.OWNER_CALLING).post(
            "/api/schedules/pipelines",
            json={
                "name": "Doomed",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": "Team/Nightly",
            },
        )

        deleted = client_for(self.OWNER_STORED).delete(
            "/api/schedules/pipelines",
            params={"schedule_path": "Team/Nightly"},
        )
        assert deleted.status_code == status.HTTP_204_NO_CONTENT

        remaining = client_for(self.OWNER_CALLING).get(
            "/api/schedules/pipelines", params={"schedule_path": "team/nightly"}
        )
        assert remaining.json()["total_count"] == 1
        assert remaining.json()["schedules"][0]["id"] == survivor

    def test_a_genuinely_different_owner_is_still_refused(
        self,
        folding_owner_db_engine: sqlalchemy.Engine,
        client_for: ClientFactory,
    ) -> None:
        """Folding widens the namespace; it does not remove the scope.

        Dropping the owner predicate entirely would pass every test above.
        """
        client_for(self.OWNER_STORED).post(
            "/api/schedules/pipelines",
            json={
                "name": "Nightly",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": "team/nightly",
            },
        )

        stranger = client_for("maria@example.com")

        listed = stranger.get("/api/schedules/pipelines")
        assert listed.json()["schedules"] == []
        assert listed.json()["total_count"] == 0

        looked_up = stranger.get(
            "/api/schedules/pipelines", params={"schedule_path": "team/nightly"}
        )
        assert looked_up.json()["total_count"] == 0

        assert (
            stranger.delete(
                "/api/schedules/pipelines",
                params={"schedule_path": "team/nightly"},
            ).status_code
            == status.HTTP_404_NOT_FOUND
        )


class TestIdAddressedRoutesUseTheSameOwnerComparator:
    """Ownership must not depend on how the caller addressed the row.

    The id routes compared owners in Python (`user_details.name != created_by`)
    while the path routes scoped their SELECT and let the column decide. On a
    folding deployment that is two different rules, and the same caller got two
    different answers: `Jose@example.com` could list and PATCH their schedule by
    PATH, and was 403'd on that same row by ID -- told it "was created by
    jose@example.com, not Jose@example.com". The CLI carried this as an id-route
    limitation.

    Driven through `folding_owner_db_engine`, because on ordinary SQLite the two
    rules agree and every assertion below passes with the bug present. That is
    why it survived: the suite could not see it.

    Every id-addressed verb is covered -- GET, GET with the spec, PATCH, pause,
    resume, DELETE, trigger. They funnel through two helpers today, but the
    reason to enumerate them is that the rule must not become verb-dependent
    again, which is exactly the shape the original bug had.
    """

    OWNER_STORED = "alice@example.com"
    OWNER_CALLING = "Alice@example.com"
    STRANGER = "mallory@example.com"

    def _schedule_id(self, client_for: ClientFactory) -> str:
        created = client_for(self.OWNER_STORED).post(
            "/api/schedules/pipelines",
            json={
                "name": "Nightly",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": "team/nightly",
            },
        )
        assert created.status_code == status.HTTP_201_CREATED
        return created.json()["id"]

    @pytest.mark.parametrize(
        ("verb", "expected"),
        [
            pytest.param(
                lambda c, sid: c.get(f"/api/schedules/pipelines/{sid}"),
                status.HTTP_200_OK,
                id="get",
            ),
            pytest.param(
                lambda c, sid: c.get(
                    f"/api/schedules/pipelines/{sid}",
                    params={"include_spec": True},
                ),
                status.HTTP_200_OK,
                id="get-include-spec",
            ),
            pytest.param(
                lambda c, sid: c.patch(
                    f"/api/schedules/pipelines/{sid}", json={"name": "renamed"}
                ),
                status.HTTP_200_OK,
                id="patch",
            ),
            pytest.param(
                lambda c, sid: c.patch(
                    f"/api/schedules/pipelines/{sid}", json={"paused": True}
                ),
                status.HTTP_200_OK,
                id="pause",
            ),
            pytest.param(
                lambda c, sid: c.patch(
                    f"/api/schedules/pipelines/{sid}", json={"paused": False}
                ),
                status.HTTP_200_OK,
                id="resume",
            ),
            pytest.param(
                lambda c, sid: c.delete(f"/api/schedules/pipelines/{sid}"),
                status.HTTP_204_NO_CONTENT,
                id="delete",
            ),
        ],
    )
    def test_a_case_variant_caller_may_use_every_id_route(
        self,
        folding_owner_db_engine: sqlalchemy.Engine,
        client_for: ClientFactory,
        verb,
        expected: int,
    ) -> None:
        """Alice and alice are one person, by id as well as by path."""
        schedule_id = self._schedule_id(client_for)

        resp = verb(client_for(self.OWNER_CALLING), schedule_id)

        assert resp.status_code == expected, resp.text

    def test_a_case_variant_caller_may_trigger_by_id(
        self,
        folding_owner_db_engine: sqlalchemy.Engine,
        client_for: ClientFactory,
    ) -> None:
        """Separate because it needs the executor stubbed.

        Worth its own test rather than a skip in the table above: trigger reaches
        ownership through `_identity_by_id_or_404`, the other helper, and a fix
        applied to only one of the two would pass everything else here.
        """
        schedule_id = self._schedule_id(client_for)
        fake_run = api_server_sql.PipelineRunResponse(
            id="run-1", root_execution_id="exec-1"
        )

        with mock.patch.object(
            executor, "execute_pipeline_schedule", return_value=fake_run
        ):
            resp = client_for(self.OWNER_CALLING).post(
                f"/api/schedules/pipelines/{schedule_id}/trigger"
            )

        assert resp.status_code == status.HTTP_200_OK, resp.text

    @pytest.mark.parametrize(
        "verb",
        [
            pytest.param(
                lambda c, sid: c.get(f"/api/schedules/pipelines/{sid}"),
                id="get",
            ),
            pytest.param(
                lambda c, sid: c.patch(
                    f"/api/schedules/pipelines/{sid}", json={"name": "hijacked"}
                ),
                id="patch",
            ),
            pytest.param(
                lambda c, sid: c.delete(f"/api/schedules/pipelines/{sid}"),
                id="delete",
            ),
            pytest.param(
                lambda c, sid: c.post(f"/api/schedules/pipelines/{sid}/trigger"),
                id="trigger",
            ),
        ],
    )
    def test_a_genuinely_different_owner_is_still_refused(
        self,
        folding_owner_db_engine: sqlalchemy.Engine,
        client_for: ClientFactory,
        verb,
    ) -> None:
        """The negative control, without which every test above passes vacuously.

        Delegating the comparison to the database widens who counts as the
        owner; it must not stop there being one. A `_check_ownership` that simply
        returned would satisfy the whole class except this.
        """
        schedule_id = self._schedule_id(client_for)

        resp = verb(client_for(self.STRANGER), schedule_id)

        assert resp.status_code == status.HTTP_403_FORBIDDEN, resp.text

    def test_an_absent_id_is_404_and_a_foreign_one_is_403(
        self,
        folding_owner_db_engine: sqlalchemy.Engine,
        client_for: ClientFactory,
    ) -> None:
        """The split the id routes have always had, preserved on purpose.

        This is why authorization is a second statement rather than a filter on
        the resolving read: scoping the first SELECT by owner would have been
        simpler and would have collapsed every 403 here into a 404 -- a
        behaviour change this correction has no mandate to make, and one that
        would silently alter what the CLI reports.
        """
        schedule_id = self._schedule_id(client_for)
        stranger = client_for(self.STRANGER)

        assert (
            stranger.get("/api/schedules/pipelines/does-not-exist").status_code
            == status.HTTP_404_NOT_FOUND
        )
        assert (
            stranger.get(f"/api/schedules/pipelines/{schedule_id}").status_code
            == status.HTTP_403_FORBIDDEN
        )

    def test_an_admin_still_bypasses_without_a_probe(
        self,
        folding_owner_db_engine: sqlalchemy.Engine,
        db_engine: sqlalchemy.Engine,
        client_for: ClientFactory,
        admin_client: fastapi.testclient.TestClient,
    ) -> None:
        """Admin short-circuits BEFORE the ownership probe, as it did before.

        Asserted on the SQL as well as the status: an admin that reached the
        probe would still be allowed through, so a status-only test could not
        tell the short-circuit from a redundant query on every admin request.
        """
        schedule_id = self._schedule_id(client_for)

        with sql_capture.capture_sql(db_engine) as statements:
            resp = admin_client.get(f"/api/schedules/pipelines/{schedule_id}")

        assert resp.status_code == status.HTTP_200_OK
        probes = self._ownership_probes(statements)
        assert probes == [], probes

    @staticmethod
    def _ownership_probes(statements: list[str]) -> list[str]:
        """The authorization SELECT, matched on both of its predicates.

        Matched on the qualified column names. An earlier version of this looked
        for `" id = "`, which never matches -- SQLAlchemy emits
        `scheduled_pipeline_run.id = ?` -- so the filter silently found nothing
        and the admin assertion passed against a probe that had in fact run.
        """
        table = db_models.ScheduledPipelineRun.__tablename__
        return [
            s
            for s in statements
            if f"{table}.id = " in s and f"{table}.created_by = " in s
        ]

    def test_the_refusal_still_names_the_creator(
        self,
        folding_owner_db_engine: sqlalchemy.Engine,
        client_for: ClientFactory,
    ) -> None:
        """The 403 body is unchanged, and is still built from the row's own value.

        `created_by` is no longer COMPARED in Python -- only quoted. Keeping the
        message intact is deliberate: id routes disclose the creator by design
        (path routes 404 instead), and quietly dropping that would be a second,
        unrequested behaviour change riding along with this one.
        """
        schedule_id = self._schedule_id(client_for)

        resp = client_for(self.STRANGER).delete(
            f"/api/schedules/pipelines/{schedule_id}"
        )

        assert resp.status_code == status.HTTP_403_FORBIDDEN
        detail = resp.json()["detail"]
        assert self.OWNER_STORED in detail
        assert self.STRANGER in detail

    def test_no_python_owner_comparison_survives_in_the_ownership_check(
        self,
    ) -> None:
        """Structural, because a behavioural test cannot see a redundant check.

        Re-adding `user_details.name != created_by` alongside the SQL probe would
        restore the exact bug -- the Python check refuses first -- and every
        folding-fixture test above would go red in a way that reads as a fixture
        problem. Naming the shape makes the diagnosis immediate.

        Parsed rather than grepped, so a comparison inside a docstring or a
        comment explaining the history does not trip it.
        """
        source = inspect.getsource(api_routes._check_ownership)
        tree = ast.parse(textwrap.dedent(source))
        compares = [node for node in ast.walk(tree) if isinstance(node, ast.Compare)]

        assert compares == [], ast.dump(tree)
        assert "is_owned_by" in source


def validation_messages(*, response: httpx.Response) -> str:
    """Just the `msg` strings from a 422 body, joined.

    Pydantic also echoes the offending request body under `input`, so asserting against
    `str(response.json()["detail"])` passes whenever the caller merely *sent* the field --
    it reads the echo rather than the refusal. Found by review: a mutation truncating the
    validator to one field survived exactly that assertion.
    """
    return " ".join(entry["msg"] for entry in response.json()["detail"])


def validation_locations(*, response: httpx.Response) -> set[str]:
    """The field names a 422 blames, from `loc`. `extra="forbid"` puts the field there and
    leaves `msg` as a generic "Extra inputs are not permitted"."""
    return {str(part) for entry in response.json()["detail"] for part in entry["loc"]}


class TestSchedulePathAdoption:
    def test_patch_sets_path_when_stored_value_is_null(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Adoption is reachable only for rows that predate the required field.

        The row is inserted directly because create now demands a canonical path,
        so a path-less row can no longer be produced through the API. That is
        exactly the population adoption exists for.
        """
        sid = insert_pathless_schedule_row(db_engine=db_engine, name="Legacy")

        resp = client.patch(
            f"/api/schedules/pipelines/{sid}",
            json={"schedule_path": "Upi/Adopted"},
        )
        assert resp.status_code == status.HTTP_200_OK
        assert resp.json()["schedule_path"] == "Upi/Adopted"

    def test_patch_resending_the_same_canonical_value_is_a_no_op(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """So a client can replay a PATCH without special-casing this field."""
        sid = client.post(
            "/api/schedules/pipelines",
            json={
                "name": "Set",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": "upi/nightly",
            },
        ).json()["id"]

        # Re-sending the SAME value, only padded, is the no-op. A differently
        # cased value is a different path and is refused by the set-once rule --
        # see `test_a_case_variant_is_not_the_same_path_to_adopt`.
        resp = client.patch(
            f"/api/schedules/pipelines/{sid}",
            json={"schedule_path": "  upi/nightly  "},
        )
        assert resp.status_code == status.HTTP_200_OK
        assert resp.json()["schedule_path"] == "upi/nightly"

    def test_patch_refuses_to_change_an_existing_path(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Set once: rewriting it would break every caller addressing the old one."""
        sid = client.post(
            "/api/schedules/pipelines",
            json={
                "name": "Set",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": "upi/nightly",
            },
        ).json()["id"]

        resp = client.patch(
            f"/api/schedules/pipelines/{sid}",
            json={"schedule_path": "upi/something-else"},
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert "set once" in resp.json()["detail"]

    def test_adoption_collision_conflicts(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        client.post(
            "/api/schedules/pipelines",
            json={
                "name": "Holder",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": "upi/taken",
            },
        )
        sid = insert_pathless_schedule_row(db_engine=db_engine, name="Adopter")

        resp = client.patch(
            f"/api/schedules/pipelines/{sid}",
            json={"schedule_path": "upi/taken"},
        )
        assert resp.status_code == status.HTTP_409_CONFLICT

    @pytest.mark.parametrize(
        "field",
        [
            "pipeline_task_spec_from_pipeline_run_id",
            "pipeline_task_spec_from_user_pipeline_id",
            "pipeline_task_spec_from_user_pipeline_version_key",
        ],
    )
    def test_patch_refuses_a_source_transition_and_says_why(
        self,
        client: fastapi.testclient.TestClient,
        field: str,
    ) -> None:
        """Source switching is out of scope for PATCH, and the model_validator says so.

        Previously the field was unknown to the update model and silently dropped, so
        this returned 200 with the source unchanged -- a caller reading that response
        would believe the repoint had been applied. Now the refusal names the field it
        refused, and the one-source rule behind it.
        """
        sid = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": f"sweep/{field.removeprefix('pipeline_task_spec_')}",
                "name": "Inline",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
            },
        ).json()["id"]

        resp = client.patch(
            f"/api/schedules/pipelines/{sid}", json={field: "some-value"}
        )

        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        message = validation_messages(response=resp)
        assert field in message
        assert "exactly one" in message and "create a new schedule" in message

    def test_patch_names_every_source_field_the_caller_sent(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Two at once is one refusal listing both, not a refusal of whichever the model
        happened to see first -- the caller fixes the request in one round trip."""
        sid = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/both-fields",
                "name": "Inline",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
            },
        ).json()["id"]

        resp = client.patch(
            f"/api/schedules/pipelines/{sid}",
            json={
                "pipeline_task_spec_from_pipeline_run_id": "run-1",
                "pipeline_task_spec_from_user_pipeline_id": "pipeline-1",
            },
        )

        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        message = validation_messages(response=resp)
        assert "pipeline_task_spec_from_pipeline_run_id" in message
        assert "pipeline_task_spec_from_user_pipeline_id" in message

    def test_the_source_is_unchanged_after_a_refused_transition(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """The refusal is the whole effect: the schedule is still inline-sourced, which
        is what the old silent 200 also left behind but did not admit to."""
        sid = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/unchanged",
                "name": "Inline",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
            },
        ).json()["id"]

        client.patch(
            f"/api/schedules/pipelines/{sid}",
            json={"pipeline_task_spec_from_user_pipeline_id": "some-pipeline-id"},
        )

        after = client.get(
            f"/api/schedules/pipelines/{sid}", params={"include_spec": True}
        ).json()
        assert after["pipeline_task_spec_from_user_pipeline_id"] is None
        assert after["pipeline_task_spec"] is not None

    @pytest.mark.parametrize(
        ("method", "body"),
        [
            pytest.param(
                "post",
                {
                    "schedule_path": "sweep/typo-create",
                    "name": "Typo",
                    "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                    "cron_expression": "0 8 * * *",
                    "pipeline_template": {"arguments": {"d": "{{ now }}"}},
                },
                id="create",
            ),
            pytest.param(
                "patch",
                {"pipeline_template": {"arguments": {"d": "{{ now }}"}}},
                id="update",
            ),
        ],
    )
    def test_a_misspelled_field_is_refused_rather_than_dropped(
        self,
        client: fastapi.testclient.TestClient,
        method: str,
        body: dict[str, typing.Any],
    ) -> None:
        """`pipeline_template` for `pipeline_templates` used to return 201/200 having
        stored nothing, so the mistake surfaced a cron cycle later as a run using the
        spec's default. `extra="forbid"` turns it into a 422 naming the field."""
        if method == "post":
            resp = client.post("/api/schedules/pipelines", json=body)
        else:
            sid = client.post(
                "/api/schedules/pipelines",
                json={
                    "schedule_path": "sweep/typo-update",
                    "name": "Typo",
                    "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                    "cron_expression": "0 8 * * *",
                },
            ).json()["id"]
            resp = client.patch(f"/api/schedules/pipelines/{sid}", json=body)

        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert "pipeline_template" in validation_locations(response=resp)


class TestExactlyOneSource:
    """Enforced by the API, not delegated to the CHECK.

    ``ck_scheduled_pipeline_run_source`` may legitimately never install on a live
    table, so a write path that relied on it would accept bad rows exactly where
    it matters most.
    """

    def test_no_source_is_rejected(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "name": "Sourceless",
                "cron_expression": "0 8 * * *",
                "schedule_path": "sweep/p36",
            },
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert "exactly one spec source" in resp.json()["detail"]

    def test_two_sources_are_rejected(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p37",
                "name": "Two sources",
                "cron_expression": "0 8 * * *",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "pipeline_task_spec_from_user_pipeline_id": "some-id",
            },
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        detail = resp.json()["detail"]
        assert "Received 2" in detail

    def test_version_key_without_a_pipeline_id_is_rejected(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p38",
                "name": "Dangling pin",
                "cron_expression": "0 8 * * *",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "pipeline_task_spec_from_user_pipeline_version_key": "a" * 64,
            },
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert "cannot be supplied without it" in resp.json()["detail"]

    def test_inline_spec_is_still_validated(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Making the field optional must not have made it unvalidated."""
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p39",
                "name": "Bad inline",
                "cron_expression": "0 8 * * *",
                "pipeline_task_spec": BARE_COMPONENT_SPEC,
            },
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


class TestClosedSchemaTiers:
    """A closed tier is 503 with a logical code -- never 409/422, never an index name.

    Readiness is computed once per process, so these use an injected closed
    report. Production has no recompute path by design.
    """

    def test_path_write_is_refused_when_the_path_tier_is_closed(
        self,
        closed_tier_client: fastapi.testclient.TestClient,
    ) -> None:
        resp = closed_tier_client.post(
            "/api/schedules/pipelines",
            json={
                "name": "Nightly",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": "upi/nightly",
            },
        )
        assert resp.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
        assert (
            resp.json()["detail"]["code"] == api_routes.SCHEDULE_PATH_WRITES_UNAVAILABLE
        )

    def test_reference_write_is_refused_when_the_reference_tier_is_closed(
        self,
        reference_closed_client: fastapi.testclient.TestClient,
    ) -> None:
        """Uses a path-open/reference-closed report on purpose.

        Now that every create writes a path, the path gate runs first on every
        create, so under a fully closed report this would return the path code and
        the reference code would be unreachable -- the assertion would pass on the
        wrong mechanism, or the branch would go untested.
        """
        resp = reference_closed_client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p40",
                "name": "Referencing",
                "cron_expression": "0 8 * * *",
                "pipeline_task_spec_from_user_pipeline_id": "some-pipeline-id",
            },
        )
        assert resp.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
        assert (
            resp.json()["detail"]["code"]
            == api_routes.PIPELINE_REFERENCE_WRITES_UNAVAILABLE
        )

    def test_no_retry_after_header(
        self,
        closed_tier_client: fastapi.testclient.TestClient,
    ) -> None:
        """Recovery is operator-controlled, so any interval would be a false promise."""
        resp = closed_tier_client.post(
            "/api/schedules/pipelines",
            json={
                "name": "Nightly",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": "upi/nightly",
            },
        )
        assert "retry-after" not in {k.lower() for k in resp.headers}

    def test_no_physical_index_names_leak(
        self,
        closed_tier_client: fastapi.testclient.TestClient,
    ) -> None:
        resp = closed_tier_client.post(
            "/api/schedules/pipelines",
            json={
                "name": "Nightly",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": "upi/nightly",
            },
        )
        body = resp.text
        assert "uq_" not in body
        assert "ix_" not in body
        assert "scheduled_pipeline_run" not in body

    def test_every_create_is_gated_including_plain_inline(
        self,
        closed_tier_client: fastapi.testclient.TestClient,
    ) -> None:
        """Deliberate reversal of the earlier "inline is unaffected" guarantee.

        `schedule_path` is optional in the *request*, but every successful create
        stores one -- supplied or derived -- so every create writes the new column
        and every create depends on the path tier. That includes a plain inline
        create that names no pipeline and sends no path.

        The blast radius is therefore total rather than feature-scoped: on a pod
        that booted before the index migration succeeded, schedule creation stops
        entirely. Accepted because the rollout order puts the index strictly ahead
        of this code, but asserted rather than left implicit so the consequence is
        visible to whoever changes the gate next.
        """
        resp = closed_tier_client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p41",
                "name": "Plain inline",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
            },
        )
        assert resp.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
        assert (
            resp.json()["detail"]["code"] == api_routes.SCHEDULE_PATH_WRITES_UNAVAILABLE
        )

    def test_a_path_filtered_read_is_gated(
        self,
        closed_tier_client: fastapi.testclient.TestClient,
    ) -> None:
        """503, and for correctness rather than cost.

        This originally asserted 200, on the argument that a missing index makes
        a read slower but never wrong. That argument was wrong. Owner and path
        are both resolved in the database, and the path predicate is only as
        precise as the column's collation: without the conversion, case
        neighbours of the requested path can match and fill the page, so the
        caller's own row is pushed out of the result and the response says
        nothing is there. A slow answer would have been fine; a confidently
        wrong empty one is not.

        (This docstring previously described a Python exact-owner re-check
        running after the row limit. That comparator was removed when owner
        equality was delegated to the database column; the hazard is now the
        path predicate alone.)
        """
        resp = closed_tier_client.get(
            "/api/schedules/pipelines",
            params={"schedule_path": "upi/nightly"},
        )
        assert resp.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
        assert (
            resp.json()["detail"]["code"] == api_routes.SCHEDULE_PATH_WRITES_UNAVAILABLE
        )

    def test_the_unfiltered_list_is_still_not_gated(
        self,
        closed_tier_client: fastapi.testclient.TestClient,
    ) -> None:
        """The inspectability argument survives where it actually holds.

        An unfiltered list names no path, so the path collation hazard cannot
        arise: nothing is being matched against the column being converted. It
        remains available, which is what lets someone look at the data while the
        index migration is outstanding.

        (This docstring previously claimed the unfiltered list applies no owner
        predicate. It does apply one, in the database -- that was never the
        hazard here, and the claim was wrong.)
        """
        resp = closed_tier_client.get("/api/schedules/pipelines")

        assert resp.status_code == status.HTTP_200_OK

    def test_patch_adoption_is_gated(
        self,
        closed_tier_client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Inserted directly: create is unavailable under a closed path tier."""
        sid = insert_pathless_schedule_row(db_engine=db_engine, name="Legacy")

        resp = closed_tier_client.patch(
            f"/api/schedules/pipelines/{sid}",
            json={"schedule_path": "upi/adopted"},
        )
        assert resp.status_code == status.HTTP_503_SERVICE_UNAVAILABLE

    def test_ordinary_patch_is_not_gated(
        self,
        closed_tier_client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """An id-addressed edit that touches no new column still works.

        This is the one write that survives a closed path tier, and it matters:
        it is how an operator pauses a misbehaving legacy schedule while the index
        migration is still outstanding.
        """
        sid = insert_pathless_schedule_row(db_engine=db_engine, name="Legacy")

        resp = closed_tier_client.patch(
            f"/api/schedules/pipelines/{sid}",
            json={"name": "Renamed"},
        )
        assert resp.status_code == status.HTTP_200_OK
        assert resp.json()["name"] == "Renamed"

    def test_path_addressed_routes_are_gated(
        self,
        closed_tier_client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Addressing BY path depends on the unique index, so it is gated too.

        Checked for all three point routes together: a partial gate here would
        mean one addressing mode silently falls back to a table scan under load
        while the others refuse.
        """
        for method, url in (
            ("patch", "/api/schedules/pipelines?schedule_path=upi/nightly"),
            ("delete", "/api/schedules/pipelines?schedule_path=upi/nightly"),
            (
                "post",
                "/api/schedules/pipelines/trigger?schedule_path=upi/nightly",
            ),
        ):
            kwargs = {"json": {"name": "x"}} if method == "patch" else {}
            resp = getattr(closed_tier_client, method)(url, **kwargs)
            assert resp.status_code == status.HTTP_503_SERVICE_UNAVAILABLE, (
                method,
                url,
            )
            assert (
                resp.json()["detail"]["code"]
                == api_routes.SCHEDULE_PATH_WRITES_UNAVAILABLE
            )


def _save_pipeline(
    *,
    db_engine: sqlalchemy.Engine,
    user_id: str = DEFAULT_USER,
    file_path: str = "rollup/daily.pipeline.yaml",
    name: str = "saved-pipeline",
    versioning_mode: user_pipeline_db_models.PipelineVersioningMode = (
        user_pipeline_db_models.PipelineVersioningMode.FULL
    ),
) -> tuple[str, str]:
    """Save a pipeline through the real service and return (pipeline_id, version_key)."""
    spec = copy.deepcopy(SAMPLE_PIPELINE_TASK_SPEC)
    spec["componentRef"]["spec"]["name"] = name
    with sqlalchemy.orm.Session(bind=db_engine) as session:
        result = user_pipeline_services.UserPipelineService().set_pipeline(
            session=session,
            user_id=user_id,
            file_path=file_path,
            root_pipeline_task=spec,
            pipeline_run_annotations=None,
            versioning_mode=versioning_mode,
        )
        return result.pipeline.id, result.version.version_key


class TestSavedPipelineReferenceWriter:
    """The writer validates through the same path the executor submits through.

    ``get_pipeline_and_version(require_pinnable=True)`` is called rather than
    reimplemented, because the writer and the executor agreeing on what is
    pinnable is the entire reason that rule was centralized.
    """

    @staticmethod
    def _enforce_foreign_keys(
        db_engine: sqlalchemy.Engine, *, valid_pipeline_id: str
    ) -> None:
        """Make SQLite check the foreign keys, and prove that it is the FK checking.

        The suite's engine runs with `PRAGMA foreign_keys=0`, so the saved-pipeline
        foreign key exists in the DDL and is never enforced. MySQL has no such
        switch. A test that stored a dangling reference would therefore pass here
        and fail in production, which is exactly what happened: the defect below
        shipped green.

        Turning the pragma on is not enough to trust it, so this provokes a
        violation and requires the database to reject it. The first version of that
        probe omitted `created_at`, so SQLite raised NOT NULL before it ever
        evaluated the foreign key, and the probe certified enforcement it had not
        tested -- a guard against vacuity that was itself vacuous (found by review).
        Two things prevent a repeat: every other column is populated so the foreign
        key is the ONLY constraint the row can violate, and the driver's message is
        asserted to be the foreign-key one rather than merely some `IntegrityError`.

        The control matters as much as the probe. The same row with a real parent
        must INSERT cleanly, otherwise "rejected" would prove nothing -- a row
        rejected for an unrelated reason looks identical from outside.

        Deliberately local to these tests. Flipping the pragma for the whole suite
        breaks nine tests that fabricate references MySQL would already refuse, so
        it is a change with its own fallout and belongs in its own review.
        """
        with db_engine.begin() as connection:
            connection.exec_driver_sql("PRAGMA foreign_keys=ON")

        with db_engine.begin() as connection:
            assert connection.exec_driver_sql("PRAGMA foreign_keys").scalar() == 1

        def _probe_row(
            connection: sqlalchemy.Connection, *, row_id: str, parent: str
        ) -> None:
            connection.exec_driver_sql(
                "INSERT INTO scheduled_pipeline_run"
                " (id, name, cron_expression, timezone, created_by, created_at, updated_at,"
                "  pipeline_task_spec_from_user_pipeline_id, schedule_path, paused, extra_data)"
                " VALUES (?, 'fk probe', '0 8 * * *', 'UTC', 'probe@example.com',"
                "  '2026-01-01 00:00:00', '2026-01-01 00:00:00', ?, ?, 0, 'null')",
                (row_id, parent, f"probe/{row_id}"),
            )

        with pytest.raises(sqlalchemy.exc.IntegrityError) as refused:
            with db_engine.begin() as connection:
                _probe_row(
                    connection,
                    row_id="fk-probe-absent",
                    parent="no-such-pipeline-id",
                )
        # Specifically the foreign key -- not NOT NULL, not the source invariant.
        assert "FOREIGN KEY constraint failed" in str(
            refused.value.orig
        ), refused.value.orig

        # Control: identical row, real parent. If this failed too, the rejection
        # above would be evidence of nothing.
        with db_engine.begin() as connection:
            _probe_row(connection, row_id="fk-probe-present", parent=valid_pipeline_id)
        with db_engine.begin() as connection:
            connection.exec_driver_sql(
                "DELETE FROM scheduled_pipeline_run WHERE id = 'fk-probe-present'"
            )

    def test_a_reference_is_stored_as_the_id_the_lookup_resolved(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """A hex-spelled reference must be stored canonically, not as it was typed.

        `normalize_pipeline_id` accepts any spelling `uuid.UUID` accepts, so the
        preflight resolves the bare 32-character hex form and reports the reference
        good. The foreign key points at `pipeline.id`, which is hyphenated. Storing
        the request's spelling therefore wrote a value no pipeline row carries, and
        the insert was refused as a reference that does not exist -- a 404 naming
        the pipeline the preflight had just loaded, for a caller who supplied a
        spelling the API had accepted.
        """
        pipeline_id, version_key = _save_pipeline(db_engine=db_engine)
        hex_spelling = uuid.UUID(pipeline_id).hex
        assert hex_spelling != pipeline_id
        self._enforce_foreign_keys(db_engine, valid_pipeline_id=pipeline_id)

        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/hex",
                "name": "Hex spelled",
                "cron_expression": "0 8 * * *",
                "pipeline_task_spec_from_user_pipeline_id": hex_spelling,
                "pipeline_task_spec_from_user_pipeline_version_key": version_key,
            },
        )

        assert resp.status_code == status.HTTP_201_CREATED, resp.text
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            stored = session.scalar(
                sqlalchemy.select(
                    db_models.ScheduledPipelineRun.pipeline_task_spec_from_user_pipeline_id
                ).where(db_models.ScheduledPipelineRun.id == resp.json()["id"])
            )
            # The stored value must name a row that is really there, which is the
            # property the foreign key is about -- not merely differ from the input.
            referenced = session.scalar(
                sqlalchemy.select(user_pipeline_db_models.UserPipeline.id).where(
                    user_pipeline_db_models.UserPipeline.id == stored
                )
            )
        assert stored == pipeline_id
        assert referenced == pipeline_id

    def test_the_canonical_spelling_is_unaffected(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Control for the test above: the spelling that always worked still works."""
        pipeline_id, version_key = _save_pipeline(db_engine=db_engine)
        self._enforce_foreign_keys(db_engine, valid_pipeline_id=pipeline_id)

        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/canonical",
                "name": "Canonical spelled",
                "cron_expression": "0 8 * * *",
                "pipeline_task_spec_from_user_pipeline_id": pipeline_id,
                "pipeline_task_spec_from_user_pipeline_version_key": version_key,
            },
        )

        assert resp.status_code == status.HTTP_201_CREATED, resp.text
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            stored = session.scalar(
                sqlalchemy.select(
                    db_models.ScheduledPipelineRun.pipeline_task_spec_from_user_pipeline_id
                ).where(db_models.ScheduledPipelineRun.id == resp.json()["id"])
            )
        assert stored == pipeline_id

    def test_current_following_reference_is_accepted(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        pipeline_id, _ = _save_pipeline(db_engine=db_engine)

        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p44",
                "name": "Follows current",
                "cron_expression": "0 8 * * *",
                "pipeline_task_spec_from_user_pipeline_id": pipeline_id,
            },
        )
        assert resp.status_code == status.HTTP_201_CREATED
        body = resp.json()
        assert body["pipeline_task_spec_from_user_pipeline_id"] == pipeline_id
        # Null is the information: it is what makes this reference
        # current-following rather than pinned.
        assert body["pipeline_task_spec_from_user_pipeline_version_key"] is None

    def test_pinned_reference_is_accepted_for_a_full_mode_pipeline(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        pipeline_id, version_key = _save_pipeline(db_engine=db_engine)

        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p45",
                "name": "Pinned",
                "cron_expression": "0 8 * * *",
                "pipeline_task_spec_from_user_pipeline_id": pipeline_id,
                "pipeline_task_spec_from_user_pipeline_version_key": version_key,
            },
        )
        assert resp.status_code == status.HTTP_201_CREATED
        assert (
            resp.json()["pipeline_task_spec_from_user_pipeline_version_key"]
            == version_key
        )

    def test_pinning_a_disabled_mode_pipeline_is_refused(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Excluding the reserved `current` key is necessary but not sufficient.

        Switching to DISABLED does not delete historical immutable rows, so
        without this the pin would resolve a version its owner can no longer see
        or manage through the versioning APIs.
        """
        pipeline_id, version_key = _save_pipeline(
            db_engine=db_engine,
            versioning_mode=user_pipeline_db_models.PipelineVersioningMode.DISABLED,
        )

        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p46",
                "name": "Pinned to disabled",
                "cron_expression": "0 8 * * *",
                "pipeline_task_spec_from_user_pipeline_id": pipeline_id,
                "pipeline_task_spec_from_user_pipeline_version_key": version_key,
            },
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert "cannot be pinned" in resp.json()["detail"]

    def test_following_a_disabled_mode_pipeline_is_allowed(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Following the mutable `current` head is exactly what no-version means."""
        pipeline_id, _ = _save_pipeline(
            db_engine=db_engine,
            versioning_mode=user_pipeline_db_models.PipelineVersioningMode.DISABLED,
        )

        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p47",
                "name": "Follows disabled",
                "cron_expression": "0 8 * * *",
                "pipeline_task_spec_from_user_pipeline_id": pipeline_id,
            },
        )
        assert resp.status_code == status.HTTP_201_CREATED

    def test_reference_to_another_users_pipeline_is_not_found(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Reported as not found, not forbidden.

        Confirming that an id exists but belongs to someone else is itself a
        disclosure. This is also the check that stops a schedule executing another
        user's saved pipeline by id.
        """
        pipeline_id, _ = _save_pipeline(db_engine=db_engine, user_id=OTHER_USER)

        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p48",
                "name": "Someone else's",
                "cron_expression": "0 8 * * *",
                "pipeline_task_spec_from_user_pipeline_id": pipeline_id,
            },
        )
        assert resp.status_code == status.HTTP_404_NOT_FOUND

    def test_reference_to_a_missing_pipeline_is_not_found(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """A well-formed id that resolves to nothing is 404.

        Reachable in normal operation, but NOT because the reference is
        unconstrained -- `fk_scheduled_pipeline_run_user_pipeline_id` exists. The
        404 comes from the semantic preflight, which rejects far more than the
        constraint can: a soft-deleted pipeline is still physically present and
        would satisfy the foreign key, yet must not be schedulable. Ownership,
        version existence and pinnability are the same kind of rule.
        """
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p49",
                "name": "Dangling",
                "cron_expression": "0 8 * * *",
                "pipeline_task_spec_from_user_pipeline_id": (
                    "00000000-0000-4000-8000-000000000000"
                ),
            },
        )
        assert resp.status_code == status.HTTP_404_NOT_FOUND

    def test_malformed_pipeline_id_is_a_validation_error(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Distinct from the 404 above: a non-UUID is a bad request, not a miss.

        Worth pinning because both arrive through the same service call and it
        would be easy to collapse them into one status.
        """
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p50",
                "name": "Malformed ref",
                "cron_expression": "0 8 * * *",
                "pipeline_task_spec_from_user_pipeline_id": "does-not-exist",
            },
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert "valid UUID" in resp.json()["detail"]

    def test_pinning_an_unknown_version_is_not_found(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        pipeline_id, _ = _save_pipeline(db_engine=db_engine)

        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p51",
                "name": "Bad pin",
                "cron_expression": "0 8 * * *",
                "pipeline_task_spec_from_user_pipeline_id": pipeline_id,
                "pipeline_task_spec_from_user_pipeline_version_key": "d" * 64,
            },
        )
        assert resp.status_code == status.HTTP_404_NOT_FOUND

    def test_reference_and_path_can_be_set_together(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Both tiers are required, and both are open here."""
        pipeline_id, _ = _save_pipeline(db_engine=db_engine)

        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "name": "Both",
                "cron_expression": "0 8 * * *",
                "pipeline_task_spec_from_user_pipeline_id": pipeline_id,
                "schedule_path": "upi/both",
            },
        )
        assert resp.status_code == status.HTTP_201_CREATED
        body = resp.json()
        assert body["schedule_path"] == "upi/both"
        assert body["pipeline_task_spec_from_user_pipeline_id"] == pipeline_id

    def test_run_reference_is_accepted(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """A run reference is a first-class source and predates this change.

        It is not gated on the saved-pipeline reference tier, because it does not
        depend on the index that tier is about. It IS owner-validated, so the run
        must exist and belong to the caller.
        """
        run_id = insert_pipeline_run(db_engine=db_engine)

        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "sweep/p52",
                "name": "From run",
                "cron_expression": "0 8 * * *",
                "pipeline_task_spec_from_pipeline_run_id": run_id,
            },
        )
        assert resp.status_code == status.HTTP_201_CREATED
        assert resp.json()["pipeline_task_spec_from_pipeline_run_id"] == run_id


class TestRunReferenceIsNotAnExistenceOracle:
    """A run reference must be validated before it is stored.

    Previously only saved-pipeline references were checked, so a run reference was
    inserted unvalidated. Under the declared foreign key that made create an
    existence oracle: a nonexistent run id failed the INSERT and every
    IntegrityError was reported as a duplicate-path 409, while another user's real
    run id succeeded with 201. The pair of responses distinguished "this id does
    not exist" from "this id exists but is not yours", and the 201 stored a
    schedule guaranteed to fail owner validation at every fire.
    """

    def _post(
        self,
        client: fastapi.testclient.TestClient,
        *,
        run_id: str,
        schedule_path: str,
    ) -> object:
        return client.post(
            "/api/schedules/pipelines",
            json={
                "name": "From run",
                "cron_expression": "0 8 * * *",
                "schedule_path": schedule_path,
                "pipeline_task_spec_from_pipeline_run_id": run_id,
            },
        )

    def test_a_missing_run_is_not_found(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        resp = self._post(
            client,
            run_id="0123456789abcdef0123",
            schedule_path="upi/missing-run",
        )
        assert resp.status_code == status.HTTP_404_NOT_FOUND

    def test_another_users_run_is_not_found(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Refused, and refused the SAME way as a missing run."""
        run_id = insert_pipeline_run(db_engine=db_engine, created_by=OTHER_USER)

        resp = self._post(client, run_id=run_id, schedule_path="upi/foreign-run")
        assert resp.status_code == status.HTTP_404_NOT_FOUND

    def test_an_unattributed_run_is_not_found(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """`PipelineRun.created_by` is nullable; a NULL owner authorizes nobody."""
        run_id = insert_pipeline_run(db_engine=db_engine, created_by=None)

        resp = self._post(client, run_id=run_id, schedule_path="upi/orphan-run")
        assert resp.status_code == status.HTTP_404_NOT_FOUND

    def test_missing_and_foreign_are_indistinguishable(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The actual oracle test: identical status AND identical body.

        Asserted as a comparison rather than two separate expectations, because
        the leak was the *difference* between the two responses.
        """
        foreign = insert_pipeline_run(db_engine=db_engine, created_by=OTHER_USER)

        missing_resp = self._post(
            client, run_id="0123456789abcdef0123", schedule_path="upi/probe-a"
        )
        foreign_resp = self._post(client, run_id=foreign, schedule_path="upi/probe-b")

        assert missing_resp.status_code == foreign_resp.status_code
        # Only the echoed id may differ between the two messages.
        assert missing_resp.json()["detail"].replace(
            "0123456789abcdef0123", "RUN"
        ) == foreign_resp.json()["detail"].replace(foreign, "RUN")

    def test_a_refused_run_reference_stores_nothing(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The refusal must not leave the path consumed.

        Otherwise a rejected create would silently burn the caller's chosen
        identity, and the retry after fixing the run id would 409.
        """
        foreign = insert_pipeline_run(db_engine=db_engine, created_by=OTHER_USER)
        self._post(client, run_id=foreign, schedule_path="upi/reusable")

        mine = insert_pipeline_run(db_engine=db_engine)
        retry = self._post(client, run_id=mine, schedule_path="upi/reusable")
        assert retry.status_code == status.HTTP_201_CREATED

    def test_a_missing_run_is_not_reported_as_a_path_conflict(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """The mislabelling half of the defect, pinned independently.

        A create naming a nonexistent run used to return 409 about a schedule_path
        the caller had never seen used, sending them to fix the one thing that was
        fine.
        """
        resp = self._post(
            client,
            run_id="0123456789abcdef0123",
            schedule_path="upi/never-used",
        )
        assert resp.status_code != status.HTTP_409_CONFLICT
        assert "schedule_path" not in resp.text

    def test_a_genuine_path_collision_is_still_a_conflict(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Narrowing the IntegrityError classification must not lose the real 409."""
        run_id = insert_pipeline_run(db_engine=db_engine)
        first = self._post(client, run_id=run_id, schedule_path="upi/taken-once")
        assert first.status_code == status.HTTP_201_CREATED

        second = self._post(client, run_id=run_id, schedule_path="upi/taken-once")
        assert second.status_code == status.HTTP_409_CONFLICT

    def test_the_conflict_response_names_no_physical_index(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        run_id = insert_pipeline_run(db_engine=db_engine)
        self._post(client, run_id=run_id, schedule_path="upi/leak-check")
        resp = self._post(client, run_id=run_id, schedule_path="upi/leak-check")

        body = resp.text
        assert "uq_" not in body
        assert "ix_" not in body
        assert "scheduled_pipeline_run" not in body


class TestPathAddressedLifecycle:
    """PATCH/DELETE/trigger addressed by `?schedule_path=`.

    The path is a query parameter rather than a path segment because paths contain
    `/`, so `upi/nightly` in the URL position would be indistinguishable from a
    nested route. That makes the collection URL the addressable resource, which is
    why these live on `_API_BASE` itself.

    Every case here is also asserted against the id-addressed route, because two
    addressing modes reaching the same row through different code is exactly how
    they drift.
    """

    _PATH = "upi/nightly"

    def _create(
        self,
        client: fastapi.testclient.TestClient,
        *,
        schedule_path: str | None = None,
    ) -> str:
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "name": "Nightly",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": schedule_path or self._PATH,
            },
        )
        assert resp.status_code == status.HTTP_201_CREATED, resp.text
        return resp.json()["id"]

    def test_patch_by_path_updates_the_same_row_as_patch_by_id(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        sid = self._create(client)

        resp = client.patch(
            "/api/schedules/pipelines",
            params={"schedule_path": self._PATH},
            json={"name": "Renamed by path"},
        )
        assert resp.status_code == status.HTTP_200_OK
        assert resp.json()["name"] == "Renamed by path"
        # Same row, not a second one.
        assert resp.json()["id"] == sid
        assert client.get(f"/api/schedules/pipelines/{sid}").json()["name"] == (
            "Renamed by path"
        )

    def test_patch_by_path_accepts_a_non_canonical_spelling(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Lookup normalizes exactly as the write did, or writes become unreachable.

        Surrounding whitespace is normalized; case is NOT, because case is part
        of the identity. A trailing '/' is not normalized either --
        it is a 422 from the canonicalizer, asserted in
        `test_an_invalid_path_is_a_validation_error_not_a_miss`'s sibling cases --
        so it deliberately is not used as the non-canonical example here.
        """
        self._create(client)

        resp = client.patch(
            "/api/schedules/pipelines",
            params={"schedule_path": "  upi/nightly  "},
            json={"name": "Found anyway"},
        )
        assert resp.status_code == status.HTTP_200_OK
        assert resp.json()["name"] == "Found anyway"

    def test_patch_by_path_cannot_repath(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """The path is the address, so changing it would rewrite the addressing."""
        self._create(client)

        resp = client.patch(
            "/api/schedules/pipelines",
            params={"schedule_path": self._PATH},
            json={"schedule_path": "upi/somewhere-else"},
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert "path-addressed" in resp.json()["detail"]

    def test_patch_by_path_rejects_repath_even_to_the_same_value(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Refused on the addressing mode, not on the value.

        A same-value no-op is accepted by the id route; here the field itself is
        not addressable, so accepting it conditionally would make the rule depend
        on what the caller happened to send.
        """
        self._create(client)

        resp = client.patch(
            "/api/schedules/pipelines",
            params={"schedule_path": self._PATH},
            json={"schedule_path": self._PATH},
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_delete_by_path_removes_the_row_and_the_scheduler_job(
        self,
        client: fastapi.testclient.TestClient,
        scheduler_svc: services.SchedulerService,
    ) -> None:
        sid = self._create(client)

        with mock.patch.object(scheduler_svc, "remove_schedule") as remove:
            resp = client.request(
                "DELETE",
                "/api/schedules/pipelines",
                params={"schedule_path": self._PATH},
            )
        assert resp.status_code == status.HTTP_204_NO_CONTENT
        # The job is removed by id even though the row was addressed by path.
        remove.assert_called_once_with(schedule_id=sid)
        assert (
            client.get(f"/api/schedules/pipelines/{sid}").status_code
            == status.HTTP_404_NOT_FOUND
        )

    def test_id_addressed_delete_still_works(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """The new route is additive; the id route is the one every caller uses."""
        sid = self._create(client)

        resp = client.delete(f"/api/schedules/pipelines/{sid}")
        assert resp.status_code == status.HTTP_204_NO_CONTENT

    def test_trigger_by_path_fires_the_same_schedule(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        sid = self._create(client)
        fake_run = api_server_sql.PipelineRunResponse(
            id="run-789",
            root_execution_id="exec-789",
        )

        with mock.patch.object(
            executor,
            "execute_pipeline_schedule",
            return_value=fake_run,
        ) as execute:
            resp = client.post(
                "/api/schedules/pipelines/trigger",
                params={"schedule_path": self._PATH},
            )
        assert resp.status_code == status.HTTP_200_OK
        # Resolution happens at the API boundary: the executor is still called by
        # id, so it needs no path awareness at all. `manual=True` because this path
        # bypasses APScheduler, so no schedule_time was published for the fire.
        execute.assert_called_once_with(pipeline_schedule_id=sid, manual=True)
        assert resp.json()["schedule_id"] == sid

    def test_trigger_by_path_on_a_paused_schedule_is_a_conflict(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Parity check: same refusal as the id route, from the shared handler."""
        sid = self._create(client)
        client.patch(f"/api/schedules/pipelines/{sid}", json={"paused": True})

        resp = client.post(
            "/api/schedules/pipelines/trigger",
            params={"schedule_path": self._PATH},
        )
        assert resp.status_code == status.HTTP_409_CONFLICT

    def test_literal_trigger_route_does_not_shadow_the_id_route(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Both trigger shapes coexist.

        Worth pinning because the literal `/trigger` segment sits where an id
        would go for a collection-level POST. They differ in depth today, so there
        is no ambiguity, but that is a property of the current shapes rather than
        something guaranteed.
        """
        sid = self._create(client)
        fake_run = api_server_sql.PipelineRunResponse(
            id="run-abc",
            root_execution_id="exec-abc",
        )

        with mock.patch.object(
            executor, "execute_pipeline_schedule", return_value=fake_run
        ):
            by_id = client.post(f"/api/schedules/pipelines/{sid}/trigger")
            by_path = client.post(
                "/api/schedules/pipelines/trigger",
                params={"schedule_path": self._PATH},
            )
        assert by_id.status_code == status.HTTP_200_OK
        assert by_path.status_code == status.HTTP_200_OK
        assert by_id.json()["schedule_id"] == by_path.json()["schedule_id"] == sid

    def test_a_missing_path_is_not_found(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        resp = client.patch(
            "/api/schedules/pipelines",
            params={"schedule_path": "upi/never-created"},
            json={"name": "x"},
        )
        assert resp.status_code == status.HTTP_404_NOT_FOUND

    def test_another_users_path_is_not_found_rather_than_forbidden(
        self,
        client: fastapi.testclient.TestClient,
        other_user_client: fastapi.testclient.TestClient,
    ) -> None:
        """Foreign and absent are indistinguishable on purpose.

        A path is unique only per owner, so lookup is owner-scoped rather than
        looked up globally and then ownership-checked -- an unscoped read could
        return someone else's row, leaving nothing correct to check. 403 would also
        confirm that the path is taken by another user.
        """
        self._create(client)

        resp = other_user_client.patch(
            "/api/schedules/pipelines",
            params={"schedule_path": self._PATH},
            json={"name": "Hijacked"},
        )
        assert resp.status_code == status.HTTP_404_NOT_FOUND

        # And the owner's row is untouched.
        listed = client.get(
            "/api/schedules/pipelines", params={"schedule_path": self._PATH}
        ).json()
        assert listed["schedules"][0]["name"] == "Nightly"

    def test_another_users_path_cannot_be_deleted(
        self,
        client: fastapi.testclient.TestClient,
        other_user_client: fastapi.testclient.TestClient,
    ) -> None:
        """The destructive case of the same rule, asserted separately."""
        sid = self._create(client)

        resp = other_user_client.request(
            "DELETE",
            "/api/schedules/pipelines",
            params={"schedule_path": self._PATH},
        )
        assert resp.status_code == status.HTTP_404_NOT_FOUND
        assert client.get(f"/api/schedules/pipelines/{sid}").status_code == (
            status.HTTP_200_OK
        )

    def test_admin_does_not_resolve_another_users_path(
        self,
        client: fastapi.testclient.TestClient,
        admin_client: fastapi.testclient.TestClient,
    ) -> None:
        """Admin stays in its own path namespace.

        Deliberately unlike the id-addressed routes, where admin may act on
        another user's schedule. A path names at most one row *per owner*, so a
        cross-user path resolution would be ambiguous by construction rather than
        merely permissive.
        """
        self._create(client)

        resp = admin_client.request(
            "DELETE",
            "/api/schedules/pipelines",
            params={"schedule_path": self._PATH},
        )
        assert resp.status_code == status.HTTP_404_NOT_FOUND

    def test_path_addressed_routes_require_the_query_parameter(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Without it, DELETE on a collection would read as "delete everything"."""
        resp = client.request("DELETE", "/api/schedules/pipelines")
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_an_invalid_path_is_a_validation_error_not_a_miss(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Canonicalization runs before lookup, so a malformed path cannot 404.

        Reporting it as not-found would tell the caller their syntax was fine and
        the schedule was gone.
        """
        resp = client.patch(
            "/api/schedules/pipelines",
            params={"schedule_path": "Bad Path With Spaces"},
            json={"name": "x"},
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


class TestSchedulePathDerivation:
    """Omitting `schedule_path` derives one; it never stores NULL.

    This is what keeps the endpoint backward compatible while still guaranteeing
    every new row has a canonical identity -- the set of rows needing the PR4
    backfill stops growing without breaking a single existing caller.

    Derivation is lenient (names already exist and cannot be rejected) while an
    explicitly supplied path is strict (the caller chose it). Those are separate
    functions on purpose, and the boundary between them is what most of these
    tests are about.
    """

    def _create(
        self,
        client: fastapi.testclient.TestClient,
        **overrides: object,
    ) -> dict:
        body: dict = {
            "name": "Nightly Rollup",
            "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
            "cron_expression": "0 8 * * *",
        }
        body.update(overrides)
        resp = client.post("/api/schedules/pipelines", json=body)
        assert resp.status_code == status.HTTP_201_CREATED, resp.text
        return resp.json()

    def test_omitted_path_is_derived_and_echoed(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        body = self._create(client)

        assert body["schedule_path"] == f"schedules/nightly-rollup-{body['id']}"

    def test_derived_path_is_persisted_not_just_echoed(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """The response echoing a value proves nothing about what was stored."""
        body = self._create(client)

        fetched = client.get(f"/api/schedules/pipelines/{body['id']}").json()
        assert fetched["schedule_path"] == body["schedule_path"]

    def test_explicit_null_is_treated_as_omitted(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        body = self._create(client, schedule_path=None)

        assert body["schedule_path"] == f"schedules/nightly-rollup-{body['id']}"

    def test_the_id_suffix_is_the_real_row_id(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """The suffix must be the actual primary key, not a second random value.

        Pinned because the id is normally an insert default that does not exist
        until the INSERT. It is generated explicitly so the derived path can
        contain it, and this asserts that explicit value is the one that was
        actually stored as the id.
        """
        body = self._create(client)

        assert body["schedule_path"].endswith(f"-{body['id']}")
        listed = client.get(
            "/api/schedules/pipelines",
            params={"schedule_path": body["schedule_path"]},
        ).json()
        assert [s["id"] for s in listed["schedules"]] == [body["id"]]

    def test_duplicate_names_get_distinct_paths(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Names are neither unique nor immutable, which is why the id is in there.

        Without the id these two would collide on the per-owner unique index, and
        an ordinary duplicate name would start failing with a 409.
        """
        first = self._create(client, name="Same Name")
        second = self._create(client, name="Same Name")

        assert first["schedule_path"] != second["schedule_path"]
        assert first["schedule_path"] == f"schedules/same-name-{first['id']}"
        assert second["schedule_path"] == f"schedules/same-name-{second['id']}"

    def test_a_renamed_schedule_keeps_its_derived_path(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """The other half of why the name alone is not enough.

        A path is set once, so it cannot track a rename. Deriving from the name
        alone would leave a path that silently stops describing its schedule.
        """
        body = self._create(client, name="Original")
        original_path = body["schedule_path"]

        renamed = client.patch(
            f"/api/schedules/pipelines/{body['id']}",
            json={"name": "Renamed"},
        ).json()

        assert renamed["name"] == "Renamed"
        assert renamed["schedule_path"] == original_path

    def test_unicode_names_fold_to_an_ascii_skeleton(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """NFKD-folded rather than dropped, so accented Latin stays legible."""
        body = self._create(client, name="Ünïcode Rëport")

        assert body["schedule_path"] == f"schedules/unicode-report-{body['id']}"

    def test_a_name_with_no_ascii_skeleton_falls_back(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """CJK has no ASCII skeleton, so it legitimately reduces to nothing.

        A derived path must still exist, so the fallback slug is used rather than
        rejecting a name the caller is already allowed to have.
        """
        body = self._create(client, name="日次ロールアップ")

        assert body["schedule_path"] == f"schedules/schedule-{body['id']}"

    def test_a_punctuation_only_name_falls_back(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        body = self._create(client, name="!!! ---")

        assert body["schedule_path"] == f"schedules/schedule-{body['id']}"

    def test_a_traversal_shaped_name_cannot_produce_a_traversal_path(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """`..` and `/` in a NAME must not become structure in the path.

        The slug is a single segment by construction: separators collapse to '-'
        and leading dots are stripped, so traversal is impossible rather than
        filtered.
        """
        body = self._create(client, name="../../etc/passwd")

        path = body["schedule_path"]
        assert path == f"schedules/etc-passwd-{body['id']}"
        # Exactly two segments: the namespace and the derived one.
        assert len(path.split("/")) == 2
        assert ".." not in path

    def test_a_very_long_name_truncates_the_slug_and_keeps_the_whole_id(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Only the slug is truncated: shortening the id would break uniqueness.

        The name is at its own 255-character maximum, which already overflows the
        path budget once the namespace and id suffix are accounted for.
        """
        body = self._create(client, name="a" * 255)

        path = body["schedule_path"]
        assert len(path) <= 255
        assert path.startswith("schedules/")
        assert path.endswith(f"-{body['id']}")

    def test_two_long_identical_names_still_get_distinct_paths(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Truncation must not reintroduce the collision the id exists to prevent."""
        name = "a" * 255
        first = self._create(client, name=name)
        second = self._create(client, name=name)

        assert first["schedule_path"] != second["schedule_path"]
        assert len(first["schedule_path"]) <= 255
        assert len(second["schedule_path"]) <= 255

    def test_a_derived_create_issues_exactly_one_insert(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """No flush-then-update.

        Obtaining the id via flush() would hold an uncommitted write lock, which
        deadlocks against APScheduler's separate connection on SQLite -- the exact
        hazard the create path already documents. So the derived path must be
        present in the single INSERT.
        """
        statements: list[str] = []

        def _record(
            conn, cursor, statement, parameters, context, executemany
        ):  # noqa: ANN001, ARG001
            if "scheduled_pipeline_run" in statement:
                statements.append(statement.strip().split()[0].upper())

        sqlalchemy.event.listen(db_engine, "before_cursor_execute", _record)
        try:
            self._create(client)
        finally:
            sqlalchemy.event.remove(db_engine, "before_cursor_execute", _record)

        assert statements.count("INSERT") == 1
        assert "UPDATE" not in statements

    def test_an_explicit_path_is_not_derived(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        body = self._create(client, schedule_path="UPI/Chosen")

        assert body["schedule_path"] == "UPI/Chosen"

    @pytest.mark.parametrize("supplied", ["", "   ", "\t"])
    def test_an_explicit_empty_path_is_invalid_not_missing(
        self,
        client: fastapi.testclient.TestClient,
        supplied: str,
    ) -> None:
        """Sending the field means choosing the identity, so it is validated.

        Treating whitespace as omission would silently substitute a derived path
        for the one the caller tried to set.
        """
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "name": "Nightly",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": supplied,
            },
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_an_explicit_invalid_path_is_refused_rather_than_repaired(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """The strict normalizer applies to explicit input; the lenient slug does not."""
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "name": "Nightly",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": "Bad Path With Spaces",
            },
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_a_derived_path_is_addressable_by_the_path_routes(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Round-trip: derivation is useless if the result cannot be addressed."""
        body = self._create(client)

        resp = client.patch(
            "/api/schedules/pipelines",
            params={"schedule_path": body["schedule_path"]},
            json={"name": "Renamed by derived path"},
        )
        assert resp.status_code == status.HTTP_200_OK
        assert resp.json()["id"] == body["id"]

    def test_a_derived_create_is_still_gated_by_the_path_tier(
        self,
        closed_tier_client: fastapi.testclient.TestClient,
    ) -> None:
        """Derivation does not sneak past the readiness gate.

        Checked because the gate now fires for a request that never mentions a
        path, which is the counter-intuitive half of the contract.
        """
        resp = closed_tier_client.post(
            "/api/schedules/pipelines",
            json={
                "name": "Nightly",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
            },
        )
        assert resp.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
        assert (
            resp.json()["detail"]["code"] == api_routes.SCHEDULE_PATH_WRITES_UNAVAILABLE
        )


class TestLegacyPathGeneratorUnits:
    """Direct tests for the generator PR4's backfill will reuse.

    Exercised through the API above as well, but pinned here because the backfill
    will call it with rows the API cannot produce, and because the exact shape is
    a compatibility contract between this PR and that one.
    """

    def test_generated_paths_are_always_canonical(self) -> None:
        """The lenient slug must always satisfy the strict validator.

        This is the invariant that lets derivation and explicit input converge on
        one guarantee about stored bytes.
        """
        for name in (
            "Nightly Rollup",
            "Ünïcode",
            "日次",
            "!!!",
            "../../etc/passwd",
            "a" * 400,
            "trailing---",
            "...leading",
            "MiXeD__Case..Dots",
        ):
            path = schedule_paths.generate_legacy_schedule_path(
                name=name, schedule_id="0123456789abcdef0123"
            )
            # Idempotent under the strict normalizer == already canonical.
            assert schedule_paths.canonicalize_schedule_path(path) == path

    def test_slug_folds_and_collapses(self) -> None:
        assert schedule_paths.ascii_slug("Nightly   Rollup") == "nightly-rollup"
        assert schedule_paths.ascii_slug("Ünïcode Rëport") == "unicode-report"
        assert schedule_paths.ascii_slug("keep.dots_and-dashes") == (
            "keep.dots_and-dashes"
        )

    def test_slug_falls_back_when_nothing_survives(self) -> None:
        assert schedule_paths.ascii_slug("日次") == "schedule"
        assert schedule_paths.ascii_slug("") == "schedule"
        assert schedule_paths.ascii_slug("   ") == "schedule"

    def test_generated_path_never_exceeds_the_cap(self) -> None:
        path = schedule_paths.generate_legacy_schedule_path(
            name="x" * 1000, schedule_id="0123456789abcdef0123"
        )
        assert len(path) <= schedule_paths.MAX_SCHEDULE_PATH_LENGTH

    def test_an_id_too_long_to_fit_is_refused_rather_than_truncated(
        self,
    ) -> None:
        """There is no correct answer here, so it raises.

        Truncating the id would silently trade away the uniqueness the id exists
        to provide -- and it would do so only for the longest ids, which is the
        worst possible failure mode to discover later. Unreachable with real
        20-character ids; reachable only if the id format changes, which is
        precisely when a loud failure is wanted.
        """
        long_id = "z" * (schedule_paths.MAX_SCHEDULE_PATH_LENGTH - 5)

        with pytest.raises(schedule_paths.SchedulePathValidationError, match="no room"):
            schedule_paths.generate_legacy_schedule_path(
                name="Some Name", schedule_id=long_id
            )

    def test_the_whole_id_survives_a_name_that_fills_the_budget(self) -> None:
        """The reachable case: long name, real id. The id is kept intact."""
        schedule_id = "0123456789abcdef0123"
        path = schedule_paths.generate_legacy_schedule_path(
            name="a" * 1000, schedule_id=schedule_id
        )
        assert path.endswith(f"-{schedule_id}")
        assert len(path) == schedule_paths.MAX_SCHEDULE_PATH_LENGTH


class TestConcurrentPathAdoption:
    """NULL -> path adoption must be race-safe on every dialect.

    The original implementation read the row unlocked, tested
    `schedule.schedule_path is None` against that in-memory value, then committed
    unconditionally. Two concurrent adoptions could therefore both observe NULL
    and the LAST writer won, silently repathing a row whose path is documented as
    set once -- so a caller could have their chosen identity replaced by someone
    else's, with a 200 telling them theirs had been stored.

    Fixed with a compare-and-set (`... WHERE schedule_path IS NULL`) rather than a
    locking read, because SQLite ignores row locks and a lock-based fix could not
    be regression-tested at all here.
    """

    def _cas(
        self,
        session: sqlalchemy.orm.Session,
        *,
        schedule_id: str,
        path: str,
    ) -> int:
        result = session.execute(
            sqlalchemy.update(db_models.ScheduledPipelineRun)
            .where(
                db_models.ScheduledPipelineRun.id == schedule_id,
                db_models.ScheduledPipelineRun.schedule_path.is_(None),
            )
            .values(schedule_path=path)
        )
        return result.rowcount

    def test_two_sessions_both_seeing_null_produce_one_winner(
        self,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The database-level invariant, driven by two real sessions.

        Both read NULL before either writes, which is precisely the interleaving
        the unlocked read allowed. The second compare-and-set must match no row.
        """
        sid = insert_pathless_schedule_row(db_engine=db_engine)

        with (
            sqlalchemy.orm.Session(bind=db_engine) as first,
            sqlalchemy.orm.Session(bind=db_engine) as second,
        ):
            # Both observe NULL: the stale read that caused the bug.
            assert first.get(db_models.ScheduledPipelineRun, sid).schedule_path is None
            assert second.get(db_models.ScheduledPipelineRun, sid).schedule_path is None

            assert self._cas(first, schedule_id=sid, path="upi/first") == 1
            first.commit()

            # The loser matches nothing, so it cannot overwrite the winner.
            assert self._cas(second, schedule_id=sid, path="upi/second") == 0
            second.commit()

        with sqlalchemy.orm.Session(bind=db_engine) as check:
            stored = check.get(db_models.ScheduledPipelineRun, sid)
            assert stored.schedule_path == "upi/first"

    def _patch_with_race(
        self,
        db_engine: sqlalchemy.Engine,
        *,
        schedule_id: str,
        winner_path: str,
    ) -> object:
        """Commit a competing adoption between the handler's read and its CAS.

        Injected at `_canonical_schedule_path`, which the handler calls after the
        unlocked read and before the compare-and-set, so the interleaving is
        deterministic instead of thread-timing dependent.
        """
        real = api_routes._canonical_schedule_path
        state = {"raced": False}

        def _racing(*, schedule_path: str) -> str:
            if not state["raced"]:
                state["raced"] = True
                with sqlalchemy.orm.Session(bind=db_engine) as other:
                    self._cas(other, schedule_id=schedule_id, path=winner_path)
                    other.commit()
            return real(schedule_path=schedule_path)

        return mock.patch.object(api_routes, "_canonical_schedule_path", _racing)

    def test_a_losing_request_is_refused_rather_than_reported_as_success(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The end-to-end consequence, through the real route.

        Before the fix this returned 200 and the caller's path, having actually
        overwritten the winner's. Now the winner stands and the loser is told the
        path is set once.
        """
        sid = insert_pathless_schedule_row(db_engine=db_engine)

        with self._patch_with_race(
            db_engine, schedule_id=sid, winner_path="upi/winner"
        ):
            resp = client.patch(
                f"/api/schedules/pipelines/{sid}",
                json={"schedule_path": "upi/loser"},
            )

        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert "set once" in resp.json()["detail"]

        with sqlalchemy.orm.Session(bind=db_engine) as check:
            assert (
                check.get(db_models.ScheduledPipelineRun, sid).schedule_path
                == "upi/winner"
            )

    def test_losing_to_an_identical_value_is_still_the_documented_no_op(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Same-value semantics survive the race.

        Two clients adopting the SAME canonical path is a replay, not a conflict,
        so the loser must see success -- the stored value is what it asked for.
        """
        sid = insert_pathless_schedule_row(db_engine=db_engine)

        with self._patch_with_race(
            db_engine, schedule_id=sid, winner_path="upi/agreed"
        ):
            resp = client.patch(
                f"/api/schedules/pipelines/{sid}",
                # Byte-identical to the winner's value: with case preserved, a
                # differently cased spelling is a different path and would be
                # the set-once refusal instead of the documented replay.
                json={"schedule_path": "upi/agreed"},
            )

        assert resp.status_code == status.HTTP_200_OK
        assert resp.json()["schedule_path"] == "upi/agreed"

    def test_losing_to_a_concurrent_delete_is_not_found(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The other way the CAS can match nothing: the row is gone."""
        sid = insert_pathless_schedule_row(db_engine=db_engine)
        real = api_routes._canonical_schedule_path
        state = {"raced": False}

        def _racing(*, schedule_path: str) -> str:
            if not state["raced"]:
                state["raced"] = True
                with sqlalchemy.orm.Session(bind=db_engine) as other:
                    other.execute(
                        sqlalchemy.delete(db_models.ScheduledPipelineRun).where(
                            db_models.ScheduledPipelineRun.id == sid
                        )
                    )
                    other.commit()
            return real(schedule_path=schedule_path)

        with mock.patch.object(api_routes, "_canonical_schedule_path", _racing):
            resp = client.patch(
                f"/api/schedules/pipelines/{sid}",
                json={"schedule_path": "upi/orphaned"},
            )

        assert resp.status_code == status.HTTP_404_NOT_FOUND

    def test_the_cas_cannot_touch_another_owners_row(
        self,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """`created_by` is in the predicate, so the statement is owner-scoped.

        Belt and braces: the handler only reaches this with a row it already
        ownership-checked, but the statement should not depend on that.
        """
        foreign = insert_pathless_schedule_row(
            db_engine=db_engine, created_by=OTHER_USER
        )

        with sqlalchemy.orm.Session(bind=db_engine) as session:
            result = session.execute(
                sqlalchemy.update(db_models.ScheduledPipelineRun)
                .where(
                    db_models.ScheduledPipelineRun.id == foreign,
                    db_models.ScheduledPipelineRun.created_by == DEFAULT_USER,
                    db_models.ScheduledPipelineRun.schedule_path.is_(None),
                )
                .values(schedule_path="upi/stolen")
            )
            session.commit()
            assert result.rowcount == 0

        with sqlalchemy.orm.Session(bind=db_engine) as check:
            assert (
                check.get(db_models.ScheduledPipelineRun, foreign).schedule_path is None
            )


class TestNonAsciiCannotNormalizeIntoAscii:
    """Non-ASCII is rejected outright, so no fold can rewrite it into ASCII.

    Historically this rule had to run *before* a lowercase step, because a
    handful of non-ASCII characters lowercase into ASCII and checking after the
    fold silently stored a different character than the caller sent. There is no
    fold any more, so the ordering hazard is gone -- but the rejection is not,
    and these tests keep it: without it a look-alike could still reach another
    caller's schedule through Unicode normalization or accent folding.
    """

    KELVIN = "\u212a"  # lowercases to ASCII 'k'

    def test_kelvin_sign_is_rejected(self) -> None:
        with pytest.raises(schedule_paths.SchedulePathValidationError, match="ASCII"):
            schedule_paths.canonicalize_schedule_path(f"team/{self.KELVIN}elvin")

    def test_kelvin_sign_does_not_alias_an_ascii_path(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """The exploitable consequence: two spellings must not be one identity."""
        created = client.post(
            "/api/schedules/pipelines",
            json={
                "name": "Real",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": "team/kelvin",
            },
        )
        assert created.status_code == status.HTTP_201_CREATED

        # The look-alike must not resolve to the row above.
        resp = client.patch(
            "/api/schedules/pipelines",
            params={"schedule_path": f"team/{self.KELVIN}elvin"},
            json={"name": "Hijacked"},
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_ascii_uppercase_is_accepted_and_preserved(self) -> None:
        """The rule must not become "reject anything that is not lowercase"."""
        assert schedule_paths.canonicalize_schedule_path("Upi/Nightly") == "Upi/Nightly"

    @pytest.mark.parametrize(
        "codepoint",
        [
            0x212A,  # KELVIN SIGN -> 'k'
            0x2126,  # OHM SIGN -> omega (stays non-ASCII)
            0x1E9E,  # capital sharp s -> non-ASCII
            0x017F,  # long s
            0xFF21,  # fullwidth A
        ],
    )
    def test_a_range_of_compatibility_characters_is_rejected(
        self,
        codepoint: int,
    ) -> None:
        """Only the first actually bypassed, but the rule should not be per-character."""
        with pytest.raises(schedule_paths.SchedulePathValidationError):
            schedule_paths.canonicalize_schedule_path(f"team/{chr(codepoint)}x")

    def test_derivation_still_accepts_unicode_names(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Tightening the STRICT normalizer must not break lenient derivation.

        A caller may not *send* a non-ASCII path, but a schedule may certainly be
        named with one, and its derived path has to keep working.
        """
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "name": f"Kelvin {chr(0x212A)} Report",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
            },
        )
        assert resp.status_code == status.HTTP_201_CREATED
        body = resp.json()
        assert body["schedule_path"] == f"schedules/kelvin-k-report-{body['id']}"


class TestAdoptionDoesNotCorruptTheRestOfThePatch:
    """A combined PATCH must keep ordinary semantics for its other fields.

    Nothing in the request model or the contract says a PATCH may only adopt a
    path, so adopting one alongside a rename has to behave like both operations.
    Two separate bugs broke that, and both reported success or a misleading
    conflict rather than failing loudly.
    """

    def _race_same_path(
        self,
        db_engine: sqlalchemy.Engine,
        *,
        schedule_id: str,
        winner_path: str,
    ) -> object:
        real = api_routes._canonical_schedule_path
        state = {"raced": False}

        def _racing(*, schedule_path: str) -> str:
            if not state["raced"]:
                state["raced"] = True
                with sqlalchemy.orm.Session(bind=db_engine) as other:
                    other.execute(
                        sqlalchemy.update(db_models.ScheduledPipelineRun)
                        .where(
                            db_models.ScheduledPipelineRun.id == schedule_id,
                            db_models.ScheduledPipelineRun.schedule_path.is_(None),
                        )
                        .values(schedule_path=winner_path)
                    )
                    other.commit()
            return real(schedule_path=schedule_path)

        return mock.patch.object(api_routes, "_canonical_schedule_path", _racing)

    def test_a_same_value_losing_adoption_keeps_the_other_field_changes(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The reconciliation read must not discard unflushed mutations.

        Sessions are built with `autoflush=False`, so a PATCH's other field
        changes are still pending when the losing branch re-reads the winner's
        path. Re-reading the *entity* with populate_existing overwrote them, so
        this returned 200 while silently dropping the caller's rename.
        """
        sid = insert_pathless_schedule_row(db_engine=db_engine, name="before")

        with self._race_same_path(db_engine, schedule_id=sid, winner_path="upi/agreed"):
            resp = client.patch(
                f"/api/schedules/pipelines/{sid}",
                json={"schedule_path": "upi/agreed", "name": "after"},
            )

        assert resp.status_code == status.HTTP_200_OK
        body = resp.json()
        assert body["schedule_path"] == "upi/agreed"
        assert body["name"] == "after", "the rename was silently dropped"

        with sqlalchemy.orm.Session(bind=db_engine) as check:
            stored = check.get(db_models.ScheduledPipelineRun, sid)
            assert stored.name == "after"
            assert stored.schedule_path == "upi/agreed"

    def test_a_same_value_losing_adoption_still_applies_scheduler_fields(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
        scheduler_svc: services.SchedulerService,
    ) -> None:
        """Same defect, for the fields with a side effect outside the database.

        Worth separating: dropping these silently would leave the stored cron and
        the live APScheduler job disagreeing.
        """
        sid = insert_pathless_schedule_row(db_engine=db_engine, name="before")

        with self._race_same_path(db_engine, schedule_id=sid, winner_path="upi/agreed"):
            with mock.patch.object(scheduler_svc, "update_schedule") as update:
                resp = client.patch(
                    f"/api/schedules/pipelines/{sid}",
                    json={
                        "schedule_path": "upi/agreed",
                        "cron_expression": "30 9 * * *",
                        "paused": True,
                    },
                )

        assert resp.status_code == status.HTTP_200_OK
        assert resp.json()["cron_expression"] == "30 9 * * *"
        assert resp.json()["paused"] is True
        update.assert_called_once()
        assert update.call_args.kwargs["cron_expression"] == "30 9 * * *"
        assert update.call_args.kwargs["paused"] is True

        with sqlalchemy.orm.Session(bind=db_engine) as check:
            stored = check.get(db_models.ScheduledPipelineRun, sid)
            assert stored.cron_expression == "30 9 * * *"
            assert stored.paused is True

    def test_a_non_path_integrity_failure_is_never_a_path_conflict(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The adoption CAS succeeding does not make the next failure about paths.

        A reference-sourced row plus an inline spec violates the source check
        constraint at commit. That was reported as `schedule_path 'upi/adopt' is
        already used by another schedule` -- for a path no row had ever held --
        because the catch treated "we were adopting" as proof of a collision.
        """
        run_id = insert_pipeline_run(db_engine=db_engine)
        sid = insert_pathless_schedule_row(db_engine=db_engine, run_reference=run_id)

        resp = client.patch(
            f"/api/schedules/pipelines/{sid}",
            json={
                "schedule_path": "upi/adopt",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
            },
        )

        # Refused before the write, with a message about the actual problem --
        # not a path conflict, and not an opaque 500 from the check constraint.
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert "one source" in resp.json()["detail"]
        assert "schedule_path" not in resp.json()["detail"]

        # And nothing about the path was persisted or invented.
        with sqlalchemy.orm.Session(bind=db_engine) as check:
            assert check.get(db_models.ScheduledPipelineRun, sid).schedule_path is None
        assert (
            check_path_unused(db_engine=db_engine, path="upi/adopt") is True
        ), "the refused path must remain free"

    def test_replacing_the_inline_spec_of_an_inline_row_still_works(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The new refusal must not catch the ordinary case.

        An inline-sourced row has no reference, so updating its spec is a normal
        edit and stays allowed -- including alongside a path adoption.
        """
        sid = insert_pathless_schedule_row(db_engine=db_engine)
        replacement = copy.deepcopy(SAMPLE_PIPELINE_TASK_SPEC)
        replacement["componentRef"]["spec"]["name"] = "replaced"

        resp = client.patch(
            f"/api/schedules/pipelines/{sid}",
            json={
                "schedule_path": "upi/inline-adopt",
                "pipeline_task_spec": replacement,
            },
        )
        assert resp.status_code == status.HTTP_200_OK
        assert resp.json()["schedule_path"] == "upi/inline-adopt"
        assert (
            resp.json()["pipeline_task_spec"]["componentRef"]["spec"]["name"]
            == "replaced"
        )

    def test_a_real_collision_during_adoption_is_still_a_conflict(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Narrowing the catch must not lose the genuine 409."""
        client.post(
            "/api/schedules/pipelines",
            json={
                "name": "Holder",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": "upi/held",
            },
        )
        sid = insert_pathless_schedule_row(db_engine=db_engine, name="adopter")

        resp = client.patch(
            f"/api/schedules/pipelines/{sid}",
            json={"schedule_path": "upi/held"},
        )
        assert resp.status_code == status.HTTP_409_CONFLICT


def check_path_unused(*, db_engine: sqlalchemy.Engine, path: str) -> bool:
    with sqlalchemy.orm.Session(bind=db_engine) as session:
        return (
            session.scalar(
                sqlalchemy.select(db_models.ScheduledPipelineRun.id).where(
                    db_models.ScheduledPipelineRun.schedule_path == path
                )
            )
            is None
        )


def _set_path_directly(
    *, db_engine: sqlalchemy.Engine, schedule_id: str, path: str
) -> None:
    """Write a path bypassing the API, for rows the API would not let us shape."""
    with sqlalchemy.orm.Session(bind=db_engine) as session:
        session.execute(
            sqlalchemy.update(db_models.ScheduledPipelineRun)
            .where(db_models.ScheduledPipelineRun.id == schedule_id)
            .values(schedule_path=path)
        )
        session.commit()


class TestPathLookupIdentityCheckIsExactOnThePath:
    """The path half of a path route's identity may not depend on the collation.

    Once canonicalization stopped folding case, a lookup for 'Team/Nightly'
    could resolve the row stored at 'team/nightly' on a folding column: same
    owner, different schedule, silently substituted -- and through DELETE,
    irreversibly. The in-process re-check refuses that.

    The OWNER half used to be re-checked here too, on the premise that a folding
    collation matching `Jose`'s row for a caller named `jose` was a security
    failure. It is not: that is one person, and the re-check was refusing them
    their own schedule. The owner comparison is the database's alone now, and
    `test_a_case_variant_owner_reaches_their_own_row` below pins the inversion
    rather than deleting the coverage, because "we used to 404 here" is the fact
    a reviewer needs.

    SQLite's `=` is case-sensitive, so this suite cannot produce a loose match
    for real -- which is why the path bug was invisible here. These tests
    simulate the database returning a loosely-matched row, which is what MySQL
    would hand back. `test_schedule_queries` covers the same ground against a
    genuinely folding SQLite column; both are kept, because this one proves the
    HTTP status and that one proves the query.
    """

    @contextlib.contextmanager
    def _simulate_loose_match(self, returned: db_models.ScheduledPipelineRun):
        """Make every path locator return `returned`, as a folding MySQL would.

        Patched at the query helpers rather than at `Session.scalar`, because the
        verbs no longer read the same way: PATCH and trigger take identity
        COLUMNS (`Session.execute`) and DELETE takes a payload-free entity. A
        simulation aimed at one of those would quietly stop covering the others,
        which is the failure mode this comment exists to prevent -- the exact
        path re-check is one shared function, so all three have to keep proving
        it.
        """
        identity = schedule_queries.ScheduleIdentity(
            id=returned.id,
            created_by=returned.created_by,
            schedule_path=returned.schedule_path,
            paused=returned.paused,
        )
        with (
            mock.patch.object(
                schedule_queries,
                "owned_schedule_identity_by_path",
                return_value=identity,
            ) as identity_lookup,
            mock.patch.object(
                schedule_queries,
                "owned_schedule_stub_by_path",
                return_value=returned,
            ) as stub_lookup,
        ):
            yield
        # One of them must actually have served the row, or the test asserted a
        # status that some unrelated miss produced.
        assert identity_lookup.call_count + stub_lookup.call_count == 1

    def _row_owned_by(
        self, db_engine: sqlalchemy.Engine, *, owner: str, path: str
    ) -> db_models.ScheduledPipelineRun:
        sid = insert_pathless_schedule_row(db_engine=db_engine, created_by=owner)
        _set_path_directly(db_engine=db_engine, schedule_id=sid, path=path)
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            row = session.get(db_models.ScheduledPipelineRun, sid)
            session.expunge(row)
            return row

    def test_a_case_variant_owner_reaches_their_own_row(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The inversion, stated as one.

        The stored owner differs from the caller only in case, so a folding
        deployment matches and hands the row over. This route used to 404 it.
        Nothing in the request path may re-impose that: user identity is not
        case-sensitive, and the deployment's comparator is the whole rule.

        PATCH rather than a read, because a write reaching the right row is the
        stronger claim and the one the removed residual actually blocked.
        """
        own = self._row_owned_by(
            db_engine, owner=DEFAULT_USER.upper(), path="team/report"
        )
        assert own.created_by != DEFAULT_USER, "the two spellings must actually differ"

        with self._simulate_loose_match(own):
            resp = client.patch(
                "/api/schedules/pipelines?schedule_path=team/report",
                json={"name": "renamed"},
            )

        assert resp.status_code == status.HTTP_200_OK
        assert resp.json()["name"] == "renamed"

    def test_the_tier_gate_still_covers_the_path_collation(
        self,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Why the path residual is defence in depth and not the only guard.

        Every path-addressed route requires the SCHEDULE_PATH tier, and the path
        collation step belongs to it, so a folding path column closes those
        routes with a 503 before any locator runs. Asserted here because the
        residual's tests read as if the bug were live, and it is not -- the depth
        is real but the exposure is not, and a comment claiming otherwise would
        misdescribe the system to whoever reads it next.
        """
        steps = database_migrations._WRITE_TIER_STEPS["path_writes"]()

        for step in database_migrations._COLLATION_STEPS:
            assert step in steps

    @pytest.mark.parametrize(
        "call",
        [
            pytest.param(
                lambda c: c.patch(
                    "/api/schedules/pipelines?schedule_path=Team/Report",
                    json={"name": "hijacked"},
                ),
                id="patch",
            ),
            pytest.param(
                lambda c: c.delete(
                    "/api/schedules/pipelines?schedule_path=Team/Report"
                ),
                id="delete",
            ),
            pytest.param(
                lambda c: c.post(
                    "/api/schedules/pipelines/trigger?schedule_path=Team/Report"
                ),
                id="trigger",
            ),
        ],
    )
    def test_a_case_variant_path_cannot_reach_the_owners_other_schedule(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
        call,
    ) -> None:
        """The surviving half, and the one DELETE makes destructive.

        The caller owns the row, so ownership is not in question. Only the path
        comparison stands between 'Team/Report' and the schedule at 'team/report'.
        """
        own = self._row_owned_by(db_engine, owner=DEFAULT_USER, path="team/report")
        assert own.created_by == DEFAULT_USER, "the row must belong to the caller"

        with self._simulate_loose_match(own):
            resp = call(client)

        assert resp.status_code == status.HTTP_404_NOT_FOUND
        assert resp.json()["detail"] == "Schedule not found"

    def test_the_refusal_does_not_disclose_the_row_it_declined(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """404 and not the 403 `_check_ownership` raises.

        A 403 names the creator, which on this route would convert a near-miss
        into the disclosure that 404-for-both exists to prevent. Asserted on the
        path near-miss, since that is the refusal these routes still produce.
        """
        own = self._row_owned_by(db_engine, owner=DEFAULT_USER, path="team/report")

        with self._simulate_loose_match(own):
            resp = client.delete("/api/schedules/pipelines?schedule_path=Team/Report")

        assert resp.status_code == status.HTTP_404_NOT_FOUND
        assert "team/report" not in resp.text
        assert DEFAULT_USER not in resp.text
        assert "created by" not in resp.text.lower()

    def test_an_exactly_addressed_row_is_still_reachable(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The re-check must not break the ordinary case."""
        sid = insert_pathless_schedule_row(db_engine=db_engine)
        _set_path_directly(db_engine=db_engine, schedule_id=sid, path="team/mine")

        resp = client.patch(
            "/api/schedules/pipelines?schedule_path=team/mine",
            json={"name": "renamed"},
        )
        assert resp.status_code == status.HTTP_200_OK
        assert resp.json()["name"] == "renamed"


class TestReferenceDeletionRaceDegradesToNotFound:
    """A reference deleted between preflight and commit must not be a 500.

    Validation cannot hold its transaction open (the executor's resolver rolls
    back), so there is an unavoidable window where the foreign key can fail at
    commit. Before this, only path collisions were translated, so the race
    surfaced as an unhandled IntegrityError.

    NOTE: SQLite does not enforce foreign keys unless `PRAGMA foreign_keys=ON`,
    and nothing in this suite enables it -- so the FK cannot actually fire here.
    The integrity error is therefore injected at commit, which tests the thing
    the finding is about (how the failure is CLASSIFIED) without pretending the
    dialect enforces something it does not.
    """

    def _fail_commit_once(self) -> object:
        real = sqlalchemy.orm.Session.commit
        state = {"failed": False}

        def _commit(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            if not state["failed"]:
                state["failed"] = True
                raise sqlalchemy.exc.IntegrityError(
                    "INSERT INTO scheduled_pipeline_run ...",
                    {},
                    Exception("FOREIGN KEY constraint failed"),
                )
            return real(self, *args, **kwargs)

        return mock.patch.object(sqlalchemy.orm.Session, "commit", _commit)

    def test_a_hex_spelled_reference_does_not_turn_an_unrelated_fault_into_a_404(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Classification must ask about the resolved id, not the requested spelling.

        After an `IntegrityError` this endpoint asks whether a referenced parent
        stopped existing, and re-raises when every parent is present -- an error it
        did not cause is not its to reinterpret. That question is asked by primary
        key, so it has to be asked with the id the lookup RESOLVED. Asked with a
        bare 32-hex spelling it answers "missing" for a pipeline that is sitting
        right there, and every unrelated integrity fault becomes a 404 blaming a
        reference that never vanished.

        The parent is deliberately left in place. The failure is injected at commit,
        so the only thing under test is how it gets classified.
        """
        pipeline_id, version_key = _save_pipeline(db_engine=db_engine)
        hex_spelling = uuid.UUID(pipeline_id).hex
        assert hex_spelling != pipeline_id

        with self._fail_commit_once():
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                client.post(
                    "/api/schedules/pipelines",
                    json={
                        "name": "hex-unrelated-fault",
                        "cron_expression": "0 8 * * *",
                        "pipeline_task_spec_from_user_pipeline_id": hex_spelling,
                        "pipeline_task_spec_from_user_pipeline_version_key": version_key,
                    },
                )

        with sqlalchemy.orm.Session(bind=db_engine) as session:
            still_there = session.scalar(
                sqlalchemy.select(user_pipeline_db_models.UserPipeline.id).where(
                    user_pipeline_db_models.UserPipeline.id == pipeline_id
                )
            )
        assert still_there == pipeline_id

    def test_a_hex_spelled_reference_whose_parent_really_vanished_is_still_a_404(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The other side of the same boundary: a genuine race still degrades to 404.

        Resolving the spelling must not cost the endpoint the behaviour it already
        had. The pipeline is hard-deleted after the preflight resolved it, so the
        parent named by the RESOLVED id is really gone.
        """
        pipeline_id, version_key = _save_pipeline(db_engine=db_engine)
        hex_spelling = uuid.UUID(pipeline_id).hex

        real_validate = api_routes._validate_pipeline_reference

        def _validate_then_delete(**kwargs: object) -> str:
            resolved = real_validate(**kwargs)  # type: ignore[arg-type]
            # Core connection, not a Session: `_fail_commit_once` patches
            # `Session.commit` and fires once, so committing the deletion through a
            # Session would spend the injected failure here instead of on the
            # endpoint's write, and the test would prove nothing.
            with db_engine.begin() as connection:
                connection.execute(
                    sqlalchemy.delete(
                        user_pipeline_db_models.UserPipelineVersion
                    ).where(
                        user_pipeline_db_models.UserPipelineVersion.pipeline_id
                        == pipeline_id
                    )
                )
                connection.execute(
                    sqlalchemy.delete(user_pipeline_db_models.UserPipeline).where(
                        user_pipeline_db_models.UserPipeline.id == pipeline_id
                    )
                )
            return resolved

        with self._fail_commit_once():
            with mock.patch.object(
                api_routes,
                "_validate_pipeline_reference",
                _validate_then_delete,
            ):
                resp = client.post(
                    "/api/schedules/pipelines",
                    json={
                        "name": "hex-real-race",
                        "cron_expression": "0 8 * * *",
                        "pipeline_task_spec_from_user_pipeline_id": hex_spelling,
                        "pipeline_task_spec_from_user_pipeline_version_key": version_key,
                    },
                )

        assert resp.status_code == status.HTTP_404_NOT_FOUND, resp.text
        assert resp.json()["detail"] == "Referenced pipeline or run not found"

    def test_a_run_deleted_after_validation_is_reported_as_not_found(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        run_id = insert_pipeline_run(db_engine=db_engine)

        real_validate = api_routes._validate_run_reference

        def _validate_then_delete(*, session, run_id: str, created_by: str) -> None:
            real_validate(session=session, run_id=run_id, created_by=created_by)
            # The race: the run goes away after it was legitimately validated.
            # Deleted through a raw connection, NOT an ORM Session: the commit
            # injector below patches `Session.commit`, so a Session here would
            # swallow the injected failure meant for the handler's own commit.
            with db_engine.begin() as conn:
                conn.execute(
                    sqlalchemy.delete(bts.PipelineRun).where(
                        bts.PipelineRun.id == run_id
                    )
                )

        with mock.patch.object(
            api_routes, "_validate_run_reference", _validate_then_delete
        ):
            with self._fail_commit_once():
                resp = client.post(
                    "/api/schedules/pipelines",
                    json={
                        "name": "racer",
                        "pipeline_task_spec_from_pipeline_run_id": run_id,
                        "cron_expression": "0 8 * * *",
                    },
                )

        assert (
            resp.status_code == status.HTTP_404_NOT_FOUND
        ), "a deletion race must degrade to the documented not-found, not 500"
        # Same STATUS as preflight, which is the whole contract being claimed.
        # The bodies differ -- the preflight names the offending reference and
        # this cannot -- so asserting a shared substring would overstate it.
        assert resp.json()["detail"] == "Referenced pipeline or run not found"

    def test_an_unrelated_integrity_error_still_propagates(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Classification must stay narrow: no reference gone, no path taken."""
        run_id = insert_pipeline_run(db_engine=db_engine)

        with self._fail_commit_once():
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                client.post(
                    "/api/schedules/pipelines",
                    json={
                        "name": "unrelated",
                        "pipeline_task_spec_from_pipeline_run_id": run_id,
                        "cron_expression": "0 8 * * *",
                    },
                )

    def test_the_semantic_validator_is_never_re_run_after_failure(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """After an IntegrityError, ask about EXISTENCE and nothing else.

        `_validate_pipeline_reference` also filters soft deletion, ownership,
        version existence and pinnability -- none of which a single-column
        foreign key can violate. Re-running it here would let any of those,
        changed concurrently, turn an unrelated integrity fault into a 404 about
        a reference that was never the cause.

        The saved-pipeline column DOES carry a foreign key now
        (`fk_scheduled_pipeline_run_user_pipeline_id`); an earlier version of
        this test asserted the opposite and justified the behaviour by the
        constraint's absence. The behaviour was right, the reason was not.
        Existence is re-checked by primary key instead.
        """
        pipeline_id, _ = _save_pipeline(db_engine=db_engine)

        real_validate = api_routes._validate_pipeline_reference
        real_vanished = api_routes._a_referenced_parent_vanished
        calls: list[int] = []
        classified_with: list[str | None] = []

        def _counting_validate(**kwargs: object) -> str:
            calls.append(1)
            # The return value is the id the writer stores. Swallowing it here
            # would make the mock change behaviour rather than observe it: the
            # handler would persist None and this test would be exercising a
            # code path production never takes.
            return real_validate(**kwargs)  # type: ignore[arg-type,no-any-return]

        def _watch_classification(**kwargs: object) -> bool:
            # Observed where the resolved id is actually CONSUMED. Asserting on a
            # value the wrapper merely recorded would not notice a wrapper that
            # recorded it and then returned None.
            classified_with.append(typing.cast("str | None", kwargs["pipeline_id"]))
            return real_vanished(**kwargs)  # type: ignore[arg-type,no-any-return]

        with self._fail_commit_once():
            with mock.patch.object(
                api_routes,
                "_a_referenced_parent_vanished",
                _watch_classification,
            ):
                with mock.patch.object(
                    api_routes,
                    "_validate_pipeline_reference",
                    _counting_validate,
                ):
                    with pytest.raises(sqlalchemy.exc.IntegrityError):
                        client.post(
                            "/api/schedules/pipelines",
                            json={
                                "name": "unrelated-saved-pipeline",
                                "pipeline_task_spec_from_user_pipeline_id": pipeline_id,
                                "cron_expression": "0 8 * * *",
                            },
                        )

        # Exactly one call: the preflight. A second call would be the semantic
        # validator re-running after a failed commit, which must not happen --
        # it also filters soft deletion, ownership, version existence and
        # pinnability, so it can report 404/422 for an IntegrityError that had
        # nothing to do with a missing parent. Raw parent existence by primary
        # key is the only predicate a foreign key actually asserts.
        assert calls == [1]
        # And the resolved id reached the classifier. This is what keeps the mock
        # honest: a wrapper that dropped the return value would leave production's
        # `canonical_pipeline_id` as None here, so the count above would still be 1
        # while the test silently exercised a path the real handler never takes.
        assert classified_with == [pipeline_id]


class TestPathFilteredReadsRestOnAVerifiedUniqueIndex:
    """A path-filtered GET is cheap because the index is PROVEN, not assumed.

    The earlier version of this class asserted the opposite architecture: that
    reads were never tier gated, and that filtering on a unique key was itself
    enough to keep the work bounded. Both halves failed. A filter on a unique
    key says nothing when the unique index does not exist yet, and the exact
    owner comparison happens in Python after the row limit, so collation
    neighbours can crowd out the caller's own row and produce a confident empty
    answer.

    The gate is what supplies the missing premise. Once the path tier is
    verified, the index exists and is enforced under the same collation as the
    predicate, so at most one row can match: LIMIT 1 is exact rather than
    arbitrary, and the total can be derived instead of aggregated.

    Costs are asserted as emitted statements, not timings: SQLite has neither
    the missing index nor the table size that make the difference matter, so a
    duration here would prove nothing.
    """

    def _record_statements(
        self, db_engine: sqlalchemy.Engine
    ) -> tuple[object, list[tuple[str, tuple]]]:
        statements: list[tuple[str, tuple]] = []

        def _record(
            conn, cursor, statement, parameters, context, executemany
        ):  # noqa: ANN001, ARG001
            if "scheduled_pipeline_run" in statement:
                statements.append(
                    (" ".join(statement.split()), tuple(parameters or ()))
                )

        class _Listener:
            def __enter__(self) -> list[tuple[str, tuple]]:
                sqlalchemy.event.listen(db_engine, "before_cursor_execute", _record)
                return statements

            def __exit__(self, *exc: object) -> None:
                sqlalchemy.event.remove(db_engine, "before_cursor_execute", _record)

        return _Listener(), statements

    def _create(
        self, client: fastapi.testclient.TestClient, *, schedule_path: str
    ) -> None:
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "name": "Nightly",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": schedule_path,
            },
        )
        assert resp.status_code == status.HTTP_201_CREATED, resp.text

    def test_a_path_filtered_read_asks_for_one_row_not_a_page(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """LIMIT 1, whatever `page_size` says.

        One is correct only because the read is gated on the tier that proves
        the unique index exists, and that index is enforced under the same
        collation as the predicate -- so at most one row can match. Ungated,
        one row would have been a silently arbitrary choice among collation
        neighbours, which is why this read is now refused rather than bounded
        when the tier is closed.
        """
        self._create(client, schedule_path="upi/only")
        listener, statements = self._record_statements(db_engine)

        with listener:
            resp = client.get(
                "/api/schedules/pipelines",
                params={"schedule_path": "upi/only", "page_size": 100},
            )

        assert resp.status_code == status.HTTP_200_OK
        # The LIMIT is a bound parameter, so the value is asserted rather than
        # the SQL text: `LIMIT ?` says nothing about how many rows were asked for.
        limited = [
            (sql, params) for sql, params in statements if "LIMIT" in sql.upper()
        ]
        assert limited, statements
        # SQLAlchemy renders `LIMIT ? OFFSET ?`, so the row cap is the parameter
        # bound to LIMIT, not the last one.
        for sql, params in limited:
            limit_value = params[-2] if "OFFSET" in sql.upper() else params[-1]
            assert (
                limit_value == 1
            ), f"a path filter must ask for 1 row, not a page: {sql} {params}"

    def test_a_path_filtered_read_never_issues_a_count(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The aggregate is the one statement no LIMIT can bound.

        A COUNT must visit every candidate row before it can answer. On the
        healthy indexed path that is simply wasteful -- the verified unique
        index already fixes the answer at 0 or 1, and the rows in hand say
        which. The closed-tier case no longer arises here at all, because the
        filtered read is refused before any query is issued.
        """
        self._create(client, schedule_path="upi/only")
        listener, statements = self._record_statements(db_engine)

        with listener:
            client.get("/api/schedules/pipelines", params={"schedule_path": "upi/only"})

        assert not any(
            "COUNT(" in sql.upper() for sql, _params in statements
        ), statements

    def test_the_unfiltered_list_still_counts(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The bound comes from uniqueness, so it must not leak to the plain list."""
        self._create(client, schedule_path="upi/only")
        listener, statements = self._record_statements(db_engine)

        with listener:
            client.get("/api/schedules/pipelines")

        assert any("COUNT(" in sql.upper() for sql, _params in statements)

    def test_the_derived_total_matches_what_the_count_would_have_said(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Deriving the total must not change the answer."""
        self._create(client, schedule_path="upi/present")

        found = client.get(
            "/api/schedules/pipelines", params={"schedule_path": "upi/present"}
        ).json()
        missing = client.get(
            "/api/schedules/pipelines", params={"schedule_path": "upi/absent"}
        ).json()

        assert found["total_count"] == 1
        assert len(found["schedules"]) == 1
        assert missing["total_count"] == 0
        assert missing["schedules"] == []

    def test_a_path_filtered_response_offers_no_next_page(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """At most one row exists, so a cursor could only return nothing.

        Pinned because the row count now equals the limit on every hit, which is
        exactly the condition the unfiltered branch uses to offer a cursor.
        """
        self._create(client, schedule_path="upi/only")

        body = client.get(
            "/api/schedules/pipelines",
            params={"schedule_path": "upi/only", "page_size": 1},
        ).json()

        assert body["next_page_token"] is None

    def test_the_gate_refuses_before_issuing_any_query(
        self,
        closed_tier_client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """A refused lookup must not pay for the scan it refused to risk."""
        listener, statements = self._record_statements(db_engine)

        with listener:
            resp = closed_tier_client.get(
                "/api/schedules/pipelines",
                params={"schedule_path": "upi/anything"},
            )

        assert resp.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
        selected = [
            sql for sql, _ in statements if "scheduled_pipeline_run" in sql.lower()
        ]
        assert not selected, f"the gate ran after querying: {selected}"


class TestRawLengthBoundDoesNotContradictTheCanonicalRule:
    """The 255 cap applies after trimming, so raw input must not enforce it.

    A leading space plus 255 canonical characters is 256 raw. Enforcing 255 on
    the raw value rejected it at the request model, while a lookup canonicalized
    first and would have resolved the row -- writes and reads disagreeing at
    exactly the boundary.
    """

    def _max_length_path(self) -> str:
        # "team/" + filler == exactly MAX_SCHEDULE_PATH_LENGTH.
        filler = "a" * (schedule_paths.MAX_SCHEDULE_PATH_LENGTH - len("team/"))
        path = f"team/{filler}"
        assert len(path) == schedule_paths.MAX_SCHEDULE_PATH_LENGTH
        return path

    def test_a_max_length_path_with_surrounding_whitespace_is_accepted(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        path = self._max_length_path()
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "name": "Boundary",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": f"  {path}  ",
            },
        )
        assert resp.status_code == status.HTTP_201_CREATED
        assert resp.json()["schedule_path"] == path

    def test_the_written_boundary_path_is_reachable_by_lookup(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """The point of the finding: writes and lookups must agree."""
        path = self._max_length_path()
        client.post(
            "/api/schedules/pipelines",
            json={
                "name": "Boundary",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": path,
            },
        )
        resp = client.patch(
            f"/api/schedules/pipelines?schedule_path=  {path}  ",
            json={"name": "renamed"},
        )
        assert resp.status_code == status.HTTP_200_OK
        assert resp.json()["name"] == "renamed"

    def test_a_canonical_path_over_the_cap_is_still_refused(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Raising the raw bound must not raise the canonical one."""
        too_long = "team/" + "a" * schedule_paths.MAX_SCHEDULE_PATH_LENGTH
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "name": "TooLong",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": too_long,
            },
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert str(schedule_paths.MAX_SCHEDULE_PATH_LENGTH) in resp.text

    def test_absurd_raw_input_is_still_bounded(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """The raw guard still exists; it is just not the canonical rule."""
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "name": "Absurd",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": "a"
                * (schedule_paths.MAX_RAW_SCHEDULE_PATH_LENGTH + 1),
            },
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


class TestBothForeignKeyedParentsAreCheckedAfterAnIntegrityError:
    """Existence by id, for every reference column that has a constraint.

    The earlier handler checked only the run column, reasoning that the
    saved-pipeline column had no foreign key. That was true when written and
    false once `fk_scheduled_pipeline_run_user_pipeline_id` was installed, so a
    vanished saved pipeline reached the bare re-raise and became a 500.
    """

    def _fail_commit_once(self, before_raising: object = None) -> object:
        real = sqlalchemy.orm.Session.commit
        state = {"failed": False}

        def _commit(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            if not state["failed"]:
                state["failed"] = True
                if before_raising is not None:
                    before_raising()  # type: ignore[operator]
                raise sqlalchemy.exc.IntegrityError(
                    "INSERT INTO scheduled_pipeline_run ...",
                    {},
                    Exception("FOREIGN KEY constraint failed"),
                )
            return real(self, *args, **kwargs)

        return mock.patch.object(sqlalchemy.orm.Session, "commit", _commit)

    def test_a_vanished_saved_pipeline_degrades_to_not_found(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        pipeline_id, _ = _save_pipeline(db_engine=db_engine)

        def _hard_delete() -> None:
            # Between preflight and commit. Deleting it up front would 404 at the
            # preflight and never reach the handler under test -- which is how an
            # earlier version of this test passed even with the saved-pipeline
            # column removed from the check entirely.
            with sqlalchemy.orm.Session(db_engine) as session:
                session.execute(
                    sqlalchemy.delete(user_pipeline_db_models.UserPipeline).where(
                        user_pipeline_db_models.UserPipeline.id == pipeline_id
                    )
                )
                session.commit()

        with self._fail_commit_once(before_raising=_hard_delete):
            resp = client.post(
                "/api/schedules/pipelines",
                json={
                    "name": "vanished-pipeline",
                    "pipeline_task_spec_from_user_pipeline_id": pipeline_id,
                    "cron_expression": "0 8 * * *",
                },
            )

        assert resp.status_code == status.HTTP_404_NOT_FOUND
        # Generic on purpose: timing must not distinguish "never existed" from
        # "existed and vanished".
        assert "not found" in resp.json()["detail"].lower()

    def test_a_soft_deleted_saved_pipeline_still_re_raises(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """`deleted_at` leaves the row, so the constraint was satisfied.

        The distinction that makes raw existence the right question: a
        soft-deleted parent cannot have failed the foreign key, so an error
        raised while one exists is some other fault and must not be relabelled.

        The soft delete has to land BETWEEN preflight and commit, because the
        preflight filters `deleted_at` and would 404 first -- which is the real
        interleaving anyway, and the reason the earlier handler's reasoning
        mattered at all.
        """
        pipeline_id, _ = _save_pipeline(db_engine=db_engine)

        def _soft_delete_then_fail() -> None:
            with sqlalchemy.orm.Session(db_engine) as session:
                session.execute(
                    sqlalchemy.update(user_pipeline_db_models.UserPipeline)
                    .where(user_pipeline_db_models.UserPipeline.id == pipeline_id)
                    .values(deleted_at=datetime.datetime.now(datetime.timezone.utc))
                )
                session.commit()

        with self._fail_commit_once(before_raising=_soft_delete_then_fail):
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                client.post(
                    "/api/schedules/pipelines",
                    json={
                        "name": "soft-deleted-pipeline",
                        "pipeline_task_spec_from_user_pipeline_id": pipeline_id,
                        "cron_expression": "0 8 * * *",
                    },
                )

    def test_a_vanished_run_still_degrades_to_not_found(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The behaviour that already worked must survive the rewrite."""
        run_id = insert_pipeline_run(db_engine=db_engine)

        def _hard_delete() -> None:
            with sqlalchemy.orm.Session(db_engine) as session:
                session.execute(
                    sqlalchemy.delete(bts.PipelineRun).where(
                        bts.PipelineRun.id == run_id
                    )
                )
                session.commit()

        with self._fail_commit_once(before_raising=_hard_delete):
            resp = client.post(
                "/api/schedules/pipelines",
                json={
                    "name": "vanished-run",
                    "pipeline_task_spec_from_pipeline_run_id": run_id,
                    "cron_expression": "0 8 * * *",
                },
            )

        assert resp.status_code == status.HTTP_404_NOT_FOUND


class TestTheSchedulerRouterOwnsItsErrorContract:
    """Thread 3899412382: the duplicate handlers, and what they were hiding.

    The local `except` clauses really were byte-identical to the app-wide
    handlers -- but deleting them exposed that this module's 404/422 mapping was
    only present because `setup_user_pipeline_routes` happened to run first.
    Mount the schedule routes alone and a missing saved pipeline became a 500.
    So the registration is now explicit here, and this test is what stops it
    being deleted again as "already done elsewhere".
    """

    def test_a_missing_pipeline_is_404_without_the_user_pipeline_routes(
        self,
    ) -> None:
        app = fastapi.FastAPI()

        @app.get("/boom")
        def _boom() -> None:
            raise user_pipeline_errors.PipelineNotFoundError(
                "Pipeline 'x' was not found."
            )

        api_routes.setup_pipeline_schedule_routes(
            app=app,
            get_session=lambda: None,
            scheduler_svc=mock.MagicMock(),
            user_details_getter=lambda: None,
            schema_report=database_migrations.MigrationReport(dialect="sqlite"),
        )

        resp = fastapi.testclient.TestClient(app, raise_server_exceptions=False).get(
            "/boom"
        )

        assert resp.status_code == status.HTTP_404_NOT_FOUND
        assert resp.json() == {"detail": "Pipeline 'x' was not found."}

    def test_a_validation_error_is_422_on_the_same_terms(self) -> None:
        app = fastapi.FastAPI()

        @app.get("/boom")
        def _boom() -> None:
            raise user_pipeline_errors.PipelineValidationError("bad pin")

        api_routes.setup_pipeline_schedule_routes(
            app=app,
            get_session=lambda: None,
            scheduler_svc=mock.MagicMock(),
            user_details_getter=lambda: None,
            schema_report=database_migrations.MigrationReport(dialect="sqlite"),
        )

        resp = fastapi.testclient.TestClient(app, raise_server_exceptions=False).get(
            "/boom"
        )

        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert resp.json() == {"detail": "bad pin"}


class TestTheCanonicalizerExamplesAreExecutable:
    """Thread 3898476527: examples in the docstring, kept honest by running them.

    Documentation that is only prose rots silently. These parse the examples out
    of `canonicalize_schedule_path.__doc__` and execute them, so an example that
    stops being true fails here rather than misleading the next reader.
    """

    @staticmethod
    def _examples() -> tuple[list[tuple[str, str]], list[str]]:
        doc = schedule_paths.canonicalize_schedule_path.__doc__ or ""
        accepted: list[tuple[str, str]] = []
        rejected: list[str] = []
        for raw in doc.splitlines():
            line = raw.strip()
            if not line.startswith('"'):
                continue
            if "->" in line:
                left, right = line.split("->", 1)
                accepted.append(
                    (
                        ast.literal_eval(left.strip()),
                        ast.literal_eval(right.split("#")[0].strip()),
                    )
                )
            else:
                rejected.append(ast.literal_eval(line.split("#")[0].strip()))
        return accepted, rejected

    def test_the_examples_were_actually_parsed(self) -> None:
        """Guards the parser: silently finding nothing would make this vacuous."""
        accepted, rejected = self._examples()

        assert len(accepted) >= 4
        assert len(rejected) >= 10

    def test_every_accepted_example_canonicalizes_as_documented(self) -> None:
        accepted, _ = self._examples()

        for supplied, expected in accepted:
            assert (
                schedule_paths.canonicalize_schedule_path(supplied) == expected
            ), supplied

    def test_every_rejected_example_is_rejected(self) -> None:
        _, rejected = self._examples()

        for supplied in rejected:
            with pytest.raises(schedule_paths.SchedulePathValidationError):
                schedule_paths.canonicalize_schedule_path(supplied)

    def test_the_case_preservation_claim_holds(self) -> None:
        """The docstring's claim: two spellings, two identities."""
        assert schedule_paths.canonicalize_schedule_path(
            "Upi/Nightly"
        ) != schedule_paths.canonicalize_schedule_path("upi/nightly")


class TestTheSchemaTierGateCannotBeMisspelledOrMismatched:
    """Threads 3899406730 and 3898671981: one type replaces two loose strings."""

    def test_the_wire_codes_are_unchanged(self) -> None:
        """These are published; a StrEnum must not alter them."""
        assert (
            api_routes.SCHEDULE_PATH_WRITES_UNAVAILABLE
            == "schedule_path_writes_unavailable"
        )
        assert (
            api_routes.PIPELINE_REFERENCE_WRITES_UNAVAILABLE
            == "pipeline_reference_writes_unavailable"
        )
        assert isinstance(api_routes.SCHEDULE_PATH_WRITES_UNAVAILABLE, str)

    def test_each_tier_reports_its_own_code(self) -> None:
        """The mismatch that separate `tier=`/`code=` arguments allowed."""
        assert (
            api_routes.SchemaTier.SCHEDULE_PATH.code
            == api_routes.SchedulerErrorCode.SCHEDULE_PATH_WRITES_UNAVAILABLE
        )
        assert (
            api_routes.SchemaTier.PIPELINE_REFERENCE.code
            == api_routes.SchedulerErrorCode.PIPELINE_REFERENCE_WRITES_UNAVAILABLE
        )

    def test_readiness_reads_the_real_attribute(self) -> None:
        """No `getattr`: a renamed report field breaks the type check, not prod."""
        mixed = path_ready_reference_closed_schema_report()
        closed = closed_schema_report()

        assert api_routes.SchemaTier.SCHEDULE_PATH.is_ready(schema_report=mixed) is True
        assert (
            api_routes.SchemaTier.PIPELINE_REFERENCE.is_ready(schema_report=mixed)
            is False
        )
        assert (
            api_routes.SchemaTier.SCHEDULE_PATH.is_ready(schema_report=closed) is False
        )

    def test_every_tier_maps_to_a_distinct_code(self) -> None:
        codes = {tier.code for tier in api_routes.SchemaTier}

        assert len(codes) == len(list(api_routes.SchemaTier))

    def test_the_gate_raises_503_carrying_the_tier_code(self) -> None:
        with pytest.raises(fastapi.HTTPException) as caught:
            api_routes._require_schema_tier(
                schema_report=closed_schema_report(),
                tier=api_routes.SchemaTier.SCHEDULE_PATH,
                feature="Schedule paths",
            )

        assert caught.value.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
        assert caught.value.detail["code"] == "schedule_path_writes_unavailable"
        # Still no physical schema name in a public body.
        assert "uq_" not in str(caught.value.detail)
        assert "ix_" not in str(caught.value.detail)


class TestTheFilteredReadAppliesNoOwnerRuleOfItsOwn:
    """The list's owner scoping is the database's, and only the database's.

    Both halves matter and they pull in opposite directions, so they are
    asserted together:

    - the read must not ADD an owner rule. pi-40's finding was read as "the
      path-filtered list is the remaining way to be handed another user's
      schedule", and a Python post-filter comparing `created_by` exactly was
      added to close it. On a folding deployment that post-filter dropped the
      caller's OWN rows -- `Jose` querying the schedule stored under `jose` got
      an empty list for a path that visibly exists -- because it treated a case
      variant as a different person. It is not.

    - the read must not DROP the owner rule either. Removing the post-filter is
      only safe because the scoping predicate is still in the SQL, and a change
      that lost it would pass every test that only checks the caller's own rows
      come back.
    """

    def test_a_case_variant_owner_is_served_their_own_row(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """SQLite's `=` is case-sensitive, so the folding match is simulated.

        That is the same reason the post-filter looked harmless in this suite:
        SQLite had already excluded the row in SQL, so the Python check never
        changed an outcome here and its cost was invisible. The database is made
        to hand back a case-variant row, exactly as MySQL's `_ci` default would,
        and the row must survive the handler.
        """
        neighbour_id = insert_pathless_schedule_row(
            db_engine=db_engine,
            created_by=DEFAULT_USER.upper(),
        )
        _set_path_directly(
            db_engine=db_engine, schedule_id=neighbour_id, path="upi/shared"
        )
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            neighbour_row = session.get(db_models.ScheduledPipelineRun, neighbour_id)
            assert neighbour_row is not None
            session.expunge(neighbour_row)

        real_scalars = sqlalchemy.orm.Session.scalars
        state = {"served": False}

        def _scalars(self, statement, *args, **kwargs):  # type: ignore[no-untyped-def]
            if not state["served"] and "schedule_path" in str(statement):
                state["served"] = True
                return _FakeScalarResult([neighbour_row])
            return real_scalars(self, statement, *args, **kwargs)

        with mock.patch.object(sqlalchemy.orm.Session, "scalars", _scalars):
            resp = client.get(
                "/api/schedules/pipelines",
                params={"schedule_path": "upi/shared"},
            )

        assert state["served"], "the simulation never fired; the test would be vacuous"
        assert resp.status_code == status.HTTP_200_OK
        returned = {row["id"] for row in resp.json()["schedules"]}
        assert (
            neighbour_id in returned
        ), "the caller's own row was filtered out in Python"

    def test_the_scoping_predicate_is_still_in_the_sql(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Removing the post-filter must not become removing the scope.

        Read off the emitted statement rather than the response, because a
        response-level assertion cannot distinguish "scoped in SQL" from "the
        fixture happened to contain only the caller's rows" -- and the whole
        point of deleting the Python check is that SQL is now the only place the
        rule lives.
        """
        insert_pathless_schedule_row(db_engine=db_engine, created_by=OTHER_USER)

        with sql_capture.capture_sql(db_engine) as statements:
            resp = client.get("/api/schedules/pipelines")

        assert resp.status_code == status.HTTP_200_OK
        selects = [s for s in statements if "FROM scheduled_pipeline_run" in s]
        assert selects, "no read reached the database"
        for statement in selects:
            assert "created_by" in statement, statement

    def test_another_users_row_is_not_returned(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The behavioural pair to the statement assertion above.

        A genuinely different owner -- not a case variant -- stays invisible,
        through both the unfiltered list and a path filter that would otherwise
        match their row.
        """
        foreign_id = insert_pathless_schedule_row(
            db_engine=db_engine, created_by=OTHER_USER
        )
        _set_path_directly(
            db_engine=db_engine, schedule_id=foreign_id, path="upi/theirs"
        )

        listed = client.get("/api/schedules/pipelines")
        assert foreign_id not in {row["id"] for row in listed.json()["schedules"]}

        filtered = client.get(
            "/api/schedules/pipelines", params={"schedule_path": "upi/theirs"}
        )
        assert filtered.status_code == status.HTTP_200_OK
        assert filtered.json()["schedules"] == []
        assert filtered.json()["total_count"] == 0

    def test_a_cursor_cannot_be_combined_with_a_path_filter(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """pi-38 and pi-40 both reproduced this: the cursor hid an existing row.

        The cursor's `WHERE (updated_at, id) < (...)` was applied before
        `total_count` was derived from the rows in hand, so a filtered GET with a
        cursor reported `total_count = 0` for a schedule that exists.
        """
        resp = client.get(
            "/api/schedules/pipelines",
            params={
                "schedule_path": "upi/anything",
                "page_token": "1970-01-01T00:00:00+00:00~0",
            },
        )

        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert "page_token" in str(resp.json()["detail"])


class _FakeScalarResult:
    """Minimal stand-in for `Session.scalars(...)`, which only `.all()` is used on."""

    def __init__(self, rows: list[object]) -> None:
        self._rows = rows

    def all(self) -> list[object]:
        return list(self._rows)


#: What a schedule row costs to hydrate: the inline spec is JSON, `extra_data` is
#: arbitrary JSON, the last submission result is TEXT, and none of them is needed
#: to decide who may act on the row or to hand its id to the executor.
#:
#: `extra_data` is listed even though it is usually small, because "usually" is
#: not a bound: a regression that dropped the spec and kept an arbitrary JSON
#: column would otherwise pass a test whose prose says no payload is read.
_SCHEDULE_PAYLOAD_COLUMNS = (
    "pipeline_task_spec",
    "extra_data",
    "last_run_submission_result",
)


class TestPathRoutesAuthorizeBeforeHydrating:
    """Regression: a locator should not load a payload to refuse a request.

    These assert the SQL, not the status code -- every one of these routes
    answered identically before the projection, which is the point: the
    behaviour is unchanged and only the columns moved. A test that checked
    responses would pass just as happily with the payload back.
    """

    def _owned_schedule(
        self,
        client: fastapi.testclient.TestClient,
        *,
        path: str = "team/report",
    ) -> str:
        created = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": path,
                "name": "Nightly",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
            },
        )
        assert created.status_code == status.HTTP_201_CREATED
        return created.json()["id"]

    @staticmethod
    def _schedule_selects(statements: list[str]) -> list[str]:
        return sql_capture.selects_from(statements, table="scheduled_pipeline_run")

    def test_a_trigger_by_path_reads_no_payload_column(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The executor reloads the schedule itself, from the id."""
        self._owned_schedule(client)
        fake_run = api_server_sql.PipelineRunResponse(
            id="run-1", root_execution_id="exec-1"
        )

        with (
            mock.patch.object(
                executor, "execute_pipeline_schedule", return_value=fake_run
            ),
            sql_capture.capture_sql(db_engine) as statements,
        ):
            resp = client.post(
                "/api/schedules/pipelines/trigger?schedule_path=team/report"
            )

        assert resp.status_code == status.HTTP_200_OK
        reads = self._schedule_selects(statements)
        assert len(reads) == 1
        sql_capture.asserted_absent(reads, columns=_SCHEDULE_PAYLOAD_COLUMNS)

    def test_the_id_addressed_trigger_reads_the_same_shape(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Parity in SHAPE: one operation, two addresses, no payload either way.

        Not parity in statement count, and the difference is the point. The path
        form scopes its single SELECT by owner, so resolving the row and
        authorizing it are the same statement. The id form cannot do that: it has
        to distinguish absent (404) from foreign (403), which a scoped read
        cannot, so it resolves unscoped and then probes
        `WHERE id = :id AND created_by = :caller` separately.

        Two reads is therefore the cost of keeping the 403 -- paid so the owner
        comparison is still the database's on this route. It used to be one read
        plus a Python `!=`, which was cheaper and gave the same caller a
        different answer by id than by path.

        Both reads are asserted payload-free, which is what this class is
        actually about; a regression that reloads the entity to authorize would
        fail on the columns, not the count.
        """
        schedule_id = self._owned_schedule(client)
        fake_run = api_server_sql.PipelineRunResponse(
            id="run-2", root_execution_id="exec-2"
        )

        with (
            mock.patch.object(
                executor, "execute_pipeline_schedule", return_value=fake_run
            ),
            sql_capture.capture_sql(db_engine) as statements,
        ):
            resp = client.post(f"/api/schedules/pipelines/{schedule_id}/trigger")

        assert resp.status_code == status.HTTP_200_OK
        reads = self._schedule_selects(statements)
        assert len(reads) == 2, reads
        table = db_models.ScheduledPipelineRun.__tablename__
        probes = [
            r for r in reads if f"{table}.id = " in r and f"{table}.created_by = " in r
        ]
        assert (
            len(probes) == 1
        ), "the authorization probe must be scoped in SQL, not in Python"
        sql_capture.asserted_absent(reads, columns=_SCHEDULE_PAYLOAD_COLUMNS)

    def test_a_paused_schedule_is_still_refused_before_the_executor(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """`paused` is in the projection precisely so this stays a 409."""
        schedule_id = self._owned_schedule(client)
        assert (
            client.patch(
                f"/api/schedules/pipelines/{schedule_id}", json={"paused": True}
            ).status_code
            == 200
        )

        with (
            mock.patch.object(executor, "execute_pipeline_schedule") as execute,
            sql_capture.capture_sql(db_engine) as statements,
        ):
            resp = client.post(
                "/api/schedules/pipelines/trigger?schedule_path=team/report"
            )

        assert resp.status_code == status.HTTP_409_CONFLICT
        assert execute.call_count == 0
        sql_capture.asserted_absent(
            self._schedule_selects(statements),
            columns=_SCHEDULE_PAYLOAD_COLUMNS,
        )

    def test_a_delete_by_path_reads_no_payload_column_and_still_deletes(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """`session.delete` needs an instance, not a loaded one."""
        schedule_id = self._owned_schedule(client)

        with sql_capture.capture_sql(db_engine) as statements:
            resp = client.delete("/api/schedules/pipelines?schedule_path=team/report")

        assert resp.status_code == status.HTTP_204_NO_CONTENT
        reads = self._schedule_selects(statements)
        assert len(reads) == 1
        sql_capture.asserted_absent(reads, columns=_SCHEDULE_PAYLOAD_COLUMNS)
        assert any(
            statement.lstrip().upper().startswith("DELETE") for statement in statements
        )
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            assert session.get(db_models.ScheduledPipelineRun, schedule_id) is None

    def test_a_patch_by_path_authorizes_on_identity_before_loading_the_row(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """PATCH does need the whole row -- after it knows the caller owns it.

        The trade is explicit: one extra indexed single-row SELECT on the
        authorized path, in exchange for an unauthorized one hydrating nothing.
        """
        self._owned_schedule(client)

        with sql_capture.capture_sql(db_engine) as statements:
            resp = client.patch(
                "/api/schedules/pipelines?schedule_path=team/report",
                json={"name": "renamed"},
            )

        assert resp.status_code == status.HTTP_200_OK
        assert resp.json()["name"] == "renamed"
        reads = self._schedule_selects(statements)
        # Locator, full load, post-commit refresh.
        assert len(reads) == 3
        sql_capture.asserted_absent(reads[:1], columns=_SCHEDULE_PAYLOAD_COLUMNS)
        assert all(
            sql_capture.mentions(reads[0], column=column)
            for column in ("id", "created_by")
        )

    @pytest.mark.parametrize(
        ("verb", "call"),
        [
            pytest.param(
                "patch",
                lambda c: c.patch(
                    "/api/schedules/pipelines?schedule_path=team/absent",
                    json={"name": "x"},
                ),
                id="patch",
            ),
            pytest.param(
                "delete",
                lambda c: c.delete(
                    "/api/schedules/pipelines?schedule_path=team/absent"
                ),
                id="delete",
            ),
            pytest.param(
                "trigger",
                lambda c: c.post(
                    "/api/schedules/pipelines/trigger?schedule_path=team/absent"
                ),
                id="trigger",
            ),
        ],
    )
    def test_a_request_that_will_be_refused_hydrates_nothing(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
        verb: str,
        call,
    ) -> None:
        del verb
        self._owned_schedule(client)

        with sql_capture.capture_sql(db_engine) as statements:
            resp = call(client)

        assert resp.status_code == status.HTTP_404_NOT_FOUND
        reads = self._schedule_selects(statements)
        assert len(reads) == 1
        sql_capture.asserted_absent(reads, columns=_SCHEDULE_PAYLOAD_COLUMNS)


class TestScheduleCreateValidatesWithoutLoadingAPipelineBody:
    """Regression:, at the endpoint rather than the service.

    The preflight is the only reason the schedule writer touches the pipeline
    tables at all, and it never reads what it loads.
    """

    def _saved_pipeline(self, db_engine: sqlalchemy.Engine) -> str:
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            written = user_pipeline_services.UserPipelineService().set_pipeline(
                session=session,
                user_id=DEFAULT_USER,
                file_path="pipelines/nightly.yaml",
                root_pipeline_task=copy.deepcopy(SAMPLE_PIPELINE_TASK_SPEC),
                pipeline_run_annotations=None,
                versioning_mode=user_pipeline_db_models.PipelineVersioningMode.FULL,
            )
            return written.pipeline.id

    def test_creating_a_reference_schedule_selects_no_pipeline_payload(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        pipeline_id = self._saved_pipeline(db_engine)

        with sql_capture.capture_sql(db_engine) as statements:
            resp = client.post(
                "/api/schedules/pipelines",
                json={
                    "schedule_path": "team/from-saved",
                    "name": "From saved",
                    "pipeline_task_spec_from_user_pipeline_id": pipeline_id,
                    "cron_expression": "0 8 * * *",
                },
            )

        assert resp.status_code == status.HTTP_201_CREATED
        version_reads = sql_capture.selects_from(statements, table="pipeline_version")
        assert version_reads
        sql_capture.asserted_absent(
            version_reads,
            columns=(
                "root_pipeline_task",
                "pipeline_run_annotations",
                "extra_data",
            ),
        )
        # The pipeline row carries arbitrary JSON of its own, and validation has
        # no more business reading that than it has reading the task body.
        pipeline_reads = sql_capture.selects_from(statements, table="pipeline")
        assert pipeline_reads
        sql_capture.asserted_absent(pipeline_reads, columns=("extra_data",))


class TestTheDeleteStubRefusesToBeReadFrom:
    """The payload-free entity is deletable, not usable.

    `load_only(..., raiseload=True)` is what keeps a future edit from reaching
    through the stub for a field and paying for a second SELECT nobody counted.
    Asserted directly on the query helper, because by the time a route touched it
    the mistake would already be a silent extra statement in production.
    """

    def test_reading_a_payload_column_off_the_stub_raises(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        created = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": "team/stub",
                "name": "Stubbed",
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
            },
        )
        assert created.status_code == status.HTTP_201_CREATED

        with sqlalchemy.orm.Session(bind=db_engine) as session:
            stub = schedule_queries.owned_schedule_stub_by_path(
                session=session,
                created_by=DEFAULT_USER,
                canonical_path="team/stub",
            )
            assert stub is not None
            # The two it is allowed to answer.
            assert stub.id == created.json()["id"]
            assert stub.created_by == DEFAULT_USER
            with pytest.raises(sqlalchemy.exc.InvalidRequestError):
                _ = stub.pipeline_task_spec


#: What a LIST response never carries. `last_run_submission_result` is absent
#: from this tuple on purpose, unlike `_SCHEDULE_PAYLOAD_COLUMNS`: the list
#: serializes it, so a projection that dropped it would return null for a field
#: the response promises. Only the two genuinely unbounded columns are refused.
_LIST_UNPROJECTED_COLUMNS = ("pipeline_task_spec", "extra_data")


class TestTheListDoesNotHydrateWhatItDoesNotReturn:
    """Regression: `include_spec=False` decided what to PRINT, not what to READ.

    The handler already declined to serialize `pipeline_task_spec`, and by the
    time it declined, a page of whole pipeline definitions had been read off the
    wire and turned into Python dicts. The response was right and the cost was
    paid anyway -- which is why these assert the SQL. Every one of them passed
    before the projection existed.
    """

    def _schedule_selects(self, statements: list[str]) -> list[str]:
        return sql_capture.selects_from(statements, table="scheduled_pipeline_run")

    def _create(
        self, client: fastapi.testclient.TestClient, *, path: str, name: str
    ) -> str:
        created = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": path,
                "name": name,
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
            },
        )
        assert created.status_code == status.HTTP_201_CREATED
        return created.json()["id"]

    def test_a_page_reads_no_payload_column(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        self._create(client, path="team/one", name="One")
        self._create(client, path="team/two", name="Two")

        with sql_capture.capture_sql(db_engine) as statements:
            resp = client.get("/api/schedules/pipelines")

        assert resp.status_code == status.HTTP_200_OK
        assert len(resp.json()["schedules"]) == 2
        sql_capture.asserted_absent(
            self._schedule_selects(statements),
            columns=_LIST_UNPROJECTED_COLUMNS,
        )

    def test_a_path_filtered_read_reads_no_payload_column(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The branch the regression targets, and it shares the statement."""
        self._create(client, path="team/one", name="One")

        with sql_capture.capture_sql(db_engine) as statements:
            resp = client.get("/api/schedules/pipelines?schedule_path=team/one")

        assert resp.status_code == status.HTTP_200_OK
        assert resp.json()["total_count"] == 1
        sql_capture.asserted_absent(
            self._schedule_selects(statements),
            columns=_LIST_UNPROJECTED_COLUMNS,
        )

    def test_a_cursor_page_reads_no_payload_column_either(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The second page is where a payload read would cost the most."""
        for index in range(3):
            self._create(client, path=f"team/p{index}", name=f"Page {index}")
        token = client.get("/api/schedules/pipelines", params={"page_size": 1}).json()[
            "next_page_token"
        ]
        assert token

        with sql_capture.capture_sql(db_engine) as statements:
            resp = client.get(
                "/api/schedules/pipelines",
                params={"page_size": 1, "page_token": token},
            )

        assert resp.status_code == status.HTTP_200_OK
        sql_capture.asserted_absent(
            self._schedule_selects(statements),
            columns=_LIST_UNPROJECTED_COLUMNS,
        )

    def test_the_page_is_not_paid_for_twice(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """A plain `defer` would answer a stray access with a SELECT per row.

        The column count is not the only way this regresses: an unprojected
        column that something still touches turns into N+1 statements, which the
        absence assertions above would happily pass. Two statements, always: the
        page and its count.
        """
        for index in range(3):
            self._create(client, path=f"team/n{index}", name=f"N {index}")

        with sql_capture.capture_sql(db_engine) as statements:
            resp = client.get("/api/schedules/pipelines")

        assert resp.status_code == status.HTTP_200_OK
        assert len(self._schedule_selects(statements)) == 2

    def test_the_response_still_carries_every_field_it_promised(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The projection must be invisible from outside.

        Named individually rather than by a "no nulls" sweep, because the fields
        at risk are precisely the ones that are legitimately null on some rows.
        `last_run_submission_result` is here because it is TEXT and was the
        tempting third column to drop -- the list returns it, so it is loaded.
        """
        schedule_id = self._create(client, path="team/full", name="Full")
        # Written directly rather than by firing the schedule: the point is that
        # both stored columns survive the projection, and a real fire would leave
        # that depending on the executor's own behaviour.
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            row = session.get(db_models.ScheduledPipelineRun, schedule_id)
            assert row is not None
            row.last_run_at = datetime.datetime(
                2024, 5, 1, 8, 0, tzinfo=datetime.timezone.utc
            )
            row.last_run_submission_result = db_models.SubmissionResult.SUCCESS.value
            session.commit()

        listed = client.get("/api/schedules/pipelines").json()["schedules"][0]

        assert listed["id"] == schedule_id
        assert listed["name"] == "Full"
        assert listed["schedule_path"] == "team/full"
        assert listed["cron_expression"] == "0 8 * * *"
        assert listed["timezone"] == "UTC"
        assert listed["paused"] is False
        assert listed["created_by"] == DEFAULT_USER
        assert listed["created_at"] and listed["updated_at"]
        assert listed["last_run_at"]
        assert (
            listed["last_run_submission_result"]
            == db_models.SubmissionResult.SUCCESS.value
        )
        # Deliberately withheld, exactly as before: the list has never returned
        # it, and a null field is omitted from the serialized response entirely.
        assert listed.get("pipeline_task_spec") is None

    def test_reading_the_spec_off_a_listed_row_raises_rather_than_reloading(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """`raiseload=True`, not a bare defer, and the difference is the point.

        A deferred column answers a later access with a silent extra SELECT --
        the exact cost this removes, reintroduced invisibly. Asserted on the
        projection itself, because through the endpoint the mistake shows up only
        as a statement count nobody reads.
        """
        self._create(client, path="team/raise", name="Raises")

        with sqlalchemy.orm.Session(bind=db_engine) as session:
            row = session.scalars(
                sqlalchemy.select(db_models.ScheduledPipelineRun).options(
                    *schedule_queries.LIST_PROJECTION
                )
            ).one()

            assert row.name == "Raises"
            assert row.last_run_submission_result is None
            with pytest.raises(sqlalchemy.exc.InvalidRequestError):
                _ = row.pipeline_task_spec

    def test_the_withheld_columns_are_real_columns(self) -> None:
        """An exclusion set is only a projection while the names still exist.

        A renamed column would silently start being hydrated again, and every
        assertion above would still pass -- they name the same stale string.
        """
        mapped = {
            attribute.key
            for attribute in sqlalchemy.inspect(
                db_models.ScheduledPipelineRun
            ).column_attrs
        }

        assert schedule_queries._UNPROJECTED_LIST_COLUMNS <= mapped
        assert (
            set(_LIST_UNPROJECTED_COLUMNS) == schedule_queries._UNPROJECTED_LIST_COLUMNS
        )


class TestTheOwnerScopedListHasAnAccessPath:
    """Regression: owner scoping added a filter without adding a path for it.

    `WHERE created_by = ? ORDER BY updated_at DESC, id DESC LIMIT n` against an
    index keyed only on `(updated_at, id)` is an ordered scan under a
    low-selectivity filter: the server walks the table newest-first discarding
    other people's rows, and the LIMIT bounds the answer rather than the work.

    Executed, not argued. These replay the statements the endpoint really emitted
    through SQLite's planner, so the claim being tested is "this index serves this
    statement", not "this index looks right".

    What that does NOT prove: SQLite's planner is not MySQL's, and no EXPLAIN has
    been taken against a live MySQL instance -- the standing coverage
    gap recorded in SCHEDULER_DESIGN.md. What it does prove is the part that is
    dialect-independent and was actually wrong: that an index exists whose
    leading column is the equality and whose remaining keys are the sort, so a
    planner that wants one can find it.
    """

    _INDEX = "ix_scheduled_pipeline_run_created_by_updated_at_id"

    @staticmethod
    def _plan(db_engine: sqlalchemy.Engine, statement: str) -> str:
        """SQLite's chosen plan for a statement, with placeholder parameters.

        Values are irrelevant to the plan here: nothing runs ANALYZE, so the
        planner has no statistics and chooses on shape alone.
        """
        with db_engine.connect() as conn:
            rows = conn.exec_driver_sql(
                f"EXPLAIN QUERY PLAN {statement}",
                tuple([None] * statement.count("?")),
            ).all()
        return "\n".join(str(row) for row in rows)

    def _list_statements(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
        url: str,
        params: dict[str, object] | None = None,
    ) -> tuple[str, str]:
        """The page SELECT and the count SELECT the endpoint emitted."""
        with sql_capture.capture_sql(db_engine) as statements:
            assert client.get(url, params=params).status_code == status.HTTP_200_OK
        reads = sql_capture.selects_from(statements, table="scheduled_pipeline_run")
        pages = [read for read in reads if "count(" not in read.lower()]
        counts = [read for read in reads if "count(" in read.lower()]
        assert len(pages) == 1 and len(counts) == 1
        return pages[0], counts[0]

    @pytest.fixture
    def _populated(self, client: fastapi.testclient.TestClient) -> None:
        for index in range(3):
            created = client.post(
                "/api/schedules/pipelines",
                json={
                    "schedule_path": f"team/plan{index}",
                    "name": f"Plan {index}",
                    "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                    "cron_expression": "0 8 * * *",
                },
            )
            assert created.status_code == status.HTTP_201_CREATED

    def test_a_page_searches_the_owner_index_and_sorts_nothing(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
        _populated: None,
    ) -> None:
        """Both halves matter, and the second is why the key order is what it is.

        SEARCH rather than SCAN says the owner equality became a range. The
        absence of a temporary B-tree says the sort came out of the index inside
        that range -- which an index of `(created_by, schedule_path)` would have
        given the first half of and not the second.
        """
        page, _count = self._list_statements(
            client, db_engine, "/api/schedules/pipelines"
        )
        plan = self._plan(db_engine, page)

        assert self._INDEX in plan, plan
        assert "SEARCH" in plan, plan
        assert "SCAN" not in plan, plan
        assert "TEMP B-TREE" not in plan.upper(), plan

    def test_a_cursor_page_rides_the_same_key(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
        _populated: None,
    ) -> None:
        """`(updated_at, id) < (:u, :i)` narrows the range instead of filtering it.

        The cursor is the part of pagination that gets slower the deeper a caller
        goes, so it is the part that most needs to be a range and not a
        post-filter.
        """
        token = client.get("/api/schedules/pipelines", params={"page_size": 1}).json()[
            "next_page_token"
        ]
        assert token
        page, _count = self._list_statements(
            client,
            db_engine,
            "/api/schedules/pipelines",
            params={"page_size": 1, "page_token": token},
        )
        plan = self._plan(db_engine, page)

        assert self._INDEX in plan, plan
        assert "SEARCH" in plan, plan
        assert "TEMP B-TREE" not in plan.upper(), plan

    def test_the_total_count_does_not_read_the_table(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
        _populated: None,
    ) -> None:
        """The one statement in the handler that no LIMIT bounds.

        It counts one owner's rows, so it must reach them through an index rather
        than by reading every schedule in the system. Which index the planner
        picks is its business -- both candidates lead with `created_by`.
        """
        _page, count = self._list_statements(
            client, db_engine, "/api/schedules/pipelines"
        )
        plan = self._plan(db_engine, count)

        assert "SEARCH" in plan, plan
        assert "SCAN scheduled_pipeline_run" not in plan, plan

    def test_the_index_is_a_migration_target_and_not_a_tier(self) -> None:
        """Installed by startup, gating nothing.

        Both halves are decisions. Leaving it out of the target set would mean an
        index that only fresh databases have; adding a gate for it would refuse
        the list until the migration lands, trading a slow answer for no answer --
        the opposite of the trade made for `schedule_path`, where an absent index
        makes the answer WRONG rather than merely expensive.
        """
        assert self._INDEX in {
            spec.name for spec in database_migrations._TARGET_INDEXES
        }

        tier_indexes = {
            step
            for tier in database_migrations._WRITE_TIER_STEPS.values()
            for step in tier()
            if step.startswith("index:")
        }
        assert f"index:{self._INDEX}" not in tier_indexes


class TestSchedulePathIsCaseSensitive:
    """`Foo/Bar` and `foo/bar` are two schedules, not one.

    This is the behaviour the whole change exists for, so it is asserted through
    the API for every verb rather than only on the canonicalizer: the identity
    has to survive the write, the read, the update and the delete, and each of
    those reaches the column by a different query.

    SQLite compares TEXT byte-for-byte, so it reproduces the intended MySQL
    behaviour here. What it CANNOT reproduce is the failure: with a case-folding
    collation MySQL would reject the second create as a duplicate and could
    answer a lookup with the wrong row, and no SQLite test can show that. The
    column's collation is therefore asserted separately, on the model and in the
    migration, in `test_db_models.py` and `test_database_migrations.py` -- those
    are the tests that fail if the database half is dropped.
    """

    UPPER = "Team/Nightly"
    LOWER = "team/nightly"

    def _create(
        self,
        client: fastapi.testclient.TestClient,
        *,
        schedule_path: str,
        name: str,
    ) -> str:
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "name": name,
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": schedule_path,
            },
        )
        assert resp.status_code == status.HTTP_201_CREATED, resp.text
        return resp.json()["id"]

    def test_both_spellings_can_exist_at_once(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """The create that a case-folding unique index would refuse as a duplicate."""
        upper_id = self._create(client, schedule_path=self.UPPER, name="Upper")
        lower_id = self._create(client, schedule_path=self.LOWER, name="Lower")

        assert upper_id != lower_id

    def test_each_spelling_is_stored_exactly_as_supplied(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        upper_id = self._create(client, schedule_path=self.UPPER, name="Upper")
        lower_id = self._create(client, schedule_path=self.LOWER, name="Lower")

        by_id = {
            sid: client.get(f"/api/schedules/pipelines/{sid}").json()["schedule_path"]
            for sid in (upper_id, lower_id)
        }
        assert by_id == {upper_id: self.UPPER, lower_id: self.LOWER}

    def test_a_read_by_path_returns_only_its_own_spelling(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """The wrong-row read: the exact failure a folding collation produces."""
        self._create(client, schedule_path=self.UPPER, name="Upper")
        self._create(client, schedule_path=self.LOWER, name="Lower")

        for path, expected_name in (
            (self.UPPER, "Upper"),
            (self.LOWER, "Lower"),
        ):
            body = client.get(
                "/api/schedules/pipelines", params={"schedule_path": path}
            ).json()
            assert [s["schedule_path"] for s in body["schedules"]] == [path]
            assert [s["name"] for s in body["schedules"]] == [expected_name]
            assert body["total_count"] == 1

    def test_an_update_by_path_moves_only_its_own_row(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        self._create(client, schedule_path=self.UPPER, name="Upper")
        self._create(client, schedule_path=self.LOWER, name="Lower")

        resp = client.patch(
            "/api/schedules/pipelines",
            params={"schedule_path": self.UPPER},
            json={"name": "Only the upper one"},
        )
        assert resp.status_code == status.HTTP_200_OK
        assert resp.json()["schedule_path"] == self.UPPER

        untouched = client.get(
            "/api/schedules/pipelines", params={"schedule_path": self.LOWER}
        ).json()
        assert [s["name"] for s in untouched["schedules"]] == ["Lower"]

    def test_a_delete_by_path_removes_only_its_own_row(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """The destructive one, and the reason this is not merely cosmetic."""
        self._create(client, schedule_path=self.UPPER, name="Upper")
        self._create(client, schedule_path=self.LOWER, name="Lower")

        deleted = client.delete(
            "/api/schedules/pipelines", params={"schedule_path": self.UPPER}
        )
        assert deleted.status_code == status.HTTP_204_NO_CONTENT

        survivor = client.get(
            "/api/schedules/pipelines", params={"schedule_path": self.LOWER}
        ).json()
        assert [s["name"] for s in survivor["schedules"]] == ["Lower"]
        gone = client.get(
            "/api/schedules/pipelines", params={"schedule_path": self.UPPER}
        ).json()
        assert gone["schedules"] == []

    def test_a_case_variant_is_not_the_same_path_to_adopt(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Set-once applies per exact value, so a re-case is a change, not a replay."""
        sid = self._create(client, schedule_path=self.LOWER, name="Lower")

        resp = client.patch(
            f"/api/schedules/pipelines/{sid}",
            json={"schedule_path": self.UPPER},
        )

        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert "set once" in resp.json()["detail"]

    def test_the_canonicalizer_accepts_upper_case_segments(self) -> None:
        """The regex, not just the endpoint: a lower-only charset would 422 these."""
        for path in (
            "Team/Nightly",
            "TEAM/V1.2/RUN_A",
            "A",
            "Mixed/Case-With_Dots.v2",
        ):
            assert schedule_paths.canonicalize_schedule_path(path) == path

    def test_a_derived_path_is_still_lower_case(self) -> None:
        """Derivation is unchanged: there is no caller-chosen case to preserve."""
        derived = schedule_paths.generate_legacy_schedule_path(
            name="Nightly Sweep", schedule_id="ABCdef0123456789"
        )

        assert derived == derived.lower()
        # And it must still satisfy the strict normalizer, which now permits
        # upper case -- so this cannot pass merely because the rule got laxer.
        assert schedule_paths.canonicalize_schedule_path(derived) == derived


class TestALockFailureIsNotAFiveHundred:
    """Regression: contention on the unique key is not always an IntegrityError.

    InnoDB's duplicate-key check takes a shared lock on the conflicting index
    record, so a second writer waits -- and the wait can end as a deadlock (1213)
    or a lock-wait timeout (1205) rather than a duplicate. Both arrive as
    `OperationalError`, which the handlers did not catch, so a contended path
    became a 500.

    SQLite cannot produce InnoDB lock contention, so the failure is INJECTED at
    commit. That is deliberate and is the same technique the foreign-key race
    tests use: what is under test is how the failure is CLASSIFIED, and injecting
    it tests exactly that without pretending the dialect has row locks.
    """

    @staticmethod
    def _fail_commit_once(exc: Exception) -> object:
        real = sqlalchemy.orm.Session.commit
        state = {"failed": False}

        def _commit(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            if not state["failed"]:
                state["failed"] = True
                raise exc
            return real(self, *args, **kwargs)

        return mock.patch.object(sqlalchemy.orm.Session, "commit", _commit)

    @staticmethod
    def _driver_error(errno: object, message: str) -> sqlalchemy.exc.OperationalError:
        """Shaped like PyMySQL's, because the code under test reads `orig.args[0]`.

        PyMySQL raises `OperationalError(errno, message)` with an INTEGER errno,
        and that integer is the whole basis for the retry-safe answer. An earlier
        version of these helpers put the code inside a formatted STRING, which
        made every one of them pass against a handler that read no code at all --
        the tests could not tell a deadlock from a dropped connection, which is
        precisely the distinction they exist to enforce.
        """
        return sqlalchemy.exc.OperationalError(
            "INSERT INTO scheduled_pipeline_run ...",
            {},
            pymysql.err.OperationalError(errno, message),
        )

    @classmethod
    def _deadlock(cls) -> sqlalchemy.exc.OperationalError:
        return cls._driver_error(
            1213,
            "Deadlock found when trying to get lock; try restarting transaction",
        )

    @classmethod
    def _lock_timeout(cls) -> sqlalchemy.exc.OperationalError:
        return cls._driver_error(
            1205, "Lock wait timeout exceeded; try restarting transaction"
        )

    @classmethod
    def _lost_connection(cls) -> sqlalchemy.exc.OperationalError:
        return cls._driver_error(2013, "Lost connection to MySQL server during query")

    def _post(
        self,
        client: fastapi.testclient.TestClient,
        *,
        schedule_path: str,
        name: str = "racer",
    ) -> object:
        return client.post(
            "/api/schedules/pipelines",
            json={
                "name": name,
                "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
                "cron_expression": "0 8 * * *",
                "schedule_path": schedule_path,
            },
        )

    @pytest.mark.parametrize("failure", ["deadlock", "lock_timeout"])
    def test_a_lock_failure_on_a_taken_path_is_the_ordinary_conflict(
        self,
        client: fastapi.testclient.TestClient,
        failure: str,
    ) -> None:
        """The common case: the caller lost the race, so 409 is the true answer."""
        assert (
            self._post(client, schedule_path="race/same", name="winner").status_code
            == status.HTTP_201_CREATED
        )

        exc = self._deadlock() if failure == "deadlock" else self._lock_timeout()
        with self._fail_commit_once(exc):
            resp = self._post(client, schedule_path="race/same", name="loser")

        assert resp.status_code == status.HTTP_409_CONFLICT
        assert "already used" in resp.json()["detail"]

    def test_a_lock_failure_with_no_winner_is_retryable_not_broken(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Nobody owns the path, so this is contention, not the caller's fault."""
        with self._fail_commit_once(self._deadlock()):
            resp = self._post(client, schedule_path="race/nobody")

        assert resp.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
        assert resp.headers["Retry-After"] == "1"
        assert "repeated" in resp.json()["detail"]["message"].lower()

    def test_the_retry_it_advertises_actually_succeeds(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """A 503 that cannot be retried would be a lie. The row must be absent."""
        with self._fail_commit_once(self._deadlock()):
            assert (
                self._post(client, schedule_path="race/retry").status_code
                == status.HTTP_503_SERVICE_UNAVAILABLE
            )

        again = self._post(client, schedule_path="race/retry")
        assert again.status_code == status.HTTP_201_CREATED
        listed = client.get(
            "/api/schedules/pipelines", params={"schedule_path": "race/retry"}
        ).json()
        assert listed["total_count"] == 1

    def test_a_lock_failure_never_reports_a_missing_reference(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """A lock says nothing about a parent row, so 404 would send the caller wrong."""
        run_id = insert_pipeline_run(db_engine=db_engine)

        with self._fail_commit_once(self._deadlock()):
            resp = client.post(
                "/api/schedules/pipelines",
                json={
                    "name": "referencing",
                    "cron_expression": "0 8 * * *",
                    "schedule_path": "race/reference",
                    "pipeline_task_spec_from_pipeline_run_id": run_id,
                },
            )

        assert resp.status_code == status.HTTP_503_SERVICE_UNAVAILABLE

    def test_a_dropped_connection_does_not_get_the_no_write_promise(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """2013 is not 1213, and the difference is the whole guarantee.

        A deadlock victim provably wrote nothing. A lost connection may have had
        its write COMMITTED with only the response lost, so answering the
        retry-safe 503 would promise something the server cannot know. It escapes
        as a 500, which honestly says the outcome is unknown -- and a caller that
        retries a lost-but-applied write would otherwise get a 409 on its own
        path and conclude somebody else took it.
        """
        with self._fail_commit_once(self._lost_connection()):
            with pytest.raises(sqlalchemy.exc.OperationalError):
                self._post(client, schedule_path="race/dropped")

    def test_a_driver_error_with_no_integer_code_is_not_guessed_at(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Fail closed on an unrecognised shape rather than assume it is a deadlock.

        The cost of missing a real deadlock is one 500 on a request the client
        would have retried anyway. The cost of guessing the other way is a false
        no-write guarantee, so an unparseable errno must not be waved through.
        """
        with self._fail_commit_once(
            self._driver_error("1213", "deadlock, as a string")
        ):
            with pytest.raises(sqlalchemy.exc.OperationalError):
                self._post(client, schedule_path="race/stringly")

    def test_the_contention_503_names_itself(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """Identifiable by a POSITIVE code, never by the absence of one.

        An infrastructure 503 -- a proxy, a load balancer, a pod shut down
        mid-request -- is uncoded and carries the opposite guarantee about
        whether the write landed. A client classifying on absence would apply
        "nothing was written" to responses this service never issued, so the code
        has to be present and distinct from the readiness code.
        """
        with self._fail_commit_once(self._deadlock()):
            resp = self._post(client, schedule_path="race/coded")

        assert resp.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
        detail = resp.json()["detail"]
        assert (
            detail["code"]
            == api_routes.SchedulerErrorCode.SCHEDULE_PATH_LOCK_CONTENTION.value
        )
        assert (
            detail["code"]
            != api_routes.SchedulerErrorCode.SCHEDULE_PATH_WRITES_UNAVAILABLE.value
        )

    def test_an_integrity_error_is_still_classified_by_its_own_branch(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """The new branch must not have absorbed the old one."""
        assert (
            self._post(client, schedule_path="race/plain", name="winner").status_code
            == status.HTTP_201_CREATED
        )

        resp = self._post(client, schedule_path="race/plain", name="loser")

        assert resp.status_code == status.HTTP_409_CONFLICT

    def test_a_patch_that_adopted_nothing_is_not_blamed_on_a_path(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """A lock failure on an unrelated PATCH must not be reported as 409."""
        assert (
            self._post(client, schedule_path="race/held", name="holder").status_code
            == status.HTTP_201_CREATED
        )
        other = self._post(client, schedule_path="race/other", name="other")
        sid = other.json()["id"]

        with self._fail_commit_once(self._deadlock()):
            resp = client.patch(
                f"/api/schedules/pipelines/{sid}", json={"name": "renamed"}
            )

        assert resp.status_code == status.HTTP_503_SERVICE_UNAVAILABLE

    def test_a_no_op_replay_is_not_reported_as_a_conflict(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """The path IS taken -- by this very row -- so the proof alone is not enough.

        Re-sending the value a schedule already holds is a documented no-op, not
        an adoption. Passing the path to the classifier unconditionally would let
        `_path_is_taken` answer True about the caller's own row and turn a
        transient lock failure into a permanent 409 the caller can never clear.
        """
        created = self._post(client, schedule_path="race/mine", name="mine")
        sid = created.json()["id"]

        with self._fail_commit_once(self._deadlock()):
            resp = client.patch(
                f"/api/schedules/pipelines/{sid}",
                json={"schedule_path": "race/mine", "name": "renamed"},
            )

        assert resp.status_code == status.HTTP_503_SERVICE_UNAVAILABLE

    def test_the_adoption_update_is_covered_too(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The CAS writes the same unique key, so it has the same lock exposure.

        Injected on the UPDATE rather than the commit, because that statement has
        its own handler: the create's commit branch says nothing about it.
        """
        assert (
            self._post(client, schedule_path="cas/taken", name="holder").status_code
            == status.HTTP_201_CREATED
        )
        sid = insert_pathless_schedule_row(db_engine=db_engine)

        real_execute = sqlalchemy.orm.Session.execute
        state = {"failed": False}

        def _execute(self, statement, *args, **kwargs):  # type: ignore[no-untyped-def]
            if not state["failed"] and isinstance(statement, sqlalchemy.Update):
                state["failed"] = True
                raise self_deadlock()
            return real_execute(self, statement, *args, **kwargs)

        self_deadlock = self._deadlock
        with mock.patch.object(sqlalchemy.orm.Session, "execute", _execute):
            resp = client.patch(
                f"/api/schedules/pipelines/{sid}",
                json={"schedule_path": "cas/taken"},
            )

        # The path belongs to somebody else, so the proof holds and 409 is true.
        assert resp.status_code == status.HTTP_409_CONFLICT


#: Stands in for a real id when resolving which route a case dispatches to.
_SAMPLE_SCHEDULE_ID = "00000000-0000-0000-0000-000000000000"


@dataclasses.dataclass(frozen=True)
class _PathCase:
    """One path-addressed surface and the request that exercises it.

    Single source of truth. It drives the pytest parametrization AND the
    coverage assertion, so a case cannot be deleted from the run while the
    surface still counts as covered -- which is exactly what a separate
    hand-maintained set of route keys allowed.

    There is deliberately no declared route key. An earlier version carried one
    and fed it to the coverage assertion while the request was built from a
    separate method/url pair, so changing only the method left coverage still
    counting DELETE while the suite actually drove GET. The route is now
    resolved by matching the request the test really sends against the live
    router, so the two cannot disagree.

    `url` is a template; `{schedule_id}` is filled from a seeded row.
    """

    ident: str
    method: str
    url: str
    payload: dict[str, object] | None
    #: "no-query" surfaces must refuse having asked the database nothing.
    #: "no-write" surfaces must read first -- see the PATCH-by-id rationale.
    rule: str = "no-query"


_CREATE_BODY: dict[str, object] = {
    "name": "Nightly",
    "cron_expression": "0 8 * * *",
    "pipeline_task_spec": SAMPLE_PIPELINE_TASK_SPEC,
}

_PATH_CASES: tuple[_PathCase, ...] = (
    _PathCase(
        ident="create-with-explicit-path",
        method="post",
        url="/api/schedules/pipelines",
        payload={**_CREATE_BODY, "schedule_path": "upi/nightly"},
    ),
    _PathCase(
        ident="create-deriving-a-path",
        method="post",
        url="/api/schedules/pipelines",
        payload=dict(_CREATE_BODY),
    ),
    _PathCase(
        ident="list-by-path",
        method="get",
        url="/api/schedules/pipelines?schedule_path=upi/nightly",
        payload=None,
    ),
    _PathCase(
        ident="patch-by-path",
        method="patch",
        url="/api/schedules/pipelines?schedule_path=upi/nightly",
        payload={"name": "x"},
    ),
    _PathCase(
        ident="delete-by-path",
        method="delete",
        url="/api/schedules/pipelines?schedule_path=upi/nightly",
        payload=None,
    ),
    _PathCase(
        ident="trigger-by-path",
        method="post",
        url="/api/schedules/pipelines/trigger?schedule_path=upi/nightly",
        payload=None,
    ),
    _PathCase(
        ident="patch-by-id-adopting-a-path",
        method="patch",
        url="/api/schedules/pipelines/{schedule_id}",
        payload={"schedule_path": "upi/adopted"},
        rule="no-write",
    ),
)

_NO_QUERY_CASES = [case for case in _PATH_CASES if case.rule == "no-query"]
_NO_WRITE_CASES = [case for case in _PATH_CASES if case.rule == "no-write"]


class TestEveryPathSurfaceHonoursTheClosedTier:
    """What each path surface owes a closed path tier. There are two rules.

    The headline used to be "no path route touches the table before its gate",
    which this class then spends half its length disproving. PATCH-by-id must
    query first, and two of its outcomes are deliberately not the tier refusal
    at all. A name that promises the stricter rule everywhere is a claim the
    tests reject, and the docstring is read far more often than the assertions.

    **Rule one, for directly path-addressed operations** -- the five surfaces
    that carry a path in the request, including a create that derives one.
    Each must answer a coded 503 having touched `scheduled_pipeline_run` zero
    times. Emitted SQL is the evidence, and it is the one thing a reordering
    cannot fake: the assertion is about what the database was actually asked,
    not about how the handler is written.

    **Rule two, for PATCH-by-id, where addressing a path is conditional.** The
    request only adopts a path if the stored one is NULL, which cannot be known
    without reading the owned row first. So this surface reads, and gates only
    the NULL-to-path transition: it must write nothing when refused. Restating
    the path a row already has is not a transition and succeeds; changing an
    existing path is the contract's 422. Neither is a tier refusal, and forcing
    rule one onto this surface would gate every id-addressed edit -- the
    operator lockout the closed tier exists to avoid.

    An earlier version of this file also carried a static analyzer that walked
    the AST of `api_routes` and tried to prove a gate call dominated every
    resolver call. It was removed rather than repaired. Across review it
    admitted six distinct false certifications -- a gate matched by substring, a
    gate merely contained in a statement rather than executed, an enum matched by
    trailing name, a resolver reached through an alias, and finally a gate
    function or enum locally shadowed by a no-op. Each fix was correct and each
    time the next layer of the same problem appeared underneath, because proving
    that a name at a call site refers to the intended object is name resolution,
    and a partial Python name resolver living in a test file is not evidence
    anybody should rely on. The decisive measurement: a no-op gate shadowed
    inside an existing route left the analyzer entirely green, while the
    behavioural test below failed on the same mutation.

    What is claimed here is therefore narrower and true. These tests exercise the
    real application through a client and observe the real database. They do not
    prove anything about routes that do not exist yet. The route-table snapshot
    is what covers that gap, and it covers it by forcing a human to look, not by
    understanding new code.

    Two limits of the snapshot, found by attacking it rather than by writing it:
    any new or re-addressed route fails it regardless of classification, because
    the method/path set changes -- so the review trigger holds even where the
    classification is wrong. What the classification has to get right is an
    EXISTING route quietly becoming path-addressed. Aliased parameters and
    optional body models are handled for that reason. A body model that nests
    another model carrying `schedule_path` is still not detected, and closing
    that would mean walking arbitrary pydantic graphs; it is recorded here
    instead of being silently assumed away.
    """

    #: Every route the scheduler registers, and whether it addresses a schedule
    #: by path. Reviewed by hand; the snapshot test below fails if reality drifts.
    _EXPECTED_ROUTES: typing.ClassVar[dict[tuple[str, str], bool]] = {
        ("POST", "/api/schedules/pipelines"): True,
        ("GET", "/api/schedules/pipelines"): True,
        ("PATCH", "/api/schedules/pipelines"): True,
        ("DELETE", "/api/schedules/pipelines"): True,
        ("POST", "/api/schedules/pipelines/trigger"): True,
        ("PATCH", "/api/schedules/pipelines/{pipeline_schedule_id}"): True,
        ("GET", "/api/schedules/pipelines/{pipeline_schedule_id}"): False,
        ("DELETE", "/api/schedules/pipelines/{pipeline_schedule_id}"): False,
        (
            "POST",
            "/api/schedules/pipelines/{pipeline_schedule_id}/trigger",
        ): False,
    }

    @staticmethod
    def _concrete_types(annotation: object) -> collections.abc.Iterator[object]:
        """Yield the concrete classes inside a union, `Optional`, or `Annotated`.

        `Request | None` is not a class, so a naive `issubclass` check silently
        skips it. That is not hypothetical: adding an optional body model to an
        existing route was one of two shapes that slipped past the first version
        of this classification.
        """
        if hasattr(annotation, "__metadata__"):
            yield from TestEveryPathSurfaceHonoursTheClosedTier._concrete_types(
                annotation.__origin__
            )
            return
        arguments = typing.get_args(annotation)
        if not arguments:
            yield annotation
            return
        for argument in arguments:
            yield from TestEveryPathSurfaceHonoursTheClosedTier._concrete_types(
                argument
            )

    @staticmethod
    def _dependant_graph(dependant: object) -> collections.abc.Iterator[object]:
        """Every dependant reachable from a route, including nested `Depends`.

        Only the top level was inspected before. pi-55 showed a route declaring
        `resolved = Depends(by_path)` where `by_path` takes the query parameter:
        the route's own `query_params` is empty, the parameter lives on the
        sub-dependant, and the route was classified as naming no path while
        being fully path-addressed.

        Cycle-safe by identity, because a dependency graph is not guaranteed to
        be a tree and a test that hangs is worse than one that fails.
        """
        seen: set[int] = set()
        stack = [dependant]
        while stack:
            current = stack.pop()
            if current is None or id(current) in seen:
                continue
            seen.add(id(current))
            yield current
            stack.extend(getattr(current, "dependencies", None) or [])

    @classmethod
    def _names_a_path(cls, route: object) -> bool:
        """Can a request to this route name a schedule path?

        Read from the dependant FastAPI resolved, not from the handler's Python
        signature. The signature says what the author called a parameter; the
        dependant says what the wire accepts. `fastapi.Query(alias=
        "schedule_path")` addresses a path while being invisible to a signature
        check, and an optional `Model | None` body is not a class at all.

        Body models are inspected one level deep. A model nesting another model
        that carries the field is not detected -- see the class docstring.
        """
        root = getattr(route, "dependant", None)
        if root is None:
            return False
        for dependant in cls._dependant_graph(root):
            for field in list(dependant.query_params) + list(dependant.path_params):
                if "schedule_path" in {field.alias, field.name}:
                    return True
            for field in dependant.body_params or []:
                annotation = getattr(field.field_info, "annotation", None)
                for candidate in cls._concrete_types(annotation):
                    if (
                        inspect.isclass(candidate)
                        and issubclass(candidate, pydantic.BaseModel)
                        and "schedule_path" in candidate.model_fields
                    ):
                        return True
        return False

    @classmethod
    def _route_for_request(
        cls, client: fastapi.testclient.TestClient, method: str, url: str
    ) -> tuple[str, str]:
        """Which route does the app actually dispatch this request to?

        Coverage is derived from this rather than from a key written beside the
        case, so a case cannot claim to exercise one route while driving
        another.
        """
        scope = {
            "type": "http",
            "method": method.upper(),
            "path": url.split("?")[0],
            "root_path": "",
            "headers": [],
            "query_string": b"",
        }
        matched: list[tuple[str, str]] = []

        def walk(router: object) -> None:
            for route in getattr(router, "routes", []):
                if getattr(route, "endpoint", None) is None:
                    nested = getattr(route, "original_router", None) or getattr(
                        route, "app", None
                    )
                    if nested is not None:
                        walk(nested)
                    continue
                match, _ = route.matches(scope)
                if match == starlette.routing.Match.FULL and not matched:
                    matched.append((method.upper(), route.path))

        walk(client.app)
        assert matched, f"no route matched {method.upper()} {url}"
        return matched[0]

    @classmethod
    def _registered_routes(
        cls, client: fastapi.testclient.TestClient
    ) -> dict[tuple[str, str], bool]:
        """Read the live route table and say which routes can name a path.

        Walks the router the application actually serves, so it sees what is
        registered rather than what the source appears to declare.
        """
        found: dict[tuple[str, str], bool] = {}

        def walk(router: object, prefix: str = "") -> None:
            for route in getattr(router, "routes", []):
                path = prefix + getattr(route, "path", "")
                if getattr(route, "endpoint", None) is None:
                    nested = getattr(route, "original_router", None) or getattr(
                        route, "app", None
                    )
                    if nested is not None:
                        walk(nested, path)
                    continue
                for method in getattr(route, "methods", None) or []:
                    if method not in {"HEAD", "OPTIONS"}:
                        found[(method, path)] = cls._names_a_path(route)

        walk(client.app)
        return {
            key: value
            for key, value in found.items()
            if "/schedules/pipelines" in key[1]
        }

    def test_the_route_table_matches_the_reviewed_snapshot(
        self, closed_tier_client: fastapi.testclient.TestClient
    ) -> None:
        """Any new scheduler route, or any change in how one is addressed, fails here.

        Be clear about what this does and does not do. It does NOT understand a
        new route or decide whether it is safe -- nothing in this file can. It
        fails, names the route, and makes someone extend the matrix below or
        record the route as an exception. That is a review trigger, not an
        invariant, and it is the honest replacement for a static analyzer that
        claimed to be the latter and was not.
        """
        assert self._registered_routes(closed_tier_client) == self._EXPECTED_ROUTES

    @pytest.mark.parametrize(
        ("shape", "names_a_path", "why"),
        [
            pytest.param("direct", True, "the ordinary case", id="direct-query-param"),
            pytest.param(
                "aliased",
                True,
                "the wire name, not the Python name",
                id="aliased-query-param",
            ),
            pytest.param(
                "nested",
                True,
                "pi-55's finding: parameter on a sub-dependant",
                id="nested-dependency",
            ),
            pytest.param(
                "deeper",
                True,
                "two levels, because one is not recursion",
                id="doubly-nested",
            ),
            pytest.param("body", True, "carried by a body model", id="body-model"),
            pytest.param(
                "none",
                False,
                "a route naming no path must not be swept in",
                id="no-path-anywhere",
            ),
        ],
    )
    def test_the_classification_sees_a_path_however_it_arrives(
        self, shape: str, names_a_path: bool, why: str
    ) -> None:
        """Built against real FastAPI, so it fails if the framework's shape changes.

        Synthetic rather than mutated into `api_routes`, because these are shapes
        the module does not currently use; the claim is that the classification
        would see them if it did.
        """
        app = fastapi.FastAPI()

        def by_path(schedule_path: str) -> str:
            return schedule_path

        def outer(inner: str = fastapi.Depends(by_path)) -> str:
            return inner

        if shape == "direct":

            @app.get("/r")
            def _r(schedule_path: str) -> None: ...

        elif shape == "aliased":

            @app.get("/r")
            def _r(
                sneaky: str = fastapi.Query(alias="schedule_path"),
            ) -> None: ...

        elif shape == "nested":

            @app.get("/r")
            def _r(resolved: str = fastapi.Depends(by_path)) -> None: ...

        elif shape == "deeper":

            @app.get("/r")
            def _r(resolved: str = fastapi.Depends(outer)) -> None: ...

        elif shape == "body":

            @app.post("/r")
            def _r(
                request: api_routes.PipelineScheduleUpdateRequest,
            ) -> None: ...

        else:

            @app.get("/r")
            def _r(other: str) -> None: ...

        route = next(r for r in app.routes if getattr(r, "path", None) == "/r")

        assert self._names_a_path(route) is names_a_path, why

    def test_every_path_surface_is_covered_by_the_matrix(
        self, closed_tier_client: fastapi.testclient.TestClient
    ) -> None:
        """Every route the snapshot calls path-addressed must be driven by this class.

        Without this the snapshot and the tests can drift apart silently: a route
        could be correctly recorded as path-addressed and still never be exercised
        under a closed tier. Five surfaces are driven by the zero-query matrix;
        PATCH-by-id is driven by its own test, for the reason given there.
        """
        addressed = {
            route
            for route, names_a_path in self._EXPECTED_ROUTES.items()
            if names_a_path
        }
        exercised = {
            self._route_for_request(
                closed_tier_client,
                case.method,
                case.url.format(schedule_id=_SAMPLE_SCHEDULE_ID),
            )
            for case in _PATH_CASES
        }

        assert addressed == exercised, addressed.symmetric_difference(exercised)

        # Route keys alone are too coarse. Both create cases answer on the same
        # (POST, /api/schedules/pipelines) key, so deleting the derived-path one
        # leaves the route set identical while the claim that a create DERIVING
        # a path is gated stops being tested at all. The variants are therefore
        # pinned by name.
        # Every rule value must be consumed by a test above. A case given an
        # unrecognised rule would otherwise sit in the registry, satisfy
        # coverage, and never be executed by anything.
        assert {case.rule for case in _PATH_CASES} == {"no-query", "no-write"}
        assert len(_NO_QUERY_CASES) + len(_NO_WRITE_CASES) == len(_PATH_CASES)

        assert {case.ident for case in _PATH_CASES} == {
            "create-with-explicit-path",
            "create-deriving-a-path",
            "list-by-path",
            "patch-by-path",
            "delete-by-path",
            "trigger-by-path",
            "patch-by-id-adopting-a-path",
        }

    @staticmethod
    def _seed(db_engine: sqlalchemy.Engine, *, name: str, path: str | None) -> str:
        """Insert a row, optionally carrying a path, bypassing the closed create.

        Survival has to be shown for rows that already HAVE a path, not only for
        legacy NULL-path ones. A guard like
        `if schedule.schedule_path is not None: require_path_tier()` sits after
        resolution and is invisible to a pathless fixture, which is the same
        conditional-after-resolution class as the absent-id version, one level
        later.
        """
        schedule_id = insert_pathless_schedule_row(db_engine=db_engine, name=name)
        if path is not None:
            with sqlalchemy.orm.Session(bind=db_engine) as session:
                session.execute(
                    sqlalchemy.update(db_models.ScheduledPipelineRun)
                    .where(db_models.ScheduledPipelineRun.id == schedule_id)
                    .values(schedule_path=path)
                )
                session.commit()
        return schedule_id

    @staticmethod
    @contextlib.contextmanager
    def _recording(db_engine: sqlalchemy.Engine):  # noqa: ANN205
        statements: list[str] = []

        def _record(
            conn, cursor, statement, parameters, context, executemany
        ):  # noqa: ANN001, ARG001
            if "scheduled_pipeline_run" in statement.lower():
                statements.append(" ".join(statement.split()))

        sqlalchemy.event.listen(db_engine, "before_cursor_execute", _record)
        try:
            yield statements
        finally:
            sqlalchemy.event.remove(db_engine, "before_cursor_execute", _record)

    @pytest.mark.parametrize("case", _NO_QUERY_CASES, ids=lambda case: case.ident)
    def test_the_refusal_costs_no_query(
        self,
        closed_tier_client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
        case: _PathCase,
    ) -> None:
        with self._recording(db_engine) as statements:
            resp = getattr(closed_tier_client, case.method)(
                case.url,
                **({"json": case.payload} if case.payload is not None else {}),
            )

        assert resp.status_code == status.HTTP_503_SERVICE_UNAVAILABLE, resp.text
        assert (
            resp.json()["detail"]["code"] == api_routes.SCHEDULE_PATH_WRITES_UNAVAILABLE
        )
        assert statements == [], statements

    def test_the_recorder_would_have_seen_a_query(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The control that stops the assertion above passing vacuously.

        `assert statements == []` is satisfied just as well by a listener that
        never fires. With the tier OPEN the identical route must record at least
        one statement against the table.
        """
        with self._recording(db_engine) as statements:
            resp = client.get(
                "/api/schedules/pipelines",
                params={"schedule_path": "upi/nothing-here"},
            )

        assert resp.status_code == status.HTTP_200_OK
        assert statements != []

    @pytest.mark.parametrize("case", _NO_WRITE_CASES, ids=lambda case: case.ident)
    def test_adoption_by_id_refuses_before_writing_the_path(
        self,
        closed_tier_client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
        case: _PathCase,
    ) -> None:
        """The one path surface that is allowed to query first, and why.

        PATCH-by-id cannot know it is adopting a path until it has read the row:
        the request is only an adoption if the stored path is currently NULL. So
        the rule the other surfaces obey -- refuse having asked the database
        nothing -- is the wrong rule here, and asserting it would be asserting
        something false. Demanding it would force the handler to gate every
        id-addressed edit, which is exactly the operator lockout the closed tier
        is designed to avoid.

        What must hold is that it reads, refuses, and writes nothing. The row is
        inserted directly because create is unavailable under a closed tier.
        """
        schedule_id = self._seed(db_engine, name="Legacy", path=None)

        with self._recording(db_engine) as statements:
            resp = getattr(closed_tier_client, case.method)(
                case.url.format(schedule_id=schedule_id),
                **({"json": case.payload} if case.payload is not None else {}),
            )

        assert resp.status_code == status.HTTP_503_SERVICE_UNAVAILABLE, resp.text
        assert (
            resp.json()["detail"]["code"] == api_routes.SCHEDULE_PATH_WRITES_UNAVAILABLE
        )
        writes = [
            statement
            for statement in statements
            if statement.upper().startswith(("UPDATE", "INSERT", "DELETE"))
        ]
        assert writes == [], writes

    def test_the_write_recorder_would_have_seen_a_write(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Non-vacuity for the assertion above.

        `writes == []` passes just as well if the filter never matches anything.
        With the tier OPEN the identical adoption must record a write.
        """
        schedule_id = insert_pathless_schedule_row(db_engine=db_engine, name="Legacy")

        with self._recording(db_engine) as statements:
            resp = client.patch(
                f"/api/schedules/pipelines/{schedule_id}",
                json={"schedule_path": "upi/adopted"},
            )

        assert resp.status_code == status.HTTP_200_OK, resp.text
        assert [
            s for s in statements if s.upper().startswith("UPDATE")
        ] != [], statements

    def test_a_same_path_replay_is_not_refused(
        self,
        closed_tier_client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Restating the path a row already has is not an adoption, so no gate.

        This pins the boundary the PATCH-by-id rationale rests on. The handler
        must READ the row and then decide: adoption only happens where the
        stored path is NULL. A gate placed on `request.schedule_path is not
        None` before the lookup would satisfy every other test in this class and
        still refuse this request -- turning a documented no-op replay, and any
        edit that merely carries the current path alongside a rename, into a 503
        for an operator who changed nothing about the path.
        """
        path = "upi/already-there"
        schedule_id = insert_pathless_schedule_row(db_engine=db_engine, name="Legacy")
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            session.execute(
                sqlalchemy.update(db_models.ScheduledPipelineRun)
                .where(db_models.ScheduledPipelineRun.id == schedule_id)
                .values(schedule_path=path)
            )
            session.commit()

        resp = closed_tier_client.patch(
            f"/api/schedules/pipelines/{schedule_id}",
            json={"schedule_path": path, "name": "Renamed"},
        )

        assert resp.status_code == status.HTTP_200_OK, resp.text
        assert resp.json()["name"] == "Renamed"
        assert resp.json()["schedule_path"] == path

    def test_changing_an_existing_path_is_still_refused_as_a_conflict(
        self,
        closed_tier_client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The other half of the boundary: a real change is not quietly allowed.

        A row that already has a path cannot be repathed, and that refusal is a
        422 decided after the read -- not the tier's 503. Pinning both sides
        stops the previous test being satisfied by a handler that simply stopped
        looking at `schedule_path` altogether.
        """
        schedule_id = insert_pathless_schedule_row(db_engine=db_engine, name="Legacy")
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            session.execute(
                sqlalchemy.update(db_models.ScheduledPipelineRun)
                .where(db_models.ScheduledPipelineRun.id == schedule_id)
                .values(schedule_path="upi/original")
            )
            session.commit()

        resp = closed_tier_client.patch(
            f"/api/schedules/pipelines/{schedule_id}",
            json={"schedule_path": "upi/different"},
        )

        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT, resp.text
        # Not merely the status: FastAPI answers 422 for request validation too,
        # with a list of field errors. Asserting the code alone would let this
        # test pass on a malformed request that never reached the contract at
        # all -- the same vacuity that made the old survival matrix worthless.
        assert "cannot be changed" in resp.json()["detail"]
        assert "upi/original" in resp.json()["detail"]

    @pytest.mark.parametrize(
        "path", [None, "upi/existing"], ids=["legacy-pathless", "path-bearing"]
    )
    def test_an_operator_can_still_read_a_schedule_by_id(
        self,
        closed_tier_client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
        path: str | None,
    ) -> None:
        """Reading a real row by id must return that row, not merely avoid a 503.

        The earlier version of this asked for an absent id and accepted any
        status that was not the path-tier refusal. That passes if the handler
        404s everything, and it passes if a gate is added after the row is
        resolved and quietly hides the data. Seeding a row and asserting its
        identity comes back is what makes the survival claim mean anything.
        """
        schedule_id = self._seed(db_engine, name="Legacy", path=path)

        resp = closed_tier_client.get(f"/api/schedules/pipelines/{schedule_id}")

        assert resp.status_code == status.HTTP_200_OK, resp.text
        assert resp.json()["id"] == schedule_id
        assert resp.json()["name"] == "Legacy"
        assert resp.json()["schedule_path"] == path

    def test_an_operator_can_still_list_schedules(
        self,
        closed_tier_client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """The unfiltered list must contain the seeded row.

        Asserting 200 on an empty database proves nothing: a handler that
        returns an empty page under a closed tier would pass. Inspectability is
        the entire justification for leaving this surface open, so the row has
        to actually appear.
        """
        pathless = self._seed(db_engine, name="Legacy", path=None)
        pathed = self._seed(db_engine, name="Modern", path="upi/existing")

        resp = closed_tier_client.get("/api/schedules/pipelines")

        assert resp.status_code == status.HTTP_200_OK, resp.text
        listed = [item["id"] for item in resp.json()["schedules"]]
        # BOTH, so a list quietly narrowed to NULL-path rows fails here.
        assert {pathless, pathed} <= set(listed), listed

    @pytest.mark.parametrize(
        "path", [None, "upi/existing"], ids=["legacy-pathless", "path-bearing"]
    )
    def test_an_operator_can_still_trigger_a_schedule_by_id(
        self,
        closed_tier_client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
        path: str | None,
    ) -> None:
        """Triggering by id must reach the executor, not just avoid a 503."""
        schedule_id = self._seed(db_engine, name="Legacy", path=path)
        fake_run = api_server_sql.PipelineRunResponse(
            id="run-123", root_execution_id="exec-123"
        )

        with mock.patch.object(
            executor, "execute_pipeline_schedule", return_value=fake_run
        ) as execute:
            resp = closed_tier_client.post(
                f"/api/schedules/pipelines/{schedule_id}/trigger"
            )

        assert resp.status_code == status.HTTP_200_OK, resp.text
        assert execute.call_count == 1

    @pytest.mark.parametrize(
        "path", [None, "upi/existing"], ids=["legacy-pathless", "path-bearing"]
    )
    def test_an_operator_can_still_delete_a_schedule_by_id(
        self,
        closed_tier_client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
        path: str | None,
    ) -> None:
        """Deleting by id must actually remove the row.

        This is the runaway-schedule escape hatch. A handler that answers 204
        and deletes nothing would satisfy a status-only assertion.
        """
        schedule_id = self._seed(db_engine, name="Legacy", path=path)

        resp = closed_tier_client.delete(f"/api/schedules/pipelines/{schedule_id}")

        assert resp.status_code == status.HTTP_204_NO_CONTENT, resp.text
        assert closed_tier_client.get(
            f"/api/schedules/pipelines/{schedule_id}"
        ).status_code == (status.HTTP_404_NOT_FOUND)


#: A root task declaring the inputs the templates below address. Templates are never
#: validated against these -- the declaration is here so the fixture reads as a pipeline
#: someone would really template, not because a key has to match one.
TEMPLATABLE_TASK_SPEC = {
    "componentRef": {
        "name": "pl_abc123",
        "spec": {
            "name": "test-pipeline",
            "inputs": [{"name": "as_of_date"}, {"name": "region"}],
            "implementation": {"graph": {"tasks": {}}},
        },
    },
}


class TestPipelineTemplatesOnSchedules:
    """The HTTP boundary for the `pipeline_templates` envelope.

    What each rung rejects is tested in tests/templating/arguments/test_envelopes.py.
    What is tested here is the wiring: that a rejection is a 422 with the ladder's own
    message, that an acceptance is stored, and that it survives a round trip.
    """

    def _create(
        self,
        client: fastapi.testclient.TestClient,
        *,
        path: str,
        templates: dict[str, object] | None,
    ) -> httpx.Response:
        body: dict[str, object] = {
            "schedule_path": path,
            "name": path,
            "pipeline_task_spec": TEMPLATABLE_TASK_SPEC,
            "cron_expression": "0 8 * * *",
        }
        if templates is not None:
            body["pipeline_templates"] = templates
        return client.post("/api/schedules/pipelines", json=body)

    def test_a_schedule_created_without_the_field_reports_no_templates(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The envelope is always emitted, so a client can tell "none" from an older
        server that does not know the field."""
        resp = self._create(client, path="tpl/none", templates=None)

        assert resp.status_code == status.HTTP_201_CREATED
        assert resp.json()["pipeline_templates"] == {"arguments": {}}

    def test_templates_survive_a_create_and_a_read(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        templates = {
            "arguments": {
                "as_of_date": "{{ schedule_time | shift('-1d') | date }}",
                "region": "ca-central-1",
            }
        }

        created = self._create(client, path="tpl/roundtrip", templates=templates)
        assert created.status_code == status.HTTP_201_CREATED
        fetched = client.get(f"/api/schedules/pipelines/{created.json()['id']}")

        assert created.json()["pipeline_templates"] == templates
        assert fetched.json()["pipeline_templates"] == templates

    def test_schedule_time_is_accepted_because_a_schedule_is_saved_as_cron(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The kind is a property of the row, not of any one fire. A schedule fired by
        hand has no schedule_time, and `coalesce` is how a template survives that -- which is why
        this is accepted here and refused on a subscription."""
        resp = self._create(
            client,
            path="tpl/scheduled",
            templates={"arguments": {"as_of_date": "{{ schedule_time | date }}"}},
        )

        assert resp.status_code == status.HTTP_201_CREATED

    def test_an_unknown_sibling_key_inside_the_envelope_is_ignored(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Rollout tolerance: a newer client may send a key this server has never heard
        of. The leniency is inside the envelope only."""
        resp = self._create(
            client,
            path="tpl/sibling",
            templates={
                "arguments": {"region": "ca"},
                "invented_later": {"a": 1},
            },
        )

        assert resp.status_code == status.HTTP_201_CREATED
        assert resp.json()["pipeline_templates"] == {"arguments": {"region": "ca"}}

    def test_a_key_no_input_declares_is_stored_rather_than_refused(
        self,
        client: fastapi.testclient.TestClient,
    ) -> None:
        """A key matching no declared input is accepted and read back.

        `TEMPLATABLE_TASK_SPEC` declares `as_of_date` and `region`; `not_an_input` is
        neither, and is stored anyway. Run submission refuses an argument for an
        undeclared input when the run is submitted, and that is the only check that can
        answer for a referenced pipeline as well as an inline spec -- validating here
        would reject on one schedule source what it waves through on another.
        """
        resp = self._create(
            client,
            path="tpl/undeclared",
            templates={"arguments": {"not_an_input": "{{ now | date }}"}},
        )
        assert resp.status_code == status.HTTP_201_CREATED, resp.text
        assert resp.json()["pipeline_templates"] == {
            "arguments": {"not_an_input": "{{ now | date }}"}
        }

    @pytest.mark.parametrize(
        ("templates", "expected"),
        [
            (
                {"arguments": [1, 2]},
                "must be a map of string to string; got list",
            ),
            (
                {"arguments": {"as_of_date": 3}},
                "must be a map of string to string; 'as_of_date' is int",
            ),
            (
                {"arguments": {"as_of_date": "{{ schedule_time"}},
                "Invalid template for 'as_of_date'",
            ),
            (
                {"arguments": {"as_of_date": "{{ now | upper }}"}},
                "unknown filter 'upper'",
            ),
        ],
        ids=[
            "arguments not an object",
            "value not a string",
            "unparseable",
            "unknown filter",
        ],
    )
    def test_a_bad_envelope_is_a_422_carrying_the_ladders_own_message(
        self,
        client: fastapi.testclient.TestClient,
        templates: dict[str, object],
        expected: str,
    ) -> None:
        """422 rather than 400, matching the cron and task-spec rejections already on
        this endpoint."""
        resp = self._create(client, path="tpl/bad", templates=templates)

        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert expected in resp.json()["detail"]

    def test_nothing_is_written_when_the_envelope_is_refused(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The ladder runs before any persistence, so a rejected create leaves no row."""
        self._create(
            client,
            path="tpl/norow",
            templates={"arguments": {"as_of_date": "{{ x }}"}},
        )

        listed = client.get("/api/schedules/pipelines").json()["schedules"]

        assert [s for s in listed if s["name"] == "tpl/norow"] == []

    def test_a_patch_replaces_the_stored_templates(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        created = self._create(
            client,
            path="tpl/patch",
            templates={"arguments": {"region": "ca-central-1"}},
        )

        patched = client.patch(
            f"/api/schedules/pipelines/{created.json()['id']}",
            json={"pipeline_templates": {"arguments": {"region": "us-east-1"}}},
        )

        assert patched.json()["pipeline_templates"] == {
            "arguments": {"region": "us-east-1"}
        }

    def test_a_patch_that_omits_the_field_leaves_the_templates_alone(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Omission and `{}` are different intents, which is why the field is nullable."""
        created = self._create(
            client,
            path="tpl/keep",
            templates={"arguments": {"region": "ca-central-1"}},
        )

        patched = client.patch(
            f"/api/schedules/pipelines/{created.json()['id']}",
            json={"paused": True},
        )

        assert patched.json()["pipeline_templates"] == {
            "arguments": {"region": "ca-central-1"}
        }

    @pytest.mark.parametrize(
        "envelope",
        [{}, {"future_key": 1}],
        ids=["empty-envelope", "unknown-sibling-only"],
    )
    def test_an_envelope_naming_no_arguments_does_not_delete_them(
        self, client: fastapi.testclient.TestClient, envelope: dict[str, object]
    ) -> None:
        """Forward compatibility must not cost data. Unknown keys are ignored so a newer
        client can talk to this server, and ignoring them used to reduce the envelope to
        an empty map -- which is the clear instruction, answered 200."""
        created = self._create(
            client,
            path="tpl/forward",
            templates={"arguments": {"region": "ca-central-1"}},
        )

        patched = client.patch(
            f"/api/schedules/pipelines/{created.json()['id']}",
            json={"pipeline_templates": envelope},
        )

        assert patched.status_code == status.HTTP_200_OK, patched.text
        assert patched.json()["pipeline_templates"] == {
            "arguments": {"region": "ca-central-1"}
        }

    @pytest.mark.parametrize(
        "value", [[], "", 0], ids=["empty-list", "empty-string", "zero"]
    )
    def test_a_falsy_arguments_value_is_refused_and_keeps_the_templates(
        self, client: fastapi.testclient.TestClient, value: object
    ) -> None:
        """A malformed body must not read as an empty map, which would clear the row."""
        created = self._create(
            client,
            path="tpl/falsy",
            templates={"arguments": {"region": "ca-central-1"}},
        )

        patched = client.patch(
            f"/api/schedules/pipelines/{created.json()['id']}",
            json={"pipeline_templates": {"arguments": value}},
        )

        assert patched.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        read = client.get(f"/api/schedules/pipelines/{created.json()['id']}")
        assert read.json()["pipeline_templates"] == {
            "arguments": {"region": "ca-central-1"}
        }

    def test_an_empty_envelope_clears_the_templates(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The only way to remove one. Section 1: the key is dropped rather than stored
        as `{}`, so a cleared row is indistinguishable from one that never had any."""
        created = self._create(
            client,
            path="tpl/clear",
            templates={"arguments": {"region": "ca-central-1"}},
        )

        patched = client.patch(
            f"/api/schedules/pipelines/{created.json()['id']}",
            json={"pipeline_templates": {"arguments": {}}},
        )

        assert patched.json()["pipeline_templates"] == {"arguments": {}}

    def test_a_patch_with_a_bad_template_is_refused_and_changes_nothing(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        created = self._create(
            client,
            path="tpl/badpatch",
            templates={"arguments": {"region": "ca"}},
        )
        schedule_id = created.json()["id"]

        refused = client.patch(
            f"/api/schedules/pipelines/{schedule_id}",
            json={"pipeline_templates": {"arguments": {"region": "{{ nope }}"}}},
        )
        after = client.get(f"/api/schedules/pipelines/{schedule_id}")

        assert refused.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert after.json()["pipeline_templates"] == {"arguments": {"region": "ca"}}


class TestTheManualTriggerEndpointRenders:
    """The fifth caller: POST /trigger bypasses APScheduler, so it has no schedule_time.

    It reaches the same executor as a scheduled fire, which is why a template rooted in
    `now` must still render here while one rooted in `schedule_time` must not.
    """

    MANUAL_SPEC: typing.ClassVar[dict[str, object]] = {
        "componentRef": {
            "name": "pl_manual",
            "spec": {
                "name": "manual-templated",
                "inputs": [
                    {"name": "as_of_date", "optional": True},
                    {"name": "region", "optional": True},
                ],
                "implementation": {"graph": {"tasks": {}}},
            },
        },
    }

    def _schedule(
        self,
        client: fastapi.testclient.TestClient,
        *,
        templates: dict[str, str],
        path: str,
    ) -> str:
        resp = client.post(
            "/api/schedules/pipelines",
            json={
                "schedule_path": path,
                "name": path,
                "pipeline_task_spec": self.MANUAL_SPEC,
                "cron_expression": "0 8 * * *",
                "pipeline_templates": {"arguments": templates},
            },
        )
        assert resp.status_code == 201, resp.text
        return resp.json()["id"]

    @staticmethod
    def _arguments_of(
        db_engine: sqlalchemy.Engine, *, response: httpx.Response
    ) -> dict[str, object]:
        """The created run's root-task arguments. The trigger response carries ids only."""
        run_id = response.json()["pipeline_run_response"]["id"]
        with sqlalchemy.orm.Session(bind=db_engine) as session:
            run = session.get(bts.PipelineRun, run_id)
            assert run is not None
            return dict(run.root_execution.task_spec.get("arguments") or {})

    def test_a_hand_fired_schedule_renders_a_now_template(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """A manual fire has a trigger time even though it has no scheduled time."""
        schedule_id = self._schedule(
            client, templates={"region": "ca"}, path="manual/constant"
        )

        resp = client.post(f"/api/schedules/pipelines/{schedule_id}/trigger")

        assert resp.status_code == 200, resp.text
        assert self._arguments_of(db_engine, response=resp) == {"region": "ca"}

    def test_a_hand_fired_schedule_drops_a_schedule_time_template(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Nothing invents a schedule_time for a fire APScheduler never dispatched."""
        schedule_id = self._schedule(
            client,
            templates={
                "as_of_date": "{{ schedule_time | date }}",
                "region": "ca",
            },
            path="manual/schedule-time",
        )

        resp = client.post(f"/api/schedules/pipelines/{schedule_id}/trigger")

        assert resp.status_code == 200, resp.text
        assert self._arguments_of(db_engine, response=resp) == {"region": "ca"}
