"""Notice API tests.

The notice routes are mounted with `notices.api_routes.setup_notice_routes` here
instead of being part of `api_router.setup_routes`. `setup_routes` is still
called, since it registers the exception handlers (404 / 422) and the lifespan
that creates the DB tables.
"""

# The `== True` / `== False` assertions are kept as they are upstream, so that the
# shape of this file stays easy to diff against the PR.
# ruff: noqa: E712

import datetime

import fastapi
import pytest
from cloud_pipelines_backend import api_router, database_ops
from cloud_pipelines_backend.notices import api_routes, service
from fastapi import testclient
from sqlalchemy import orm

ADMIN_USER_NAME = "admin user"
NON_ADMIN_USER_NAME = "regular user"

ACTIVE_NOTICES_URL = "/api/notices/active"
ADMIN_NOTICES_URL = "/api/admin/notices"


def _make_user_details(name: str, *, is_admin: bool) -> api_router.UserDetails:
    return api_router.UserDetails(
        name=name,
        permissions=api_router.Permissions(read=True, write=True, admin=is_admin),
    )


class _TestApi:
    """A test API client that can switch between an admin and a non-admin user."""

    def __init__(self, client: testclient.TestClient, current_user_details: dict):
        self.client = client
        self._current_user_details = current_user_details

    def become_non_admin(self):
        self._current_user_details["user_details"] = _make_user_details(
            NON_ADMIN_USER_NAME, is_admin=False
        )


@pytest.fixture(name="api")
def api_fixture():
    db_engine = database_ops.create_db_engine(database_uri="sqlite://")
    app = fastapi.FastAPI()
    current_user_details = {
        "user_details": _make_user_details(ADMIN_USER_NAME, is_admin=True)
    }

    def get_user_details():
        return current_user_details["user_details"]

    def get_session():
        with orm.Session(autocommit=False, autoflush=False, bind=db_engine) as session:
            yield session

    api_router.setup_routes(
        app=app,
        db_engine=db_engine,
        user_details_getter=get_user_details,
        do_skip_backfill=True,
    )
    api_routes.setup_notice_routes(
        app=app,
        get_session=get_session,
        user_details_getter=get_user_details,
    )
    # The context manager triggers the lifespan event that creates the DB tables.
    with testclient.TestClient(app) as client:
        yield _TestApi(client=client, current_user_details=current_user_details)


def _parse_datetime(value: str) -> datetime.datetime:
    # `datetime.fromisoformat` only supports the "Z" suffix since Python 3.11.
    return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))


def _get_current_time() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone.utc)


def _make_notice_request(**overrides) -> dict:
    notice = {
        "title": "Scheduled maintenance",
        "body": "The service will be unavailable for 10 minutes.",
        "variant": "warning",
    }
    notice.update(overrides)
    return notice


def _create_notice(api: _TestApi, **overrides) -> dict:
    response = api.client.post(
        ADMIN_NOTICES_URL, json=_make_notice_request(**overrides)
    )
    assert response.status_code == 200, response.text
    return response.json()


def _get_active_notices(api: _TestApi) -> list[dict]:
    response = api.client.get(ACTIVE_NOTICES_URL)
    assert response.status_code == 200, response.text
    return response.json()["notices"]


def test_active_notices_are_empty_by_default(api: _TestApi):
    response = api.client.get(ACTIVE_NOTICES_URL)
    assert response.status_code == 200, response.text
    assert response.json() == {"notices": []}
    assert response.headers["Cache-Control"] == "no-store"


def test_admin_can_create_notice(api: _TestApi):
    starts_at = _get_current_time() - datetime.timedelta(hours=1)
    notice = _create_notice(
        api,
        action_url="https://example.com/status",
        action_text="View details",
        starts_at=starts_at.isoformat(),
        is_dismissible=True,
    )
    assert notice["id"]
    assert notice["title"] == "Scheduled maintenance"
    assert notice["body"] == "The service will be unavailable for 10 minutes."
    assert notice["variant"] == "warning"
    assert notice["action_url"] == "https://example.com/status"
    assert notice["action_text"] == "View details"
    assert _parse_datetime(notice["starts_at"]) == starts_at
    assert notice["ends_at"] is None
    assert notice["is_enabled"] == True
    assert notice["is_dismissible"] == True
    assert notice["deleted_at"] is None
    assert notice["created_by"] == ADMIN_USER_NAME
    assert notice["updated_by"] == ADMIN_USER_NAME
    assert _parse_datetime(notice["created_at"])
    assert _parse_datetime(notice["updated_at"])

    get_response = api.client.get(f"{ADMIN_NOTICES_URL}/{notice['id']}")
    assert get_response.status_code == 200, get_response.text
    assert get_response.json() == notice

    list_response = api.client.get(ADMIN_NOTICES_URL)
    assert list_response.status_code == 200, list_response.text
    assert list_response.json() == {"notices": [notice]}


def test_active_notices_include_enabled_notice_in_window(api: _TestApi):
    current_time = _get_current_time()
    created_notice = _create_notice(
        api,
        starts_at=(current_time - datetime.timedelta(hours=1)).isoformat(),
        ends_at=(current_time + datetime.timedelta(hours=1)).isoformat(),
    )
    active_notices = _get_active_notices(api)
    assert len(active_notices) == 1
    active_notice = active_notices[0]
    assert active_notice["id"] == created_notice["id"]
    # The public response must not expose the admin-only fields.
    assert set(active_notice) == {
        "id",
        "title",
        "body",
        "variant",
        "action_url",
        "action_text",
        "starts_at",
        "ends_at",
        "is_dismissible",
        "created_at",
        "updated_at",
    }


def test_active_notices_exclude_disabled_notice(api: _TestApi):
    _create_notice(api, is_enabled=False)
    assert _get_active_notices(api) == []


def test_active_notices_exclude_future_notice(api: _TestApi):
    starts_at = _get_current_time() + datetime.timedelta(hours=1)
    _create_notice(api, starts_at=starts_at.isoformat())
    assert _get_active_notices(api) == []


def test_active_notices_exclude_expired_notice(api: _TestApi):
    current_time = _get_current_time()
    _create_notice(
        api,
        starts_at=(current_time - datetime.timedelta(hours=2)).isoformat(),
        ends_at=(current_time - datetime.timedelta(hours=1)).isoformat(),
    )
    assert _get_active_notices(api) == []


def test_patch_updates_fields_and_updated_at(api: _TestApi):
    notice = _create_notice(
        api, action_url="https://example.com/status", action_text="Details"
    )
    ends_at = _get_current_time() + datetime.timedelta(hours=1)
    response = api.client.patch(
        f"{ADMIN_NOTICES_URL}/{notice['id']}",
        json={
            "title": "  Updated title  ",
            "variant": "info",
            "is_enabled": False,
            "ends_at": ends_at.isoformat(),
        },
    )
    assert response.status_code == 200, response.text
    updated_notice = response.json()
    assert updated_notice["title"] == "Updated title"
    assert updated_notice["variant"] == "info"
    assert updated_notice["is_enabled"] == False
    assert _parse_datetime(updated_notice["ends_at"]) == ends_at
    assert updated_notice["body"] == notice["body"]
    assert updated_notice["action_url"] == notice["action_url"]
    assert updated_notice["action_text"] == notice["action_text"]
    assert updated_notice["is_dismissible"] == notice["is_dismissible"]
    assert updated_notice["created_at"] == notice["created_at"]
    # Not `>`: MySQL `DATETIME` has second precision, so two writes within the same
    # second get the same timestamp (SQLite keeps microseconds and would hide that).
    assert _parse_datetime(updated_notice["updated_at"]) >= _parse_datetime(
        notice["updated_at"]
    )


def test_delete_soft_deletes_notice(api: _TestApi):
    notice = _create_notice(api)
    assert len(_get_active_notices(api)) == 1

    response = api.client.delete(f"{ADMIN_NOTICES_URL}/{notice['id']}")
    assert response.status_code == 200, response.text
    deleted_notice = response.json()
    assert _parse_datetime(deleted_notice["deleted_at"])
    assert deleted_notice["updated_by"] == ADMIN_USER_NAME

    assert _get_active_notices(api) == []
    list_response = api.client.get(ADMIN_NOTICES_URL)
    assert list_response.json() == {"notices": []}
    list_response_2 = api.client.get(
        ADMIN_NOTICES_URL, params={"include_deleted": True}
    )
    assert [b["id"] for b in list_response_2.json()["notices"]] == [notice["id"]]
    get_response = api.client.get(f"{ADMIN_NOTICES_URL}/{notice['id']}")
    assert get_response.status_code == 200, get_response.text
    assert get_response.json()["deleted_at"] == deleted_notice["deleted_at"]


def test_deleted_notice_cannot_be_updated(api: _TestApi):
    notice = _create_notice(api)
    # The request is made outside the `assert` so that it still runs when the
    # assertions are stripped (`python -O`).
    delete_response = api.client.delete(f"{ADMIN_NOTICES_URL}/{notice['id']}")
    assert delete_response.status_code == 200, delete_response.text

    response = api.client.patch(
        f"{ADMIN_NOTICES_URL}/{notice['id']}", json={"title": "New title"}
    )
    assert response.status_code == 422, response.text
    get_response = api.client.get(f"{ADMIN_NOTICES_URL}/{notice['id']}")
    assert get_response.json()["title"] == notice["title"]


def test_markdown_body_is_stored_verbatim(api: _TestApi):
    # The backend stores the body as opaque text: rendering it is up to the frontend.
    body = "See [the status page](https://status.example.com) for **updates**."
    notice = _create_notice(api, body=body)
    assert notice["body"] == body
    assert _get_active_notices(api)[0]["body"] == body


def test_non_admin_cannot_create_update_or_delete_notices(api: _TestApi):
    notice = _create_notice(api)
    api.become_non_admin()

    create_response = api.client.post(ADMIN_NOTICES_URL, json=_make_notice_request())
    assert create_response.status_code == 403, create_response.text

    update_response = api.client.patch(
        f"{ADMIN_NOTICES_URL}/{notice['id']}", json={"title": "New title"}
    )
    assert update_response.status_code == 403, update_response.text

    delete_response = api.client.delete(f"{ADMIN_NOTICES_URL}/{notice['id']}")
    assert delete_response.status_code == 403, delete_response.text

    list_response = api.client.get(ADMIN_NOTICES_URL)
    assert list_response.status_code == 403, list_response.text

    # Reading the active notices does not require admin permissions.
    assert len(_get_active_notices(api)) == 1


@pytest.mark.parametrize(
    "notice_overrides",
    [
        {"action_url": "example.com"},
        {"action_url": "javascript:alert(1)"},
        {"action_url": "ftp://example.com"},
        {
            "starts_at": "2026-01-02T00:00:00+00:00",
            "ends_at": "2026-01-01T00:00:00+00:00",
        },
        {
            "starts_at": "2026-01-01T00:00:00+00:00",
            "ends_at": "2026-01-01T00:00:00+00:00",
        },
        {"title": "   "},
        # Derived from the limits instead of hard-coded, so that changing a limit
        # does not silently turn these into "valid input" cases.
        {"title": "x" * (service.MAX_NOTICE_TITLE_LENGTH + 1)},
        {"body": ""},
        {"body": "x" * (service.MAX_NOTICE_BODY_LENGTH + 1)},
        # The URL text requires a URL.
        {"action_text": "View details"},
        {
            "action_text": "x" * (service.MAX_NOTICE_ACTION_TEXT_LENGTH + 1),
            "action_url": "https://example.com",
        },
    ],
)
def test_invalid_notice_is_rejected(api: _TestApi, notice_overrides: dict):
    response = api.client.post(
        ADMIN_NOTICES_URL, json=_make_notice_request(**notice_overrides)
    )
    assert response.status_code == 422, response.text
    assert _get_active_notices(api) == []


@pytest.mark.parametrize("variant", ["critical", "", "WARNING", None])
def test_invalid_notice_variant_is_rejected(api: _TestApi, variant):
    response = api.client.post(
        ADMIN_NOTICES_URL, json=_make_notice_request(variant=variant)
    )
    assert response.status_code == 422, response.text
    assert _get_active_notices(api) == []


def test_invalid_notice_update_is_rejected(api: _TestApi):
    notice = _create_notice(
        api,
        starts_at="2026-01-01T00:00:00+00:00",
        action_url="https://example.com/status",
        action_text="View details",
    )
    notice_url = f"{ADMIN_NOTICES_URL}/{notice['id']}"

    for invalid_update in [
        {"variant": "critical"},
        {"action_url": "example.com"},
        {"title": " "},
        # Before the existing `starts_at`.
        {"ends_at": "2025-01-01T00:00:00+00:00"},
    ]:
        response = api.client.patch(notice_url, json=invalid_update)
        assert response.status_code == 422, f"{invalid_update=}: {response.text}"

    get_response = api.client.get(notice_url)
    assert get_response.json() == notice


def test_notice_datetimes_without_timezone_are_rejected(api: _TestApi):
    response = api.client.post(
        ADMIN_NOTICES_URL,
        json=_make_notice_request(starts_at="2026-01-01T12:00:00"),
    )
    assert response.status_code == 422, response.text
    assert _get_active_notices(api) == []


def test_notice_datetimes_are_converted_to_utc(api: _TestApi):
    notice = _create_notice(
        api,
        starts_at="2026-01-01T12:00:00+02:00",
        ends_at="2026-01-01T12:00:00-05:00",
    )
    assert notice["starts_at"] == "2026-01-01T10:00:00Z"
    assert notice["ends_at"] == "2026-01-01T17:00:00Z"


def test_notice_not_found(api: _TestApi):
    # The requests are made outside the `assert`s so that they still run when the
    # assertions are stripped (`python -O`).
    notice_url = f"{ADMIN_NOTICES_URL}/no-such-id"
    get_response = api.client.get(notice_url)
    assert get_response.status_code == 404, get_response.text

    patch_response = api.client.patch(notice_url, json={"title": "New title"})
    assert patch_response.status_code == 404, patch_response.text

    delete_response = api.client.delete(notice_url)
    assert delete_response.status_code == 404, delete_response.text


def test_active_notices_are_sorted(api: _TestApi):
    current_time = _get_current_time()
    notice_without_start = _create_notice(api, title="No start time")
    notice_older = _create_notice(
        api,
        title="Older",
        starts_at=(current_time - datetime.timedelta(hours=2)).isoformat(),
    )
    notice_newer = _create_notice(
        api,
        title="Newer",
        starts_at=(current_time - datetime.timedelta(hours=1)).isoformat(),
    )
    # `starts_at` descending, with the notices without a start time last.
    assert [notice["id"] for notice in _get_active_notices(api)] == [
        notice_newer["id"],
        notice_older["id"],
        notice_without_start["id"],
    ]


if __name__ == "__main__":
    pytest.main()
