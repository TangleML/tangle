import datetime

import fastapi
from fastapi import testclient
import pytest

from cloud_pipelines_backend import api_router
from cloud_pipelines_backend import database_ops

ADMIN_USER_NAME = "admin user"
NON_ADMIN_USER_NAME = "regular user"

ACTIVE_BANNERS_URL = "/api/banners/active"
ADMIN_BANNERS_URL = "/api/admin/banners"


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

    api_router.setup_routes(
        app=app,
        db_engine=db_engine,
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


def _make_banner_request(**overrides) -> dict:
    banner = {
        "title": "Scheduled maintenance",
        "body": "The service will be unavailable for 10 minutes.",
        "variant": "warning",
    }
    banner.update(overrides)
    return banner


def _create_banner(api: _TestApi, **overrides) -> dict:
    response = api.client.post(
        ADMIN_BANNERS_URL, json=_make_banner_request(**overrides)
    )
    assert response.status_code == 200, response.text
    return response.json()


def _get_active_banners(api: _TestApi) -> list[dict]:
    response = api.client.get(ACTIVE_BANNERS_URL)
    assert response.status_code == 200, response.text
    return response.json()["banners"]


def test_active_banners_are_empty_by_default(api: _TestApi):
    response = api.client.get(ACTIVE_BANNERS_URL)
    assert response.status_code == 200, response.text
    assert response.json() == {"banners": []}
    assert response.headers["Cache-Control"] == "no-store"


def test_admin_can_create_banner(api: _TestApi):
    starts_at = _get_current_time() - datetime.timedelta(hours=1)
    banner = _create_banner(
        api,
        action_url="https://example.com/status",
        action_text="View details",
        starts_at=starts_at.isoformat(),
        is_dismissible=True,
    )
    assert banner["id"]
    assert banner["title"] == "Scheduled maintenance"
    assert banner["body"] == "The service will be unavailable for 10 minutes."
    assert banner["variant"] == "warning"
    assert banner["action_url"] == "https://example.com/status"
    assert banner["action_text"] == "View details"
    assert _parse_datetime(banner["starts_at"]) == starts_at
    assert banner["ends_at"] is None
    assert banner["is_enabled"] == True
    assert banner["is_dismissible"] == True
    assert banner["deleted_at"] is None
    assert banner["created_by"] == ADMIN_USER_NAME
    assert banner["updated_by"] == ADMIN_USER_NAME
    assert _parse_datetime(banner["created_at"])
    assert _parse_datetime(banner["updated_at"])

    get_response = api.client.get(f"{ADMIN_BANNERS_URL}/{banner['id']}")
    assert get_response.status_code == 200, get_response.text
    assert get_response.json() == banner

    list_response = api.client.get(ADMIN_BANNERS_URL)
    assert list_response.status_code == 200, list_response.text
    assert list_response.json() == {"banners": [banner]}


def test_active_banners_include_enabled_banner_in_window(api: _TestApi):
    current_time = _get_current_time()
    created_banner = _create_banner(
        api,
        starts_at=(current_time - datetime.timedelta(hours=1)).isoformat(),
        ends_at=(current_time + datetime.timedelta(hours=1)).isoformat(),
    )
    active_banners = _get_active_banners(api)
    assert len(active_banners) == 1
    active_banner = active_banners[0]
    assert active_banner["id"] == created_banner["id"]
    # The public response must not expose the admin-only fields.
    assert set(active_banner) == {
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


def test_active_banners_exclude_disabled_banner(api: _TestApi):
    _create_banner(api, is_enabled=False)
    assert _get_active_banners(api) == []


def test_active_banners_exclude_future_banner(api: _TestApi):
    starts_at = _get_current_time() + datetime.timedelta(hours=1)
    _create_banner(api, starts_at=starts_at.isoformat())
    assert _get_active_banners(api) == []


def test_active_banners_exclude_expired_banner(api: _TestApi):
    current_time = _get_current_time()
    _create_banner(
        api,
        starts_at=(current_time - datetime.timedelta(hours=2)).isoformat(),
        ends_at=(current_time - datetime.timedelta(hours=1)).isoformat(),
    )
    assert _get_active_banners(api) == []


def test_patch_updates_fields_and_updated_at(api: _TestApi):
    banner = _create_banner(
        api, action_url="https://example.com/status", action_text="Details"
    )
    ends_at = _get_current_time() + datetime.timedelta(hours=1)
    response = api.client.patch(
        f"{ADMIN_BANNERS_URL}/{banner['id']}",
        json={
            "title": "  Updated title  ",
            "variant": "info",
            "is_enabled": False,
            "ends_at": ends_at.isoformat(),
        },
    )
    assert response.status_code == 200, response.text
    updated_banner = response.json()
    assert updated_banner["title"] == "Updated title"
    assert updated_banner["variant"] == "info"
    assert updated_banner["is_enabled"] == False
    assert _parse_datetime(updated_banner["ends_at"]) == ends_at
    assert updated_banner["body"] == banner["body"]
    assert updated_banner["action_url"] == banner["action_url"]
    assert updated_banner["action_text"] == banner["action_text"]
    assert updated_banner["is_dismissible"] == banner["is_dismissible"]
    assert updated_banner["created_at"] == banner["created_at"]
    # Not `>`: MySQL `DATETIME` has second precision, so two writes within the same
    # second get the same timestamp (SQLite keeps microseconds and would hide that).
    assert _parse_datetime(updated_banner["updated_at"]) >= _parse_datetime(
        banner["updated_at"]
    )


def test_delete_soft_deletes_banner(api: _TestApi):
    banner = _create_banner(api)
    assert len(_get_active_banners(api)) == 1

    response = api.client.delete(f"{ADMIN_BANNERS_URL}/{banner['id']}")
    assert response.status_code == 200, response.text
    deleted_banner = response.json()
    assert _parse_datetime(deleted_banner["deleted_at"])
    assert deleted_banner["updated_by"] == ADMIN_USER_NAME

    assert _get_active_banners(api) == []
    list_response = api.client.get(ADMIN_BANNERS_URL)
    assert list_response.json() == {"banners": []}
    list_response_2 = api.client.get(
        ADMIN_BANNERS_URL, params={"include_deleted": True}
    )
    assert [b["id"] for b in list_response_2.json()["banners"]] == [banner["id"]]
    get_response = api.client.get(f"{ADMIN_BANNERS_URL}/{banner['id']}")
    assert get_response.status_code == 200, get_response.text
    assert get_response.json()["deleted_at"] == deleted_banner["deleted_at"]


def test_deleted_banner_cannot_be_updated(api: _TestApi):
    banner = _create_banner(api)
    assert api.client.delete(f"{ADMIN_BANNERS_URL}/{banner['id']}").status_code == 200

    response = api.client.patch(
        f"{ADMIN_BANNERS_URL}/{banner['id']}", json={"title": "New title"}
    )
    assert response.status_code == 422, response.text
    assert api.client.get(f"{ADMIN_BANNERS_URL}/{banner['id']}").json()["title"] == (
        banner["title"]
    )


def test_markdown_body_is_stored_verbatim(api: _TestApi):
    # The backend stores the body as opaque text: rendering it is up to the frontend.
    body = "See [the status page](https://status.example.com) for **updates**."
    banner = _create_banner(api, body=body)
    assert banner["body"] == body
    assert _get_active_banners(api)[0]["body"] == body


def test_non_admin_cannot_create_update_or_delete_banners(api: _TestApi):
    banner = _create_banner(api)
    api.become_non_admin()

    create_response = api.client.post(ADMIN_BANNERS_URL, json=_make_banner_request())
    assert create_response.status_code == 403, create_response.text

    update_response = api.client.patch(
        f"{ADMIN_BANNERS_URL}/{banner['id']}", json={"title": "New title"}
    )
    assert update_response.status_code == 403, update_response.text

    delete_response = api.client.delete(f"{ADMIN_BANNERS_URL}/{banner['id']}")
    assert delete_response.status_code == 403, delete_response.text

    list_response = api.client.get(ADMIN_BANNERS_URL)
    assert list_response.status_code == 403, list_response.text

    # Reading the active banners does not require admin permissions.
    assert len(_get_active_banners(api)) == 1


@pytest.mark.parametrize(
    "banner_overrides",
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
        {"title": "x" * 121},
        {"body": ""},
        {"body": "x" * 2001},
        # The URL text requires a URL.
        {"action_text": "View details"},
        {"action_text": "x" * 81, "action_url": "https://example.com"},
    ],
)
def test_invalid_banner_is_rejected(api: _TestApi, banner_overrides: dict):
    response = api.client.post(
        ADMIN_BANNERS_URL, json=_make_banner_request(**banner_overrides)
    )
    assert response.status_code == 422, response.text
    assert _get_active_banners(api) == []


@pytest.mark.parametrize("variant", ["critical", "", "WARNING", None])
def test_invalid_banner_variant_is_rejected(api: _TestApi, variant):
    response = api.client.post(
        ADMIN_BANNERS_URL, json=_make_banner_request(variant=variant)
    )
    assert response.status_code == 422, response.text
    assert _get_active_banners(api) == []


def test_invalid_banner_update_is_rejected(api: _TestApi):
    banner = _create_banner(
        api,
        starts_at="2026-01-01T00:00:00+00:00",
        action_url="https://example.com/status",
        action_text="View details",
    )
    banner_url = f"{ADMIN_BANNERS_URL}/{banner['id']}"

    for invalid_update in [
        {"variant": "critical"},
        {"action_url": "example.com"},
        {"title": " "},
        # Before the existing `starts_at`.
        {"ends_at": "2025-01-01T00:00:00+00:00"},
    ]:
        response = api.client.patch(banner_url, json=invalid_update)
        assert response.status_code == 422, f"{invalid_update=}: {response.text}"

    assert api.client.get(banner_url).json() == banner


def test_banner_datetimes_without_timezone_are_rejected(api: _TestApi):
    response = api.client.post(
        ADMIN_BANNERS_URL, json=_make_banner_request(starts_at="2026-01-01T12:00:00")
    )
    assert response.status_code == 422, response.text
    assert _get_active_banners(api) == []


def test_banner_datetimes_are_converted_to_utc(api: _TestApi):
    banner = _create_banner(
        api,
        starts_at="2026-01-01T12:00:00+02:00",
        ends_at="2026-01-01T12:00:00-05:00",
    )
    assert banner["starts_at"] == "2026-01-01T10:00:00Z"
    assert banner["ends_at"] == "2026-01-01T17:00:00Z"


def test_banner_not_found(api: _TestApi):
    assert api.client.get(f"{ADMIN_BANNERS_URL}/no-such-id").status_code == 404
    assert (
        api.client.patch(
            f"{ADMIN_BANNERS_URL}/no-such-id", json={"title": "New title"}
        ).status_code
        == 404
    )
    assert api.client.delete(f"{ADMIN_BANNERS_URL}/no-such-id").status_code == 404


def test_active_banners_are_sorted(api: _TestApi):
    current_time = _get_current_time()
    banner_without_start = _create_banner(api, title="No start time")
    banner_older = _create_banner(
        api,
        title="Older",
        starts_at=(current_time - datetime.timedelta(hours=2)).isoformat(),
    )
    banner_newer = _create_banner(
        api,
        title="Newer",
        starts_at=(current_time - datetime.timedelta(hours=1)).isoformat(),
    )
    # `starts_at` descending, with the banners without a start time last.
    assert [banner["id"] for banner in _get_active_banners(api)] == [
        banner_newer["id"],
        banner_older["id"],
        banner_without_start["id"],
    ]


if __name__ == "__main__":
    pytest.main()
