"""Route-level tests for the trigger configuration API, driven through a real TestClient.

Named test_trigger_api_routes so the module basename stays unique across the suite: the repo
has no __init__.py in tests and pytest runs in the default prepend import mode.
"""

import datetime
from typing import Any

import fastapi.testclient
import httpx
import pytest
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend import backend_types_sql as bts
from tests.triggers.conftest import (
    ADMIN_USER,
    DEFAULT_USER,
    DELETED_PIPELINE_ID,
    FULL_PIPELINE_ID,
    OTHER_PINNABLE_VERSION,
    OTHER_USER,
    OTHER_USER_PIPELINE_ID,
    PINNABLE_VERSION,
    SEEDED_PIPELINE_ID,
    UNKNOWN_VERSION,
)
from cloud_pipelines_backend.triggers import api_routes, db_models
from cloud_pipelines_backend.user_pipelines import db_models as user_pipeline_db_models
from cloud_pipelines_backend.utils import db as db_utils

_PATH = "/api/triggers/subscriptions"
_LOOKUP = f"{_PATH}/lookup"
# The width of `trigger_event_state.event_name`, which is what the request models cap against.
_MAX_NAME_LENGTH = api_routes._MAX_NAME_LENGTH
_MAX_EXPIRE_SECONDS = api_routes._MAX_EXPIRE_SECONDS


def _payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "name": "nightly-retrain",
        "condition": {
            "op": "all",
            "children": [{"event": "a"}, {"event": "b"}],
        },
        "pipeline_task_spec_from_user_pipeline_id": SEEDED_PIPELINE_ID,
    }
    payload.update(overrides)
    return payload


def _nested(*, depth: int) -> dict[str, Any]:
    """A chain of single-child `all` branches `depth` levels tall, a leaf at the bottom."""
    condition: dict[str, Any] = {"event": "a"}
    for _ in range(depth - 1):
        condition = {"op": "all", "children": [condition]}
    return condition


def _their_payload(**overrides: Any) -> dict[str, Any]:
    """The same request, aimed at a pipeline `other_user_client` actually owns.

    A subscription may only target a live pipeline of the caller's, so a test about *anything
    else* -- who may create, how names are scoped -- has to name the right target or it fails
    on a 404 that is not what it is asking about.
    """
    return _payload(
        pipeline_task_spec_from_user_pipeline_id=OTHER_USER_PIPELINE_ID,
        **overrides,
    )


class TestCreate:
    def test_returns_201_and_the_created_subscription(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        response = client.post(_PATH, json=_payload())
        assert response.status_code == 201, response.text
        body = response.json()
        assert body["name"] == "nightly-retrain"
        assert body["condition"] == _payload()["condition"]
        assert body["enabled"] is True
        assert body["cycle"] == 0
        assert body["id"]

    def test_the_target_pipeline_is_stored_and_echoed(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """The response is checked against the row, not just against the request.

        A route that echoed the posted value back without storing it would satisfy a
        response-only assertion, and the subscription would fire with nothing to start.
        """
        body = client.post(_PATH, json=_payload()).json()
        assert body["pipeline_task_spec_from_user_pipeline_id"] == SEEDED_PIPELINE_ID
        stored = session.get(db_models.TriggerSubscription, body["id"])
        assert stored is not None
        assert stored.pipeline_task_spec_from_user_pipeline_id == SEEDED_PIPELINE_ID

    def test_a_request_without_a_pipeline_is_rejected(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """422 at the boundary, not an IntegrityError at the insert.

        The column is NOT NULL, so a body with no target could only ever end as a 500. This
        is the request model saying so first, and it is what a default on the field would
        quietly undo.
        """
        payload = _payload()
        del payload["pipeline_task_spec_from_user_pipeline_id"]
        response = client.post(_PATH, json=payload)
        assert response.status_code == 422, response.text
        locations = [error["loc"] for error in response.json()["detail"]]
        assert ["body", "pipeline_task_spec_from_user_pipeline_id"] in locations

    def test_created_by_is_the_authenticated_caller(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        response = client.post(_PATH, json=_payload())
        assert response.json()["created_by"] == DEFAULT_USER

    def test_a_client_supplied_created_by_is_rejected(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """422 rather than silently ignored, so a caller is never told a lie about ownership."""
        response = client.post(
            _PATH, json=_payload(created_by="someone-else@example.com")
        )
        assert response.status_code == 422, response.text
        assert any(
            error["loc"][-1] == "created_by" for error in response.json()["detail"]
        )

    def test_the_name_column_and_the_blob_agree(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        row = session.get(db_models.TriggerSubscription, subscription_id)
        assert row is not None
        assert row.name == "nightly-retrain"
        assert row.definition["name"] == "nightly-retrain"

    def test_the_event_set_is_derived_and_opened_empty(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        states = session.scalars(
            sqlalchemy.select(db_models.TriggerEventState).where(
                db_models.TriggerEventState.subscription_id == subscription_id
            )
        ).all()
        assert {state.event_name for state in states} == {"a", "b"}
        assert all(state.filled_at is None for state in states)

    def test_creating_does_not_trigger(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """No condition in the grammar holds with zero events, so nothing can fire on create."""
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        history = session.scalars(
            sqlalchemy.select(db_models.TriggerHistory).where(
                db_models.TriggerHistory.subscription_id == subscription_id
            )
        ).all()
        assert history == []

    def test_any_authenticated_caller_may_create(
        self, other_user_client: fastapi.testclient.TestClient
    ) -> None:
        """Creation is not restricted — only changing someone else's subscription is."""
        assert other_user_client.post(_PATH, json=_their_payload()).status_code == 201


class TestCreateValidation:
    def test_a_malformed_condition_is_a_422_not_a_500(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        response = client.post(
            _PATH,
            json=_payload(condition={"op": "some", "children": [{"event": "a"}]}),
        )
        assert response.status_code == 422, response.text

    def test_an_empty_name_is_a_422(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        assert client.post(_PATH, json=_payload(name="")).status_code == 422

    def test_a_missing_condition_is_a_422(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        assert client.post(_PATH, json={"name": "n"}).status_code == 422

    def test_an_unknown_extra_key_is_a_422(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        assert client.post(_PATH, json=_payload(colour="red")).status_code == 422

    def test_a_bad_expire_seconds_is_a_422_not_a_later_500(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The blob is stored verbatim, so a coerced value would raise at trigger time."""
        for expire_seconds in (0, -60, "60", True):
            response = client.post(
                _PATH,
                json=_payload(
                    condition={"event": "a", "expire_seconds": expire_seconds}
                ),
            )
            assert response.status_code == 422, (expire_seconds, response.text)

    def test_an_over_long_expire_seconds_is_a_422(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Found by review: `gt=0` alone let a value through that no backend can hold.

        `trigger_event_state.expire_seconds` is an INTEGER, so MySQL refuses anything past
        2**31-1 with a 500 from `event_state.sync`. SQLite has no such width and stores it
        happily, which is worse: the value survives the request and overflows later, when
        `event_state._expires_at` adds it to `filled_at`.
        """
        for expire_seconds in (
            _MAX_EXPIRE_SECONDS + 1,
            2**31,
            1_000_000_000_000,
        ):
            response = client.post(
                _PATH,
                json=_payload(
                    condition={"event": "a", "expire_seconds": expire_seconds}
                ),
            )
            assert response.status_code == 422, (expire_seconds, response.text)

    def test_expire_seconds_at_the_cap_is_accepted(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The bound is a ceiling, not an off-by-one that rejects the largest legal window."""
        response = client.post(
            _PATH,
            json=_payload(
                condition={"event": "a", "expire_seconds": _MAX_EXPIRE_SECONDS}
            ),
        )
        assert response.status_code == 201, response.text

    def test_the_expiry_cap_cannot_overflow_a_datetime(self) -> None:
        """The reason for the ceiling, asserted against the arithmetic it protects."""
        latest = db_models.db_utils.utc_now() + datetime.timedelta(
            seconds=_MAX_EXPIRE_SECONDS
        )
        assert latest.year < datetime.MAXYEAR
        assert _MAX_EXPIRE_SECONDS < 2**31, "must fit the INTEGER column on MySQL"

    def test_a_patch_cannot_widen_expire_seconds(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        response = client.patch(
            f"{_PATH}/{subscription_id}",
            json={
                "condition": {
                    "event": "a",
                    "expire_seconds": _MAX_EXPIRE_SECONDS + 1,
                }
            },
        )
        assert response.status_code == 422, response.text

    def test_an_unknown_event_name_is_accepted(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """No producer registry: a subscription may be set up before its producer ships."""
        response = client.post(
            _PATH, json=_payload(condition={"event": "nothing-emits-this"})
        )
        assert response.status_code == 201, response.text

    def test_an_event_name_at_the_column_width_round_trips_to_a_state_row(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """The end the unit tests cannot reach: the validated name is what lands in the column.

        SQLite would accept an over-long name silently, so this asserts the length survives
        rather than asserting the database refuses — refusal is the API's job, tested below.
        """
        at_cap = "e" * _MAX_NAME_LENGTH
        response = client.post(_PATH, json=_payload(condition={"event": at_cap}))
        assert response.status_code == 201, response.text
        stored = session.scalars(
            sqlalchemy.select(db_models.TriggerEventState.event_name).where(
                db_models.TriggerEventState.subscription_id == response.json()["id"]
            )
        ).all()
        assert stored == [at_cap]

    def test_an_event_name_over_the_column_width_is_a_422(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Refused at the boundary, because `event_name` is a PK column that cannot be widened.

        A readiness event name is an annotation value, capped by the producer at 64KB — so a
        name past this column's width is a legal emission, and the API is where it stops.
        """
        response = client.post(
            _PATH,
            json=_payload(condition={"event": "e" * (_MAX_NAME_LENGTH + 1)}),
        )
        assert response.status_code == 422, response.text
        assert any(
            error["loc"][-1] == "event" for error in response.json()["detail"]
        ), response.text

    def test_a_patch_cannot_widen_an_event_name(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        response = client.patch(
            f"{_PATH}/{subscription_id}",
            json={"condition": {"event": "e" * (_MAX_NAME_LENGTH + 1)}},
        )
        assert response.status_code == 422, response.text


class TestList:
    def test_returns_the_page_and_a_total(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        for index in range(3):
            client.post(_PATH, json=_payload(name=f"sub-{index}"))
        body = client.get(_PATH).json()
        assert body["total_count"] == 3
        assert len(body["subscriptions"]) == 3
        assert body["next_page_token"] is None

    def test_pages_without_repeating_or_skipping(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        for index in range(5):
            client.post(_PATH, json=_payload(name=f"sub-{index}"))
        seen: list[str] = []
        token: str | None = None
        for _ in range(5):
            params = {"page_size": 2}
            if token:
                params["page_token"] = token
            body = client.get(_PATH, params=params).json()
            seen.extend(item["id"] for item in body["subscriptions"])
            token = body["next_page_token"]
            if not token:
                break
        assert len(seen) == 5
        assert len(set(seen)) == 5

    def test_a_malformed_page_token_is_a_422(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        assert client.get(_PATH, params={"page_token": "nonsense"}).status_code == 422
        assert (
            client.get(_PATH, params={"page_token": "not-a-date~abc"}).status_code
            == 422
        )

    def test_reads_are_not_filtered_by_creator(
        self,
        client: fastapi.testclient.TestClient,
        other_user_client: fastapi.testclient.TestClient,
    ) -> None:
        client.post(_PATH, json=_payload())
        body = other_user_client.get(_PATH).json()
        assert body["total_count"] == 1
        assert body["subscriptions"][0]["created_by"] == DEFAULT_USER


class TestTheLastPage:
    """A full page is not the same question as a page with more behind it.

    The list fetches `page_size + 1` rows and returns at most `page_size` of them, so the
    token answers "is there a next page" from a row it saw rather than inferring it from the
    page being full. Inferring it costs a client one empty request whenever the result count
    divides exactly by `page_size` — the case these tests pin, and the one the older
    `test_pages_without_repeating_or_skipping` misses by using 5 rows in pages of 2.
    """

    @staticmethod
    def _walk(
        client: fastapi.testclient.TestClient, *, page_size: int
    ) -> tuple[int, list[str]]:
        """Page until the token runs out, as a client is meant to. Returns requests and ids."""
        seen: list[str] = []
        token: str | None = None
        requests = 0
        while True:
            params: dict[str, Any] = {"page_size": page_size}
            if token:
                params["page_token"] = token
            body = client.get(_PATH, params=params).json()
            requests += 1
            seen.extend(item["id"] for item in body["subscriptions"])
            token = body["next_page_token"]
            if not token:
                return requests, seen
            assert requests <= 10, "paging did not terminate"

    @pytest.mark.parametrize(
        ("total", "page_size", "expected_requests"),
        [
            # The regression: 4 rows in pages of 2 is two full pages and nothing behind them.
            (4, 2, 2),
            # One page, exactly filled — the smallest form of the same bug.
            (2, 2, 1),
            # A partial last page already terminated correctly; it must keep doing so.
            (5, 2, 3),
            # Fewer rows than a page: one request, and the token was never plausible.
            (1, 2, 1),
            # Nothing at all still costs exactly one request.
            (0, 2, 1),
            # An exact multiple at a wider page, so the fix is not an artefact of page_size=2.
            (6, 3, 2),
        ],
    )
    def test_paging_costs_no_empty_request(
        self,
        client: fastapi.testclient.TestClient,
        total: int,
        page_size: int,
        expected_requests: int,
    ) -> None:
        for index in range(total):
            client.post(_PATH, json=_payload(name=f"sub-{index}"))
        requests, seen = self._walk(client, page_size=page_size)
        assert requests == expected_requests
        # The walk still has to be complete and duplicate-free, not merely short.
        assert len(seen) == total
        assert len(set(seen)) == total

    def test_the_probe_row_is_not_returned_in_the_page(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Asking for 2 of 3 returns 2. The extra row is read to be counted, not served."""
        for index in range(3):
            client.post(_PATH, json=_payload(name=f"sub-{index}"))
        body = client.get(_PATH, params={"page_size": 2}).json()
        assert len(body["subscriptions"]) == 2
        assert body["next_page_token"] is not None
        # total_count still counts the whole filtered set, not the page or the probe.
        assert body["total_count"] == 3

    def test_the_probe_row_is_not_skipped_by_the_token(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The cursor comes from the last returned row, so the probe leads the next page.

        Encoding the probe instead would lose exactly one subscription per page boundary, a
        loss the aggregate id count above would still catch but would not localise.
        """
        for index in range(3):
            client.post(_PATH, json=_payload(name=f"sub-{index}"))
        first = client.get(_PATH, params={"page_size": 2}).json()
        second = client.get(
            _PATH,
            params={"page_size": 2, "page_token": first["next_page_token"]},
        ).json()
        assert [item["id"] for item in second["subscriptions"]] == [
            item["id"] for item in client.get(_PATH).json()["subscriptions"][2:]
        ]

    def test_a_full_page_under_a_filter_also_ends_the_walk(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The probe rides the same WHERE as the page, so a filtered walk terminates too."""
        for index in range(2):
            client.post(_PATH, json=_payload(name=f"keep-{index}"))
        for index in range(3):
            client.post(_PATH, json=_payload(name=f"drop-{index}"))
        body = client.get(
            _PATH, params={"page_size": 2, "name_contains": "keep"}
        ).json()
        assert len(body["subscriptions"]) == 2
        assert body["total_count"] == 2
        assert body["next_page_token"] is None


class TestListFilters:
    def test_name_contains(self, client: fastapi.testclient.TestClient) -> None:
        client.post(_PATH, json=_payload(name="nightly-retrain"))
        client.post(_PATH, json=_payload(name="weekly-report"))
        body = client.get(_PATH, params={"name_contains": "retrain"}).json()
        assert [item["name"] for item in body["subscriptions"]] == ["nightly-retrain"]
        assert body["total_count"] == 1, "total_count respects the filter"

    def test_a_percent_is_matched_literally(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The filter is a substring, not a pattern: `%` is a character to look for.

        Unescaped this is the worst of the three, because `?name_contains=%` does not fail
        loudly — it quietly returns every subscription, with a `total_count` to match.
        """
        client.post(_PATH, json=_payload(name="50%-off-report"))
        client.post(_PATH, json=_payload(name="nightly-retrain"))
        body = client.get(_PATH, params={"name_contains": "%"}).json()
        assert [item["name"] for item in body["subscriptions"]] == ["50%-off-report"]
        assert body["total_count"] == 1, "the count is escaped the same way as the page"

    def test_an_underscore_is_matched_literally(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """`_` is LIKE's single-character wildcard, so unescaped it would also match `jobXone`."""
        client.post(_PATH, json=_payload(name="job_one"))
        client.post(_PATH, json=_payload(name="jobXone"))
        body = client.get(_PATH, params={"name_contains": "job_one"}).json()
        assert [item["name"] for item in body["subscriptions"]] == ["job_one"]
        assert body["total_count"] == 1

    def test_the_escape_character_itself_is_matched_literally(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """`autoescape` picks its own escape character, which must not become unsearchable.

        SQLAlchemy emits `ESCAPE '/'`, so a `/` in the needle has to be escaped by it too. A
        half-done escaping would make this query either match nothing or raise.
        """
        client.post(_PATH, json=_payload(name="eu/west-nightly"))
        client.post(_PATH, json=_payload(name="eu-west-nightly"))
        body = client.get(_PATH, params={"name_contains": "eu/west"}).json()
        assert [item["name"] for item in body["subscriptions"]] == ["eu/west-nightly"]
        assert body["total_count"] == 1

    def test_a_needle_of_only_wildcards_matches_nothing(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        # Nothing here is named with a wildcard character, so the honest answer is an empty page.
        client.post(_PATH, json=_payload(name="nightly-retrain"))
        client.post(_PATH, json=_payload(name="weekly-report"))
        body = client.get(_PATH, params={"name_contains": "%_%"}).json()
        assert body["subscriptions"] == []
        assert body["total_count"] == 0

    def test_event_name(self, client: fastapi.testclient.TestClient) -> None:
        client.post(_PATH, json=_payload(name="has-a", condition={"event": "a"}))
        client.post(_PATH, json=_payload(name="has-z", condition={"event": "z"}))
        body = client.get(_PATH, params={"event_name": "z"}).json()
        assert [item["name"] for item in body["subscriptions"]] == ["has-z"]

    def test_an_over_long_event_name_filter_is_a_422(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """It could only ever match nothing, so say so rather than return an empty page."""
        response = client.get(
            _PATH, params={"event_name": "e" * (_MAX_NAME_LENGTH + 1)}
        )
        assert response.status_code == 422, response.text

    def test_an_over_long_name_contains_filter_is_a_422(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The substring filter is a scan, so the pattern it scans with is capped like the rest.

        `name` is itself capped at this width, so a longer needle cannot match anything — the
        cap costs no reachable query and keeps an unbounded string out of the `LIKE`.
        """
        response = client.get(
            _PATH, params={"name_contains": "e" * (_MAX_NAME_LENGTH + 1)}
        )
        assert response.status_code == 422, response.text

    def test_event_name_does_not_duplicate_a_subscription_naming_it_twice(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """One event state per event, and EXISTS rather than a JOIN, so the row appears once."""
        client.post(
            _PATH,
            json=_payload(
                name="twice",
                condition={
                    "op": "any",
                    "children": [{"event": "a"}, {"event": "a"}],
                },
            ),
        )
        body = client.get(_PATH, params={"event_name": "a"}).json()
        assert len(body["subscriptions"]) == 1

    def test_enabled(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        enabled_id = client.post(_PATH, json=_payload(name="on")).json()["id"]
        disabled_id = client.post(_PATH, json=_payload(name="off")).json()["id"]
        row = session.get(db_models.TriggerSubscription, disabled_id)
        assert row is not None
        row.enabled = False
        session.commit()
        assert [
            item["id"]
            for item in client.get(_PATH, params={"enabled": True}).json()[
                "subscriptions"
            ]
        ] == [enabled_id]
        assert [
            item["id"]
            for item in client.get(_PATH, params={"enabled": False}).json()[
                "subscriptions"
            ]
        ] == [disabled_id]


class TestDetail:
    def test_404_for_an_unknown_id(self, client: fastapi.testclient.TestClient) -> None:
        assert client.get(f"{_PATH}/nope").status_code == 404

    def test_missing_lists_what_it_waits_for(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        body = client.get(f"{_PATH}/{subscription_id}").json()
        assert sorted(body["missing"]) == ["a", "b"]
        assert body["live"] == {}
        assert body["last_triggered_cycle"] is None
        assert body["last_triggered_at"] is None

    def test_live_shows_a_fresh_arrival(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        state = session.get(db_models.TriggerEventState, (subscription_id, "a"))
        assert state is not None
        state.filled_at = db_models.db_utils.utc_now()
        state.last_emission_event_id = "emission-1"
        session.commit()
        body = client.get(f"{_PATH}/{subscription_id}").json()
        assert body["live"] == {"a": "emission-1"}
        assert body["missing"] == ["b"]

    def test_missing_omits_a_choice_already_settled(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """`all(any(a, b), c)` with `a` live is waiting for `c` alone.

        `b` still has a row in `trigger_event_state` and no arrival, so a set difference over
        the condition's event names reports it. Emitting it would change nothing.
        """
        subscription_id = client.post(
            _PATH,
            json=_payload(
                name="a-choice",
                condition={
                    "op": "all",
                    "children": [
                        {
                            "op": "any",
                            "children": [{"event": "a"}, {"event": "b"}],
                        },
                        {"event": "c"},
                    ],
                },
            ),
        ).json()["id"]
        state = session.get(db_models.TriggerEventState, (subscription_id, "a"))
        assert state is not None
        state.filled_at = db_models.db_utils.utc_now()
        state.last_emission_event_id = "emission-1"
        session.commit()

        body = client.get(f"{_PATH}/{subscription_id}").json()
        assert body["live"] == {"a": "emission-1"}
        assert body["missing"] == ["c"], "b is under a branch that is already settled"

    def test_a_satisfied_condition_is_waiting_for_nothing(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        # A GET does not trigger, so this reads a subscription that is ready to fire. It is
        # waiting on nothing, even though its condition still names `b`.
        subscription_id = client.post(
            _PATH,
            json=_payload(
                name="ready",
                condition={
                    "op": "all",
                    "children": [
                        {
                            "op": "any",
                            "children": [{"event": "a"}, {"event": "b"}],
                        },
                        {"event": "c"},
                    ],
                },
            ),
        ).json()["id"]
        for event in ("a", "c"):
            state = session.get(db_models.TriggerEventState, (subscription_id, event))
            assert state is not None
            state.filled_at = db_models.db_utils.utc_now()
            state.last_emission_event_id = f"emission-{event}"
        session.commit()

        body = client.get(f"{_PATH}/{subscription_id}").json()
        assert sorted(body["live"]) == ["a", "c"]
        assert body["missing"] == []

    def test_last_fire_is_reported(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        session.add(
            db_models.TriggerHistory(
                subscription_id=subscription_id,
                cycle=3,
                matched_events={"events": ["a", "b"]},
                triggered_by={"a": "e1", "b": "e2"},
            )
        )
        session.commit()
        body = client.get(f"{_PATH}/{subscription_id}").json()
        assert body["last_triggered_cycle"] == 3
        assert body["last_triggered_at"] is not None

    def test_the_history_lookup_does_not_load_the_json_columns(
        self,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """Found by review: the detail route ORM-loaded the whole history row for two scalars.

        `matched_events` snapshots the subscription definition, whose size and depth are not
        capped, so the widest column on the row was being read on every GET that had history.
        Asserted against the emitted SQL, because a narrower SELECT is invisible in the body.
        """
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        session.add(
            db_models.TriggerHistory(
                subscription_id=subscription_id,
                cycle=3,
                matched_events={"events": ["a", "b"]},
                triggered_by={"a": "e1", "b": "e2"},
            )
        )
        session.commit()

        statements: list[str] = []

        @sqlalchemy.event.listens_for(db_engine, "before_cursor_execute")
        def _record(conn: Any, cursor: Any, statement: str, *args: Any) -> None:
            statements.append(statement)

        try:
            assert client.get(f"{_PATH}/{subscription_id}").status_code == 200
        finally:
            sqlalchemy.event.remove(db_engine, "before_cursor_execute", _record)

        history_reads = [text for text in statements if "trigger_history" in text]
        assert history_reads, "expected the detail route to read trigger_history"
        for text in history_reads:
            assert "matched_events" not in text, text
            assert "triggered_by" not in text, text
            assert "extra_data" not in text, text

    def test_detail_echoes_created_by_and_updated_at(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        body = client.get(f"{_PATH}/{subscription_id}").json()
        assert body["created_by"] == DEFAULT_USER
        assert body["updated_at"]

    def test_detail_is_open_to_another_caller(
        self,
        client: fastapi.testclient.TestClient,
        other_user_client: fastapi.testclient.TestClient,
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        assert other_user_client.get(f"{_PATH}/{subscription_id}").status_code == 200


class TestKeysetTiebreaker:
    """`updated_at` alone is not unique, so `id` is what makes paging total.

    Without the id tiebreaker in both the ORDER BY and the cursor comparison, rows sharing a
    timestamp can be returned twice or skipped entirely — and every other pagination test here
    happens to give each row a distinct `updated_at`, so this is the only one that notices.
    """

    def test_rows_sharing_a_timestamp_page_exactly_once(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        for index in range(6):
            client.post(_PATH, json=_payload(name=f"sub-{index}"))
        collision = db_models.db_utils.utc_now()
        for row in session.scalars(
            sqlalchemy.select(db_models.TriggerSubscription)
        ).all():
            row.updated_at = collision
        session.commit()

        seen: list[str] = []
        token: str | None = None
        for _ in range(10):
            params: dict[str, Any] = {"page_size": 2}
            if token:
                params["page_token"] = token
            body = client.get(_PATH, params=params).json()
            seen.extend(item["id"] for item in body["subscriptions"])
            token = body["next_page_token"]
            if not token:
                break

        assert len(seen) == 6, f"expected every row once, got {len(seen)}"
        assert len(set(seen)) == 6, "a row was returned on more than one page"


class TestPatch:
    def test_the_creator_may_edit(self, client: fastapi.testclient.TestClient) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        response = client.patch(
            f"{_PATH}/{subscription_id}",
            json={"condition": {"event": "only-a"}},
        )
        assert response.status_code == 200, response.text
        assert response.json()["condition"] == {"event": "only-a"}

    def test_an_admin_may_edit_someone_elses(
        self,
        client: fastapi.testclient.TestClient,
        admin_client: fastapi.testclient.TestClient,
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        response = admin_client.patch(
            f"{_PATH}/{subscription_id}", json={"name": "renamed"}
        )
        assert response.status_code == 200, response.text
        assert response.json()["name"] == "renamed"

    def test_a_stranger_gets_403(
        self,
        client: fastapi.testclient.TestClient,
        other_user_client: fastapi.testclient.TestClient,
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        response = other_user_client.patch(
            f"{_PATH}/{subscription_id}", json={"name": "nope"}
        )
        assert response.status_code == 403, response.text
        assert DEFAULT_USER in response.json()["detail"]

    def test_a_refused_edit_changes_nothing(
        self,
        client: fastapi.testclient.TestClient,
        other_user_client: fastapi.testclient.TestClient,
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        other_user_client.patch(f"{_PATH}/{subscription_id}", json={"name": "nope"})
        assert (
            client.get(f"{_PATH}/{subscription_id}").json()["name"] == "nightly-retrain"
        )

    def test_created_by_is_not_patchable(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        response = client.patch(
            f"{_PATH}/{subscription_id}",
            json={"created_by": "someone-else@example.com"},
        )
        assert response.status_code == 422, response.text

    def test_404_for_an_unknown_id(self, client: fastapi.testclient.TestClient) -> None:
        assert client.patch(f"{_PATH}/nope", json={"name": "x"}).status_code == 404

    def test_the_blob_is_replaced_not_merged(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        client.patch(f"{_PATH}/{subscription_id}", json={"condition": {"event": "c"}})
        row = session.get(db_models.TriggerSubscription, subscription_id)
        assert row is not None
        assert row.definition["condition"] == {"event": "c"}

    def test_a_rename_keeps_the_column_and_the_blob_in_step(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        client.patch(f"{_PATH}/{subscription_id}", json={"name": "renamed"})
        session.expire_all()
        row = session.get(db_models.TriggerSubscription, subscription_id)
        assert row is not None
        assert row.name == "renamed"
        assert row.definition["name"] == "renamed"

    def test_a_metadata_only_edit_leaves_the_event_set_alone(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        state = session.get(db_models.TriggerEventState, (subscription_id, "a"))
        assert state is not None
        state.filled_at = db_models.db_utils.utc_now()
        state.last_emission_event_id = "emission-1"
        session.commit()
        response = client.patch(f"{_PATH}/{subscription_id}", json={"name": "renamed"})
        assert response.json()["triggered"] is False
        assert client.get(f"{_PATH}/{subscription_id}").json()["live"] == {
            "a": "emission-1"
        }


class TestPatchThatTriggers:
    def test_removing_the_last_unfilled_event_triggers(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """An edit is judged against the state it leaves, so this starts a run on the spot."""
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        state = session.get(db_models.TriggerEventState, (subscription_id, "a"))
        assert state is not None
        state.filled_at = db_models.db_utils.utc_now()
        state.last_emission_event_id = "emission-1"
        session.commit()

        response = client.patch(
            f"{_PATH}/{subscription_id}", json={"condition": {"event": "a"}}
        )
        body = response.json()
        assert body["triggered"] is True, body
        # The claimed cycle is 0; the row's own counter has already moved past it.
        assert body["triggered_cycle"] == 0
        assert body["cycle"] == 1
        # The run the edit started, reported so a PATCH that fires is not a surprise.
        assert body["pipeline_run_id"]

    def test_an_edit_that_still_waits_reports_what_is_missing(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        body = client.patch(
            f"{_PATH}/{subscription_id}",
            json={
                "condition": {
                    "op": "all",
                    "children": [{"event": "x"}, {"event": "y"}],
                }
            },
        ).json()
        assert body["triggered"] is False
        assert sorted(body["missing"]) == ["x", "y"]
        assert body["reason"] == "awaiting_events"


class TestMissingSaysWhetherItLooked:
    """Found by review: `missing: []` on a PATCH that never evaluated read as "waiting for
    nothing", and contradicted the detail route on the same row in the same second. Three
    values now: a list of names, `[]` for "evaluated, nothing outstanding", null for
    "not evaluated".
    """

    def test_a_rename_answers_null_because_it_evaluated_nothing(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]

        body = client.patch(f"{_PATH}/{subscription_id}", json={"name": "r1"}).json()

        assert body["missing"] is None
        # In step with `reason`, which was already null on this path.
        assert body["reason"] is None
        assert body["triggered"] is False

    def test_the_rename_no_longer_contradicts_the_detail_route(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]

        patched = client.patch(f"{_PATH}/{subscription_id}", json={"name": "r1"}).json()
        detail = client.get(f"{_PATH}/{subscription_id}").json()

        # The row is waiting for both events. The PATCH declines to answer rather than
        # answering differently -- which is the whole of the fix.
        assert sorted(detail["missing"]) == ["a", "b"]
        assert patched["missing"] is None

    def test_a_patch_on_a_disabled_subscription_answers_null(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """`maybe_trigger` returns above the evaluation, so nothing weighed the condition."""
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        client.patch(f"{_PATH}/{subscription_id}", json={"enabled": False})

        body = client.patch(
            f"{_PATH}/{subscription_id}", json={"condition": {"event": "z"}}
        ).json()

        assert body["reason"] == "subscription_disabled"
        assert body["missing"] is None

    def test_an_edit_that_fires_answers_the_empty_list(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """`[]` is still an answer: evaluated, and nothing is outstanding."""
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        state = session.get(db_models.TriggerEventState, (subscription_id, "a"))
        assert state is not None
        state.filled_at = db_models.db_utils.utc_now()
        state.last_emission_event_id = "emission-1"
        session.commit()

        body = client.patch(
            f"{_PATH}/{subscription_id}", json={"condition": {"event": "a"}}
        ).json()

        assert body["triggered"] is True
        assert body["missing"] == []

    def test_re_enabling_evaluates_and_so_answers_a_list(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The other evaluating path: the condition is untouched, but disabled -> enabled."""
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        client.patch(f"{_PATH}/{subscription_id}", json={"enabled": False})

        body = client.patch(f"{_PATH}/{subscription_id}", json={"enabled": True}).json()

        assert body["reason"] == "awaiting_events"
        assert sorted(body["missing"]) == ["a", "b"]

    def test_the_by_name_route_answers_the_same_way(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        created = client.post(_PATH, json=_payload()).json()

        body = client.patch(
            _LOOKUP,
            params={
                "name": created["name"],
                "created_by": created["created_by"],
            },
            json={"name": "renamed-by-the-natural-key"},
        ).json()

        # A rename leaves the condition alone, so this route evaluated nothing either.
        assert body["missing"] is None, body
        assert body["reason"] is None

    def test_a_re_target_does_evaluate_because_it_can_recover_a_lost_run(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Re-pointing is not metadata: it is the recovery path for a deleted target, so it
        re-evaluates and must answer with a list rather than null.
        """
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]

        body = client.patch(
            f"{_PATH}/{subscription_id}",
            json={"pipeline_task_spec_from_user_pipeline_id": FULL_PIPELINE_ID},
        ).json()

        assert sorted(body["missing"]) == ["a", "b"], body
        assert body["reason"] == "awaiting_events"


def _delete_behind(*, db_engine: sqlalchemy.Engine, subscription_id: str) -> None:
    """Delete the row from a second session, standing in for a concurrent DELETE request.

    A separate `Session` on the same engine is as close as a single-process test gets to two
    requests racing: the route is holding an instance the database no longer has a row for,
    which is the whole of the condition being tested.
    """
    with orm.Session(bind=db_engine) as other:
        other.delete(other.get(db_models.TriggerSubscription, subscription_id))
        other.commit()


class TestPatchRacesDelete:
    """Found by review: a PATCH whose row is deleted underneath it used to be a 500.

    Losing that race is an ordinary "it's gone" — the same request a moment later would have
    been 404'd while resolving the id — but SQLAlchemy reports it as a `StaleDataError` or an
    `InvalidRequestError`, and an uncaught one of those is a 500 with a stack trace in it.

    Which of the two surfaces depends on where the delete lands, so both timings are here.
    They are staged by wrapping the service call: deleting *before* it leaves the edit to be
    flushed against a row that is gone, deleting *after* it leaves nothing to flush and a
    refresh that cannot find its row.
    """

    @staticmethod
    def _race(
        *,
        monkeypatch: pytest.MonkeyPatch,
        db_engine: sqlalchemy.Engine,
        subscription_id: str,
        before: bool,
    ) -> None:
        real = api_routes.service.update_subscription

        def racing_update(**kwargs: Any) -> Any:
            if before:
                _delete_behind(db_engine=db_engine, subscription_id=subscription_id)
                return real(**kwargs)
            result = real(**kwargs)
            _delete_behind(db_engine=db_engine, subscription_id=subscription_id)
            return result

        monkeypatch.setattr(api_routes.service, "update_subscription", racing_update)

    @pytest.mark.parametrize("before", [True, False])
    def test_it_is_a_404_whenever_the_delete_lands(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
        monkeypatch: pytest.MonkeyPatch,
        before: bool,
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        self._race(
            monkeypatch=monkeypatch,
            db_engine=db_engine,
            subscription_id=subscription_id,
            before=before,
        )

        response = client.patch(f"{_PATH}/{subscription_id}", json={"enabled": False})

        assert response.status_code == 404, response.text
        assert response.json()["detail"] == "Subscription not found"

    def test_the_by_name_route_reports_it_the_same_way(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Both PATCH routes share `_apply_update`, so the guard has to hold for both."""
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        self._race(
            monkeypatch=monkeypatch,
            db_engine=db_engine,
            subscription_id=subscription_id,
            before=True,
        )

        response = client.patch(
            _LOOKUP, params={"name": "nightly-retrain"}, json={"enabled": False}
        )

        assert response.status_code == 404, response.text

    def test_the_session_is_left_usable(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The 404 is produced by a query, so it can only be reached after a rollback.

        A session still holding a failed flush raises on the next statement rather than
        answering it — so a body that says "not found" is itself the evidence the route cleaned
        up before looking.
        """
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        self._race(
            monkeypatch=monkeypatch,
            db_engine=db_engine,
            subscription_id=subscription_id,
            before=True,
        )
        client.patch(f"{_PATH}/{subscription_id}", json={"enabled": False})

        with orm.Session(bind=db_engine) as check:
            assert check.get(db_models.TriggerSubscription, subscription_id) is None

    def test_an_unrelated_failure_is_not_dressed_up_as_a_404(
        self,
        client: fastapi.testclient.TestClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """`InvalidRequestError` is a broad base class, so the row has the last word.

        Misusing a session raises the same type as losing this race. The route re-reads the
        row before answering, so a failure with the row still present is re-raised rather than
        reported as a deletion that did not happen.
        """
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]

        def exploding_update(**_kwargs: Any) -> Any:
            raise sqlalchemy.exc.InvalidRequestError("nothing to do with a deletion")

        monkeypatch.setattr(api_routes.service, "update_subscription", exploding_update)

        with pytest.raises(sqlalchemy.exc.InvalidRequestError):
            client.patch(f"{_PATH}/{subscription_id}", json={"enabled": False})


class TestEnabled:
    def test_disabling_freezes_rather_than_clears(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        state = session.get(db_models.TriggerEventState, (subscription_id, "a"))
        assert state is not None
        state.filled_at = db_models.db_utils.utc_now()
        state.last_emission_event_id = "emission-1"
        session.commit()

        assert (
            client.patch(
                f"{_PATH}/{subscription_id}", json={"enabled": False}
            ).status_code
            == 200
        )
        body = client.get(f"{_PATH}/{subscription_id}").json()
        assert body["enabled"] is False
        assert body["live"] == {
            "a": "emission-1"
        }, "the arrival survived being switched off"

    def test_disabling_does_not_bump_the_cycle(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        client.patch(f"{_PATH}/{subscription_id}", json={"enabled": False})
        assert client.get(f"{_PATH}/{subscription_id}").json()["cycle"] == 0

    def test_re_enabling_resumes_where_it_was(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        state = session.get(db_models.TriggerEventState, (subscription_id, "a"))
        assert state is not None
        state.filled_at = db_models.db_utils.utc_now()
        state.last_emission_event_id = "emission-1"
        session.commit()
        client.patch(f"{_PATH}/{subscription_id}", json={"enabled": False})
        client.patch(f"{_PATH}/{subscription_id}", json={"enabled": True})
        body = client.get(f"{_PATH}/{subscription_id}").json()
        assert body["enabled"] is True
        assert body["live"] == {"a": "emission-1"}


class TestDelete:
    def test_the_creator_may_delete(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        assert client.delete(f"{_PATH}/{subscription_id}").status_code == 204
        assert client.get(f"{_PATH}/{subscription_id}").status_code == 404

    def test_an_admin_may_delete_someone_elses(
        self,
        client: fastapi.testclient.TestClient,
        admin_client: fastapi.testclient.TestClient,
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        assert admin_client.delete(f"{_PATH}/{subscription_id}").status_code == 204

    def test_a_stranger_gets_403_and_the_row_survives(
        self,
        client: fastapi.testclient.TestClient,
        other_user_client: fastapi.testclient.TestClient,
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        assert other_user_client.delete(f"{_PATH}/{subscription_id}").status_code == 403
        assert client.get(f"{_PATH}/{subscription_id}").status_code == 200

    def test_404_for_an_unknown_id(self, client: fastapi.testclient.TestClient) -> None:
        assert client.delete(f"{_PATH}/nope").status_code == 404

    def test_history_outlives_the_subscription(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        session.add(
            db_models.TriggerHistory(
                subscription_id=subscription_id,
                cycle=0,
                matched_events={"events": ["a"]},
                triggered_by={"a": "emission-1"},
            )
        )
        session.commit()
        assert client.delete(f"{_PATH}/{subscription_id}").status_code == 204
        session.expire_all()
        history = session.scalars(
            sqlalchemy.select(db_models.TriggerHistory).where(
                db_models.TriggerHistory.subscription_id == subscription_id
            )
        ).all()
        assert len(history) == 1


class TestUpdatedAt:
    """`updated_at` carries `onupdate`, so the question is whether every path issues an UPDATE.

    It matters twice over: it is what a client sees as "last changed", and it is the first
    column of the listing's keyset, so a path that failed to stamp it would also wedge
    pagination.
    """

    @staticmethod
    def _updated_at(
        *, client: fastapi.testclient.TestClient, subscription_id: str
    ) -> str:
        return client.get(f"{_PATH}/{subscription_id}").json()["updated_at"]

    def test_a_condition_edit_stamps_it(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        before = self._updated_at(client=client, subscription_id=subscription_id)
        client.patch(f"{_PATH}/{subscription_id}", json={"condition": {"event": "c"}})
        assert self._updated_at(client=client, subscription_id=subscription_id) > before

    def test_a_rename_stamps_it(self, client: fastapi.testclient.TestClient) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        before = self._updated_at(client=client, subscription_id=subscription_id)
        client.patch(f"{_PATH}/{subscription_id}", json={"name": "renamed"})
        assert self._updated_at(client=client, subscription_id=subscription_id) > before

    def test_toggling_enabled_stamps_it(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        before = self._updated_at(client=client, subscription_id=subscription_id)
        client.patch(f"{_PATH}/{subscription_id}", json={"enabled": False})
        assert self._updated_at(client=client, subscription_id=subscription_id) > before

    def test_a_trigger_stamps_it(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """The fence bump is a write to the row, so triggering counts as a mutation too."""
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        state = session.get(db_models.TriggerEventState, (subscription_id, "a"))
        assert state is not None
        state.filled_at = db_models.db_utils.utc_now()
        state.last_emission_event_id = "emission-1"
        session.commit()
        before = self._updated_at(client=client, subscription_id=subscription_id)
        body = client.patch(
            f"{_PATH}/{subscription_id}", json={"condition": {"event": "a"}}
        ).json()
        assert body["triggered"] is True
        assert self._updated_at(client=client, subscription_id=subscription_id) > before

    def test_created_at_never_moves(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        created_at = client.get(f"{_PATH}/{subscription_id}").json()["created_at"]
        client.patch(f"{_PATH}/{subscription_id}", json={"name": "renamed"})
        assert (
            client.get(f"{_PATH}/{subscription_id}").json()["created_at"] == created_at
        )


class TestUnauthenticated:
    """Every route inherits the 401 from the dependency, so this is one case per verb."""

    def test_every_route_is_401(
        self, unauthenticated_client: fastapi.testclient.TestClient
    ) -> None:
        client = unauthenticated_client
        assert client.post(_PATH, json=_payload()).status_code == 401
        assert client.get(_PATH).status_code == 401
        assert client.get(f"{_PATH}/any-id").status_code == 401
        assert client.patch(f"{_PATH}/any-id", json={"name": "x"}).status_code == 401
        assert client.delete(f"{_PATH}/any-id").status_code == 401
        key = {"name": "any-name"}
        assert client.get(_LOOKUP, params=key).status_code == 401
        assert client.patch(_LOOKUP, params=key, json={"name": "x"}).status_code == 401
        assert client.delete(_LOOKUP, params=key).status_code == 401

    def test_reads_are_open_but_not_anonymous(
        self, unauthenticated_client: fastapi.testclient.TestClient
    ) -> None:
        """ "Open to anyone authenticated" is not "open to anyone"."""
        assert unauthenticated_client.get(_PATH).status_code == 401


class TestCrudRoundTrip:
    def test_create_read_update_delete(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        created = client.post(_PATH, json=_payload()).json()
        subscription_id = created["id"]

        fetched = client.get(f"{_PATH}/{subscription_id}").json()
        assert fetched["id"] == subscription_id
        assert fetched["condition"] == _payload()["condition"]

        listed = client.get(_PATH).json()
        assert [item["id"] for item in listed["subscriptions"]] == [subscription_id]

        updated = client.patch(
            f"{_PATH}/{subscription_id}",
            json={"name": "renamed", "condition": {"event": "z"}},
        ).json()
        assert updated["name"] == "renamed"
        assert updated["condition"] == {"event": "z"}

        assert client.delete(f"{_PATH}/{subscription_id}").status_code == 204
        assert client.get(f"{_PATH}/{subscription_id}").status_code == 404
        assert client.get(_PATH).json()["total_count"] == 0


class TestNoOpEdit:
    def test_resending_the_same_condition_changes_nothing(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """An identical edit is a no-op by construction: both sides of the sync diff are empty."""
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        state = session.get(db_models.TriggerEventState, (subscription_id, "a"))
        assert state is not None
        filled_at = db_models.db_utils.utc_now()
        state.filled_at = filled_at
        state.last_emission_event_id = "emission-1"
        session.commit()

        body = client.patch(
            f"{_PATH}/{subscription_id}",
            json={"condition": _payload()["condition"]},
        ).json()
        assert body["triggered"] is False
        # The arrival survived an edit that did not mention it.
        assert client.get(f"{_PATH}/{subscription_id}").json()["live"] == {
            "a": "emission-1"
        }

    def test_a_no_op_edit_does_not_bump_the_cycle(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        client.patch(
            f"{_PATH}/{subscription_id}",
            json={"condition": _payload()["condition"]},
        )
        assert client.get(f"{_PATH}/{subscription_id}").json()["cycle"] == 0


class TestReconcileKeepsSurvivors:
    def test_an_unrelated_edit_leaves_a_filled_event_alone(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """`a` is in both the old and the new condition, so its arrival must survive."""
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        state = session.get(db_models.TriggerEventState, (subscription_id, "a"))
        assert state is not None
        state.filled_at = db_models.db_utils.utc_now()
        state.last_emission_event_id = "emission-1"
        session.commit()

        client.patch(
            f"{_PATH}/{subscription_id}",
            json={
                "condition": {
                    "op": "all",
                    "children": [{"event": "a"}, {"event": "c"}],
                }
            },
        )
        body = client.get(f"{_PATH}/{subscription_id}").json()
        assert body["live"] == {"a": "emission-1"}, "the survivor kept its arrival"
        assert body["missing"] == ["c"], "b is gone, c is new and empty"

    def test_a_removed_event_loses_its_arrival(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """Removing an event deletes its row; re-adding it comes back empty, not restored."""
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        state = session.get(db_models.TriggerEventState, (subscription_id, "b"))
        assert state is not None
        state.filled_at = db_models.db_utils.utc_now()
        state.last_emission_event_id = "emission-b"
        session.commit()

        client.patch(f"{_PATH}/{subscription_id}", json={"condition": {"event": "a"}})
        client.patch(
            f"{_PATH}/{subscription_id}",
            json={
                "condition": {
                    "op": "all",
                    "children": [{"event": "a"}, {"event": "b"}],
                }
            },
        )
        assert client.get(f"{_PATH}/{subscription_id}").json()["live"] == {}


class TestPageTokenAcrossAnEdit:
    def test_an_old_token_still_pages_the_untouched_rows(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """A token stays decodable across an edit, and unedited rows are each seen once.

        The edited row moves: the keyset orders by `updated_at DESC`, so editing a row sends it
        to the front, ahead of a cursor already past that point. That is inherent to ordering by
        last-changed rather than a defect — asserted here so the behaviour is documented.
        """
        ids = [
            client.post(_PATH, json=_payload(name=f"sub-{i}")).json()["id"]
            for i in range(4)
        ]
        first = client.get(_PATH, params={"page_size": 2}).json()
        token = first["next_page_token"]
        assert token

        # Edit one of the rows already returned on page one.
        client.patch(
            f"{_PATH}/{first['subscriptions'][0]['id']}", json={"name": "moved"}
        )

        second = client.get(_PATH, params={"page_size": 2, "page_token": token}).json()
        assert len(second["subscriptions"]) == 2, "the token still pages"
        seen = [item["id"] for item in first["subscriptions"]] + [
            item["id"] for item in second["subscriptions"]
        ]
        assert len(set(seen)) == 4, "every row accounted for exactly once"
        assert set(seen) == set(ids)


class TestDisabledNeverTriggers:
    """Found by review: an edit that disables and satisfies in one PATCH used to start a run."""

    def test_a_patch_that_disables_and_satisfies_does_not_trigger(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        subscription_id = client.post(
            _PATH,
            json=_payload(
                condition={
                    "op": "all",
                    "children": [{"event": "x"}, {"event": "y"}],
                }
            ),
        ).json()["id"]
        state = session.get(db_models.TriggerEventState, (subscription_id, "x"))
        assert state is not None
        state.filled_at = db_models.db_utils.utc_now()
        state.last_emission_event_id = "em-1"
        session.commit()

        body = client.patch(
            f"{_PATH}/{subscription_id}",
            json={"enabled": False, "condition": {"event": "x"}},
        ).json()
        assert body["triggered"] is False
        assert body["reason"] == "subscription_disabled"
        assert body["cycle"] == 0, "the fence counter did not move"
        assert (
            body["triggered_cycle"] is None
        ), "nothing triggered, so no cycle was claimed"
        # The arrival it would have consumed is still there.
        assert client.get(f"{_PATH}/{subscription_id}").json()["live"] == {"x": "em-1"}

    def test_an_already_disabled_subscription_does_not_trigger_on_an_edit(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        state = session.get(db_models.TriggerEventState, (subscription_id, "a"))
        assert state is not None
        state.filled_at = db_models.db_utils.utc_now()
        state.last_emission_event_id = "em-1"
        session.commit()
        client.patch(f"{_PATH}/{subscription_id}", json={"enabled": False})

        body = client.patch(
            f"{_PATH}/{subscription_id}", json={"condition": {"event": "a"}}
        ).json()
        assert body["triggered"] is False
        assert body["reason"] == "subscription_disabled"

    def test_re_enabling_a_satisfied_subscription_triggers(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """Otherwise it sits satisfied and dormant: every event it waits on is already filled,
        so no further emission would ever arrive to prompt a re-check."""
        subscription_id = client.post(
            _PATH, json=_payload(condition={"event": "a"})
        ).json()["id"]
        client.patch(f"{_PATH}/{subscription_id}", json={"enabled": False})
        state = session.get(db_models.TriggerEventState, (subscription_id, "a"))
        assert state is not None
        state.filled_at = db_models.db_utils.utc_now()
        state.last_emission_event_id = "em-1"
        session.commit()

        body = client.patch(f"{_PATH}/{subscription_id}", json={"enabled": True}).json()
        assert body["triggered"] is True, body
        assert body["triggered_cycle"] == 0

    def test_re_enabling_an_unsatisfied_subscription_does_not_trigger(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        client.patch(f"{_PATH}/{subscription_id}", json={"enabled": False})
        body = client.patch(f"{_PATH}/{subscription_id}", json={"enabled": True}).json()
        assert body["triggered"] is False
        assert body["reason"] == "awaiting_events"

    def test_a_plain_rename_never_triggers(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """The off->on transition is what prompts a re-check, not the rename it rides with."""
        subscription_id = client.post(
            _PATH, json=_payload(condition={"event": "a"})
        ).json()["id"]
        state = session.get(db_models.TriggerEventState, (subscription_id, "a"))
        assert state is not None
        state.filled_at = db_models.db_utils.utc_now()
        state.last_emission_event_id = "em-1"
        session.commit()
        body = client.patch(
            f"{_PATH}/{subscription_id}", json={"name": "renamed"}
        ).json()
        assert body["triggered"] is False


class TestTriggeredCycleIsOnlySetWhenSomethingTriggered:
    """Found by review: the condition path copied `result.cycle` whatever the verdict was.

    `maybe_trigger` reports the subscription's current cycle on every outcome, including the
    ones where it declined to start a run — so an unconditional copy answered `triggered:
    false` alongside `triggered_cycle: 0`, contradicting both the response contract and the
    metadata-only branch of the same handler.
    """

    def test_a_disabling_edit_reports_no_triggered_cycle(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """The reported repro: satisfy the condition and disable in the same PATCH.

        The edit drops `b`, which is what leaves the condition satisfied by the already-filled
        `a`. Resending the stored condition verbatim would no longer reach `maybe_trigger` at
        all, so it would pass this test without ever running the code it was written for.
        """
        subscription_id = client.post(
            _PATH,
            json=_payload(
                condition={
                    "op": "all",
                    "children": [{"event": "a"}, {"event": "b"}],
                }
            ),
        ).json()["id"]
        state = session.get(db_models.TriggerEventState, (subscription_id, "a"))
        assert state is not None
        state.filled_at = db_models.db_utils.utc_now()
        state.last_emission_event_id = "em-1"
        session.commit()

        body = client.patch(
            f"{_PATH}/{subscription_id}",
            json={"enabled": False, "condition": {"event": "a"}},
        ).json()
        assert body["triggered"] is False
        assert body["reason"] == "subscription_disabled"
        assert body["triggered_cycle"] is None, body

    def test_an_edit_that_still_waits_reports_no_triggered_cycle(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        body = client.patch(
            f"{_PATH}/{subscription_id}",
            json={
                "condition": {
                    "op": "all",
                    "children": [{"event": "x"}, {"event": "y"}],
                }
            },
        ).json()
        assert body["triggered"] is False
        assert body["reason"] == "awaiting_events"
        assert body["triggered_cycle"] is None, body

    def test_an_edit_that_triggers_still_reports_its_cycle(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """The other half: the guard must not blank out a cycle that was really claimed."""
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        state = session.get(db_models.TriggerEventState, (subscription_id, "a"))
        assert state is not None
        state.filled_at = db_models.db_utils.utc_now()
        state.last_emission_event_id = "em-1"
        session.commit()

        body = client.patch(
            f"{_PATH}/{subscription_id}", json={"condition": {"event": "a"}}
        ).json()
        assert body["triggered"] is True, body
        assert (
            body["triggered_cycle"] == 0
        ), "cycle 0 is a real claimed cycle, not a missing one"
        assert body["cycle"] == 1

    def test_both_patch_branches_agree(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """The metadata-only branch and the condition branch answer the same shape."""
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        client.patch(f"{_PATH}/{subscription_id}", json={"enabled": False})

        metadata_only = client.patch(
            f"{_PATH}/{subscription_id}", json={"name": "renamed"}
        ).json()
        with_condition = client.patch(
            f"{_PATH}/{subscription_id}", json={"condition": {"event": "a"}}
        ).json()
        assert metadata_only["triggered"] is False
        assert with_condition["triggered"] is False
        assert (
            metadata_only["triggered_cycle"]
            == with_condition["triggered_cycle"]
            is None
        )
        # Neither branch evaluated the condition -- the first never looked, the second was
        # turned away for being disabled -- so both decline to say what is outstanding.
        assert metadata_only["missing"] is with_condition["missing"] is None


class TestConflictingExpiries:
    """Found by review: the field rules cannot see across the tree, so this reached the service."""

    def test_the_same_event_twice_with_different_expiries_is_a_422(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        response = client.post(
            _PATH,
            json=_payload(
                condition={
                    "op": "all",
                    "children": [
                        {"event": "a"},
                        {"event": "a", "expire_seconds": 60},
                    ],
                }
            ),
        )
        assert response.status_code == 422, response.text

    def test_the_same_event_twice_with_the_same_expiry_is_fine(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        response = client.post(
            _PATH,
            json=_payload(
                condition={
                    "op": "any",
                    "children": [
                        {"event": "a", "expire_seconds": 60},
                        {"event": "a", "expire_seconds": 60},
                    ],
                }
            ),
        )
        assert response.status_code == 201, response.text

    def test_a_patch_with_conflicting_expiries_is_a_422(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        response = client.patch(
            f"{_PATH}/{subscription_id}",
            json={
                "condition": {
                    "op": "all",
                    "children": [
                        {"event": "a"},
                        {"event": "a", "expire_seconds": 5},
                    ],
                }
            },
        )
        assert response.status_code == 422, response.text


class TestConditionDepth:
    """Found by review: an uncapped tree is a stored 500, not a rejected request.

    From depth 128 pydantic-core's serializer refuses the tree, but only on the way *out*: the
    row is committed by then, so the POST 500s with the subscription in the database and every
    later read that has to serialize it — the shared listing above all — 500s for every caller
    until someone deletes it by name. Past 253 the validation guard trips instead and the
    request is refused, but as a `Field required` on `EventCondition`, which names neither
    depth nor the real problem. `_MAX_CONDITION_DEPTH` puts one honest 422 in front of both.

    These go through the client rather than the request model on purpose: the model tests in
    test_trigger_api_models cannot see whether a row committed or whether the listing survived.
    """

    _CAP = api_routes._MAX_CONDITION_DEPTH
    # The shallowest tree that used to commit and then poison every read of it.
    _WAS_POISON = 128

    def test_a_tree_at_the_cap_is_created(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        response = client.post(_PATH, json=_payload(condition=_nested(depth=self._CAP)))
        assert response.status_code == 201, response.text

    def test_one_level_past_the_cap_is_a_422_naming_the_depth(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        response = client.post(
            _PATH, json=_payload(condition=_nested(depth=self._CAP + 1))
        )
        assert response.status_code == 422, response.text
        assert "levels deep" in response.text

    def test_the_depth_that_used_to_poison_the_listing_is_refused(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """The regression itself: refused at the door, nothing stored, listing still served.

        Without the cap this POST raises `PydanticSerializationError` out of the response
        serializer with the row already committed, and the two assertions after it fail.
        """
        response = client.post(
            _PATH, json=_payload(condition=_nested(depth=self._WAS_POISON))
        )
        assert response.status_code == 422, response.text
        assert (
            session.scalars(sqlalchemy.select(db_models.TriggerSubscription)).all()
            == []
        )
        assert client.get(_PATH).status_code == 200

    def test_a_patch_past_the_cap_is_refused_and_changes_nothing(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """PATCH is the other write path, and it must not be the way around the door."""
        created = client.post(_PATH, json=_payload()).json()
        response = client.patch(
            f"{_PATH}/{created['id']}",
            json={"condition": _nested(depth=self._WAS_POISON)},
        )
        assert response.status_code == 422, response.text
        assert "levels deep" in response.text
        after = client.get(f"{_PATH}/{created['id']}")
        assert after.status_code == 200, after.text
        assert after.json()["condition"] == _payload()["condition"]


def _nested_body(*, depth: int, name: str = "nightly-retrain") -> bytes:
    """A create payload whose condition is `depth` levels tall, built as raw JSON text.

    Not `json.dumps` on a dict: the encoder recurses once per level and raises `RecursionError`
    itself somewhere past three thousand, which would make the test fail for a reason that has
    nothing to do with the server.
    """
    condition = (
        '{"op":"all","children":[' * (depth - 1) + '{"event":"a"}' + "]}" * (depth - 1)
    )
    return (
        f'{{"name":"{name}","condition":{condition},'
        f'"pipeline_task_spec_from_user_pipeline_id":"{SEEDED_PIPELINE_ID}"}}'
    ).encode()


class TestARefusedConditionIsCheapToRefuse:
    """Found by review: past a few hundred levels the *refusal* was the failure.

    `_MAX_CONDITION_DEPTH` used to be enforced only on a model pydantic had already built, and
    pydantic's own guard trips first from 254. The error it raises echoes the rejected payload
    back under `input`, and encoding that echo costs a stack frame per level: at 300 the 422
    weighed 5.4MB, and from ~485 the encoder ran out of stack and the refusal became a 500 --
    a request that is cheap to send and expensive to turn away.

    `_refuse_a_condition_too_deep_to_validate` moves the same cap in front of pydantic, on the
    raw body, so every one of those depths is the ordinary 422 with nothing large attached.
    """

    _JSON = {"content-type": "application/json"}
    # Past the depth where the encoder used to exhaust the stack.
    _WAS_A_500 = 600
    # Past pydantic's own guard, where the message used to stop naming depth at all.
    _WAS_ILLEGIBLE = 300

    def test_the_depth_that_used_to_500_the_refusal_is_a_422(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        response = client.post(
            _PATH,
            content=_nested_body(depth=self._WAS_A_500),
            headers=self._JSON,
        )
        assert response.status_code == 422, response.text
        assert "levels deep" in response.text

    def test_the_refusal_does_not_echo_the_payload_back(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The amplification, which the 500 was only the loudest symptom of."""
        response = client.post(
            _PATH,
            content=_nested_body(depth=self._WAS_ILLEGIBLE),
            headers=self._JSON,
        )
        assert response.status_code == 422, response.text
        assert len(response.content) < 1000, len(response.content)

    def test_past_pydantics_own_guard_the_message_still_names_the_depth(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """It used to be `Field required` on `EventCondition`, which names neither."""
        response = client.post(
            _PATH,
            content=_nested_body(depth=self._WAS_ILLEGIBLE),
            headers=self._JSON,
        )
        assert (
            f"nests {self._WAS_ILLEGIBLE} levels deep" in response.text
        ), response.text

    def test_the_by_name_patch_is_guarded_too(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Three routes take a condition in the body, and the guard is per-route."""
        client.post(_PATH, json=_payload())
        condition = '{"op":"all","children":[' * (self._WAS_A_500 - 1)
        condition += '{"event":"a"}' + "]}" * (self._WAS_A_500 - 1)
        response = client.patch(
            _LOOKUP,
            params={"name": "nightly-retrain"},
            content=('{"condition":' + condition + "}").encode(),
            headers=self._JSON,
        )
        assert response.status_code == 422, response.text
        assert "levels deep" in response.text

    def test_the_by_id_patch_is_guarded_too(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """At a depth the model validator never sees, so only the guard can refuse it."""
        created = client.post(_PATH, json=_payload()).json()
        condition = '{"op":"all","children":[' * (self._WAS_A_500 - 1)
        condition += '{"event":"a"}' + "]}" * (self._WAS_A_500 - 1)
        response = client.patch(
            f"{_PATH}/{created['id']}",
            content=('{"condition":' + condition + "}").encode(),
            headers=self._JSON,
        )
        assert response.status_code == 422, response.text
        assert "levels deep" in response.text

    def test_an_empty_body_is_still_pydantics_complaint_to_make(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """There is no JSON to walk, and asking for some raises rather than returning None."""
        response = client.post(_PATH, content=b"", headers=self._JSON)
        assert response.status_code == 422, response.text

    def test_a_body_that_is_not_an_object_is_left_to_pydantic(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The guard steps aside for anything it cannot read a `condition` out of."""
        response = client.post(_PATH, content=b"[1, 2, 3]", headers=self._JSON)
        assert response.status_code == 422, response.text
        assert "levels deep" not in response.text

    def test_a_body_without_a_condition_is_left_to_pydantic(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """A metadata-only PATCH has no condition to walk, and must not be refused for it."""
        created = client.post(_PATH, json=_payload()).json()
        response = client.patch(f"{_PATH}/{created['id']}", json={"enabled": False})
        assert response.status_code == 200, response.text


class TestNameWhitespace:
    """Found by review: a padded name used to store as typed, and the key is matched byte-exact.

    `' nightly '` and `'nightly'` are the same name to everyone except the routes, so a row
    stored with the padding was a row nobody could address again — while `'nightly'` stayed
    free for a second, different subscription to claim. Stripping at the door on both the write
    and the read side is what keeps one typed name meaning one row.
    """

    _PADDED = "  nightly-retrain  "

    def test_a_padded_name_is_stored_stripped(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        response = client.post(_PATH, json=_payload(name=self._PADDED))
        assert response.status_code == 201, response.text
        assert response.json()["name"] == "nightly-retrain"

    def test_a_row_created_padded_is_addressable_by_the_name_that_was_meant(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The regression itself: stored as typed, this lookup 404s and the row is orphaned."""
        client.post(_PATH, json=_payload(name=self._PADDED))
        response = client.get(_LOOKUP, params={"name": "nightly-retrain"})
        assert response.status_code == 200, response.text

    def test_a_padded_lookup_finds_the_row(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The read side strips too, so the key is spelled the same way on both sides."""
        created = client.post(_PATH, json=_payload()).json()
        response = client.get(_LOOKUP, params={"name": self._PADDED})
        assert response.status_code == 200, response.text
        assert response.json()["id"] == created["id"]

    def test_padding_does_not_buy_a_second_row_under_the_same_name(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        assert client.post(_PATH, json=_payload()).status_code == 201
        response = client.post(_PATH, json=_payload(name=self._PADDED))
        assert response.status_code == 409, response.text

    def test_a_rename_to_a_padded_name_is_stripped(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        created = client.post(_PATH, json=_payload()).json()
        response = client.patch(
            f"{_PATH}/{created['id']}", json={"name": "  renamed  "}
        )
        assert response.status_code == 200, response.text
        assert client.get(f"{_PATH}/{created['id']}").json()["name"] == "renamed"

    def test_a_padded_delete_reaches_the_row(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """`_reject_inexact_key` compares after the strip, so this is an exact key, not a near one."""
        client.post(_PATH, json=_payload())
        assert client.delete(_LOOKUP, params={"name": self._PADDED}).status_code == 204
        assert (
            client.get(_LOOKUP, params={"name": "nightly-retrain"}).status_code == 404
        )

    def test_a_name_of_nothing_but_spaces_is_a_422(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """`min_length` is checked after the strip, so this cannot slip in as a blank name."""
        response = client.post(_PATH, json=_payload(name="   "))
        assert response.status_code == 422, response.text

    def test_inner_whitespace_is_left_alone(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Stripping the ends is normalisation; rewriting the middle would be renaming."""
        response = client.post(_PATH, json=_payload(name="nightly retrain"))
        assert response.status_code == 201, response.text
        assert response.json()["name"] == "nightly retrain"


class TestTheNaturalKey:
    """UNIQUE (created_by, name) — the handle a caller addresses a subscription by.

    The point of the constraint is that a CI job can PATCH `nightly` without persisting its own
    name-to-id map. That only holds if a duplicate is refused, so the enforcement tests are the
    load-bearing ones — and the permissive tests below are what stop someone "fixing" a failure
    by narrowing the key to UNIQUE (name), which would let one caller squat every name.

    Every case goes through the route rather than the session, because the thing under test is
    the translation: uncaught, the constraint turns a well-formed request into a 500 and a page.
    """

    def test_the_same_caller_cannot_create_the_name_twice(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        assert client.post(_PATH, json=_payload()).status_code == 201
        response = client.post(_PATH, json=_payload())
        assert response.status_code == 409, response.text

    def test_the_conflict_names_the_name_and_the_per_creator_scope(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """A bare "already exists" reads as globally taken and sends callers to `nightly-2`."""
        client.post(_PATH, json=_payload())
        detail = client.post(_PATH, json=_payload()).json()["detail"]
        assert "nightly-retrain" in detail
        assert "unique per creator" in detail

    def test_a_duplicate_leaves_the_original_untouched(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The rejected insert must not take the row it collided with down with it."""
        original_id = client.post(_PATH, json=_payload()).json()["id"]
        client.post(_PATH, json=_payload())
        listed = client.get(_PATH).json()["subscriptions"]
        assert [entry["id"] for entry in listed] == [original_id]

    def test_two_callers_may_hold_the_same_name(
        self,
        client: fastapi.testclient.TestClient,
        other_user_client: fastapi.testclient.TestClient,
    ) -> None:
        """The scope is per creator: `nightly` is free for everyone but its owner."""
        assert client.post(_PATH, json=_payload()).status_code == 201
        assert other_user_client.post(_PATH, json=_their_payload()).status_code == 201

    def test_one_caller_may_hold_many_names(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        assert client.post(_PATH, json=_payload(name="nightly")).status_code == 201
        assert client.post(_PATH, json=_payload(name="weekly")).status_code == 201

    def test_a_rename_onto_a_taken_name_is_a_409(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """UNIQUE is checked on UPDATE as well as INSERT — there is no rename loophole."""
        client.post(_PATH, json=_payload(name="nightly"))
        subscription_id = client.post(_PATH, json=_payload(name="weekly")).json()["id"]
        response = client.patch(f"{_PATH}/{subscription_id}", json={"name": "nightly"})
        assert response.status_code == 409, response.text

    def test_a_rename_carried_by_a_condition_edit_is_also_a_409(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The other update path: a body with a condition takes a different branch in the route.

        It writes `name` too, so it needs the same handler. Without it this one request shape
        stays a 500 while the metadata-only rename is a clean 409.
        """
        client.post(_PATH, json=_payload(name="nightly"))
        subscription_id = client.post(_PATH, json=_payload(name="weekly")).json()["id"]
        response = client.patch(
            f"{_PATH}/{subscription_id}",
            json={"name": "nightly", "condition": {"event": "c"}},
        )
        assert response.status_code == 409, response.text

    def test_a_failed_rename_leaves_the_old_name_in_place(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The rollback has to restore the row, not leave it half-renamed in the session."""
        client.post(_PATH, json=_payload(name="nightly"))
        subscription_id = client.post(_PATH, json=_payload(name="weekly")).json()["id"]
        client.patch(f"{_PATH}/{subscription_id}", json={"name": "nightly"})
        assert client.get(f"{_PATH}/{subscription_id}").json()["name"] == "weekly"

    def test_renaming_a_subscription_to_its_own_name_is_not_a_conflict(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """A no-op rename collides with itself only if the UPDATE is written as delete+insert."""
        subscription_id = client.post(_PATH, json=_payload(name="nightly")).json()["id"]
        response = client.patch(f"{_PATH}/{subscription_id}", json={"name": "nightly"})
        assert response.status_code == 200, response.text

    def test_a_deleted_name_can_be_claimed_again(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Rows are hard-deleted here, so the name frees immediately — unlike user_pipelines."""
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        assert client.delete(f"{_PATH}/{subscription_id}").status_code == 204
        assert client.post(_PATH, json=_payload()).status_code == 201

    def test_a_caller_may_take_a_name_another_caller_already_holds(
        self,
        client: fastapi.testclient.TestClient,
        other_user_client: fastapi.testclient.TestClient,
    ) -> None:
        """Names are scoped per creator: the first caller's row blocks neither of these."""
        client.post(_PATH, json=_payload())
        taken = other_user_client.post(_PATH, json=_their_payload())
        assert taken.status_code == 201, taken.text
        released = other_user_client.delete(f"{_PATH}/{taken.json()['id']}")
        assert released.status_code == 204, released.text


class TestLookupRouteIsNotShadowed:
    """`/lookup` and an id are both one path segment, so registration order decides.

    Starlette matches in registration order, so a `/lookup` declared after
    `/{subscription_id}` is unreachable: every request lands on `get_subscription` carrying
    the literal string "lookup" as an id, and comes back a 404 that blames the wrong thing.
    Nothing warns about that at import time, which is why it is pinned here rather than left
    to the comment beside the routes.
    """

    def test_an_unknown_name_is_answered_by_the_lookup_route(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The 404 body is the discriminator: only the lookup route echoes the key."""
        response = client.get(_LOOKUP, params={"name": "no-such-name"})
        assert response.status_code == 404, response.text
        assert response.json()["detail"] == (
            f"No subscription named 'no-such-name' for '{DEFAULT_USER}'"
        )
        assert response.json()["detail"] != "Subscription not found"

    def test_the_id_route_still_answers_a_real_id(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The control: adding the literal must not have stolen the parameterised route."""
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        assert client.get(f"{_PATH}/{subscription_id}").status_code == 200

    def test_an_id_can_never_collide_with_the_literal(self) -> None:
        """Why shadowing the *other* way is not a risk worth guarding.

        Ids are fixed-width lowercase hex, so no generated id can ever be the string "lookup"
        and become unaddressable behind the literal route.
        """
        assert db_utils.ID_LENGTH != len("lookup")
        generated = bts.generate_unique_id()
        assert len(generated) == db_utils.ID_LENGTH
        assert set(generated) <= set("0123456789abcdef")


class TestLookupByName:
    """Reading a subscription by `(created_by, name)` instead of by id."""

    def test_it_finds_what_the_caller_created(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        created = client.post(_PATH, json=_payload()).json()
        found = client.get(_LOOKUP, params={"name": "nightly-retrain"})
        assert found.status_code == 200, found.text
        assert found.json()["id"] == created["id"]

    def test_created_by_defaults_to_the_caller(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The everyday call carries the name alone; the owner is implied."""
        client.post(_PATH, json=_payload())
        found = client.get(_LOOKUP, params={"name": "nightly-retrain"})
        assert found.status_code == 200, found.text
        assert found.json()["created_by"] == DEFAULT_USER

    def test_it_returns_exactly_what_the_id_route_returns(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The two ways of addressing one subscription must not describe it differently."""
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        by_id = client.get(f"{_PATH}/{subscription_id}")
        by_name = client.get(_LOOKUP, params={"name": "nightly-retrain"})
        assert by_id.status_code == by_name.status_code == 200
        assert by_id.json() == by_name.json()

    def test_it_carries_the_detail_fields_not_just_the_summary(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """It is the detail response, so `live` and `missing` come with it."""
        client.post(_PATH, json=_payload())
        body = client.get(_LOOKUP, params={"name": "nightly-retrain"}).json()
        assert sorted(body["missing"]) == ["a", "b"]
        assert body["live"] == {}

    def test_an_explicit_created_by_reaches_another_callers_subscription(
        self,
        client: fastapi.testclient.TestClient,
        other_user_client: fastapi.testclient.TestClient,
    ) -> None:
        """Reads are open, exactly as they are on the listing."""
        created = other_user_client.post(_PATH, json=_their_payload()).json()
        found = client.get(
            _LOOKUP,
            params={"name": "nightly-retrain", "created_by": OTHER_USER},
        )
        assert found.status_code == 200, found.text
        assert found.json()["id"] == created["id"]

    def test_another_callers_name_is_not_found_under_the_default_owner(
        self,
        client: fastapi.testclient.TestClient,
        other_user_client: fastapi.testclient.TestClient,
    ) -> None:
        """The key is the pair, so the same name in another namespace is simply absent."""
        other_user_client.post(_PATH, json=_their_payload())
        found = client.get(_LOOKUP, params={"name": "nightly-retrain"})
        assert found.status_code == 404, found.text

    def test_two_callers_holding_one_name_are_told_apart(
        self,
        client: fastapi.testclient.TestClient,
        other_user_client: fastapi.testclient.TestClient,
    ) -> None:
        """The whole point of the pair: one name, two rows, no ambiguity."""
        mine = client.post(_PATH, json=_payload()).json()["id"]
        theirs = other_user_client.post(_PATH, json=_their_payload()).json()["id"]
        assert mine != theirs
        assert (
            client.get(_LOOKUP, params={"name": "nightly-retrain"}).json()["id"] == mine
        )
        assert (
            client.get(
                _LOOKUP,
                params={"name": "nightly-retrain", "created_by": OTHER_USER},
            ).json()["id"]
            == theirs
        )

    @pytest.mark.parametrize(
        "name",
        [
            "team/nightly",
            "nightly report",
            "a?b&c=d",
            "hash#tag",
            "plus+signed",
            "100%",
            "unicode-\u00e9\u00e0\u4e2d\u6587",
        ],
        ids=[
            "slash",
            "space",
            "query-delimiters",
            "hash",
            "plus",
            "percent",
            "unicode",
        ],
    )
    def test_a_name_the_path_could_not_carry_round_trips(
        self, client: fastapi.testclient.TestClient, name: str
    ) -> None:
        """Why the key is a query parameter and not a path segment.

        `name` has no charset constraint, so every one of these is a legal subscription name.
        In a path segment the slash would have to be `%2F`, which proxies routinely normalise
        or reject; as a query parameter it is just a character.
        """
        created = client.post(_PATH, json=_payload(name=name)).json()
        found = client.get(_LOOKUP, params={"name": name})
        assert found.status_code == 200, found.text
        assert found.json()["id"] == created["id"]
        assert found.json()["name"] == name

    def test_a_missing_name_is_a_422(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The key is required: a bare `/lookup` is a malformed request, not a listing."""
        assert client.get(_LOOKUP).status_code == 422

    def test_an_empty_name_is_a_422(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        assert client.get(_LOOKUP, params={"name": ""}).status_code == 422

    def test_an_overlong_name_is_a_422(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Capped like every other name field, so an oversized key never reaches the query."""
        response = client.get(_LOOKUP, params={"name": "x" * (_MAX_NAME_LENGTH + 1)})
        assert response.status_code == 422


class TestPatchByName:
    """Editing by natural key. A body and a query string on one request is ordinary HTTP."""

    def test_it_edits_the_named_subscription(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        response = client.patch(
            _LOOKUP, params={"name": "nightly-retrain"}, json={"enabled": False}
        )
        assert response.status_code == 200, response.text
        assert response.json()["id"] == subscription_id
        assert response.json()["enabled"] is False

    def test_it_agrees_with_patching_by_id(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Both routes run the same body, so only the resolution step may differ."""
        client.post(_PATH, json=_payload(name="by-id"))
        client.post(_PATH, json=_payload(name="by-name"))
        edit: dict[str, Any] = {"condition": {"event": "z"}, "enabled": False}

        by_id_target = client.get(_LOOKUP, params={"name": "by-id"}).json()["id"]
        by_id = client.patch(f"{_PATH}/{by_id_target}", json=edit).json()
        by_name = client.patch(_LOOKUP, params={"name": "by-name"}, json=edit).json()

        ignored = {"id", "name", "created_at", "updated_at"}
        assert {k: v for k, v in by_id.items() if k not in ignored} == {
            k: v for k, v in by_name.items() if k not in ignored
        }

    def test_a_rename_by_name_moves_the_key_it_was_found_by(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """After a rename the old key is gone — the lookup key is the live name, not an alias."""
        client.post(_PATH, json=_payload())
        renamed = client.patch(
            _LOOKUP,
            params={"name": "nightly-retrain"},
            json={"name": "renamed"},
        )
        assert renamed.status_code == 200, renamed.text
        assert (
            client.get(_LOOKUP, params={"name": "nightly-retrain"}).status_code == 404
        )
        assert client.get(_LOOKUP, params={"name": "renamed"}).status_code == 200

    def test_a_rename_by_name_does_not_trigger(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The metadata-only path is shared, so a rename cannot start a run here either."""
        client.post(_PATH, json=_payload())
        body = client.patch(
            _LOOKUP,
            params={"name": "nightly-retrain"},
            json={"name": "renamed"},
        ).json()
        assert body["triggered"] is False
        assert body["triggered_cycle"] is None

    def test_a_rename_onto_a_taken_name_is_a_409(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        client.post(_PATH, json=_payload(name="first"))
        client.post(_PATH, json=_payload(name="second"))
        response = client.patch(
            _LOOKUP, params={"name": "second"}, json={"name": "first"}
        )
        assert response.status_code == 409, response.text

    def test_another_callers_subscription_is_a_403(
        self,
        client: fastapi.testclient.TestClient,
        other_user_client: fastapi.testclient.TestClient,
    ) -> None:
        """Resolving by name is not an authorization bypass: the service still refuses."""
        client.post(_PATH, json=_payload())
        response = other_user_client.patch(
            _LOOKUP,
            params={"name": "nightly-retrain", "created_by": DEFAULT_USER},
            json={"enabled": False},
        )
        assert response.status_code == 403, response.text

    def test_an_admin_may_edit_another_callers_subscription(
        self,
        client: fastapi.testclient.TestClient,
        admin_client: fastapi.testclient.TestClient,
    ) -> None:
        client.post(_PATH, json=_payload())
        response = admin_client.patch(
            _LOOKUP,
            params={"name": "nightly-retrain", "created_by": DEFAULT_USER},
            json={"enabled": False},
        )
        assert response.status_code == 200, response.text
        assert response.json()["enabled"] is False

    def test_an_admins_default_owner_is_still_themselves(
        self,
        client: fastapi.testclient.TestClient,
        admin_client: fastapi.testclient.TestClient,
    ) -> None:
        """Being an admin widens what you may edit, not what an omitted `created_by` means."""
        client.post(_PATH, json=_payload())
        response = admin_client.patch(
            _LOOKUP, params={"name": "nightly-retrain"}, json={"enabled": False}
        )
        assert response.status_code == 404, response.text
        assert ADMIN_USER in response.json()["detail"]

    def test_an_unknown_name_is_a_404(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        response = client.patch(
            _LOOKUP, params={"name": "no-such-name"}, json={"enabled": False}
        )
        assert response.status_code == 404, response.text

    def test_a_client_supplied_created_by_in_the_body_is_still_rejected(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """`created_by` is a key here, never a field: the body may not carry one."""
        client.post(_PATH, json=_payload())
        response = client.patch(
            _LOOKUP,
            params={"name": "nightly-retrain"},
            json={"created_by": OTHER_USER},
        )
        assert response.status_code == 422, response.text


class TestDeleteByName:
    def test_it_deletes_the_named_subscription(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        client.post(_PATH, json=_payload())
        assert (
            client.delete(_LOOKUP, params={"name": "nightly-retrain"}).status_code
            == 204
        )
        assert (
            client.get(_LOOKUP, params={"name": "nightly-retrain"}).status_code == 404
        )

    def test_the_name_frees_immediately(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Rows are hard-deleted, so a delete-by-name releases the key it used."""
        client.post(_PATH, json=_payload())
        client.delete(_LOOKUP, params={"name": "nightly-retrain"})
        assert client.post(_PATH, json=_payload()).status_code == 201

    def test_it_deletes_only_the_named_owners_row(
        self,
        client: fastapi.testclient.TestClient,
        other_user_client: fastapi.testclient.TestClient,
    ) -> None:
        """The dangerous mistake this route could make, and does not."""
        mine = client.post(_PATH, json=_payload()).json()["id"]
        theirs = other_user_client.post(_PATH, json=_their_payload()).json()["id"]
        assert (
            other_user_client.delete(
                _LOOKUP, params={"name": "nightly-retrain"}
            ).status_code
            == 204
        )
        assert client.get(f"{_PATH}/{mine}").status_code == 200
        assert client.get(f"{_PATH}/{theirs}").status_code == 404

    def test_another_callers_subscription_is_a_403(
        self,
        client: fastapi.testclient.TestClient,
        other_user_client: fastapi.testclient.TestClient,
    ) -> None:
        client.post(_PATH, json=_payload())
        response = other_user_client.delete(
            _LOOKUP,
            params={"name": "nightly-retrain", "created_by": DEFAULT_USER},
        )
        assert response.status_code == 403, response.text
        assert (
            client.get(_LOOKUP, params={"name": "nightly-retrain"}).status_code == 200
        )

    def test_an_admin_may_delete_another_callers_subscription(
        self,
        client: fastapi.testclient.TestClient,
        admin_client: fastapi.testclient.TestClient,
    ) -> None:
        client.post(_PATH, json=_payload())
        response = admin_client.delete(
            _LOOKUP,
            params={"name": "nightly-retrain", "created_by": DEFAULT_USER},
        )
        assert response.status_code == 204, response.text

    def test_an_unknown_name_is_a_404(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        assert client.delete(_LOOKUP, params={"name": "no-such"}).status_code == 404


class TestTheByNameWritesTakeTheSameLockAsTheIdOnes:
    """Found by review: a write addressed by name used to resolve its row with a plain read.

    Both doors reach the same two tables. `update_subscription` and `delete_subscription` write
    trigger_event_state, and an arriving emission locks the subscription first and touches
    trigger_event_state second. A write that never locked the subscription took those two in the
    opposite order, and opposite orders on the same pair of rows is the shape a deadlock needs.

    SQLite renders no FOR UPDATE, so the lock itself cannot be observed. What is asserted is the
    part a reader of this code can get wrong: that the write goes through the one locking helper
    every other writer uses, and does so before anything touches trigger_event_state. The spy
    and the statement log append to one list, so the assertion is about a single timeline.
    """

    @staticmethod
    def _timeline(
        *, monkeypatch: pytest.MonkeyPatch, db_engine: sqlalchemy.Engine
    ) -> list[str]:
        events: list[str] = []
        real = api_routes.service.lock_subscription_until_commit

        def _spy(
            *, session: orm.Session, subscription_id: str
        ) -> db_models.TriggerSubscription | None:
            events.append(f"LOCK {subscription_id}")
            return real(session=session, subscription_id=subscription_id)

        monkeypatch.setattr(api_routes.service, "lock_subscription_until_commit", _spy)

        def _record(
            conn, cursor, statement, parameters, context, executemany
        ) -> None:  # noqa: ANN001
            events.append(" ".join(statement.split()))

        sqlalchemy.event.listen(db_engine, "before_cursor_execute", _record)
        return events

    @staticmethod
    def _first(events: list[str], *, containing: str) -> int:
        for index, event in enumerate(events):
            if containing in event:
                return index
        raise AssertionError(f"nothing matched {containing!r}: {events}")

    def test_a_patch_by_name_locks_the_row_before_touching_event_state(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        events = self._timeline(monkeypatch=monkeypatch, db_engine=db_engine)

        response = client.patch(
            _LOOKUP,
            params={"name": "nightly-retrain"},
            json={"condition": {"op": "all", "children": [{"event": "a"}]}},
        )

        assert response.status_code == 200, response.text
        assert self._first(events, containing=f"LOCK {subscription_id}") < self._first(
            events, containing="trigger_event_state"
        )

    def test_a_delete_by_name_locks_the_row_before_touching_event_state(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        events = self._timeline(monkeypatch=monkeypatch, db_engine=db_engine)

        response = client.delete(_LOOKUP, params={"name": "nightly-retrain"})

        assert response.status_code == 204, response.text
        assert self._first(events, containing=f"LOCK {subscription_id}") < self._first(
            events, containing="trigger_event_state"
        )

    def test_the_lock_is_keyed_by_the_id_the_name_resolved_to(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The name is not lockable; the id it resolves to is, and it is the right id."""
        client.post(_PATH, json=_payload(name="other"))
        wanted = client.post(_PATH, json=_payload()).json()["id"]
        events = self._timeline(monkeypatch=monkeypatch, db_engine=db_engine)

        client.patch(
            _LOOKUP, params={"name": "nightly-retrain"}, json={"enabled": False}
        )

        assert [event for event in events if event.startswith("LOCK ")] == [
            f"LOCK {wanted}"
        ]

    def test_a_read_by_name_still_takes_no_lock(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A GET that locked rows would make listing a subscription block a trigger."""
        client.post(_PATH, json=_payload())
        events = self._timeline(monkeypatch=monkeypatch, db_engine=db_engine)

        assert (
            client.get(_LOOKUP, params={"name": "nightly-retrain"}).status_code == 200
        )
        assert [event for event in events if event.startswith("LOCK ")] == []

    def test_a_row_deleted_between_resolving_and_locking_is_a_404(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Resolving the id and locking it are two statements, so the row can vanish between.

        The concurrent DELETE is staged inside the lock call, which is the only moment the
        window is open. Losing that race is an ordinary "it's gone", not a 500.
        """
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        real = api_routes.service.lock_subscription_until_commit

        def _delete_then_lock(
            *, session: orm.Session, subscription_id: str
        ) -> db_models.TriggerSubscription | None:
            _delete_behind(db_engine=db_engine, subscription_id=subscription_id)
            return real(session=session, subscription_id=subscription_id)

        monkeypatch.setattr(
            api_routes.service,
            "lock_subscription_until_commit",
            _delete_then_lock,
        )

        response = client.patch(
            _LOOKUP, params={"name": "nightly-retrain"}, json={"enabled": False}
        )

        assert response.status_code == 404, response.text
        with orm.Session(bind=db_engine) as check:
            assert check.get(db_models.TriggerSubscription, subscription_id) is None


class TestARefusedWriteNeverTakesTheLock:
    """Found by review: the row lock used to be taken before anyone asked if the caller may write.

    A caller who is about to be told "not yours" would hold `SELECT ... FOR UPDATE` on somebody
    else's subscription for the rest of the request, and every emission arriving for that
    subscription queues behind it. The check needs nothing the lock provides: it reads
    `created_by`, which no route can patch.

    Same spy as the class above, so "was the lock taken" is one list to look at.
    """

    @pytest.fixture()
    def locks(self, monkeypatch: pytest.MonkeyPatch) -> list[str]:
        taken: list[str] = []
        real = api_routes.service.lock_subscription_until_commit

        def _spy(
            *, session: orm.Session, subscription_id: str
        ) -> db_models.TriggerSubscription | None:
            taken.append(subscription_id)
            return real(session=session, subscription_id=subscription_id)

        monkeypatch.setattr(api_routes.service, "lock_subscription_until_commit", _spy)
        return taken

    @pytest.fixture()
    def theirs(
        self, other_user_client: fastapi.testclient.TestClient
    ) -> dict[str, Any]:
        return other_user_client.post(_PATH, json=_their_payload()).json()

    def test_a_stranger_s_patch_by_id_is_refused_before_the_lock(
        self,
        client: fastapi.testclient.TestClient,
        theirs: dict[str, Any],
        locks: list[str],
    ) -> None:
        response = client.patch(f"{_PATH}/{theirs['id']}", json={"enabled": False})

        assert response.status_code == 403, response.text
        assert locks == []

    def test_a_stranger_s_delete_by_id_is_refused_before_the_lock(
        self,
        client: fastapi.testclient.TestClient,
        theirs: dict[str, Any],
        locks: list[str],
    ) -> None:
        response = client.delete(f"{_PATH}/{theirs['id']}")

        assert response.status_code == 403, response.text
        assert locks == []

    def test_a_stranger_s_patch_by_name_is_refused_before_the_lock(
        self,
        client: fastapi.testclient.TestClient,
        theirs: dict[str, Any],
        locks: list[str],
    ) -> None:
        response = client.patch(
            _LOOKUP,
            params={"name": theirs["name"], "created_by": OTHER_USER},
            json={"enabled": False},
        )

        assert response.status_code == 403, response.text
        assert locks == []

    def test_a_stranger_s_delete_by_name_is_refused_before_the_lock(
        self,
        client: fastapi.testclient.TestClient,
        theirs: dict[str, Any],
        locks: list[str],
    ) -> None:
        response = client.delete(
            _LOOKUP, params={"name": theirs["name"], "created_by": OTHER_USER}
        )

        assert response.status_code == 403, response.text
        assert locks == []

    def test_the_owner_s_write_still_takes_it(
        self,
        other_user_client: fastapi.testclient.TestClient,
        theirs: dict[str, Any],
        locks: list[str],
    ) -> None:
        """The control: an empty list must mean "refused", not "nothing locks any more"."""
        response = other_user_client.patch(
            f"{_PATH}/{theirs['id']}", json={"enabled": False}
        )

        assert response.status_code == 200, response.text
        assert locks == [theirs["id"]]


class TestWritesInsistOnAnExactKey:
    """The collation guard: why a write by name is stricter than a read by name.

    MySQL compares these columns under `utf8mb4_0900_ai_ci`, so in production `?name=nightly`
    resolves a row stored as `Nightly`. PostgreSQL and SQLite both compare case-sensitively, so
    neither can produce that match against a test engine — which is exactly the asymmetry the
    guard exists for, and the reason the guard itself is tested directly here rather than
    through a request that SQLite would simply answer 404.
    """

    def _row(self, *, name: str, created_by: str) -> db_models.TriggerSubscription:
        """A detached row: the guard reads two columns and never touches a session."""
        return db_models.TriggerSubscription(
            name=name,
            created_by=created_by,
            definition={"name": name, "condition": {"event": "a"}},
            pipeline_task_spec_from_user_pipeline_id=SEEDED_PIPELINE_ID,
        )

    def test_an_exact_key_passes(self) -> None:
        api_routes._reject_inexact_key(
            subscription=self._row(name="nightly", created_by=DEFAULT_USER),
            name="nightly",
            created_by=DEFAULT_USER,
        )

    def test_a_name_differing_only_by_case_is_a_409(self) -> None:
        with pytest.raises(fastapi.HTTPException) as caught:
            api_routes._reject_inexact_key(
                subscription=self._row(name="Nightly", created_by=DEFAULT_USER),
                name="nightly",
                created_by=DEFAULT_USER,
            )
        assert caught.value.status_code == 409

    def test_an_owner_differing_only_by_case_is_a_409(self) -> None:
        with pytest.raises(fastapi.HTTPException) as caught:
            api_routes._reject_inexact_key(
                subscription=self._row(name="nightly", created_by="Test@example.com"),
                name="nightly",
                created_by=DEFAULT_USER,
            )
        assert caught.value.status_code == 409

    def test_the_conflict_names_the_stored_spelling(self) -> None:
        """The message has to make the corrected retry obvious, not set a guessing game."""
        with pytest.raises(fastapi.HTTPException) as caught:
            api_routes._reject_inexact_key(
                subscription=self._row(name="Nightly", created_by=DEFAULT_USER),
                name="nightly",
                created_by=DEFAULT_USER,
            )
        assert "Nightly" in caught.value.detail
        assert "nightly" in caught.value.detail


class TestTheGuardIsOnTheWriteRoutesOnly:
    """That the guard is *wired* to PATCH and DELETE and not to GET.

    SQLite cannot produce the case-insensitive match that triggers it (nor could PostgreSQL),
    so resolution is stubbed
    out to return a row whose stored spelling differs from the key. That is the one thing this
    stub fakes; everything after it is the real route.
    """

    @pytest.fixture()
    def resolving_to_a_differently_cased_row(
        self,
        monkeypatch: pytest.MonkeyPatch,
        client: fastapi.testclient.TestClient,
    ) -> fastapi.testclient.TestClient:
        client.post(_PATH, json=_payload(name="Nightly"))

        real = api_routes._load_by_natural_key

        def _pretend_mysql_collation(
            *, session: orm.Session, name: str, created_by: str
        ) -> db_models.TriggerSubscription:
            return real(session=session, name="Nightly", created_by=created_by)

        monkeypatch.setattr(
            api_routes, "_load_by_natural_key", _pretend_mysql_collation
        )
        return client

    def test_a_read_accepts_the_case_insensitive_match(
        self,
        resolving_to_a_differently_cased_row: fastapi.testclient.TestClient,
    ) -> None:
        """A read is safe: the caller gets the row back and can see which one it is."""
        response = resolving_to_a_differently_cased_row.get(
            _LOOKUP, params={"name": "nightly"}
        )
        assert response.status_code == 200, response.text
        assert response.json()["name"] == "Nightly"

    def test_a_patch_refuses_it(
        self,
        resolving_to_a_differently_cased_row: fastapi.testclient.TestClient,
    ) -> None:
        response = resolving_to_a_differently_cased_row.patch(
            _LOOKUP, params={"name": "nightly"}, json={"enabled": False}
        )
        assert response.status_code == 409, response.text

    def test_a_delete_refuses_it(
        self,
        resolving_to_a_differently_cased_row: fastapi.testclient.TestClient,
    ) -> None:
        """The unrecoverable one: a case-blind delete would take the wrong row for good."""
        client = resolving_to_a_differently_cased_row
        assert client.delete(_LOOKUP, params={"name": "nightly"}).status_code == 409
        assert client.get(_LOOKUP, params={"name": "Nightly"}).status_code == 200

    def test_the_exact_key_still_writes(
        self,
        resolving_to_a_differently_cased_row: fastapi.testclient.TestClient,
    ) -> None:
        """The control: the guard must refuse the mismatch, not every write."""
        response = resolving_to_a_differently_cased_row.patch(
            _LOOKUP, params={"name": "Nightly"}, json={"enabled": False}
        )
        assert response.status_code == 200, response.text
        assert response.json()["enabled"] is False


class TestAnEditOnlyTriggersWhenItCouldHaveChangedTheAnswer:
    """The PATCH contract now that one handler serves every shape of edit.

    The two branches this replaced disagreed about a re-save: the metadata branch never
    evaluated, and the condition branch always did — so `{"condition": <what is already
    stored>}` could start a pipeline run while `{"name": "x"}` could not. Same edit in effect,
    different outcome depending on which field the caller happened to include.

    So the contract is per-field, not per-shape. `condition` re-evaluates only when the blob
    actually differs, `name` never does, and `enabled` does on one edge only: off->on, the
    single metadata edit that can change whether the condition may fire.

    A null `reason` is the tell that nothing was evaluated: every decline names itself.
    """

    @staticmethod
    def _fill(session: orm.Session, *, subscription_id: str, event: str) -> None:
        state = session.get(db_models.TriggerEventState, (subscription_id, event))
        assert state is not None
        state.filled_at = db_models.db_utils.utc_now()
        state.last_emission_event_id = "em-1"
        session.commit()

    @classmethod
    def _ready_to_fire(
        cls,
        client: fastapi.testclient.TestClient,
        session: orm.Session,
        *,
        name: str,
    ) -> dict[str, Any]:
        """An enabled subscription whose only event has already arrived.

        Loaded, so "did not trigger" can only mean the edit was inert — anything that reaches
        `maybe_trigger` fires. The two toggle tests share it so the direction is the one thing
        that differs between them.
        """
        created = client.post(
            _PATH, json=_payload(name=name, condition={"event": "a"})
        ).json()
        cls._fill(session, subscription_id=created["id"], event="a")
        return created

    def test_disabling_a_subscription_that_could_fire_does_not_trigger(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """Turning it off cannot make the condition start holding, so nothing is consulted.

        A null `reason` is the whole point: `subscription_disabled` would mean the edit did
        reach `maybe_trigger` and was turned away there, which is the wrong layer to stop at.
        """
        created = self._ready_to_fire(client, session, name="off")

        body = client.patch(f"{_PATH}/{created['id']}", json={"enabled": False}).json()

        assert body["triggered"] is False
        assert body["reason"] is None, "it was evaluated when it need not have been"
        assert body["triggered_cycle"] is None
        assert body["cycle"] == created["cycle"], "the fence counter did not move"
        # The arrival it would have consumed is untouched, waiting for the switch to come back.
        assert client.get(f"{_PATH}/{created['id']}").json()["live"] == {"a": "em-1"}

    def test_re_enabling_a_subscription_that_could_fire_triggers(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """The same subscription, the same arrival, the opposite toggle — and it fires.

        The off->on edit has to evaluate: every event is already filled, so no later emission
        will arrive to prompt a re-check and it would otherwise sit satisfied and dormant.
        """
        created = self._ready_to_fire(client, session, name="on")
        client.patch(f"{_PATH}/{created['id']}", json={"enabled": False})

        body = client.patch(f"{_PATH}/{created['id']}", json={"enabled": True}).json()

        assert body["triggered"] is True, body
        assert body["triggered_cycle"] == created["cycle"]

    def test_resending_the_stored_condition_does_not_trigger(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """An idempotent PATCH is not a way to start a run."""
        created = client.post(_PATH, json=_payload(condition={"event": "a"})).json()
        self._fill(session, subscription_id=created["id"], event="a")

        body = client.patch(
            f"{_PATH}/{created['id']}", json={"condition": {"event": "a"}}
        ).json()

        assert body["triggered"] is False
        assert body["reason"] is None
        assert body["cycle"] == created["cycle"]

    def test_changing_the_condition_still_triggers(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """The positive control, so the test above cannot pass by never triggering at all."""
        created = client.post(
            _PATH,
            json=_payload(
                condition={
                    "op": "all",
                    "children": [{"event": "a"}, {"event": "b"}],
                }
            ),
        ).json()
        self._fill(session, subscription_id=created["id"], event="a")

        body = client.patch(
            f"{_PATH}/{created['id']}", json={"condition": {"event": "a"}}
        ).json()

        assert body["triggered"] is True
        assert body["triggered_cycle"] == created["cycle"]

    def test_a_rename_onto_a_taken_name_is_still_a_conflict(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The 409 survived the merge on the path that carries no condition."""
        client.post(_PATH, json=_payload(name="taken"))
        other = client.post(_PATH, json=_payload(name="mine")).json()

        response = client.patch(f"{_PATH}/{other['id']}", json={"name": "taken"})

        assert response.status_code == 409, response.text


class TestPinningOnCreate:
    """`pipeline_task_spec_from_user_pipeline_version_key` on POST: the version a caller was handed, or nothing."""

    def test_omitting_the_version_stores_null_and_echoes_null(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """State ②: track whatever the pipeline's current version is when the trigger fires."""
        body = client.post(_PATH, json=_payload()).json()

        assert body["pipeline_task_spec_from_user_pipeline_version_key"] is None
        stored = session.get(db_models.TriggerSubscription, body["id"])
        assert stored is not None
        assert stored.pipeline_task_spec_from_user_pipeline_version_key is None

    def test_a_full_mode_version_is_resolved_stored_and_echoed(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """Checked against the row, not just the response.

        A route that echoed the posted version back without storing it would satisfy a
        response-only assertion while the subscription went on tracking current.
        """
        response = client.post(
            _PATH,
            json=_payload(
                pipeline_task_spec_from_user_pipeline_id=FULL_PIPELINE_ID,
                pipeline_task_spec_from_user_pipeline_version_key=PINNABLE_VERSION,
            ),
        )

        assert response.status_code == 201, response.text
        assert (
            response.json()["pipeline_task_spec_from_user_pipeline_version_key"]
            == PINNABLE_VERSION
        )
        stored = session.get(db_models.TriggerSubscription, response.json()["id"])
        assert stored is not None
        assert (
            stored.pipeline_task_spec_from_user_pipeline_version_key == PINNABLE_VERSION
        )

    def test_pinning_a_disabled_mode_pipeline_is_a_422_not_a_500(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The seeded pipeline's only row is the mutable head, which is not pinnable.

        Left to the database this is the composite foreign key's IntegrityError at flush --
        a 500 for a request that is well-formed and simply asks for something the pipeline
        cannot offer.
        """
        response = client.post(
            _PATH,
            json=_payload(
                pipeline_task_spec_from_user_pipeline_id=SEEDED_PIPELINE_ID,
                pipeline_task_spec_from_user_pipeline_version_key=PINNABLE_VERSION,
            ),
        )

        assert response.status_code == 422, response.text
        assert "full versioning" in response.json()["detail"]

    def test_an_unknown_version_is_a_422_naming_the_version(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        response = client.post(
            _PATH,
            json=_payload(
                pipeline_task_spec_from_user_pipeline_id=FULL_PIPELINE_ID,
                pipeline_task_spec_from_user_pipeline_version_key=UNKNOWN_VERSION,
            ),
        )

        assert response.status_code == 422, response.text
        assert UNKNOWN_VERSION in response.json()["detail"]

    def test_a_version_of_the_wrong_length_is_rejected_at_the_boundary(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        response = client.post(
            _PATH,
            json=_payload(
                pipeline_task_spec_from_user_pipeline_id=FULL_PIPELINE_ID,
                pipeline_task_spec_from_user_pipeline_version_key="abc",
            ),
        )

        assert response.status_code == 422, response.text


class TestTheTargetPipelineIsGuarded:
    """A subscription may only point at a live pipeline of the caller's.

    None of this is the foreign key's job and none of it is something the foreign key can do.
    Soft-deleted pipelines keep their row, so `REFERENCES pipeline(id)` is satisfied by a
    tombstone, and the constraint has no opinion at all about who owns the row.
    """

    def test_creating_against_a_soft_deleted_pipeline_is_a_404(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        response = client.post(
            _PATH,
            json=_payload(pipeline_task_spec_from_user_pipeline_id=DELETED_PIPELINE_ID),
        )

        assert response.status_code == 404, response.text

    def test_creating_against_another_users_pipeline_is_a_404_not_a_422(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """404 on purpose. A 403, or a 422 that says the version is unknown, would confirm
        the pipeline exists -- an existence oracle over every other tenant's ids."""
        response = client.post(
            _PATH,
            json=_payload(
                pipeline_task_spec_from_user_pipeline_id=OTHER_USER_PIPELINE_ID
            ),
        )

        assert response.status_code == 404, response.text

    def test_a_pin_against_another_users_pipeline_cannot_report_the_version(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The ordering guarantee: the pipeline check runs first, so the resolver's two
        distinct 422s are unreachable for a pipeline the caller does not own."""
        response = client.post(
            _PATH,
            json=_payload(
                pipeline_task_spec_from_user_pipeline_id=OTHER_USER_PIPELINE_ID,
                pipeline_task_spec_from_user_pipeline_version_key=UNKNOWN_VERSION,
            ),
        )

        assert response.status_code == 404, response.text
        assert UNKNOWN_VERSION not in response.json()["detail"]

    def test_an_admin_may_target_a_pipeline_they_do_not_own(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        """Consistent with `ensure_may_write`: an admin may already edit any subscription."""
        response = admin_client.post(
            _PATH,
            json=_payload(
                pipeline_task_spec_from_user_pipeline_id=OTHER_USER_PIPELINE_ID
            ),
        )

        assert response.status_code == 201, response.text

    def test_an_admin_still_cannot_target_a_soft_deleted_pipeline(
        self, admin_client: fastapi.testclient.TestClient
    ) -> None:
        """Ownership is a permission; a tombstone is not a pipeline."""
        response = admin_client.post(
            _PATH,
            json=_payload(pipeline_task_spec_from_user_pipeline_id=DELETED_PIPELINE_ID),
        )

        assert response.status_code == 404, response.text

    def test_moving_onto_a_soft_deleted_pipeline_is_a_404(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The guard runs against the target the request leaves behind, not the stored one."""
        created = client.post(_PATH, json=_payload()).json()

        response = client.patch(
            f"{_PATH}/{created['id']}",
            json={"pipeline_task_spec_from_user_pipeline_id": DELETED_PIPELINE_ID},
        )

        assert response.status_code == 404, response.text

    def test_moving_onto_another_users_pipeline_is_a_404(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        created = client.post(_PATH, json=_payload()).json()

        response = client.patch(
            f"{_PATH}/{created['id']}",
            json={"pipeline_task_spec_from_user_pipeline_id": OTHER_USER_PIPELINE_ID},
        )

        assert response.status_code == 404, response.text

    def test_an_unrelated_edit_does_not_re_guard_the_stored_pipeline(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """A rename must keep working after the target is deleted.

        Re-checking the stored target on every PATCH would make a subscription un-editable --
        and un-disableable, which is the one edit its owner most needs -- the moment somebody
        removed the pipeline it points at.
        """
        created = client.post(_PATH, json=_payload()).json()
        pipeline = session.get(user_pipeline_db_models.UserPipeline, SEEDED_PIPELINE_ID)
        assert pipeline is not None
        pipeline.deleted_at = db_utils.utc_now()
        session.commit()

        response = client.patch(f"{_PATH}/{created['id']}", json={"enabled": False})

        assert response.status_code == 200, response.text


class TestPinningOnUpdate:
    """PATCH: null unpins, omitted leaves it alone, and moving the target clears it."""

    def _pinned(self, client: fastapi.testclient.TestClient) -> dict[str, Any]:
        created = client.post(
            _PATH,
            json=_payload(
                pipeline_task_spec_from_user_pipeline_id=FULL_PIPELINE_ID,
                pipeline_task_spec_from_user_pipeline_version_key=PINNABLE_VERSION,
            ),
        )
        assert created.status_code == 201, created.text
        return created.json()

    def test_null_unpins(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        created = self._pinned(client)

        body = client.patch(
            f"{_PATH}/{created['id']}",
            json={"pipeline_task_spec_from_user_pipeline_version_key": None},
        ).json()

        assert body["pipeline_task_spec_from_user_pipeline_version_key"] is None
        stored = session.get(db_models.TriggerSubscription, created["id"])
        assert stored is not None
        assert stored.pipeline_task_spec_from_user_pipeline_version_key is None

    def test_an_empty_patch_leaves_the_pin_alone(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The distinction `model_fields_set` exists to make. Reading the attribute instead
        would see None for both and unpin here."""
        created = self._pinned(client)

        body = client.patch(f"{_PATH}/{created['id']}", json={}).json()

        assert (
            body["pipeline_task_spec_from_user_pipeline_version_key"]
            == PINNABLE_VERSION
        )

    def test_an_unrelated_edit_leaves_the_pin_alone(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        created = self._pinned(client)

        body = client.patch(f"{_PATH}/{created['id']}", json={"name": "renamed"}).json()

        assert (
            body["pipeline_task_spec_from_user_pipeline_version_key"]
            == PINNABLE_VERSION
        )

    def test_repinning_to_another_version_of_the_same_pipeline(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        created = self._pinned(client)

        body = client.patch(
            f"{_PATH}/{created['id']}",
            json={
                "pipeline_task_spec_from_user_pipeline_version_key": OTHER_PINNABLE_VERSION
            },
        ).json()

        assert (
            body["pipeline_task_spec_from_user_pipeline_version_key"]
            == OTHER_PINNABLE_VERSION
        )

    def test_repinning_to_an_unknown_version_is_a_422(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The create path's error arm does not cover this one; the update route needs its
        own, or a bad repin arrives as the composite foreign key's IntegrityError."""
        created = self._pinned(client)

        response = client.patch(
            f"{_PATH}/{created['id']}",
            json={"pipeline_task_spec_from_user_pipeline_version_key": UNKNOWN_VERSION},
        )

        assert response.status_code == 422, response.text

    def test_repinning_a_disabled_mode_pipeline_is_a_422(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        created = client.post(_PATH, json=_payload()).json()

        response = client.patch(
            f"{_PATH}/{created['id']}",
            json={
                "pipeline_task_spec_from_user_pipeline_version_key": PINNABLE_VERSION
            },
        )

        assert response.status_code == 422, response.text

    def test_moving_the_target_clears_the_pin(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """The stored key was resolved against the pipeline being moved away from, and a
        version is content-addressed -- so carrying it over either fails the composite foreign
        key or, when the new pipeline holds byte-identical content, silently switches to a
        version nobody chose."""
        created = self._pinned(client)

        body = client.patch(
            f"{_PATH}/{created['id']}",
            json={"pipeline_task_spec_from_user_pipeline_id": SEEDED_PIPELINE_ID},
        ).json()

        assert body["pipeline_task_spec_from_user_pipeline_id"] == SEEDED_PIPELINE_ID
        stored = session.get(db_models.TriggerSubscription, created["id"])
        assert stored is not None
        assert stored.pipeline_task_spec_from_user_pipeline_version_key is None

    def test_a_pin_cleared_by_a_move_comes_back_as_null(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """What makes clearing defensible rather than silent: the caller is told."""
        created = self._pinned(client)

        body = client.patch(
            f"{_PATH}/{created['id']}",
            json={"pipeline_task_spec_from_user_pipeline_id": SEEDED_PIPELINE_ID},
        ).json()

        assert body["pipeline_task_spec_from_user_pipeline_version_key"] is None

    def test_restating_the_same_target_does_not_clear_the_pin(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """A client that PATCHes back the whole object it just read sends the same id every
        time. Testing that an id was *sent* rather than that it *changed* drops its pin on
        every unrelated edit."""
        created = self._pinned(client)

        body = client.patch(
            f"{_PATH}/{created['id']}",
            json={
                "name": "renamed",
                "pipeline_task_spec_from_user_pipeline_id": FULL_PIPELINE_ID,
            },
        ).json()

        assert (
            body["pipeline_task_spec_from_user_pipeline_version_key"]
            == PINNABLE_VERSION
        )

    def test_moving_and_repinning_resolves_against_the_new_pipeline(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Resolve after the edit, not before it: the version belongs to the target the
        request leaves behind."""
        created = client.post(_PATH, json=_payload()).json()

        body = client.patch(
            f"{_PATH}/{created['id']}",
            json={
                "pipeline_task_spec_from_user_pipeline_id": FULL_PIPELINE_ID,
                "pipeline_task_spec_from_user_pipeline_version_key": PINNABLE_VERSION,
            },
        ).json()

        assert body["pipeline_task_spec_from_user_pipeline_id"] == FULL_PIPELINE_ID
        assert (
            body["pipeline_task_spec_from_user_pipeline_version_key"]
            == PINNABLE_VERSION
        )

    def test_moving_and_pinning_a_version_the_new_pipeline_lacks_is_a_422(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The mutant this catches resolves against the stored pipeline, where the seeded
        head does carry this version -- so it would succeed and store a key the new target
        has no row for."""
        created = self._pinned(client)

        response = client.patch(
            f"{_PATH}/{created['id']}",
            json={
                "pipeline_task_spec_from_user_pipeline_id": SEEDED_PIPELINE_ID,
                "pipeline_task_spec_from_user_pipeline_version_key": PINNABLE_VERSION,
            },
        )

        assert response.status_code == 422, response.text


def _soft_delete_pipeline(*, session: orm.Session, pipeline_id: str) -> None:
    """Delete the target out from under a subscription, the way the pipeline API does.

    A tombstone, not a DELETE: the row stays, so the foreign key is still satisfied and the
    subscription keeps pointing at something. That is exactly the state `target_pipeline_live` exists
    to report, and it cannot be reached through the trigger API, which refuses a dead target
    on every write.
    """
    pipeline = session.get(user_pipeline_db_models.UserPipeline, pipeline_id)
    assert pipeline is not None
    pipeline.deleted_at = db_utils.utc_now()
    session.commit()


class TestTargetLive:
    """Reads say whether the target pipeline is still there.

    Found by review: writes refuse a soft-deleted target, but reads said nothing about one, so
    a subscription whose pipeline had been deleted still reported `enabled: true` and a
    `missing` list -- it read as armed when nothing it could do would start a run.

    The field is on the shared response model rather than on the detail route alone, so a
    caller does not have to know which route answers the question.
    """

    def test_a_create_reports_a_live_target(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        body = client.post(_PATH, json=_payload()).json()

        assert body["target_pipeline_live"] is True

    def test_the_detail_route_reports_a_live_target(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]

        body = client.get(f"{_PATH}/{subscription_id}").json()

        assert body["target_pipeline_live"] is True

    def test_the_detail_route_turns_false_once_the_pipeline_is_deleted(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """The whole point: nothing wrote to the subscription, and its answer changed."""
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        assert (
            client.get(f"{_PATH}/{subscription_id}").json()["target_pipeline_live"]
            is True
        )

        _soft_delete_pipeline(session=session, pipeline_id=SEEDED_PIPELINE_ID)

        body = client.get(f"{_PATH}/{subscription_id}").json()
        assert body["target_pipeline_live"] is False
        # The fields that used to be the only story are unchanged, which is why they were
        # misleading on their own.
        assert body["enabled"] is True
        assert sorted(body["missing"]) == ["a", "b"]

    def test_the_lookup_route_agrees_with_the_detail_route(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        created = client.post(_PATH, json=_payload()).json()
        _soft_delete_pipeline(session=session, pipeline_id=SEEDED_PIPELINE_ID)

        by_id = client.get(f"{_PATH}/{created['id']}").json()
        by_name = client.get(
            _LOOKUP,
            params={"name": created["name"], "created_by": DEFAULT_USER},
        ).json()

        assert by_id["target_pipeline_live"] is False
        assert by_name["target_pipeline_live"] is False

    def test_the_listing_reports_each_row_separately(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """One dead target does not condemn the page, and one live one does not absolve it."""
        client.post(_PATH, json=_payload(name="doomed"))
        client.post(
            _PATH,
            json=_payload(
                name="survivor",
                pipeline_task_spec_from_user_pipeline_id=FULL_PIPELINE_ID,
            ),
        )
        _soft_delete_pipeline(session=session, pipeline_id=SEEDED_PIPELINE_ID)

        body = client.get(_PATH).json()

        by_name = {s["name"]: s["target_pipeline_live"] for s in body["subscriptions"]}
        assert by_name == {"doomed": False, "survivor": True}

    def test_the_listing_asks_the_database_once_for_the_whole_page(
        self,
        client: fastapi.testclient.TestClient,
        db_engine: sqlalchemy.Engine,
    ) -> None:
        """A per-row lookup would make a page cost `page_size` extra queries.

        Counted rather than asserted by eye: the batched call is an implementation choice a
        later edit could quietly undo, and the symptom would only ever show up under load.
        """
        for index in range(5):
            client.post(_PATH, json=_payload(name=f"sub-{index}"))
        statements: list[str] = []

        @sqlalchemy.event.listens_for(db_engine, "before_cursor_execute")
        def _record(  # type: ignore[no-untyped-def]
            conn, cursor, statement, parameters, context, executemany
        ) -> None:
            statements.append(statement)

        try:
            body = client.get(_PATH).json()
        finally:
            sqlalchemy.event.remove(db_engine, "before_cursor_execute", _record)

        assert len(body["subscriptions"]) == 5
        # `UserPipeline.__tablename__` rather than a literal: the table is called `pipeline`,
        # and a rename would otherwise turn this into a test that counts nothing and passes.
        pipeline_queries = [
            s
            for s in statements
            if f"FROM {user_pipeline_db_models.UserPipeline.__tablename__}" in s
        ]
        assert len(pipeline_queries) == 1, pipeline_queries

    def test_a_patch_reports_the_target_it_leaves_behind(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """Re-pointing at a live pipeline is how a caller recovers, so the PATCH response has
        to describe the target it now has rather than the one it arrived with."""
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        _soft_delete_pipeline(session=session, pipeline_id=SEEDED_PIPELINE_ID)
        assert (
            client.get(f"{_PATH}/{subscription_id}").json()["target_pipeline_live"]
            is False
        )

        body = client.patch(
            f"{_PATH}/{subscription_id}",
            json={"pipeline_task_spec_from_user_pipeline_id": FULL_PIPELINE_ID},
        ).json()

        assert body["target_pipeline_live"] is True

    def test_a_patch_that_does_not_touch_the_target_still_reports_it(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """A rename evaluates nothing -- `missing` is null on this path -- but liveness is a
        read, not an evaluation, so it is answered anyway."""
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        _soft_delete_pipeline(session=session, pipeline_id=SEEDED_PIPELINE_ID)

        body = client.patch(f"{_PATH}/{subscription_id}", json={"name": "r1"}).json()

        assert body["missing"] is None
        assert body["target_pipeline_live"] is False

    def test_a_dead_target_cannot_be_edited_onto_a_subscription(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """`target_pipeline_live: false` is only ever reachable by deleting the pipeline afterwards --
        the write guard is unchanged by this field, and this pins that it still refuses.
        """
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]

        response = client.patch(
            f"{_PATH}/{subscription_id}",
            json={"pipeline_task_spec_from_user_pipeline_id": DELETED_PIPELINE_ID},
        )

        assert response.status_code == 404, response.text

    def test_an_admin_reading_someone_elses_subscription_sees_the_same_answer(
        self,
        client: fastapi.testclient.TestClient,
        admin_client: fastapi.testclient.TestClient,
        session: orm.Session,
    ) -> None:
        """Liveness is not filtered by ownership: it is a fact about the row being returned,
        and the target id is in that same response already."""
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]
        _soft_delete_pipeline(session=session, pipeline_id=SEEDED_PIPELINE_ID)

        body = admin_client.get(f"{_PATH}/{subscription_id}").json()

        assert body["target_pipeline_live"] is False


class TestThePipelineIdIsStoredAsTheDatabaseSpellsIt:
    """A UUID has several legal spellings, and only one of them is in the pipeline table.

    Found by review: the guard canonicalized the id before looking the pipeline up, then the
    row stored whatever the request said. `uuid4().hex` prints 32 characters with no dashes,
    which found the pipeline and then named nothing at insert time -- a foreign-key violation,
    so a 500 on a well-formed request wherever foreign keys are enforced, which is everywhere
    except the default test engine.
    """

    # The seeded target, written the other three ways a client might reasonably send it. The
    # canonical form is derived rather than retyped so these stay one id, not four literals.
    UNDASHED = SEEDED_PIPELINE_ID.replace("-", "")
    UPPERCASE = SEEDED_PIPELINE_ID.upper()
    BRACED = f"{{{SEEDED_PIPELINE_ID}}}"
    UNDASHED_FULL = FULL_PIPELINE_ID.replace("-", "")

    def test_create_stores_the_canonical_form_of_an_undashed_id(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        created = client.post(
            _PATH,
            json=_payload(pipeline_task_spec_from_user_pipeline_id=self.UNDASHED),
        )

        assert created.status_code == 201, created.text
        stored = session.get(db_models.TriggerSubscription, created.json()["id"])
        assert stored is not None
        assert stored.pipeline_task_spec_from_user_pipeline_id == SEEDED_PIPELINE_ID

    def test_create_stores_the_canonical_form_of_an_uppercase_id(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """The quieter half: same length, so a case-insensitive collation accepts it and the
        row simply disagrees with every other reference to that pipeline."""
        created = client.post(
            _PATH,
            json=_payload(pipeline_task_spec_from_user_pipeline_id=self.UPPERCASE),
        )

        assert created.status_code == 201, created.text
        stored = session.get(db_models.TriggerSubscription, created.json()["id"])
        assert stored is not None
        assert stored.pipeline_task_spec_from_user_pipeline_id == SEEDED_PIPELINE_ID

    def test_the_response_echoes_the_canonical_id_not_the_one_sent(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """A client that stores what it gets back keeps the spelling the database agrees with."""
        body = client.post(
            _PATH,
            json=_payload(pipeline_task_spec_from_user_pipeline_id=self.UNDASHED),
        ).json()

        assert body["pipeline_task_spec_from_user_pipeline_id"] == SEEDED_PIPELINE_ID

    def test_create_survives_a_database_that_enforces_foreign_keys(
        self, fk_client: fastapi.testclient.TestClient
    ) -> None:
        """The regression itself. On the default engine the old code returned 201 too -- the
        constraint was inert -- so only this client can tell the fix from the bug.
        """
        created = fk_client.post(
            _PATH,
            json=_payload(pipeline_task_spec_from_user_pipeline_id=self.UNDASHED),
        )

        assert created.status_code == 201, created.text

    def test_a_retarget_survives_a_database_that_enforces_foreign_keys(
        self, fk_client: fastapi.testclient.TestClient
    ) -> None:
        """The update path writes the same column, and had the same defect."""
        subscription_id = fk_client.post(_PATH, json=_payload()).json()["id"]

        response = fk_client.patch(
            f"{_PATH}/{subscription_id}",
            json={
                "pipeline_task_spec_from_user_pipeline_id": FULL_PIPELINE_ID.replace(
                    "-", ""
                )
            },
        )

        assert response.status_code == 200, response.text
        assert (
            response.json()["pipeline_task_spec_from_user_pipeline_id"]
            == FULL_PIPELINE_ID
        )

    def test_resending_the_same_target_in_another_spelling_is_not_a_move(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """`target_moved` compares strings, so an unnormalized id would read as a re-target
        and take the pin down with it -- the documented consequence of moving.
        """
        created = client.post(
            _PATH,
            json=_payload(
                pipeline_task_spec_from_user_pipeline_id=FULL_PIPELINE_ID,
                pipeline_task_spec_from_user_pipeline_version_key=PINNABLE_VERSION,
            ),
        ).json()
        assert created["pipeline_task_spec_from_user_pipeline_version_key"] is not None

        body = client.patch(
            f"{_PATH}/{created['id']}",
            json={
                "pipeline_task_spec_from_user_pipeline_id": FULL_PIPELINE_ID.replace(
                    "-", ""
                )
            },
        ).json()

        assert body["pipeline_task_spec_from_user_pipeline_version_key"] == (
            created["pipeline_task_spec_from_user_pipeline_version_key"]
        )

    def test_a_spelling_the_column_cannot_hold_is_still_refused(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Braces are a legal UUID spelling to Python and 38 characters to the column, so the
        request model refuses them before normalization is reached. Pinned because widening
        that cap would silently start accepting a fourth spelling.
        """
        response = client.post(
            _PATH,
            json=_payload(pipeline_task_spec_from_user_pipeline_id=self.BRACED),
        )

        assert response.status_code == 422, response.text

    def test_something_that_is_not_a_uuid_is_still_refused_on_create(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        response = client.post(
            _PATH,
            json=_payload(pipeline_task_spec_from_user_pipeline_id="not-a-uuid"),
        )

        assert response.status_code == 422, response.text

    def test_something_that_is_not_a_uuid_is_still_refused_on_update(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Normalization moved ahead of the guard on this path, so the refusal has to be
        shown to come out at the same status it did before.
        """
        subscription_id = client.post(_PATH, json=_payload()).json()["id"]

        response = client.patch(
            f"{_PATH}/{subscription_id}",
            json={"pipeline_task_spec_from_user_pipeline_id": "not-a-uuid"},
        )

        assert response.status_code == 422, response.text

    def test_an_edit_repairs_a_row_that_was_written_before_the_fix(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """Rows already in the database hold whatever spelling their request used.

        Storing the checked id rather than the incoming one means any edit that reaches the
        guard rewrites the column in the form the pipeline table uses, so the broken rows heal
        on their next write instead of needing a backfill. Written directly, because the API
        can no longer produce one.
        """
        created = client.post(
            _PATH,
            json=_payload(pipeline_task_spec_from_user_pipeline_id=FULL_PIPELINE_ID),
        ).json()
        stored = session.get(db_models.TriggerSubscription, created["id"])
        assert stored is not None
        stored.pipeline_task_spec_from_user_pipeline_id = self.UNDASHED_FULL
        session.commit()

        body = client.patch(
            f"{_PATH}/{created['id']}",
            json={
                "pipeline_task_spec_from_user_pipeline_version_key": PINNABLE_VERSION
            },
        ).json()

        assert body["pipeline_task_spec_from_user_pipeline_id"] == FULL_PIPELINE_ID
        session.expire_all()
        assert (
            session.get(
                db_models.TriggerSubscription, created["id"]
            ).pipeline_task_spec_from_user_pipeline_id
            == FULL_PIPELINE_ID
        )


def _break_seeded_spec(*, session: orm.Session) -> None:
    """Leave the seeded pipeline alive, holding a current version that will not parse.

    `image` must be a string, so pydantic rejects the integer when the run is built. Written
    directly because the API refuses it -- which is the point: the rows that reach this state
    are the ones written before the routes existed, or under an older schema.
    """
    version = session.get(
        user_pipeline_db_models.UserPipelineVersion,
        (SEEDED_PIPELINE_ID, user_pipeline_db_models.CURRENT_VERSION_KEY),
    )
    assert version is not None
    version.root_pipeline_task = {
        "componentRef": {"spec": {"implementation": {"container": {"image": 5}}}}
    }
    session.commit()


class TestAnEditThatCannotStartItsRunAnswersInsteadOfFailing:
    """A PATCH is evaluated by the same `maybe_trigger` an arriving emission uses.

    So the reasons introduced for the fan-out are reachable from the edit path too, and the
    route reports them the same way it reports any other non-trigger: 200, `triggered: false`,
    a named `reason`. Before they were contained this PATCH was a 500 -- a well-formed request,
    aimed at a pipeline the caller owns and that is still there, answered with a stack trace
    and nothing the caller could act on.
    """

    def test_an_unbuildable_target_is_a_named_non_trigger_not_a_500(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        created = client.post(
            _PATH,
            json=_payload(
                name="unbuildable-on-patch",
                condition={
                    "op": "all",
                    "children": [{"event": "a"}, {"event": "b"}],
                },
            ),
        ).json()
        state = session.get(db_models.TriggerEventState, (created["id"], "a"))
        state.filled_at = db_models.db_utils.utc_now()
        state.last_emission_event_id = "em-1"
        session.commit()
        _break_seeded_spec(session=session)

        # Narrowing to the event already banked is what makes the condition hold.
        response = client.patch(
            f"{_PATH}/{created['id']}", json={"condition": {"event": "a"}}
        )

        assert response.status_code == 200
        body = response.json()
        assert body["triggered"] is False
        assert body["reason"] == "target_unbuildable"
        # The edit itself still applied: the PATCH is not rolled back by the failed run.
        assert body["condition"] == {"event": "a"}
        assert body["pipeline_run_id"] is None

    def test_the_condition_stays_satisfied_so_a_repair_can_still_fire_it(
        self, client: fastapi.testclient.TestClient, session: orm.Session
    ) -> None:
        """No cycle was spent, so the run is recoverable rather than lost.

        The arrival is still banked and the cycle is still 0, which is what lets a re-saved
        pipeline -- or a repoint to one that builds -- start the run on the next write.
        """
        created = client.post(
            _PATH,
            json=_payload(
                name="repairable",
                condition={
                    "op": "all",
                    "children": [{"event": "a"}, {"event": "b"}],
                },
            ),
        ).json()
        state = session.get(db_models.TriggerEventState, (created["id"], "a"))
        state.filled_at = db_models.db_utils.utc_now()
        state.last_emission_event_id = "em-1"
        session.commit()
        _break_seeded_spec(session=session)
        client.patch(f"{_PATH}/{created['id']}", json={"condition": {"event": "a"}})

        session.expire_all()
        subscription = session.get(db_models.TriggerSubscription, created["id"])
        assert subscription.cycle == 0
        assert (
            session.get(db_models.TriggerEventState, (created["id"], "a")).filled_at
            is not None
        )


class TestPipelineTemplatesOnSubscriptions:
    """The HTTP boundary for the `pipeline_templates` envelope on a subscription.

    What each rung rejects is tested in tests/templating/arguments/test_envelopes.py; what
    is tested here is the wiring, and the one thing that is genuinely different from a
    schedule -- a subscription is never scheduled, so `schedule_time` is refused at save.
    """

    def _create(
        self, client: fastapi.testclient.TestClient, **overrides: Any
    ) -> httpx.Response:
        return client.post(_PATH, json=_payload(**overrides))

    def test_templates_that_parse_are_stored_and_read_back(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """A create round-trips the envelope through `definition` and back out."""
        resp = self._create(
            client,
            pipeline_templates={"arguments": {"as_of_date": "{{ now | date }}"}},
        )
        assert resp.status_code == fastapi.status.HTTP_201_CREATED, resp.text
        assert resp.json()["pipeline_templates"] == {
            "arguments": {"as_of_date": "{{ now | date }}"}
        }

    def test_a_subscription_without_templates_still_reports_an_empty_envelope(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """`{"arguments": {}}`, not a missing field, so a client can tell no templates from
        an older server that does not know the field."""
        resp = self._create(client)
        assert resp.status_code == fastapi.status.HTTP_201_CREATED, resp.text
        assert resp.json()["pipeline_templates"] == {"arguments": {}}

    @pytest.mark.parametrize(
        "template",
        [
            "{{ schedule_time }}",
            "{{ coalesce(schedule_time, trigger_time) | date }}",
        ],
        ids=["on its own", "hidden in a coalesce arm"],
    )
    def test_schedule_time_is_refused_where_a_schedule_would_accept_it(
        self, client: fastapi.testclient.TestClient, template: str
    ) -> None:
        """The one asymmetry with the schedules API, and the reason kind reaches the ladder.

        A subscription has no schedule_time at any arrival, so this can never render -- refusing
        it at save beats failing every fire. A coalesce arm is refused for the same reason
        and not as an oversight: on a cron schedule the arm is the point, because a manual
        fire of one has none either, but a subscription has no arrival that supplies one,
        so falling back would be the only thing the expression ever did.
        """
        resp = self._create(
            client, pipeline_templates={"arguments": {"as_of_date": template}}
        )
        assert (
            resp.status_code == fastapi.status.HTTP_422_UNPROCESSABLE_CONTENT
        ), resp.text
        assert resp.json()["detail"] == (
            "Invalid template for 'as_of_date': 'schedule_time' is not available for a "
            "subscription; available sources are now, trigger_time"
        )

    def test_an_unknown_top_level_key_is_a_422_here_and_ignored_on_a_schedule(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """`extra="forbid"` on the subscription request models, absent on the schedule ones.

        It is why `pipeline_templates` had to be declared before a client could send it: an
        undeclared field is not quietly dropped here, it is a 422.
        """
        resp = self._create(client, pipeline_template={"arguments": {}})
        assert (
            resp.status_code == fastapi.status.HTTP_422_UNPROCESSABLE_CONTENT
        ), resp.text

    def test_an_unknown_key_inside_the_envelope_is_ignored(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The leniency is inside the envelope only -- rollout tolerance for a newer client
        sending a sibling this server does not know yet."""
        resp = self._create(
            client,
            pipeline_templates={
                "arguments": {"as_of_date": "{{ now | date }}"},
                "later": 1,
            },
        )
        assert resp.status_code == fastapi.status.HTTP_201_CREATED, resp.text
        assert resp.json()["pipeline_templates"] == {
            "arguments": {"as_of_date": "{{ now | date }}"}
        }

    def _patch(
        self,
        client: fastapi.testclient.TestClient,
        *,
        subscription_id: str,
        **body: Any,
    ) -> httpx.Response:
        return client.patch(f"{_PATH}/{subscription_id}", json=body)

    def test_a_patch_that_omits_the_field_leaves_the_stored_templates_alone(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Omitted is not empty. A rename must not silently drop the templates."""
        created = self._create(
            client,
            pipeline_templates={"arguments": {"as_of_date": "{{ now | date }}"}},
        )
        resp = self._patch(client, subscription_id=created.json()["id"], name="renamed")
        assert resp.status_code == fastapi.status.HTTP_200_OK, resp.text
        assert resp.json()["pipeline_templates"] == {
            "arguments": {"as_of_date": "{{ now | date }}"}
        }

    @pytest.mark.parametrize(
        "envelope",
        [{}, {"future_key": 1}],
        ids=["empty-envelope", "unknown-sibling-only"],
    )
    def test_an_envelope_naming_no_arguments_does_not_delete_them(
        self, client: fastapi.testclient.TestClient, envelope: dict[str, object]
    ) -> None:
        """Ignoring a newer client's key used to leave an empty map, which is the clear
        instruction -- so forward compatibility deleted the templates and answered 200.
        """
        created = self._create(
            client,
            pipeline_templates={"arguments": {"as_of_date": "{{ now | date }}"}},
        )

        resp = self._patch(
            client,
            subscription_id=created.json()["id"],
            pipeline_templates=envelope,
        )

        assert resp.status_code == fastapi.status.HTTP_200_OK, resp.text
        assert resp.json()["pipeline_templates"] == {
            "arguments": {"as_of_date": "{{ now | date }}"}
        }

    def test_an_empty_envelope_clears_the_stored_templates(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The other half of why the field is nullable: `{}` is an instruction, not a no-op."""
        created = self._create(
            client,
            pipeline_templates={"arguments": {"as_of_date": "{{ now | date }}"}},
        )
        resp = self._patch(
            client,
            subscription_id=created.json()["id"],
            pipeline_templates={"arguments": {}},
        )
        assert resp.status_code == fastapi.status.HTTP_200_OK, resp.text
        assert resp.json()["pipeline_templates"] == {"arguments": {}}

    def test_a_patch_replaces_the_stored_templates_wholesale(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """`region` is gone, not merged: the envelope is the unit, matching how a supplied
        condition replaces the stored one rather than merging into it."""
        created = self._create(
            client,
            pipeline_templates={
                "arguments": {"as_of_date": "{{ now | date }}", "region": "ca"}
            },
        )
        resp = self._patch(
            client,
            subscription_id=created.json()["id"],
            pipeline_templates={
                "arguments": {"as_of_date": "{{ trigger_time | date }}"}
            },
        )
        assert resp.status_code == fastapi.status.HTTP_200_OK, resp.text
        assert resp.json()["pipeline_templates"] == {
            "arguments": {"as_of_date": "{{ trigger_time | date }}"}
        }

    def test_a_rejected_patch_leaves_the_stored_templates_untouched(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """The 422 is raised before the service is called, so nothing partial is written."""
        created = self._create(
            client,
            pipeline_templates={"arguments": {"as_of_date": "{{ now | date }}"}},
        )
        subscription_id = created.json()["id"]
        rejected = self._patch(
            client,
            subscription_id=subscription_id,
            name="renamed",
            pipeline_templates={"arguments": {"as_of_date": "{{ schedule_time }}"}},
        )
        assert rejected.status_code == fastapi.status.HTTP_422_UNPROCESSABLE_CONTENT
        after = client.get(f"{_PATH}/{subscription_id}").json()
        assert after["pipeline_templates"] == {
            "arguments": {"as_of_date": "{{ now | date }}"}
        }
        assert after["name"] == "nightly-retrain"

    def test_storing_templates_does_not_disturb_the_condition_in_the_same_blob(
        self, client: fastapi.testclient.TestClient
    ) -> None:
        """Both live in `definition`, so the writer has to be additive rather than a rewrite."""
        created = self._create(client)
        resp = self._patch(
            client,
            subscription_id=created.json()["id"],
            pipeline_templates={"arguments": {"as_of_date": "{{ now | date }}"}},
        )
        assert resp.status_code == fastapi.status.HTTP_200_OK, resp.text
        assert resp.json()["condition"] == {
            "op": "all",
            "children": [{"event": "a"}, {"event": "b"}],
        }
