"""Unit tests for quota.api_routes -- the eight operator endpoints.

Three things are pinned here that the endpoints could plausibly get wrong and no other test
would catch:

- **PATCH promotes.** Raising a capacity produces no completion event, and the promotion sink is
  edge-triggered, so a group whose cap goes 0 -> 10 promotes nobody unless the handler runs the
  pass itself. The capacity tests assert on node status, not on a return count.
- **DELETE un-parks first.** `ON DELETE CASCADE` takes the claims away, and a node left at
  `UNINITIALIZED` with no claim row is unreachable forever -- the queued sweep does not select
  it and no promotion pass can find it. Asserted on `container_execution_status`, because a
  claim count going to zero is exactly what the bug looks like too.
- **Un-parking and deleting share one transaction.** A failure between them must leave every
  node still parked, never a node QUEUED with its claim already gone.

Pagination and authz follow `scheduling/pipelines/api_routes.py`; the tests here exist to catch
a divergence from it, not to re-specify it.
"""

import collections.abc
import contextlib
import datetime
from typing import Any

import fastapi
import pytest
import sqlalchemy
import sqlalchemy as sql
from fastapi import testclient
from sqlalchemy import orm

from cloud_pipelines_backend import api_router
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.emissions.handlers.quota import annotations
from cloud_pipelines_backend.quota import api_routes, db_models, promotion

KEY = annotations.QUOTA_GROUP_KEY
Status = bts.ContainerExecutionStatus
EPOCH = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)

OWNER = "alice@example.com"
STRANGER = "bob@example.com"
ADMIN = "root@example.com"

BASE = "/api/quota_groups"


def _url(key: str, *, kind: str = "id", suffix: str = "") -> str:
    """The instance URL for a group, addressed by id or by name.

    Every request in this file goes through here, so switching a test from the id form to the
    name form is one keyword and the two forms cannot drift apart in the assertions.

    Args:
        key: The id or the name, matching `kind`.
        kind: Which column `key` names -- the `{key_kind}` path segment.
        suffix: Anything after the key, e.g. "/promote" or "/claims?page_size=2".

    Returns:
        The path to request.
    """
    return f"{BASE}/{kind}/{key}{suffix}"


def _client(
    *,
    session: orm.Session,
    name: str = OWNER,
    admin: bool = False,
) -> testclient.TestClient:
    """A client whose requests run on the test's own session.

    Sharing the session is what lets a test assert on ORM objects straight after a request:
    the handler's commit and the test's query are the same transaction, so there is no window
    where the two disagree.

    Args:
        session: The test's session, handed to every handler.
        name: The caller's username.
        admin: Whether the caller carries the admin permission.

    Returns:
        A client bound to a fresh app with only the quota routes mounted.
    """
    app = fastapi.FastAPI()
    api_routes.setup_quota_group_routes(
        app=app,
        get_session=lambda: session,
        user_details_getter=lambda: api_router.UserDetails(
            name=name,
            permissions={"read": True, "write": True, "admin": admin},
        ),
    )
    return testclient.TestClient(app)


@pytest.fixture()
def client(session: orm.Session) -> testclient.TestClient:
    return _client(session=session)


def _make_group(
    *,
    session: orm.Session,
    name: str = "bq",
    capacity: int = 2,
    created_by: str = OWNER,
) -> db_models.QuotaGroup:
    group = db_models.QuotaGroup(name=name, capacity=capacity, created_by=created_by)
    session.add(group)
    session.commit()
    return group


def _make_node(
    *,
    session: orm.Session,
    node_id: str,
    group_name: str | None = "bq",
    status: Status = Status.QUEUED,
) -> bts.ExecutionNode:
    task_spec: dict[str, Any] = {}
    if group_name is not None:
        task_spec["annotations"] = {KEY: group_name}
    node = bts.ExecutionNode(task_spec=task_spec)
    node.id = node_id
    node.container_execution_status = status
    session.add(node)
    session.commit()
    return node


def _park(
    *,
    session: orm.Session,
    group_id: str,
    node_id: str,
    offset_seconds: int = 0,
) -> db_models.QuotaGroupClaim:
    """A node parked at UNINITIALIZED with a WAITING claim of a controlled age.

    `created_at` is set explicitly for the same reason as in test_quota_promotion.py: claims
    written in the same millisecond tie, and an oldest-first assertion would then be measuring
    the tie-break instead of the ordering. `offset_seconds` counts forward from EPOCH, so a
    larger value is a *younger* claim.
    """
    node = _make_node(session=session, node_id=node_id, status=Status.UNINITIALIZED)
    claim = db_models.QuotaGroupClaim(
        quota_group_id=group_id,
        execution_node_id=node.id,
        state=db_models.ClaimState.WAITING,
    )
    session.add(claim)
    session.commit()
    claim.created_at = EPOCH + datetime.timedelta(seconds=offset_seconds)
    session.commit()
    return claim


def _cancel_node(*, session: orm.Session, node_id: str) -> None:
    """Ask the node itself to terminate, the way a single-execution cancel does."""
    node = session.get(bts.ExecutionNode, node_id)
    assert node is not None
    node.extra_data = {"desired_state": "TERMINATED"}
    session.commit()


def _cancel_run_of(*, session: orm.Session, node_id: str) -> None:
    """Ask the node's *run* to terminate, the way cancelling a pipeline does.

    The run is not reachable from the node directly, so this builds the ancestry link the
    orchestrator joins through: node -> execution_ancestor -> pipeline_run.root_execution_id.
    """
    node = session.get(bts.ExecutionNode, node_id)
    assert node is not None
    root = _make_node(session=session, node_id=f"{node_id}-root", group_name=None)
    session.add(
        bts.ExecutionToAncestorExecutionLink(ancestor_execution=root, execution=node)
    )
    session.add(
        bts.PipelineRun(root_execution=root, extra_data={"desired_state": "TERMINATED"})
    )
    session.commit()


@contextlib.contextmanager
def _captured_sql(*, session: orm.Session) -> collections.abc.Iterator[list[str]]:
    """Collect every statement the engine executes inside the block.

    For the one property that cannot be asserted on a result: whether an UPDATE hands the
    database an expression to evaluate or a number Python already decided.

    Args:
        session: The test's session, whose bind is listened to.

    Yields:
        The list of statements, filled as they execute.
    """
    statements: list[str] = []
    engine = session.get_bind()

    def _record(conn: Any, cursor: Any, statement: str, *args: Any) -> None:
        statements.append(statement)

    sql.event.listen(engine, "before_cursor_execute", _record)
    try:
        yield statements
    finally:
        sql.event.remove(engine, "before_cursor_execute", _record)


def _outcomes(body: dict[str, Any]) -> dict[str, str]:
    """The report's nodes list as {execution_node_id: outcome}."""
    return {node["execution_node_id"]: node["outcome"] for node in body["nodes"]}


def _admit(
    *,
    session: orm.Session,
    group_id: str,
    node_id: str,
) -> db_models.QuotaGroupClaim:
    """A running member: node at RUNNING, claim ACTIVE. Counts against occupancy."""
    node = _make_node(session=session, node_id=node_id, status=Status.RUNNING)
    claim = db_models.QuotaGroupClaim(
        quota_group_id=group_id,
        execution_node_id=node.id,
        state=db_models.ClaimState.ACTIVE,
    )
    session.add(claim)
    session.commit()
    return claim


def _finished(
    *,
    session: orm.Session,
    group_id: str,
    node_id: str,
    offset_seconds: int = 0,
) -> db_models.QuotaGroupClaim:
    """A ledger row: the node ended and the sink released its claim.

    The pair matters -- a DONE claim always has a node in a terminal status, because the sink
    only runs on one. A test that set DONE on a RUNNING node would be checking a state the
    system cannot produce.
    """
    node = _make_node(session=session, node_id=node_id, status=Status.SUCCEEDED)
    claim = db_models.QuotaGroupClaim(
        quota_group_id=group_id,
        execution_node_id=node.id,
        state=db_models.ClaimState.DONE,
    )
    session.add(claim)
    session.commit()
    claim.created_at = EPOCH + datetime.timedelta(seconds=offset_seconds)
    session.commit()
    return claim


def _status_of(
    *,
    session: orm.Session,
    node_id: str,
) -> Status:
    session.expire_all()
    node = session.get(bts.ExecutionNode, node_id)
    assert node is not None
    return node.container_execution_status


def _claim_count(
    *,
    session: orm.Session,
    group_id: str,
) -> int:
    session.expire_all()
    return len(
        session.scalars(
            sql.select(db_models.QuotaGroupClaim).where(
                db_models.QuotaGroupClaim.quota_group_id == group_id
            )
        ).all()
    )


class TestCreate:
    def test_creates_a_group_owned_by_the_caller(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        response = client.post(BASE, json={"name": "bq", "capacity": 3})

        assert response.status_code == 201
        body = response.json()
        assert body["name"] == "bq"
        assert body["capacity"] == 3
        assert body["created_by"] == OWNER
        assert body["version"] == 0

    def test_capacity_zero_is_a_valid_group(
        self, client: testclient.TestClient
    ) -> None:
        # Not a validation error: capacity 0 is the documented way to pause a group.
        assert (
            client.post(BASE, json={"name": "paused", "capacity": 0}).status_code == 201
        )

    def test_negative_capacity_is_rejected(self, client: testclient.TestClient) -> None:
        assert (
            client.post(BASE, json={"name": "bad", "capacity": -1}).status_code == 422
        )

    def test_duplicate_name_is_a_conflict_not_a_crash(
        self, client: testclient.TestClient
    ) -> None:
        client.post(BASE, json={"name": "bq", "capacity": 1})

        response = client.post(BASE, json={"name": "bq", "capacity": 5})

        # Without the pre-check this is uq_quota_group_name surfacing as a 500.
        assert response.status_code == 409


class TestListAndGet:
    def test_counts_split_waiting_from_active(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session, capacity=1)
        _admit(session=session, group_id=group.id, node_id="running-1")
        _park(session=session, group_id=group.id, node_id="parked-1")

        body = client.get(_url(group.id)).json()

        assert body["active_count"] == 1
        assert body["waiting_count"] == 1
        # Occupancy is the gate's number, read live from node status: the RUNNING member counts,
        # the parked one does not.
        assert body["occupancy"] == 1

    def test_detail_inlines_the_claims(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session)
        _park(session=session, group_id=group.id, node_id="parked-1")

        body = client.get(_url(group.id)).json()

        assert [c["execution_node_id"] for c in body["claims"]] == ["parked-1"]

    def test_unknown_id_is_404(self, client: testclient.TestClient) -> None:
        assert client.get(_url("nope")).status_code == 404

    def test_pagination_walks_every_group_exactly_once(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        for i in range(5):
            _make_group(session=session, name=f"g{i}", capacity=1)

        seen: list[str] = []
        token: str | None = None
        for _ in range(
            10
        ):  # Bounded so a cursor that fails to advance fails the test, not the runner.
            query = f"{BASE}?page_size=2" + (f"&page_token={token}" if token else "")
            body = client.get(query).json()
            seen.extend(g["id"] for g in body["quota_groups"])
            token = body["next_page_token"]
            if not token:
                break

        assert len(seen) == 5
        assert len(set(seen)) == 5
        # total_count is the collection, not the page.
        assert client.get(f"{BASE}?page_size=2").json()["total_count"] == 5

    def test_malformed_page_token_is_422(self, client: testclient.TestClient) -> None:
        assert client.get(f"{BASE}?page_token=garbage").status_code == 422

    def test_claims_are_listed_oldest_first(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session, capacity=0)
        _park(
            session=session,
            group_id=group.id,
            node_id="young",
            offset_seconds=100,
        )
        _park(session=session, group_id=group.id, node_id="old", offset_seconds=0)

        body = client.get(_url(group.id, suffix="/claims")).json()

        # The order promotion walks them in, so a reviewer reading the list sees who is next.
        assert [c["execution_node_id"] for c in body["claims"]] == [
            "old",
            "young",
        ]

    def test_claims_pagination_walks_every_claim_exactly_once(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session, capacity=0)
        for i in range(5):
            _park(
                session=session,
                group_id=group.id,
                node_id=f"n{i}",
                offset_seconds=i,
            )

        seen: list[str] = []
        token: str | None = None
        for _ in range(10):
            query = _url(group.id, suffix="/claims?page_size=2") + (
                f"&page_token={token}" if token else ""
            )
            body = client.get(query).json()
            seen.extend(c["execution_node_id"] for c in body["claims"])
            token = body["next_page_token"]
            if not token:
                break

        assert seen == ["n0", "n1", "n2", "n3", "n4"]


class TestTheClaimsPageCarriesNoBlob:
    """The claims page sorts, and MySQL's filesort carries whatever the SELECT selected.

    `extra_data` is the claim's unbounded `{"history": [...]}` and no caller ever receives it,
    so the allow-list on the read exists to keep it out of that payload. The behaviour that
    can regress silently is not the response -- it is which columns arrive, so that is what
    is asserted here.
    """

    def test_the_page_still_serializes_every_field_it_promises(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        """The allow-list is the failure mode: trim one column too many and this goes red."""
        group = _make_group(session=session, capacity=0)
        _park(session=session, group_id=group.id, node_id="n0")

        claim = client.get(_url(group.id, suffix="/claims")).json()["claims"][0]

        assert set(claim) == {
            "quota_group_id",
            "execution_node_id",
            "state",
            "created_at",
            "updated_at",
        }
        assert all(v is not None for v in claim.values())

    def test_the_wide_columns_never_reach_the_sort(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        """Assert on the SQL the endpoint emits, not on a query the test built itself.

        A test that constructs its own `load_only` proves only that SQLAlchemy works. What
        can regress here is this endpoint's column list, so the statement it sends is what is
        captured.

        `raiseload=True` is deliberately not asserted: nothing in the request path touches a
        deferred column, so today it emits no SQL either way and there is nothing to observe.
        It is a guard against a future reader, and the honest place to say so is here rather
        than in a test that would pass with it removed.
        """
        group = _make_group(session=session, capacity=0)
        _park(session=session, group_id=group.id, node_id="n0")
        session.commit()

        statements: list[str] = []

        @sqlalchemy.event.listens_for(session.get_bind(), "before_cursor_execute")
        def _record(conn, cursor, statement, parameters, context, executemany):  # type: ignore[no-untyped-def]
            statements.append(statement)

        try:
            assert client.get(_url(group.id, suffix="/claims")).status_code == 200
        finally:
            sqlalchemy.event.remove(
                session.get_bind(), "before_cursor_execute", _record
            )

        page_selects = [
            s for s in statements if "FROM quota_group_claim" in s and "count(" not in s
        ]
        assert page_selects, "the claims page issued no SELECT to inspect"
        # `parked_at` is checked as well as `extra_data` on purpose: it was added to the model
        # after the allow-list was written and stayed out without the list being touched,
        # which is the whole reason this is an allow-list and not `defer(extra_data)`.
        for column in ("extra_data", "parked_at"):
            assert not any(
                column in s for s in page_selects
            ), f"{column} is being fetched, and the ORDER BY carries it"


class TestTheLedgerStaysOutOfTheWay:
    """DONE claims accumulate for the life of a group and must be invisible unless asked for.

    Every unfiltered read is a place a 4,000-row history could reach a client or a COUNT, so
    each one is pinned with a group holding far more DONE rows than live ones.
    """

    @staticmethod
    def _group_with_a_history(*, session: orm.Session) -> db_models.QuotaGroup:
        """One running member, one waiter, and three finished nodes behind them."""
        group = _make_group(session=session, capacity=1)
        _admit(session=session, group_id=group.id, node_id="running-1")
        _park(
            session=session,
            group_id=group.id,
            node_id="parked-1",
            offset_seconds=50,
        )
        for i in range(3):
            _finished(
                session=session,
                group_id=group.id,
                node_id=f"done-{i}",
                offset_seconds=i,
            )
        return group

    def test_the_counts_ignore_done(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = self._group_with_a_history(session=session)

        body = client.get(_url(group.id)).json()

        assert body["active_count"] == 1
        assert body["waiting_count"] == 1

    def test_occupancy_ignores_done(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        # The three finished nodes are terminal, so they never occupied anything -- but the
        # occupancy query now filters on claim state as well as node status, and getting that
        # filter wrong in the other direction would drop the live member too.
        group = self._group_with_a_history(session=session)

        assert client.get(_url(group.id)).json()["occupancy"] == 1

    def test_the_detail_endpoint_inlines_only_live_claims(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        # Unpaginated, so this is the response that grows without bound if the filter is lost.
        group = self._group_with_a_history(session=session)

        body = client.get(_url(group.id)).json()

        assert sorted(c["execution_node_id"] for c in body["claims"]) == [
            "parked-1",
            "running-1",
        ]

    def test_the_claims_endpoint_hides_done_by_default(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = self._group_with_a_history(session=session)

        body = client.get(_url(group.id, suffix="/claims")).json()

        assert sorted(c["execution_node_id"] for c in body["claims"]) == [
            "parked-1",
            "running-1",
        ]
        # The count must describe the same set as the rows under it, or a page of two claims
        # reports a total of five.
        assert body["total_count"] == 2

    @pytest.mark.parametrize(
        ("state", "expected"),
        [
            ("WAITING", ["parked-1"]),
            ("ACTIVE", ["running-1"]),
            ("DONE", ["done-0", "done-1", "done-2"]),
        ],
    )
    def test_a_state_filter_selects_exactly_that_state(
        self,
        session: orm.Session,
        client: testclient.TestClient,
        state: str,
        expected: list[str],
    ) -> None:
        # One state or the live pair; there is deliberately no value that returns everything.
        group = self._group_with_a_history(session=session)

        body = client.get(_url(group.id, suffix=f"/claims?state={state}")).json()

        assert sorted(c["execution_node_id"] for c in body["claims"]) == expected
        assert body["total_count"] == len(expected)

    def test_an_unknown_state_is_422(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        # Rejected by the OpenAPI schema, not by a hand-written check, so a generated client
        # cannot send it in the first place.
        group = _make_group(session=session)

        assert (
            client.get(_url(group.id, suffix="/claims?state=RELEASED")).status_code
            == 422
        )

    def test_the_ledger_pages(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        # The keyset index gained execution_node_id for exactly this walk.
        group = _make_group(session=session, capacity=1)
        for i in range(5):
            _finished(
                session=session,
                group_id=group.id,
                node_id=f"done-{i}",
                offset_seconds=i,
            )

        seen: list[str] = []
        token: str | None = None
        for _ in range(10):
            query = _url(group.id, suffix="/claims?state=DONE&page_size=2") + (
                f"&page_token={token}" if token else ""
            )
            body = client.get(query).json()
            seen.extend(c["execution_node_id"] for c in body["claims"])
            token = body["next_page_token"]
            if not token:
                break

        assert seen == ["done-0", "done-1", "done-2", "done-3", "done-4"]

    def test_a_page_of_groups_reports_the_same_numbers_as_each_detail(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        # The list endpoint batches the counts into two GROUP BYs while the detail endpoint
        # runs them one group at a time. Two code paths for one number, so they are compared
        # rather than each asserted against a literal.
        for i in range(3):
            group = _make_group(session=session, name=f"g{i}", capacity=1)
            _admit(session=session, group_id=group.id, node_id=f"running-{i}")
            _finished(session=session, group_id=group.id, node_id=f"done-{i}")
            for w in range(i):
                _park(
                    session=session,
                    group_id=group.id,
                    node_id=f"parked-{i}-{w}",
                    offset_seconds=w,
                )

        listed = {
            g["id"]: g
            for g in client.get(f"{BASE}?page_size=100").json()["quota_groups"]
        }

        assert len(listed) == 3
        for group_id, row in listed.items():
            detail = client.get(_url(group_id)).json()
            assert (
                row["active_count"],
                row["waiting_count"],
                row["occupancy"],
            ) == (
                detail["active_count"],
                detail["waiting_count"],
                detail["occupancy"],
            )

    def test_a_group_with_nothing_live_reports_zeroes(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        # A GROUP BY emits no row for a group it found nothing for, so the batched path has to
        # zero-fill rather than default at the call site -- otherwise this 404s on a KeyError.
        group = _make_group(session=session, capacity=1)
        _finished(session=session, group_id=group.id, node_id="done-0")

        row = client.get(f"{BASE}?page_size=100").json()["quota_groups"][0]

        assert (
            row["id"],
            row["active_count"],
            row["waiting_count"],
            row["occupancy"],
        ) == (group.id, 0, 0, 0)


class TestTrailingSlash:
    """The no-slash path is canonical; the slashed one redirects to it."""

    def test_the_collection_slash_redirects_to_the_canonical_path(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        response = client.get(f"{BASE}/", follow_redirects=False)

        # Starlette's redirect_slashes default, kept deliberately: every other router in this
        # app behaves the same way, and turning it off here would make quota the odd one out.
        # The cost is that a client following a 307 on a POST may re-issue it as a GET, which is
        # why the documented paths never carry the slash.
        assert response.status_code == 307
        assert response.headers["location"].endswith(BASE)

    def test_an_instance_slash_redirects_too(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session)

        response = client.get(_url(group.id, suffix="/"), follow_redirects=False)

        assert response.status_code == 307
        assert response.headers["location"].endswith(_url(group.id))

    def test_the_canonical_paths_do_not_redirect(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session)

        assert client.get(BASE, follow_redirects=False).status_code == 200
        assert client.get(_url(group.id), follow_redirects=False).status_code == 200


class TestNameValidation:
    """The name is an addressing key, so it has to survive a URL path unescaped."""

    _REJECTED = [
        "BQ",
        "bq_prod",
        "-bq",
        "bq-",
        "bq prod",
        "bq/prod",
        "bq.prod",
        "",
        "b" * 64,
    ]
    _ACCEPTED = ["bq", "bq-prod", "bq-prod-2", "deadbeefdeadbeefdead", "b" * 63]

    @pytest.mark.parametrize("name", _REJECTED)
    def test_create_rejects_a_name_that_is_not_a_url_safe_label(
        self, client: testclient.TestClient, name: str
    ) -> None:
        assert client.post(BASE, json={"name": name, "capacity": 1}).status_code == 422

    @pytest.mark.parametrize("name", _ACCEPTED)
    def test_a_label_is_accepted_and_addressable(
        self, client: testclient.TestClient, name: str
    ) -> None:
        assert client.post(BASE, json={"name": name, "capacity": 1}).status_code == 201
        # Accepted means usable as a key, which is the only reason the rule exists.
        assert client.get(_url(name, kind="name")).status_code == 200

    def test_the_pattern_reaches_the_openapi_schema(
        self, client: testclient.TestClient
    ) -> None:
        # Generated clients enforce it before the call, which is half the value of using
        # `pattern` rather than a validator with a nicer message.
        schema = client.get("/openapi.json").json()["components"]["schemas"][
            "QuotaGroupCreateRequest"
        ]

        assert (
            schema["properties"]["name"]["pattern"]
            == r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?$"
        )
        assert schema["properties"]["name"]["maxLength"] == 63


class TestAddressing:
    """A group answers to `/id/{id}` and to `/name/{name}`, and the kind segment says which."""

    @pytest.mark.parametrize(
        ("method", "suffix", "payload"),
        [
            ("get", "", None),
            ("patch", "", {"capacity": 9}),
            ("get", "/claims", None),
            ("delete", "/claims/parked-1", None),
            ("post", "/promote", None),
            ("delete", "", None),
        ],
    )
    def test_every_instance_route_takes_the_name_form(
        self,
        session: orm.Session,
        client: testclient.TestClient,
        method: str,
        suffix: str,
        payload: dict[str, Any] | None,
    ) -> None:
        group = _make_group(session=session, name="bq-prod")
        _park(session=session, group_id=group.id, node_id="parked-1")

        response = client.request(
            method.upper(),
            _url("bq-prod", kind="name", suffix=suffix),
            json=payload,
        )

        assert response.status_code == 200
        # Whichever key came in, the id is what goes out: names are mutable, ids are not.
        body = response.json()
        assert body.get("quota_group_id", body.get("id", group.id)) == group.id

    def test_both_forms_read_the_same_row(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session, name="bq-prod")

        by_id = client.get(_url(group.id)).json()
        by_name = client.get(_url("bq-prod", kind="name")).json()

        assert by_id == by_name

    def test_a_name_shaped_like_an_id_is_not_confused(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        # 20 hex characters is exactly what generate_unique_id produces, and it is also a legal
        # kebab-case name. This is the case that makes shape-sniffing unworkable and the kind
        # segment necessary.
        group = _make_group(session=session, name="deadbeefdeadbeefdead")

        assert (
            client.get(_url("deadbeefdeadbeefdead", kind="name")).json()["id"]
            == group.id
        )
        assert client.get(_url("deadbeefdeadbeefdead")).status_code == 404

    def test_an_unknown_kind_is_422(self, client: testclient.TestClient) -> None:
        # The Literal is enforced by the router, so a third kind never reaches a handler.
        assert client.get(_url("bq", kind="uuid")).status_code == 422

    def test_the_404_names_the_kind_that_missed(
        self, client: testclient.TestClient
    ) -> None:
        detail = client.get(_url("bq", kind="name")).json()["detail"]

        # "not found" alone leaves the caller wondering whether they used the wrong key or the
        # wrong kind of key.
        assert "name" in detail
        assert "bq" in detail


class TestAuthorization:
    """Creator or admin may mutate; anyone may read."""

    def test_a_stranger_may_read(self, session: orm.Session) -> None:
        group = _make_group(session=session)

        response = _client(session=session, name=STRANGER).get(_url(group.id))

        assert response.status_code == 200

    @pytest.mark.parametrize(
        ("method", "suffix", "payload"),
        [
            ("patch", "", {"capacity": 9}),
            ("delete", "", None),
            ("post", "/promote", None),
        ],
    )
    def test_a_stranger_may_not_mutate(
        self,
        session: orm.Session,
        method: str,
        suffix: str,
        payload: dict[str, Any] | None,
    ) -> None:
        group = _make_group(session=session)
        stranger = _client(session=session, name=STRANGER)

        response = stranger.request(
            method.upper(), _url(group.id, suffix=suffix), json=payload
        )

        assert response.status_code == 403

    def test_the_creator_may_mutate(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session, created_by=OWNER)

        assert client.patch(_url(group.id), json={"capacity": 9}).status_code == 200

    def test_an_admin_may_mutate_someone_elses_group(
        self, session: orm.Session
    ) -> None:
        group = _make_group(session=session, created_by=OWNER)

        response = _client(session=session, name=ADMIN, admin=True).patch(
            _url(group.id), json={"capacity": 9}
        )

        assert response.status_code == 200

    def test_release_claim_is_owner_only(self, session: orm.Session) -> None:
        group = _make_group(session=session)
        _park(session=session, group_id=group.id, node_id="parked-1")

        response = _client(session=session, name=STRANGER).delete(
            _url(group.id, suffix="/claims/parked-1")
        )

        assert response.status_code == 403


class TestPatch:
    def test_a_name_cannot_be_changed(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        # Not a no-op: a client still sending the old field is told. A rename would move every
        # future node to a different group while the ones already parked stayed behind.
        group = _make_group(session=session, name="mine", capacity=1)

        assert client.patch(_url(group.id), json={"name": "renamed"}).status_code == 422
        assert client.get(_url(group.id)).json()["name"] == "mine"

    def test_a_name_is_not_settable_alongside_a_real_edit(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        # The whole body is rejected, so the capacity change does not land either. Without
        # extra="forbid" this would be a 200 that silently ignored half of what was asked.
        group = _make_group(session=session, name="mine", capacity=1)

        assert (
            client.patch(
                _url(group.id), json={"name": "renamed", "capacity": 5}
            ).status_code
            == 422
        )
        assert client.get(_url(group.id)).json()["capacity"] == 1

    def test_the_update_schema_has_no_name_field(
        self, client: testclient.TestClient
    ) -> None:
        # Generated clients should not offer a rename at all.
        schema = client.get("/openapi.json").json()["components"]["schemas"][
            "QuotaGroupUpdateRequest"
        ]

        assert "name" not in schema["properties"]
        assert schema.get("additionalProperties") is False

    def test_every_edit_bumps_the_version(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session, capacity=2)

        # An admission that read the group before the edit has to lose the CAS and re-read.
        edited = client.patch(_url(group.id), json={"capacity": 3}).json()

        assert edited["version"] == 1
        assert edited["capacity"] == 3

    def test_an_empty_edit_still_bumps_the_version_and_promotes(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        # Spare capacity with a node still parked is the state S3 leaves behind: no member ever
        # finished, so nothing fired the edge-triggered sink and the waiter sat there. Guards
        # the "every" in "promote after every version bump": moving the promotion pass inside
        # `if request.capacity is not None` would leave this node parked and every
        # capacity-based test still green.
        group = _make_group(session=session, capacity=2)
        _park(session=session, group_id=group.id, node_id="stranded")

        edited = client.patch(_url(group.id), json={}).json()

        assert edited["version"] == 1
        assert _status_of(session=session, node_id="stranded") == Status.QUEUED

    def test_the_version_bump_is_computed_by_the_database(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        """The bump must be relative, and this asserts the SQL rather than the result.

        `group.version += 1` and `version = version + 1` produce the same number in every
        single-writer test, so no behavioural assertion tells them apart here: the difference
        only shows when a concurrent admission commits between this request's SELECT and its
        UPDATE, and a TestClient sharing one session cannot interleave that. What separates
        them is what reaches the database -- a literal computed in Python, or an expression
        the database evaluates against the row as it stands. So that is what is checked.
        """
        group = _make_group(session=session, capacity=1)

        with _captured_sql(session=session) as statements:
            client.patch(_url(group.id), json={"capacity": 2})

        updates = [
            s for s in statements if s.lstrip().upper().startswith("UPDATE QUOTA_GROUP")
        ]
        assert updates, statements
        # `version + ?` rather than a bound literal in the SET list.
        assert any("version + " in s for s in updates), updates

    def test_raising_capacity_promotes_up_to_the_new_limit_oldest_first(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session, capacity=0)
        for i, node_id in enumerate(["first", "second", "third", "fourth"]):
            _park(
                session=session,
                group_id=group.id,
                node_id=node_id,
                offset_seconds=i,
            )

        client.patch(_url(group.id), json={"capacity": 2})

        # The whole point of step 46: no member finished, so nothing fired the sink. If PATCH did
        # not run the pass itself, all four would still be parked.
        assert _status_of(session=session, node_id="first") == Status.QUEUED
        assert _status_of(session=session, node_id="second") == Status.QUEUED
        # And no further: capacity is 2, not "everyone waiting".
        assert _status_of(session=session, node_id="third") == Status.UNINITIALIZED
        assert _status_of(session=session, node_id="fourth") == Status.UNINITIALIZED

    def test_a_promoted_node_keeps_its_waiting_claim(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session, capacity=0)
        _park(session=session, group_id=group.id, node_id="parked-1")

        client.patch(_url(group.id), json={"capacity": 1})

        # Promotion is advisory: only the gate flips a claim to ACTIVE, so a promoted node sits
        # at QUEUED with a WAITING claim until it wins the version CAS.
        body = client.get(_url(group.id)).json()
        assert body["waiting_count"] == 1
        assert body["active_count"] == 0

    def test_lowering_capacity_never_evicts(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session, capacity=3)
        for node_id in ["run-1", "run-2", "run-3"]:
            _admit(session=session, group_id=group.id, node_id=node_id)
        _park(session=session, group_id=group.id, node_id="waiter")

        response = client.patch(_url(group.id), json={"capacity": 1})

        assert response.status_code == 200
        # Over capacity by two, and that is allowed: the cap gates admission, it does not kill.
        assert response.json()["occupancy"] == 3
        for node_id in ["run-1", "run-2", "run-3"]:
            assert _status_of(session=session, node_id=node_id) == Status.RUNNING
        # free_slots clamps at zero rather than reaching the DB as LIMIT -2.
        assert _status_of(session=session, node_id="waiter") == Status.UNINITIALIZED

    def test_capacity_zero_is_a_pause_not_a_stop(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session, capacity=2)
        _admit(session=session, group_id=group.id, node_id="running")
        _park(session=session, group_id=group.id, node_id="parked")

        response = client.patch(_url(group.id), json={"capacity": 0})

        assert response.status_code == 200
        # Nothing promoted, nothing killed: the group stops admitting and that is all.
        assert _status_of(session=session, node_id="parked") == Status.UNINITIALIZED
        assert _status_of(session=session, node_id="running") == Status.RUNNING


class TestDelete:
    def test_un_parks_every_waiter_before_the_row_goes(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session, capacity=0)
        for node_id in ["w1", "w2", "w3"]:
            _park(session=session, group_id=group.id, node_id=node_id)

        response = client.delete(_url(group.id))

        assert response.status_code == 200
        assert response.json()["released"] == 3
        # Asserted on node status, not on the claim count: CASCADE takes the claims either way,
        # so a claim count of zero is exactly what the bug looks like too.
        for node_id in ["w1", "w2", "w3"]:
            assert _status_of(session=session, node_id=node_id) == Status.QUEUED

    def test_ignores_capacity(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session, capacity=1)
        for node_id in ["w1", "w2", "w3"]:
            _park(session=session, group_id=group.id, node_id=node_id)

        client.delete(_url(group.id))

        # Unconditional: the cap is being removed, so there is nothing left to enforce.
        assert all(
            _status_of(session=session, node_id=n) == Status.QUEUED
            for n in ["w1", "w2", "w3"]
        )

    def test_leaves_running_members_alone(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session, capacity=2)
        _admit(session=session, group_id=group.id, node_id="running")

        response = client.delete(_url(group.id, suffix="?force=true"))

        # The status code is asserted, not assumed. Without `force` this group now gets a 409,
        # and a refused delete also leaves the node RUNNING -- so the outcome assertion alone
        # passes for the wrong reason.
        assert response.status_code == 200
        assert _status_of(session=session, node_id="running") == Status.RUNNING

    def test_refuses_while_a_member_is_live(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session, capacity=2)
        _admit(session=session, group_id=group.id, node_id="running")

        response = client.delete(_url(group.id))

        assert response.status_code == 409
        # The count is in the message: an operator reading it should not have to go and look
        # up how many members they were about to strand.
        assert "1 active claim(s)" in response.json()["detail"]
        # Nothing happened. The refusal is a precondition, not a partial delete.
        assert session.get(db_models.QuotaGroup, group.id) is not None
        assert _status_of(session=session, node_id="running") == Status.RUNNING

    def test_force_deletes_a_group_that_still_has_live_members(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        # The override arm, asserted on what it does and not just on the 200. Found by review:
        # the only other test passing ?force=true is about the node surviving, and it goes on
        # passing with `session.delete(group)` removed from the handler -- so nothing pinned
        # that force actually deletes anything.
        group = _make_group(session=session, capacity=2)
        _admit(session=session, group_id=group.id, node_id="running")

        response = client.delete(_url(group.id, suffix="?force=true"))

        assert response.status_code == 200
        session.expire_all()
        assert session.get(db_models.QuotaGroup, group.id) is None
        assert session.get(db_models.QuotaGroupClaim, (group.id, "running")) is None
        # And this is the doubling the 409 exists to warn about, now asserted rather than
        # described: the node is still running with no claim row, so a same-name recreate
        # reads occupancy 0 and admits a second full capacity against it.
        assert _status_of(session=session, node_id="running") == Status.RUNNING

    def test_a_group_with_only_waiters_needs_no_force(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session, capacity=0)
        _park(session=session, group_id=group.id, node_id="w1")

        # Occupancy counts nodes on their way to running, and a parked node is not one. The
        # guard is about stranding *live* members, so the queue alone must not trip it --
        # otherwise every wedged group, the ones most likely to be deleted, would need force.
        assert client.delete(_url(group.id)).status_code == 200

    def test_does_not_touch_another_groups_waiters(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        doomed = _make_group(session=session, name="doomed", capacity=0)
        bystander = _make_group(session=session, name="bystander", capacity=0)
        _park(session=session, group_id=doomed.id, node_id="mine")
        _park(session=session, group_id=bystander.id, node_id="theirs")

        client.delete(_url(doomed.id))

        # The release is scoped to the group being deleted, so a bystander group's waiters are
        # untouched. Asserted with a second group precisely because a fleet-wide release looks
        # identical from inside the deleted group.
        assert _status_of(session=session, node_id="mine") == Status.QUEUED
        assert _status_of(session=session, node_id="theirs") == Status.UNINITIALIZED

    def test_cascade_removes_the_claims(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session, capacity=0)
        _park(session=session, group_id=group.id, node_id="w1")

        client.delete(_url(group.id))

        session.expire_all()
        assert session.get(db_models.QuotaGroup, group.id) is None
        assert session.get(db_models.QuotaGroupClaim, (group.id, "w1")) is None

    def test_unknown_id_is_404(self, client: testclient.TestClient) -> None:
        assert client.delete(_url("nope")).status_code == 404

    def test_un_parking_and_deleting_share_one_transaction(
        self,
        session: orm.Session,
        monkeypatch: pytest.MonkeyPatch,
        client: testclient.TestClient,
    ) -> None:
        group = _make_group(session=session, capacity=0)
        _park(session=session, group_id=group.id, node_id="w1")

        def _explode(_obj: object) -> None:
            raise RuntimeError("delete blew up")

        monkeypatch.setattr(session, "delete", _explode)

        with pytest.raises(RuntimeError):
            client.delete(_url(group.id))
        session.rollback()

        # The failure mode this guards: un-parking committed on its own, so the node is QUEUED
        # while the group -- and the cap it was waiting on -- is still there.
        session.expire_all()
        assert session.get(db_models.QuotaGroup, group.id) is not None
        assert _status_of(session=session, node_id="w1") == Status.UNINITIALIZED


class TestReleaseClaim:
    def test_releases_a_claim_and_un_parks_the_node(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session, capacity=0)
        _park(session=session, group_id=group.id, node_id="stuck")

        response = client.delete(_url(group.id, suffix="/claims/stuck"))

        assert response.status_code == 200
        assert response.json()["execution_node_id"] == "stuck"
        session.expire_all()
        # There is no RELEASED state, so releasing is deleting -- and the node has to be un-parked
        # in the same breath or it becomes unreachable.
        assert session.get(db_models.QuotaGroupClaim, (group.id, "stuck")) is None
        assert _status_of(session=session, node_id="stuck") == Status.QUEUED

    def test_releasing_an_active_claim_frees_the_slot_for_a_waiter(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session, capacity=1)
        _admit(session=session, group_id=group.id, node_id="zombie")
        _park(session=session, group_id=group.id, node_id="waiter")

        client.delete(_url(group.id, suffix="/claims/zombie"))

        # The escape hatch's whole purpose: a stuck member is holding the only slot, and the
        # waiter behind it should move the moment the slot is freed.
        assert _status_of(session=session, node_id="waiter") == Status.QUEUED

    def test_unknown_node_is_404(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session)

        assert client.delete(_url(group.id, suffix="/claims/ghost")).status_code == 404


class TestPromote:
    def test_promotes_waiters_up_to_capacity(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session, capacity=2)
        for i, node_id in enumerate(["first", "second", "third"]):
            _park(
                session=session,
                group_id=group.id,
                node_id=node_id,
                offset_seconds=i,
            )

        body = client.post(_url(group.id, suffix="/promote")).json()

        assert body["promoted"] == 2
        assert body["capacity"] == 2
        assert _status_of(session=session, node_id="third") == Status.UNINITIALIZED
        # Every waiter gets a line, oldest first, and the one left behind says why.
        assert _outcomes(body) == {
            "first": "PROMOTED",
            "second": "PROMOTED",
            "third": "NO_CAPACITY",
        }

    def test_the_cap_bounds_the_body_and_the_report_says_so(
        self, session: orm.Session, client: testclient.TestClient, monkeypatch
    ) -> None:
        # H21. The cap is patched down rather than parking 501 nodes: 500 is a tuning value,
        # and a test that encodes it would have to be edited every time it moves. What is
        # being pinned is that the cap binds and that the two counts add up to the queue.
        monkeypatch.setattr(promotion, "_MAX_REPORT_WAITERS", 2)
        group = _make_group(session=session, capacity=1)
        for i, node_id in enumerate(["first", "second", "third", "fourth"]):
            _park(
                session=session,
                group_id=group.id,
                node_id=node_id,
                offset_seconds=i,
            )

        body = client.post(_url(group.id, suffix="/promote")).json()

        assert body["waiters_examined"] == 2
        assert body["waiters_unexamined"] == 2
        assert len(body["nodes"]) == 2
        # Oldest first, so a capped pass is an incremental one: it is the head of the queue
        # that got looked at, not an arbitrary two.
        assert _outcomes(body) == {"first": "PROMOTED", "second": "NO_CAPACITY"}
        assert _status_of(session=session, node_id="fourth") == Status.UNINITIALIZED

    def test_same_second_waiters_are_broken_by_node_id(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        """H7. Asserted on the emitted SQL, for the reason test_quota_promotion.py:163 gives.

        A behavioural version of this passes with the tiebreak deleted: SQLite satisfies the
        ORDER BY out of `ix_quota_group_claim_state_created` and walks the claims in
        `(created_at, execution_node_id)` order whether or not the query asked for it. Only
        the clause can tell the two versions apart -- and `promote_with_report` builds its
        read inline, so the statement has to be captured off the wire rather than compiled
        from a query builder.
        """
        group = _make_group(session=session, capacity=1)
        _park(session=session, group_id=group.id, node_id="w1")

        with _captured_sql(session=session) as statements:
            client.post(_url(group.id, suffix="/promote"))

        ordered = [s for s in statements if "ORDER BY" in s]
        assert len(ordered) == 1, ordered
        assert (
            "ORDER BY quota_group_claim.created_at ASC, quota_group_claim.execution_node_id ASC"
            in ordered[0]
        )

    def test_the_cap_never_refuses_capacity_the_group_is_entitled_to(
        self, session: orm.Session, client: testclient.TestClient, monkeypatch
    ) -> None:
        # `scan_limit = max(cap, slots)`. A group whose free slots outnumber the cap must
        # still fill them -- the cap bounds the *report*, and a cap that promoted fewer
        # nodes than a plain `promote()` would have been a correctness regression.
        monkeypatch.setattr(promotion, "_MAX_REPORT_WAITERS", 1)
        group = _make_group(session=session, capacity=3)
        for i, node_id in enumerate(["first", "second", "third"]):
            _park(
                session=session,
                group_id=group.id,
                node_id=node_id,
                offset_seconds=i,
            )

        body = client.post(_url(group.id, suffix="/promote")).json()

        assert body["promoted"] == 3
        assert body["waiters_unexamined"] == 0

    def test_an_uncapped_pass_reports_nothing_left_behind(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        # Zero is the value an operator reads as "this is the whole queue", so it has to be
        # the answer in the ordinary case and not just the absence of a flag.
        group = _make_group(session=session, capacity=1)
        for i, node_id in enumerate(["first", "second"]):
            _park(
                session=session,
                group_id=group.id,
                node_id=node_id,
                offset_seconds=i,
            )

        body = client.post(_url(group.id, suffix="/promote")).json()

        assert body["waiters_examined"] == 2
        assert body["waiters_unexamined"] == 0

    def test_the_report_names_the_group_it_swept(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session, name="bq-prod", capacity=1)

        body = client.post(_url(group.id, suffix="/promote")).json()

        assert body["quota_group"] == "bq-prod"
        assert body["quota_group_id"] == group.id

    def test_reports_the_occupancy_it_started_from(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session, capacity=2)
        _admit(session=session, group_id=group.id, node_id="running")
        _park(session=session, group_id=group.id, node_id="waiter")

        body = client.post(_url(group.id, suffix="/promote")).json()

        assert body["occupancy_before"] == 1
        assert body["promoted"] == 1
        # No occupancy_after: a promoted node sits at QUEUED with a WAITING claim, which
        # occupancy deliberately does not count, so the two readings can never differ.
        assert "occupancy_after" not in body

    def test_a_cancelled_node_is_released_and_un_parked(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session, capacity=1)
        _park(session=session, group_id=group.id, node_id="doomed")
        _cancel_node(session=session, node_id="doomed")

        body = client.post(_url(group.id, suffix="/promote")).json()

        assert _outcomes(body) == {"doomed": "RELEASED_CANCELLED"}
        assert body["promoted"] == 0
        # Un-parked *and* de-claimed. Un-parking is what lets the orchestrator see it at all --
        # the queued sweep skips UNINITIALIZED -- and its cancel check then takes it to
        # CANCELLED without ever launching a container. This is the S3 remedy.
        assert _status_of(session=session, node_id="doomed") == Status.QUEUED
        assert _claim_count(session=session, group_id=group.id) == 0

    def test_a_cancelled_run_releases_its_waiter(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session, capacity=1)
        _park(session=session, group_id=group.id, node_id="doomed")
        _cancel_run_of(session=session, node_id="doomed")

        body = client.post(_url(group.id, suffix="/promote")).json()

        # The node carries no desired_state of its own; the cancel is on the run two joins away.
        assert _outcomes(body)["doomed"] == "RELEASED_CANCELLED"

    def test_a_cancelled_waiter_does_not_spend_the_slot(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session, capacity=1)
        _park(
            session=session,
            group_id=group.id,
            node_id="doomed",
            offset_seconds=0,
        )
        _park(session=session, group_id=group.id, node_id="live", offset_seconds=1)
        _cancel_node(session=session, node_id="doomed")

        body = client.post(_url(group.id, suffix="/promote")).json()

        # The dead waiter is at the front of the queue. The plain sink pass would hand it the
        # only slot and un-park nobody useful; walking every waiter is what buys this.
        assert _outcomes(body) == {
            "doomed": "RELEASED_CANCELLED",
            "live": "PROMOTED",
        }
        assert _status_of(session=session, node_id="live") == Status.QUEUED

    def test_one_bad_waiter_does_not_abort_the_pass(
        self,
        session: orm.Session,
        monkeypatch: pytest.MonkeyPatch,
        client: testclient.TestClient,
    ) -> None:
        group = _make_group(session=session, capacity=1)
        _park(
            session=session,
            group_id=group.id,
            node_id="doomed",
            offset_seconds=0,
        )
        _park(session=session, group_id=group.id, node_id="live", offset_seconds=1)
        _cancel_node(session=session, node_id="doomed")

        def _explode(_obj: object) -> None:
            raise RuntimeError("releasing the claim blew up")

        monkeypatch.setattr(session, "delete", _explode)

        body = client.post(_url(group.id, suffix="/promote")).json()

        # The operator called this because the group is stuck; the rest of the queue is still
        # worth draining, so the failure is recorded against one node and the walk carries on.
        assert _outcomes(body) == {"doomed": "ERROR", "live": "PROMOTED"}

    def test_an_empty_group_promotes_nobody(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session, capacity=5)

        body = client.post(_url(group.id, suffix="/promote")).json()

        assert body["promoted"] == 0

    def test_a_paused_group_promotes_nobody(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session, capacity=0)
        _park(session=session, group_id=group.id, node_id="waiter")

        assert client.post(_url(group.id, suffix="/promote")).json()["promoted"] == 0
        assert _status_of(session=session, node_id="waiter") == Status.UNINITIALIZED

    def test_repeated_calls_are_safe(
        self, session: orm.Session, client: testclient.TestClient
    ) -> None:
        group = _make_group(session=session, capacity=1)
        _park(
            session=session,
            group_id=group.id,
            node_id="first",
            offset_seconds=0,
        )
        _park(
            session=session,
            group_id=group.id,
            node_id="second",
            offset_seconds=1,
        )

        client.post(_url(group.id, suffix="/promote"))
        second_pass = client.post(_url(group.id, suffix="/promote")).json()

        # `promote` on its own is idempotent per node and not per group: the node it moved holds
        # no occupancy yet, so its slot still reads as free and a second call would hand that
        # same slot to the next waiter. Harmless there -- the extra loses at the gate and
        # re-parks -- but an operator pressing a button twice should not have to know it, so
        # this pass counts in-flight promotions against the free slots.
        assert second_pass["promoted"] == 0
        assert _outcomes(second_pass) == {
            "first": "ALREADY_QUEUED",
            "second": "NO_CAPACITY",
        }
        assert _status_of(session=session, node_id="second") == Status.UNINITIALIZED

    def test_unknown_id_is_404(self, client: testclient.TestClient) -> None:
        assert client.post(_url("nope", suffix="/promote")).status_code == 404
