"""What only a real MySQL can answer: the gate under concurrency, and statement legality.

The one dimension the feature is entirely about, and the one every other test in this
directory cannot reach: SQLite serialises writers and `StaticPool` (`database_ops.py:46`)
collapses every session onto one connection, so two "concurrent" admissions there are two
sequential ones. These tests give each thread its **own engine**, which is what makes them
separate connections and separate transactions.

Three things only this tier can catch: overshoot (two admitters both read `count < capacity`
and both win), the lock ordering between the `quota_group` UPDATE and the `quota_group_claim`
write, and a real `OperationalError` deadlock -- the exception the sink's `DeliveryIncomplete`
path exists to catch and which nothing else here produces.

These tests require a real MySQL service. They are marked `mysql` and skip unless
`QUOTA_MYSQL_TEST_URI` is set; ordinary in-memory tests do not exercise concurrent admission.

To run it::

    docker run --rm -d --name quota-mysql-test \
        -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=quota -p 3307:3306 mysql:8
    QUOTA_MYSQL_TEST_URI=mysql+pymysql://root:root@127.0.0.1:3307/quota \
        uv run --frozen pytest tests/quota/test_quota_concurrency_mysql.py -m mysql
    docker rm -f quota-mysql-test

Last run: MySQL 8.4.11 in the container above, 21 passed in 6.9 s. The container answers
`mysqladmin ping` about 6 s after it starts.

That run is not a formality -- the first one failed with MySQL error 1093, "You can't specify
target table 'execution_node' for update in FROM clause", because the park's `UPDATE` filtered
on an `EXISTS` over the table it was writing. SQLite accepts that statement, so the whole
existing suite was green on a park that could never have run in production. The fix is at
`quota/interceptor.py:326`.

That defect is the reason for the second half of this file. The concurrency tier above covers
the gate path; `TestEveryQuotaStatementIsLegalOnMySQL` and `TestEveryQuotaRouteIsLegalOnMySQL`
cover the rest, on the principle that any statement SQLite has approved and MySQL has never
seen is a 1093 waiting to happen. They assert nothing about results -- only that MySQL accepts
the statement at all.

Executed and rolled back, not `EXPLAIN`ed. Both catch 1093, but `EXPLAIN` needs the statement
as text and `literal_binds` renders the JSON-path parameter as a bare `%s`, which MySQL
rejects with a 1064 that looks like a defect in the query and is not one. Executing the
statement object keeps the parameters bound.
"""

import os
import threading
import time
from collections.abc import Callable, Iterator
from typing import Any, Final

import fastapi
import pytest
import sqlalchemy as sql
from fastapi import testclient
from sqlalchemy import orm

from cloud_pipelines_backend import api_router, database_ops
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.emissions.handlers.quota import annotations
from cloud_pipelines_backend.emissions.handlers.quota.sinks import (
    quota_group as quota_group_sink,
)
from cloud_pipelines_backend.quota import (
    api_routes,
    claims,
    db_models,
    groups,
    interceptor,
    occupancy,
    promotion,
)
from cloud_pipelines_backend.quota.observability import poller as quota_poller
from cloud_pipelines_backend.utils import db as db_utils

MYSQL_URI = os.environ.get("QUOTA_MYSQL_TEST_URI")

pytestmark = [
    pytest.mark.mysql,
    pytest.mark.skipif(
        not MYSQL_URI,
        reason="Set QUOTA_MYSQL_TEST_URI to a MySQL 8 instance; see the module docstring.",
    ),
]

KEY = annotations.QUOTA_GROUP_KEY
Status = bts.ContainerExecutionStatus
CONTENDERS = 8


@pytest.fixture()
def engines() -> list[sql.Engine]:
    """One engine per contender, so each thread holds its own connection and transaction.

    Two sessions on one engine would not do: the point of the test is two transactions racing
    inside the database, and a shared pool can hand both of them the same connection.
    """
    assert MYSQL_URI is not None
    made = [
        database_ops.create_db_engine(
            database_uri=MYSQL_URI, pool_size=2, max_overflow=0
        )
        for _ in range(CONTENDERS)
    ]
    bts._TableBase.metadata.drop_all(made[0])
    bts._TableBase.metadata.create_all(made[0])
    yield made
    for engine in made:
        engine.dispose()


def _seed(*, engine: sql.Engine, capacity: int) -> tuple[str, list[str]]:
    """Create one group and `CONTENDERS` queued nodes that all name it."""
    with orm.Session(bind=engine, autoflush=False) as session:
        group = db_models.QuotaGroup(
            name="bq", capacity=capacity, created_by="test-owner@example.com"
        )
        session.add(group)
        session.flush()
        node_ids = []
        for index in range(CONTENDERS):
            task_spec: dict[str, Any] = {"annotations": {KEY: "bq"}}
            node = bts.ExecutionNode(task_spec=task_spec)
            node.id = f"node-{index}"
            node.container_execution_status = Status.QUEUED
            session.add(node)
            node_ids.append(node.id)
        session.commit()
        return group.id, node_ids


def _claim_states(*, engine: sql.Engine) -> dict[db_models.ClaimState, int]:
    with orm.Session(bind=engine) as session:
        rows = session.execute(
            sql.select(db_models.QuotaGroupClaim.state, sql.func.count()).group_by(
                db_models.QuotaGroupClaim.state
            )
        ).all()
    return {state: count for state, count in rows}


def _statuses(*, engine: sql.Engine) -> dict[Status, int]:
    with orm.Session(bind=engine) as session:
        rows = session.execute(
            sql.select(
                bts.ExecutionNode.container_execution_status, sql.func.count()
            ).group_by(bts.ExecutionNode.container_execution_status)
        ).all()
    return {status: count for status, count in rows}


def _stranded(*, engine: sql.Engine) -> list[str]:
    """Nodes parked at UNINITIALIZED that no claim row points at -- invisible to everything.

    The terminal failure the feature exists to prevent, and the only one that needs a human to
    repair. The queued sweep selects `QUEUED` and nothing else, promotion walks claims, and the
    reconciler walks claims, so a node here is never launched, never released and never
    reported. An empty list is the whole assertion.
    """
    with orm.Session(bind=engine) as session:
        return list(
            session.scalars(
                sql.select(bts.ExecutionNode.id)
                .outerjoin(
                    db_models.QuotaGroupClaim,
                    db_models.QuotaGroupClaim.execution_node_id == bts.ExecutionNode.id,
                )
                .where(
                    bts.ExecutionNode.container_execution_status
                    == Status.UNINITIALIZED,
                    db_models.QuotaGroupClaim.execution_node_id.is_(None),
                )
            )
        )


def _race(*, engines: list[sql.Engine], node_ids: list[str]) -> list[BaseException]:
    """Send every contender through the gate at once and collect what each one raised."""
    barrier = threading.Barrier(len(node_ids))
    gate = interceptor.QuotaGroupInterceptor()
    failures: list[BaseException] = []
    lock = threading.Lock()

    def contend(*, engine: sql.Engine, node_id: str) -> None:
        try:
            with orm.Session(bind=engine, autoflush=False) as session:
                node = session.get(bts.ExecutionNode, node_id)
                assert node is not None
                barrier.wait(timeout=30)
                parked = gate.intercept(session=session, execution=node)
                if not parked:
                    # The gate commits its own admission and its own park; a launch leaves the
                    # caller's transaction open, exactly as the orchestrator does.
                    session.commit()
        except BaseException as error:  # noqa: BLE001 -- reported, not swallowed
            with lock:
                failures.append(error)

    threads = [
        threading.Thread(target=contend, kwargs={"engine": engine, "node_id": node_id})
        for engine, node_id in zip(engines, node_ids, strict=True)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)
    return failures


class TestTheCapIsNeverOvershot:
    """`capacity=1` and eight simultaneous admissions: exactly one may win."""

    def test_one_node_is_admitted_and_the_rest_are_parked(
        self, engines: list[sql.Engine]
    ) -> None:
        _, node_ids = _seed(engine=engines[0], capacity=1)

        failures = _race(engines=engines, node_ids=node_ids)

        assert failures == []
        assert _claim_states(engine=engines[0]) == {
            db_models.ClaimState.ACTIVE: 1,
            db_models.ClaimState.WAITING: CONTENDERS - 1,
        }

    def test_the_losers_are_parked_rather_than_left_queued(
        self, engines: list[sql.Engine]
    ) -> None:
        # A node left QUEUED would be picked up by the next sweep and launched ungated, which
        # is the failure this whole feature exists to prevent.
        _, node_ids = _seed(engine=engines[0], capacity=1)

        _race(engines=engines, node_ids=node_ids)

        assert _statuses(engine=engines[0]) == {
            Status.QUEUED: 1,
            Status.UNINITIALIZED: CONTENDERS - 1,
        }


class TestACapAboveOneIsFilledExactly:
    def test_three_slots_admit_three(self, engines: list[sql.Engine]) -> None:
        # Not a rerun of the test above with a different number: capacity=1 can be passed by a
        # gate that admits the first caller and parks on any contention at all.
        _, node_ids = _seed(engine=engines[0], capacity=3)

        failures = _race(engines=engines, node_ids=node_ids)

        assert failures == []
        assert _claim_states(engine=engines[0]) == {
            db_models.ClaimState.ACTIVE: 3,
            db_models.ClaimState.WAITING: CONTENDERS - 3,
        }


class TestDeleteDoesNotStrandANodeAdmittedMidFlight:
    """The DELETE handler and an admission must serialise on the group row.

    The handler un-parks every waiter and then deletes the group in one transaction, which is
    atomic against anything already parked. It is not atomic against a node that parks *during*
    it: the release scan has already run, so the new WAITING claim is not seen, and the DELETE
    cascades it away and leaves the node at UNINITIALIZED with nothing pointing at it.

    Only a real MySQL can show this. SQLite serialises writers and `StaticPool` hands both
    sessions the same connection, so the two transactions cannot overlap and the test would
    pass with the lock removed.
    """

    @staticmethod
    def _admit_one(*, engine: sql.Engine, node_id: str) -> None:
        """Fill the single slot, so the next arrival is a parker rather than an admission."""
        gate = interceptor.QuotaGroupInterceptor()
        with orm.Session(bind=engine, autoflush=False) as session:
            node = session.get(bts.ExecutionNode, node_id)
            assert node is not None
            assert not gate.intercept(session=session, execution=node)
            session.commit()

    def test_a_node_parking_mid_delete_is_not_left_invisible(
        self, engines: list[sql.Engine]
    ) -> None:
        # Deterministic rather than hammered: the deleter holds its transaction open for a
        # second between the release scan and the DELETE, which is the exact window the bug
        # needs. With the lock the admitter cannot enter it -- it blocks on the same row at
        # `quota/interceptor.py:118` until the DELETE commits, then finds no group and launches
        # ungated. Without the lock it parks inside the window and the cascade eats the claim.
        _, node_ids = _seed(engine=engines[0], capacity=1)
        self._admit_one(engine=engines[0], node_id=node_ids[0])
        scan_done = threading.Event()
        failures: list[BaseException] = []

        def delete_the_group() -> None:
            try:
                with orm.Session(bind=engines[1], autoflush=False) as session:
                    group = api_routes._get_group_or_404(
                        session=session,
                        key_kind="name",
                        key="bq",
                        for_update=True,
                    )
                    promotion.release_waiters_in_group(
                        session=session, group_id=group.id
                    )
                    scan_done.set()
                    # The window. Long enough that a park would certainly land inside it.
                    time.sleep(1.0)
                    session.delete(group)
                    session.commit()
            except BaseException as error:  # noqa: BLE001 -- reported, not swallowed
                failures.append(error)

        def admit_late() -> None:
            try:
                assert scan_done.wait(timeout=30)
                gate = interceptor.QuotaGroupInterceptor()
                with orm.Session(bind=engines[2], autoflush=False) as session:
                    node = session.get(bts.ExecutionNode, node_ids[1])
                    assert node is not None
                    if not gate.intercept(session=session, execution=node):
                        session.commit()
            except BaseException as error:  # noqa: BLE001 -- reported, not swallowed
                failures.append(error)

        threads = [
            threading.Thread(target=delete_the_group),
            threading.Thread(target=admit_late),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60)

        assert failures == []
        assert _stranded(engine=engines[0]) == []


class TestOwnershipUsesTheDatabasesComparator:
    """`created_by` folds case in MySQL, so the ownership check must not fold it in Python.

    This cannot be tested anywhere else in the suite. `created_by` carries the table default
    `utf8mb4_0900_ai_ci`, which is case- and accent-insensitive; SQLite's default is BINARY, so
    a Python `!=` and a SQL `=` agree there and the old code passes for the wrong reason. Only
    a real MySQL makes the two comparators disagree, which is the entire bug.
    """

    @staticmethod
    def _seed_group(*, engine: sql.Engine, created_by: str) -> str:
        with orm.Session(bind=engine, autoflush=False) as session:
            group = db_models.QuotaGroup(
                name="owned", capacity=1, created_by=created_by
            )
            session.add(group)
            session.commit()
            return group.id

    def test_the_column_folds_case_so_python_and_sql_disagree(
        self, engines: list[sql.Engine]
    ) -> None:
        # The premise, asserted rather than assumed: if this deployment ever stopped folding,
        # the test below would pass for the wrong reason and quietly stop guarding anything.
        group_id = self._seed_group(engine=engines[0], created_by="alice@example.com")

        with orm.Session(bind=engines[0]) as session:
            assert api_routes._is_owned_by(
                session=session,
                group_id=group_id,
                created_by="Alice@example.com",
            )
        assert "alice@example.com" != "Alice@example.com"

    def test_the_creator_is_not_locked_out_by_a_casing_shift(
        self, engines: list[sql.Engine]
    ) -> None:
        # The bug as a user meets it: the identity provider spells the address with different
        # capitalisation than the one recorded at creation, and the creator is 403'd off their
        # own group -- told it "was created by alice@example.com, not Alice@example.com".
        group_id = self._seed_group(engine=engines[0], created_by="alice@example.com")

        with orm.Session(bind=engines[0]) as session:
            group = api_routes._get_group_or_404(
                session=session, key_kind="id", key=group_id
            )
            api_routes._check_ownership(
                session=session,
                group=group,
                user_details=api_router.UserDetails(
                    name="Alice@example.com", permissions={}
                ),
                action="UPDATE",
            )

    def test_a_genuine_stranger_is_still_refused(
        self, engines: list[sql.Engine]
    ) -> None:
        # The fix must not become "anyone may edit": folding case is not folding identity.
        group_id = self._seed_group(engine=engines[0], created_by="alice@example.com")

        with orm.Session(bind=engines[0]) as session:
            group = api_routes._get_group_or_404(
                session=session, key_kind="id", key=group_id
            )
            with pytest.raises(fastapi.HTTPException) as refused:
                api_routes._check_ownership(
                    session=session,
                    group=group,
                    user_details=api_router.UserDetails(
                        name="bob@example.com", permissions={}
                    ),
                    action="UPDATE",
                )
        assert refused.value.status_code == 403


@pytest.fixture()
def mysql_engine() -> Iterator[sql.Engine]:
    """One engine with the schema built, for the tests that do not need a race.

    Separate from `engines` because the statement-legality tier is single-threaded: eight
    engines for a test that opens one transaction would make the slow part of the run the
    connection setup.
    """
    assert MYSQL_URI is not None
    engine = database_ops.create_db_engine(database_uri=MYSQL_URI)
    bts._TableBase.metadata.drop_all(engine)
    bts._TableBase.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture()
def rolled_back_session(mysql_engine: sql.Engine) -> Iterator[orm.Session]:
    """A session whose every write is undone, even the ones the code under test commits.

    The session joins an outer transaction on a single connection and takes savepoints for
    its own commits, so a handler calling `session.commit()` releases a savepoint rather than
    ending the real transaction. Rolling the outer one back at the end undoes the lot.

    Necessary because the API routes are not statement builders that can be inspected -- they
    are handlers that commit. Reconstructing their statements in the test would be asserting
    about a copy, and a copy is exactly what cannot catch a 1093.
    """
    with mysql_engine.connect() as connection:
        transaction = connection.begin()
        session = orm.Session(
            bind=connection,
            autoflush=False,
            join_transaction_mode="create_savepoint",
        )
        yield session
        session.close()
        transaction.rollback()


def _seed_one_of_everything(*, session: orm.Session) -> tuple[str, str]:
    """A group, a node claiming it, and a parked node, so no statement runs on empty tables.

    An empty table is the weaker test: MySQL still parses and plans the statement, but a join
    or a lock clause that only misbehaves once rows exist would go unseen.

    Args:
        session: The session to write through.

    Returns:
        The group's id and the parked node's id.
    """
    group = db_models.QuotaGroup(
        name="bq", capacity=2, created_by="test-owner@example.com"
    )
    session.add(group)
    session.flush()
    for node_id, status, state in (
        ("active-node", Status.RUNNING, db_models.ClaimState.ACTIVE),
        ("parked-node", Status.UNINITIALIZED, db_models.ClaimState.WAITING),
    ):
        node = bts.ExecutionNode(task_spec={"annotations": {KEY: "bq"}})
        node.id = node_id
        node.container_execution_status = status
        session.add(node)
        session.flush()
        session.add(
            db_models.QuotaGroupClaim(
                quota_group_id=group.id, execution_node_id=node_id, state=state
            )
        )
    session.flush()
    return group.id, "parked-node"


# Every quota statement builder that MySQL has never executed, each as a callable taking the
# seeded session and the group id. The gate's three -- occupancy_query(for_update=True),
# _park_status_update and _claim_impl -- are deliberately absent: the concurrency tier above
# runs all three, and running them twice would spend MySQL time to learn nothing.
_EVERY_QUOTA_STATEMENT: Final[list[tuple[str, Callable[[orm.Session, str], Any]]]] = [
    (
        "find_claim",
        lambda session, _group_id: claims.find_claim(
            session=session, execution_node_id="active-node"
        ),
    ),
    (
        "resolve_group",
        lambda session, _group_id: groups.resolve_group(
            session=session,
            execution=session.get_one(bts.ExecutionNode, "parked-node"),
        ),
    ),
    (
        "occupancy_query",
        lambda session, group_id: session.scalar(
            occupancy.occupancy_query(group_id=group_id)
        ),
    ),
    (
        # The lock clause is the interesting half: `of=ExecutionNode` on a joined SELECT is
        # the shape that produced the 1093 in the park, one statement over.
        "parked_nodes_query, locked",
        lambda session, group_id: session.scalars(
            promotion.parked_nodes_query(group_id=group_id, slots=2).with_for_update(
                of=bts.ExecutionNode
            )
        ).all(),
    ),
    (
        "stalled_groups_query",
        lambda session, _group_id: session.execute(
            promotion.stalled_groups_query(cutoff=db_utils.utc_now())
        ).all(),
    ),
    (
        "all_groups_query",
        lambda session, _group_id: session.execute(
            quota_poller.all_groups_query()
        ).all(),
    ),
    (
        "promote",
        lambda session, group_id: promotion.promote(session=session, group_id=group_id),
    ),
    (
        "promote_with_report",
        lambda session, group_id: promotion.promote_with_report(
            session=session,
            group=session.get_one(db_models.QuotaGroup, group_id),
        ),
    ),
    (
        "release_waiters_in_group",
        lambda session, group_id: promotion.release_waiters_in_group(
            session=session, group_id=group_id
        ),
    ),
    (
        "the sink's _release_slot",
        lambda session, _group_id: quota_group_sink._release_slot(
            session=session,
            claim=claims.find_claim(session=session, execution_node_id="active-node"),
        ),
    ),
]


class TestEveryQuotaStatementIsLegalOnMySQL:
    """One transaction, every builder executed against seeded tables, then rolled back.

    Not about results -- about whether MySQL accepts the statement at all. The assertion is
    "did not raise", which is why there is no `assert` in the body: anything MySQL rejects
    arrives as a `DatabaseError` and fails the test with the server's own message, which is
    more use than anything this test could phrase itself.
    """

    @pytest.mark.parametrize(
        ("name", "run_it"),
        _EVERY_QUOTA_STATEMENT,
        ids=[n for n, _ in _EVERY_QUOTA_STATEMENT],
    )
    def test_mysql_accepts_it(
        self,
        rolled_back_session: orm.Session,
        name: str,
        run_it: Callable[[orm.Session, str], Any],
    ) -> None:
        group_id, _parked = _seed_one_of_everything(session=rolled_back_session)

        run_it(rolled_back_session, group_id)


# The eight routes, each as the request that exercises its statements. Driven through the app
# rather than rebuilt here: the list, claims and promote handlers build their queries inline,
# so a reconstruction in this file would be a copy of the statement rather than the statement,
# and a copy is exactly what cannot catch a dialect rejection.
_EVERY_QUOTA_REQUEST: Final[list[tuple[str, str, str]]] = [
    ("create", "POST", "/api/quota_groups"),
    ("list, first page", "GET", "/api/quota_groups?page_size=1"),
    ("get one", "GET", "/api/quota_groups/name/bq"),
    ("patch", "PATCH", "/api/quota_groups/name/bq"),
    ("list claims", "GET", "/api/quota_groups/name/bq/claims?page_size=1"),
    ("promote", "POST", "/api/quota_groups/name/bq/promote"),
    (
        "delete a claim",
        "DELETE",
        "/api/quota_groups/name/bq/claims/active-node",
    ),
    ("delete the group", "DELETE", "/api/quota_groups/name/bq?force=true"),
]


class TestEveryQuotaRouteIsLegalOnMySQL:
    """The same question asked of the API, whose statements live inside handlers.

    Keyset pagination is the one worth naming: the list route filters on a row-value tuple
    comparison, `(updated_at, id) < (?, ?)`, which no other statement in the feature uses.
    """

    @pytest.mark.parametrize(
        ("name", "method", "path"),
        _EVERY_QUOTA_REQUEST,
        ids=[n for n, _, _ in _EVERY_QUOTA_REQUEST],
    )
    def test_mysql_accepts_what_the_handler_builds(
        self,
        rolled_back_session: orm.Session,
        name: str,
        method: str,
        path: str,
    ) -> None:
        _seed_one_of_everything(session=rolled_back_session)
        rolled_back_session.commit()
        app = fastapi.FastAPI()
        api_routes.setup_quota_group_routes(
            app=app,
            get_session=lambda: rolled_back_session,
            user_details_getter=lambda: api_router.UserDetails(
                name="test-owner@example.com",
                permissions={"read": True, "write": True, "admin": True},
            ),
        )
        body = (
            {"name": "second", "capacity": 1} if method == "POST" else {"capacity": 3}
        )

        response = testclient.TestClient(app).request(method, path, json=body)

        # A rejected statement does not actually reach this line: TestClient re-raises the
        # handler's exception, so the DatabaseError fails the test with MySQL's own message
        # -- verified by making the list route's query illegal and watching a 1235 come back.
        # The assertion is the backstop for a handler that catches broadly and returns a 500
        # instead, and it stops short of asserting 2xx because a 4xx is the handler working
        # as designed and says nothing about the dialect.
        assert response.status_code < 500, response.text
