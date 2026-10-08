"""Reads whose correctness is an identity question, not a filtering one.

The identity is two halves held to two different rules, and every test here
exists to keep one of them from drifting into the other:

* `created_by` is NOT case-sensitive. `Jose` and `jose` are one person, and a
  query that hides one from the other is hiding a caller's own schedule.
* `schedule_path` IS case-sensitive. `Foo` and `foo` are two schedules, and a
  query that conflates them either refuses a free name or returns the wrong row.

An earlier revision held BOTH halves byte-exact, and the tests it came with
asserted that a case variant of the owner was a different principal. Those
assertions are inverted here rather than deleted quietly: they are the clearest
statement of what the contract now is, and of what it deliberately is not.
"""

import contextlib
from collections import abc

import pytest
import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend import api_router, database_ops
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.scheduling.pipelines import (
    api_routes,
    db_models,
    schedule_queries,
)
from tests.scheduling.pipelines.conftest import SAMPLE_PIPELINE_TASK_SPEC


@pytest.fixture()
def engine() -> sqlalchemy.Engine:
    engine = database_ops.create_db_engine(database_uri="sqlite://")
    bts._TableBase.metadata.create_all(engine)
    return engine


@contextlib.contextmanager
def _folding_columns(*names: str) -> abc.Iterator[sqlalchemy.Engine]:
    """A SQLite database that aliases ASCII case in the named columns.

    SQLite cannot reproduce a MySQL collation, so a compiled-SQL assertion is
    normally the best this suite can do -- and a compiled assertion cannot fail
    when somebody deletes a residual and the statement still parses. `NOCASE`
    removes that excuse: it folds ASCII case for `=`, for `LIKE` and for a
    unique index, which is the same three places MySQL's `_ci` default folds it.

    Parameterized by column because the two halves of the identity now fold in
    DIFFERENT situations, and a harness that folds both at once could not tell
    the intended behaviour from the bug:

    * `created_by` folds on any MySQL deployment, converted or not. That is the
      contract, so it is the shape correct behaviour must be proven against.
    * `schedule_path` folds only BEFORE the collation conversion has run. That
      is the bug the path residual exists to survive.

    The column types are swapped on the shared table object because `create_all`
    reads the model, and restored in a `finally`.
    """
    table = db_models.ScheduledPipelineRun.__table__
    columns = {name: table.c[name] for name in names}
    originals = {name: column.type for name, column in columns.items()}
    for column in columns.values():
        column.type = sqlalchemy.String(
            db_models.SCHEDULE_PATH_LENGTH, collation="NOCASE"
        )
    try:
        engine = database_ops.create_db_engine(database_uri="sqlite://")
        bts._TableBase.metadata.create_all(engine)
        yield engine
    finally:
        for name, column in columns.items():
            column.type = originals[name]


def folding_owner_column() -> "contextlib.AbstractContextManager[sqlalchemy.Engine]":
    """A CORRECTLY converted deployment: owner folds, path does not."""
    return _folding_columns("created_by")


def folding_path_column() -> "contextlib.AbstractContextManager[sqlalchemy.Engine]":
    """A deployment the path conversion has not reached yet."""
    return _folding_columns("schedule_path")


def _sample_schedule(
    *, created_by: str, schedule_path: str | None
) -> db_models.ScheduledPipelineRun:
    return db_models.ScheduledPipelineRun(
        name="nightly",
        cron_expression="0 8 * * *",
        timezone="UTC",
        pipeline_task_spec=SAMPLE_PIPELINE_TASK_SPEC,
        created_by=created_by,
        schedule_path=schedule_path,
    )


def _insert(
    engine: sqlalchemy.Engine, *, created_by: str, schedule_path: str | None
) -> None:
    with orm.Session(engine) as session:
        session.add(
            _sample_schedule(created_by=created_by, schedule_path=schedule_path)
        )
        session.commit()


class TestAPathCollisionIsProvenWithinTheOwnersNamespace:
    """The 409's evidence: exact on the path, owner-scoped by the database.

    `path_is_taken` is the sole proof behind a 409 -- the classifier trusts it
    rather than the driver's message -- so a wrong answer here is a permanent
    409 with no remedy, or a create that fails after being told it would not.

    These run on a byte-comparing SQLite, which proves the PATH rule end to end.
    The owner rule cannot be proven here at all, because this engine does not
    fold; see `TestCaseVariantOwnersAreOnePrincipal`.
    """

    @staticmethod
    def _schedule(
        session: orm.Session, *, created_by: str, schedule_path: str | None
    ) -> None:
        session.add(
            _sample_schedule(created_by=created_by, schedule_path=schedule_path)
        )
        session.commit()

    def test_the_owners_own_path_is_taken(self, engine: sqlalchemy.Engine) -> None:
        """The paired positive. Returning False always would pass every test below."""
        with orm.Session(engine) as session:
            self._schedule(
                session,
                created_by="jose@example.com",
                schedule_path="team/nightly",
            )

            assert schedule_queries.path_is_taken(
                session=session,
                created_by="jose@example.com",
                canonical_path="team/nightly",
            )

    def test_another_owners_identical_path_is_not(
        self, engine: sqlalchemy.Engine
    ) -> None:
        with orm.Session(engine) as session:
            self._schedule(
                session,
                created_by="maria@example.com",
                schedule_path="team/nightly",
            )

            assert not schedule_queries.path_is_taken(
                session=session,
                created_by="jose@example.com",
                canonical_path="team/nightly",
            )

    def test_a_case_variant_of_the_path_is_not(self, engine: sqlalchemy.Engine) -> None:
        """`Team/Nightly` is a different schedule, so `team/nightly` is still free."""
        with orm.Session(engine) as session:
            self._schedule(
                session,
                created_by="jose@example.com",
                schedule_path="Team/Nightly",
            )

            assert not schedule_queries.path_is_taken(
                session=session,
                created_by="jose@example.com",
                canonical_path="team/nightly",
            )

    def test_a_path_less_row_is_not_a_collision(
        self, engine: sqlalchemy.Engine
    ) -> None:
        """`schedule_path` is nullable, and NULL is not the caller's path."""
        with orm.Session(engine) as session:
            self._schedule(session, created_by="jose@example.com", schedule_path=None)

            assert not schedule_queries.path_is_taken(
                session=session,
                created_by="jose@example.com",
                canonical_path="team/nightly",
            )


class TestTheOwnerPredicateIsSharedAndDelegates:
    """One definition, and it states no comparison rule of its own.

    The list endpoint and the conflict classifier need the same comparison for
    different reasons -- pagination correctness and 409 correctness -- and the
    failure of having two copies is not a crash. It is one of them acquiring a
    residual the other lacks, which nothing but this test would notice.
    """

    def test_the_api_layer_delegates_rather_than_reimplementing(self) -> None:
        engine = sqlalchemy.create_engine("mysql+pymysql://user:pw@localhost/db")
        via_api = api_routes._owned_by_caller(
            user_details=api_router.UserDetails(
                name="jose@example.com",
                permissions=api_router.Permissions(read=True, write=True, admin=False),
            ),
        )
        direct = schedule_queries.owned_by(created_by="jose@example.com")

        compile_kwargs = {"literal_binds": True}
        assert str(via_api.compile(engine, compile_kwargs=compile_kwargs)) == str(
            direct.compile(engine, compile_kwargs=compile_kwargs)
        )

    @pytest.mark.parametrize(
        "url", ["mysql+pymysql://user:pw@localhost/db", "sqlite://"]
    )
    def test_it_compiles_to_a_plain_equality_on_every_dialect(self, url: str) -> None:
        """The mutation guard against reintroducing the byte-exact residual.

        This assertion is deliberately negative, which is unusual and is the
        point. The predicate used to be
        `created_by = :name AND CAST(CAST(created_by AS CHAR CHARACTER SET
        utf8mb4) AS BINARY) = CAST(CAST(:name ...) AS BINARY)`, added to keep
        'Jose' and 'jose' apart. They are the same person, so that residual
        excluded callers from their own rows on exactly the deployments it was
        written for. Anyone re-deriving it from first principles will reach the
        same wrong place, so the shape is named here as forbidden rather than
        merely absent.
        """
        engine = sqlalchemy.create_engine(url)
        predicate = schedule_queries.owned_by(created_by="jose@example.com")
        sql = str(
            predicate.compile(engine, compile_kwargs={"literal_binds": True})
        ).upper()

        assert "CAST" not in sql
        assert "BINARY" not in sql
        assert "BLOB" not in sql
        assert "COLLATE" not in sql
        assert " AND " not in sql

    def test_it_takes_no_session_so_it_cannot_branch_on_dialect(self) -> None:
        """Delegation is structural, not a promise in a docstring.

        The residual needed the bind to know whether to emit MySQL's charset
        conversion. Without a `Session` parameter there is nothing to branch on,
        so a future edit cannot quietly reintroduce a dialect-specific rule
        without changing the signature and every caller.
        """
        import inspect

        assert list(inspect.signature(schedule_queries.owned_by).parameters) == [
            "created_by"
        ]


class TestCaseVariantOwnersAreOnePrincipal:
    """The corrected contract, observed on a database that actually folds.

    Every test in the first class passes on byte-comparing SQLite whether or not
    an owner residual exists -- which is how the residual survived review in the
    first place. These are the ones that fail if it comes back.
    """

    def test_the_fixture_really_folds_the_owner(self) -> None:
        """Prove the harness first, or every conclusion below is unearned."""
        with folding_owner_column() as engine:
            _insert(
                engine,
                created_by="jose@example.com",
                schedule_path="team/nightly",
            )

            with orm.Session(engine) as session:
                found = session.scalar(
                    sqlalchemy.select(db_models.ScheduledPipelineRun.id).where(
                        db_models.ScheduledPipelineRun.created_by == "JOSE@example.com",
                    )
                )

            assert found is not None

    def test_the_fixture_leaves_the_path_exact(self) -> None:
        """The other half of the harness contract, asserted rather than assumed.

        If this engine folded paths too, the distinctness tests below would be
        proving nothing and would look like they were.
        """
        with folding_owner_column() as engine:
            _insert(
                engine,
                created_by="jose@example.com",
                schedule_path="team/nightly",
            )

            with orm.Session(engine) as session:
                found = session.scalar(
                    sqlalchemy.select(db_models.ScheduledPipelineRun.id).where(
                        db_models.ScheduledPipelineRun.schedule_path == "TEAM/NIGHTLY",
                    )
                )

            assert found is None

    def test_a_case_variant_owner_sees_their_own_path_as_taken(self) -> None:
        """The inverted finding.

        This asserted the OPPOSITE one commit ago: 'Jose' was not to be told
        'jose' had taken the path, on the premise that they are two principals
        and the 409 would name a schedule the caller cannot see. Under the real
        contract they are one principal, the schedule IS theirs, and the 409 is
        both true and actionable -- they can list it, open it and delete it.
        """
        with folding_owner_column() as engine:
            _insert(
                engine,
                created_by="jose@example.com",
                schedule_path="team/nightly",
            )

            with orm.Session(engine) as session:
                assert schedule_queries.path_is_taken(
                    session=session,
                    created_by="Jose@example.com",
                    canonical_path="team/nightly",
                )

    def test_a_case_variant_owner_resolves_their_own_schedule_by_path(
        self,
    ) -> None:
        """The read half of the same statement, and the one that hurt.

        With the owner residual in place this returned a row and the API layer
        then 404'd it, so a user whose identity provider capitalized their
        address could not open, patch or delete their own schedule by path.
        """
        with folding_owner_column() as engine:
            _insert(
                engine,
                created_by="jose@example.com",
                schedule_path="team/nightly",
            )

            with orm.Session(engine) as session:
                identity = schedule_queries.owned_schedule_identity_by_path(
                    session=session,
                    created_by="JOSE@example.com",
                    canonical_path="team/nightly",
                )

            assert identity is not None
            assert identity.schedule_path == "team/nightly"

    def test_paths_stay_distinct_inside_that_one_namespace(self) -> None:
        """The mixed key, proven as a pair rather than a column at a time.

        Both spellings belong to one person -- 'Jose' and 'jose' -- and are two
        separate schedules. This is the exact combination the reverted change
        made impossible: it would have refused the second create as a duplicate
        under a uniformly binary key only if the owner spellings also matched,
        and hidden one of them otherwise.
        """
        with folding_owner_column() as engine:
            _insert(
                engine,
                created_by="jose@example.com",
                schedule_path="team/Nightly",
            )
            _insert(
                engine,
                created_by="Jose@example.com",
                schedule_path="team/nightly",
            )

            with orm.Session(engine) as session:
                upper = schedule_queries.owned_schedule_identity_by_path(
                    session=session,
                    created_by="jose@example.com",
                    canonical_path="team/Nightly",
                )
                lower = schedule_queries.owned_schedule_identity_by_path(
                    session=session,
                    created_by="JOSE@example.com",
                    canonical_path="team/nightly",
                )

            assert upper is not None
            assert lower is not None
            # One namespace, two schedules: same owner rows, different ids.
            assert upper.id != lower.id
            assert upper.schedule_path == "team/Nightly"
            assert lower.schedule_path == "team/nightly"

    def test_a_genuinely_different_owner_is_still_invisible(self) -> None:
        """Folding the owner widens the namespace; it does not remove it.

        The obvious way to break the tests above is to stop scoping by owner at
        all, which would pass every one of them.
        """
        with folding_owner_column() as engine:
            _insert(
                engine,
                created_by="jose@example.com",
                schedule_path="team/nightly",
            )

            with orm.Session(engine) as session:
                assert not schedule_queries.path_is_taken(
                    session=session,
                    created_by="maria@example.com",
                    canonical_path="team/nightly",
                )
                assert (
                    schedule_queries.owned_schedule_identity_by_path(
                        session=session,
                        created_by="maria@example.com",
                        canonical_path="team/nightly",
                    )
                    is None
                )
                assert (
                    schedule_queries.owned_schedule_stub_by_path(
                        session=session,
                        created_by="maria@example.com",
                        canonical_path="team/nightly",
                    )
                    is None
                )


class TestTheLocatorsCarryTheStoredPath:
    """A path lookup must return the path it was asked for, not a neighbour.

    On a column the conversion has not reached, a lookup for 'Team/Nightly'
    resolves the row stored at 'team/nightly'. That is not a cross-owner escape
    -- the owner scope holds -- but the caller asked for a path that does not
    exist and silently received a different schedule of their own, and DELETE
    makes that irreversible.

    The locators cannot decide what that means themselves: only the caller knows
    which question it asked. So they return the stored path and the API layer
    compares.
    """

    def test_the_identity_locator_reports_the_stored_path(self) -> None:
        """Without the column, the caller has nothing to compare against."""
        with folding_path_column() as engine:
            _insert(
                engine,
                created_by="jose@example.com",
                schedule_path="team/nightly",
            )

            with orm.Session(engine) as session:
                identity = schedule_queries.owned_schedule_identity_by_path(
                    session=session,
                    created_by="jose@example.com",
                    canonical_path="Team/Nightly",
                )

            assert identity is not None
            # The database matched a neighbour, and says so honestly rather than
            # hiding it behind a row the caller will assume is theirs.
            assert identity.schedule_path == "team/nightly"

    def test_the_delete_stub_reports_the_stored_path_too(self) -> None:
        """DELETE is the verb where acting on the wrong row cannot be undone.

        The stub loads columns with `raiseload=True`, so a path left unloaded
        would raise rather than compare -- which is why this is asserted
        separately from the identity locator and not assumed to follow from it.
        """
        with folding_path_column() as engine:
            _insert(
                engine,
                created_by="jose@example.com",
                schedule_path="team/nightly",
            )

            with orm.Session(engine) as session:
                stub = schedule_queries.owned_schedule_stub_by_path(
                    session=session,
                    created_by="jose@example.com",
                    canonical_path="Team/Nightly",
                )

                assert stub is not None
                assert stub.schedule_path == "team/nightly"

    def test_a_case_variant_path_is_not_evidence_of_a_collision(self) -> None:
        """`path_is_taken` applies the same residual, and must, for the 409."""
        with folding_path_column() as engine:
            _insert(
                engine,
                created_by="jose@example.com",
                schedule_path="Team/Nightly",
            )

            with orm.Session(engine) as session:
                assert not schedule_queries.path_is_taken(
                    session=session,
                    created_by="jose@example.com",
                    canonical_path="team/nightly",
                )

    def test_an_exact_lookup_still_resolves(self) -> None:
        """The paired positive."""
        with folding_path_column() as engine:
            _insert(
                engine,
                created_by="jose@example.com",
                schedule_path="team/nightly",
            )

            with orm.Session(engine) as session:
                identity = schedule_queries.owned_schedule_identity_by_path(
                    session=session,
                    created_by="jose@example.com",
                    canonical_path="team/nightly",
                )

            assert identity is not None
            assert identity.schedule_path == "team/nightly"

    def test_the_owners_own_exact_path_is_still_evidence(self) -> None:
        """The paired positive for the classifier."""
        with folding_path_column() as engine:
            _insert(
                engine,
                created_by="jose@example.com",
                schedule_path="team/nightly",
            )

            with orm.Session(engine) as session:
                assert schedule_queries.path_is_taken(
                    session=session,
                    created_by="jose@example.com",
                    canonical_path="team/nightly",
                )
