"""Expand migration for `scheduled_pipeline_run`, split by who runs it.

`create_all()` creates missing tables but never alters an existing one, and this
repository has no Alembic version scripts. The in-repo precedent is
`cloud_pipelines_backend/database_migrations.py::migrate_secret_value_column`
(inspect the live column, then drive `alembic.operations`). This module is
owned by the scheduler and registered with the core migration entry point.

`migrate_db()` runs on **every pod at startup** and emits **column and index
DDL**: ADD COLUMN, the one exact widen, and the three secondary indexes -- the
two the write tiers gate on, plus the owner-scoped list's access path. It runs
no census: no COUNT, no GROUP BY, no application query over table data, because
repeating one on every restart is unbounded work on the boot path.
It also makes no attempt to add the source CHECK to an existing table.

That is not the same as "touches no data", and the difference is the whole
rollout risk: **ADD INDEX itself scans and sorts the table.** The server does
that work, not a query in this module, and it is why the live row count is a
deploy precondition rather than a detail.

Building indexes on the boot path requires a single scheduler instance and
a rollout that keeps the previous instance serving until its replacement is
ready. `ALGORITHM=INPLACE, LOCK=NONE` permits concurrent DML for a secondary
index add, so the build does not block the per-fire `last_run_at` write.
Operators must verify these deployment prerequisites and revisit them before
scaling the scheduler; see `_expand` for the remaining limits.

On MySQL statements use explicit algorithms and lock modes with a bounded
metadata-lock wait. Index builds permit concurrent DML; the validated foreign
key copy is allowed only below its size gate. Any dialect other than MySQL or
SQLite is refused before it can be WRITTEN to -- once, in `_expand`, before any emitter
runs, rather than relying on each statement to remember its own guard. The
refusal is scoped to emission, not to the call: verification is read-only and
runs on any dialect, so a database that needs nothing does not raise merely for
being unfamiliar (the absent-table return and the already-final fast path both
predate reaching the guard). The moment there is anything to emit, an unreviewed
dialect stops the process. The one exception to "no
copies" is the SQLite widen, which recreates the table because SQLite cannot do
otherwise; reserve this rebuild for small databases without concurrent scheduler
traffic. See `_widen`.

Missing mapped columns are fatal, because the ORM selects them and booting
anyway only defers the failure to the first request. A failed index build is
not: it leaves that write tier closed and is retried on the next boot.

The source CHECK is **verified and reported but never installed here**: only
`create_all` installs it, on a fresh database. An existing table may therefore
never acquire it, which is why no gate depends on it. PR3 gates reference/path
writes on the reported readiness tiers (`path_writes_ready`,
`reference_writes_ready`) rather than assuming a clean boot implies them.

**One foreign key**, on the saved-pipeline column, installed here under a size
gate. MySQL permits INPLACE for an FK add only while `foreign_key_checks` is
disabled -- which skips the validation the constraint exists for -- so a
validated add is ALGORITHM=COPY, and a COPY holds at least LOCK=SHARED: it blocks
the `last_run_at` write every schedule fire performs. This module used to
conclude from that that the constraint could never be installed at all. That was
wrong, and a reviewer disproved it by running the statement: a shared lock is an
outage only for as long as the copy takes, and at this table's size it takes
milliseconds. The premise the argument needed was the table's size, and it was
never checked.

So the constraint is installed while it is cheap, and refused once it is not --
`_MAX_FK_COPY_ROWS`, two orders of magnitude below the index limit, because a
blocked write is worse than a slow boot. It is not a timeless safety property:
above the gate, `reference_writes` closes and the constraint waits for a
maintenance window. The version pair gets none, and could not usefully: MATCH
SIMPLE skips a composite key whenever any column is NULL, which is the ordinary
track-current mode. The source CHECK remains defence in depth and no gate depends
on it: see `MigrationReport`.

Outcomes are decided by **re-inspection, never by driver error codes**. MySQL DDL
implicitly commits and MySQL 8 atomic DDL only makes a single statement
all-or-nothing, so there is no sequence-level rollback: each step emits its
statement and then asks the database what exists. That one mechanism buys exact
verification (complete only if the *definition* matches), concurrency
reconciliation (a pod losing an ADD COLUMN race re-inspects, finds a matching
column, continues) and fail-closed behaviour. A connection-scoped advisory lock
keeps reconciliation the rare path; on timeout the loser verifies read-only and
proceeds only if a peer finished the work.

Not here: the inline-to-saved-pipeline and `schedule_path` backfill
(application-level, resumable, separately authorized). There is no contract
phase — `schedule_path` is permanently nullable, since a unique index treats
NULLs as distinct and path presence is an API-level guarantee. Nothing is
dropped, narrowed or renamed.
"""

import collections.abc
import contextlib
import dataclasses
import enum
import logging
import re
import threading
import time
import typing
from typing import Any, Callable, Final, Iterator, Mapping, NamedTuple

import sqlalchemy
from alembic import migration as alembic_migration
from alembic import operations as alembic_operations

from cloud_pipelines_backend.scheduling.pipelines import db_models
from cloud_pipelines_backend.user_pipelines import db_models as user_pipeline_db_models

_logger = logging.getLogger(__name__)

_SCHEDULE_TABLE: Final[str] = db_models.ScheduledPipelineRun.__tablename__
# The foreign key's parent. The saved-pipeline column now carries a real
# constraint to this table, so the module points at it directly rather than only
# agreeing with the service layer on a name. There is deliberately no version
# table constant: no foreign key on the version pair means nothing to name, and
# the reasoning for that absence belongs with `_TARGET_FOREIGN_KEYS`, which
# states it.
_PIPELINE_TABLE: Final[str] = user_pipeline_db_models.UserPipeline.__tablename__

_PIPELINE_ID_COLUMN: Final[str] = "pipeline_task_spec_from_user_pipeline_id"
_VERSION_KEY_COLUMN: Final[str] = "pipeline_task_spec_from_user_pipeline_version_key"

_UQ_SCHEDULE_PATH: Final[str] = "uq_scheduled_pipeline_run_created_by_schedule_path"
_IX_REFERENCE: Final[str] = "ix_scheduled_pipeline_run_user_pipeline_id_version_key"
_IX_OWNER_PAGE: Final[str] = "ix_scheduled_pipeline_run_created_by_updated_at_id"
_CK_SOURCE: Final[str] = "ck_scheduled_pipeline_run_source"
_FK_USER_PIPELINE: Final[str] = "fk_scheduled_pipeline_run_user_pipeline_id"

#: Dialects whose statements here have been reviewed for lock behaviour.
#: The SQLite rebuild paths require small databases without concurrent scheduler
#: traffic. Anything else is refused rather than handed to Alembic's defaults -- on PostgreSQL
#: even a metadata-only ADD COLUMN takes ACCESS EXCLUSIVE, and the
#: `lock_wait_timeout` bound below is a no-op there, so there would be nothing
#: bounding a wait that queues the per-fire `last_run_at` write behind it.
_DDL_DIALECTS: Final[frozenset[str]] = frozenset({"mysql", "sqlite"})

#: Seconds a DDL statement may wait for the table's metadata lock before the
#: server gives up. Small on purpose: see `_bounded_metadata_lock_wait`.
_MYSQL_LOCK_WAIT_TIMEOUT_SECONDS: Final[int] = 3

#: Above this row count, startup declines to build an index at all.
#:
#: A policy, not a measurement, and it should not be read as one: no build-rate
#: or byte-size benchmark has ever been taken against this table, so even a table
#: at exactly this limit is not *known* to index quickly. What the number is
#: chosen against is the shape of the data -- rows here are user-created cron
#: entries, so a realistic population is hundreds -- which leaves this about
#: three orders of magnitude of headroom while still refusing the pathological
#: case. Deliberately lower than the earlier 1,000,000: a smaller limit costs a
#: cheaper probe and a shorter worst-case build, and nothing needs the headroom.
#: If a real measurement ever exists, it belongs here with a citation.
_MAX_INDEX_BUILD_ROWS: Final[int] = 100_000

#: Above this row count, startup declines to add the foreign key.
#:
#: Two orders of magnitude below the index limit, because the statement is a
#: different animal. `ADD INDEX` is `ALGORITHM=INPLACE, LOCK=NONE` and permits
#: concurrent DML; a validated `ADD CONSTRAINT ... FOREIGN KEY` cannot be
#: INPLACE at all (MySQL permits that only with `foreign_key_checks` disabled,
#: which skips the validation the constraint exists to perform), so it is a
#: table copy holding at least `LOCK=SHARED` -- writes are blocked for its
#: duration, and the per-fire `last_run_at` write is one of them.
#:
#: So the tolerable duration is the tolerable *write stall*, not the tolerable
#: boot time, and it is much shorter. 1,000 rows is chosen the same way as the
#: index limit -- against the shape of the data, not a benchmark -- and is
#: deliberately near the population this table is expected to hold for a long
#: time. The intent is explicit: install the constraint while the copy is
#: trivial, and fail closed rather than stall writes once it is not.
_MAX_FK_COPY_ROWS: Final[int] = 1_000

#: Server-side bound on the row probe, in milliseconds. Applies to the probe
#: SELECT only; a timeout is reported as an unknown size, which declines.
_ROW_PROBE_TIMEOUT_MILLISECONDS: Final[int] = 2_000

# Appending a column at the END of the row is INSTANT-eligible on MySQL 8.0.12+.
_MYSQL_INSTANT: Final[str] = "ALGORITHM=INSTANT"
# Applies to `pipeline_task_spec_from_user_pipeline_id` only -- the one column
# that already exists as a legacy placeholder and is therefore *widened* rather
# than *added*. The other targets are absent on a legacy table and are appended
# by `_add_column`. `pipeline_task_spec_from_pipeline_run_id` is never touched:
# it derives String(20) from its ForeignKey and stays 20.
#
# VARCHAR(20) -> VARCHAR(36) under utf8mb4 is 80 -> 144 bytes, both below the
# 256-byte boundary, so the one-byte length prefix survives and the extension is
# in place. Crossing 255 bytes would force a table copy — never widen further.
_MYSQL_INPLACE: Final[str] = "ALGORITHM=INPLACE, LOCK=NONE"
# The copying statements this module emits, and the reason each has no
# cheaper algorithm available. Stated explicitly for the same reason as the
# others: so the server refuses rather than silently choosing something else.
#
#   * The foreign key add. A *validated* FK add is COPY on MySQL, and the
#     alternative is `foreign_key_checks=OFF`, which produces a constraint the
#     optimizer trusts over data nobody checked.
#   * A collation change on `schedule_path`. Changing a collation rebuilds the
#     column and every index over it; there is no in-place form. It is the ONLY
#     column with a collation requirement, so the worst case on a legacy table
#     is TWO copying ALTERs in one boot -- `schedule_path` and the foreign key.
#     An earlier revision also converted `created_by` and put the worst case at
#     three; that conversion has been reverted, because owner identity is not
#     case-sensitive and the column needs nothing from this module.
#
# LOCK=SHARED is the least restrictive lock a copy accepts: concurrent reads
# continue, writes block until it finishes. EVERY emitter is therefore gated on
# `_MAX_FK_COPY_ROWS` rather than the larger index limit, and a test parses this
# module to prove that every user of this constant is reachable only behind that
# gate -- an earlier version of that test counted the literal `ALGORITHM=COPY`,
# which appears exactly once no matter how many statements interpolate it, and
# so stayed green when the second emitter was added.
_MYSQL_COPY: Final[str] = "ALGORITHM=COPY, LOCK=SHARED"

_LOCK_NAME: Final[str] = "tangle_scheduled_pipeline_run_expand"
_LOCK_TIMEOUT_SECONDS: Final[int] = 30


class SchedulerSchemaError(RuntimeError):
    """The live table cannot support the mapped model. Startup must not continue."""


class StepStatus(str, enum.Enum):
    #: Present and its definition matches the target exactly.
    ALREADY_PRESENT = "already_present"
    #: This run emitted the DDL and confirmed the result by re-inspection.
    APPLIED = "applied"
    #: Not applicable on this dialect.
    SKIPPED = "skipped"
    #: Absent or conflicting, and deliberately not forced. Never treated as done.
    BLOCKED = "blocked"


class _Verdict(enum.Enum):
    MATCHES = "matches"
    ABSENT = "absent"
    CONFLICTS = "conflicts"


@dataclasses.dataclass(frozen=True)
class StepResult:
    name: str
    status: StepStatus
    detail: str = ""


@dataclasses.dataclass
class MigrationReport:
    """Named readiness tiers covering every object the migration adds.

    Fixed names rather than a single verdict, because the tiers gate different
    things and a caller must not be able to infer one from another:

    * `columns_ready` — the three ORM-required columns, exactly. This is the
      only tier startup is fatal on, because it is the only one startup
      installs.
    * `indexes_ready` — installed here, but never fatal: startup installs the
      indexes, so a pod must not refuse to boot over one. The foreign key is
      likewise installed and likewise never fatal; it has no tier of its own,
      because the only thing that depends on it is `reference_writes_ready`.
    * `path_writes_ready` — columns plus `uq(created_by, schedule_path)`
      specifically. Named separately because it is the *only* thing standing
      between PR3 and duplicate schedule paths; an earlier version aggregated
      the indexes nowhere, so a blocked unique index coexisted with ready=True.
    * `check_ready` — the source CHECK, which only `create_all` installs. It is
      *defence in depth only*: the invariant is enforced primarily on write, so
      no tier that gates a feature may depend on this one. See
      `reference_writes_ready`.
    * `reference_writes_ready` — columns, the reference index, and the saved-
      pipeline foreign key. The FK belongs here rather than in a tier of its own
      because it is the constraint those writes rely on: a boot that declined it
      (too large a table, an orphaned row) must not report the writes ready.
      Deliberately
      NOT gated on `check_ready`: MySQL may refuse to add the CHECK online, or a
      pre-existing row may violate it, and neither is recoverable without
      stopping schedule runs. Gating the feature on it would mean an
      un-installable constraint disables the feature permanently.

    SKIPPED never counts as ready. A skipped CHECK is an absent CHECK, and
    calling that ready is exactly the "claimed success while hardening is
    blocked" failure this report exists to prevent.
    """

    dialect: str
    steps: list[StepResult] = dataclasses.field(default_factory=list)

    def add(self, name: str, status: StepStatus, detail: str = "") -> None:
        self.steps.append(StepResult(name=name, status=status, detail=detail))

    def status_of(self, name: str) -> StepStatus | None:
        return next((s.status for s in self.steps if s.name == name), None)

    def detail_of(self, name: str) -> str:
        return next((s.detail for s in self.steps if s.name == name), "")

    def _with_status(self, *statuses: StepStatus) -> list[StepResult]:
        return [s for s in self.steps if s.status in statuses]

    def _ready(self, *prefixes: str) -> bool:
        acceptable = {StepStatus.ALREADY_PRESENT, StepStatus.APPLIED}
        relevant = [s for s in self.steps if s.name.startswith(prefixes)]
        return bool(relevant) and all(s.status in acceptable for s in relevant)

    @property
    def applied(self) -> list[StepResult]:
        return self._with_status(StepStatus.APPLIED)

    @property
    def blocked(self) -> list[StepResult]:
        return self._with_status(StepStatus.BLOCKED)

    @property
    def not_ready(self) -> list[StepResult]:
        """Every step that does not count as ready, for diagnostics.

        Not `blocked`: an object nobody has installed yet is SKIPPED, so a
        report of `blocked` alone reads as `[]` in the ordinary incomplete
        state and names nothing the reader has to act on.
        """
        return [
            s
            for s in self.steps
            if s.status not in (StepStatus.ALREADY_PRESENT, StepStatus.APPLIED)
        ]

    def _reached(self, *names: str) -> bool:
        """Membership, not just status: a step never reached is not a pass."""
        return set(names).issubset({s.name for s in self.steps})

    @property
    def columns_ready(self) -> bool:
        """The only tier startup is fatal on, and the only DDL it emits."""
        names = [f"column:{spec.name}" for spec in _TARGET_COLUMNS]
        return self._reached(*names) and self._ready("column:")

    @property
    def indexes_ready(self) -> bool:
        names = [f"index:{spec.name}" for spec in _TARGET_INDEXES]
        return self._reached(*names) and self._ready("index:")

    @property
    def schema_ready(self) -> bool:
        """Columns and indexes: everything startup installs. Not the CHECK."""
        return self.columns_ready and self.indexes_ready

    def _all_ready(self, names: list[str]) -> bool:
        acceptable = {StepStatus.ALREADY_PRESENT, StepStatus.APPLIED}
        return self._reached(*names) and all(
            self.status_of(name) in acceptable for name in names
        )

    def _unexplained_reference_constraints(self) -> list[StepResult]:
        """Foreign keys on the reference columns that this application never installs.

        Named dynamically (`foreign_key:unexpected:<symbol>`), so they cannot
        appear in a fixed step list and must be matched by prefix. An earlier
        version relied on the fixed list alone, which meant an `ON DELETE
        CASCADE` added by hand -- a constraint that deletes a customer's
        schedule when a pipeline is deleted -- was reported and then ignored by
        the gate that decides whether reference writes are safe.
        """
        return [s for s in self.steps if s.name.startswith("foreign_key:unexpected:")]

    def tier_causes(self, tier: str) -> list[StepResult]:
        """The steps this tier depends on that are not ready.

        Derived from the same name list the readiness property uses, so a
        warning can never name a cause the gate does not actually consider. The
        CHECK is reported elsewhere and is deliberately not a cause. An
        unexplained foreign key on the reference columns IS a cause, but only of
        `reference_writes`: it says nothing about schedule-path uniqueness.
        """
        names = _WRITE_TIER_STEPS[tier]()
        extra = (
            self._unexplained_reference_constraints()
            if tier == "reference_writes"
            else []
        )
        unready = {s.name for s in self.not_ready}
        missing = [
            StepResult(name=n, status=StepStatus.SKIPPED, detail="never reached")
            for n in names
            if not self._reached(n)
        ]
        return (
            [s for s in self.steps if s.name in names and s.name in unready]
            + missing
            + extra
        )

    @property
    def path_writes_ready(self) -> bool:
        return self._all_ready(_WRITE_TIER_STEPS["path_writes"]())

    @property
    def check_ready(self) -> bool:
        return self._ready("check:")

    @property
    def reference_writes_ready(self) -> bool:
        """PR3's gate for writing pipeline references.

        Columns, the reference index, and the foreign key. Not `check_ready`:
        see the class docstring for why a feature gate must not depend on the
        CHECK.

        Also closed by an unexplained foreign key on the reference columns,
        even when every expected object is present. "The constraint we wanted
        exists" is not the same claim as "the schema is safe to write": a second
        constraint on the same column with `ON DELETE CASCADE` deletes a
        customer's schedule when a pipeline is deleted, and it is not made
        harmless by ours being correct.

        Both tier properties resolve through `_WRITE_TIER_STEPS` rather than
        rebuilding their own step list. They used to call `_write_tier_steps`
        directly, and when the foreign key was added to the table the gate kept
        reading the old list: a BLOCKED constraint with `ON DELETE CASCADE` and
        a tier reporting ready over it.
        """
        if self._unexplained_reference_constraints():
            return False
        return self._all_ready(_WRITE_TIER_STEPS["reference_writes"]())

    def summary(self) -> dict[str, Any]:
        return {
            "dialect": self.dialect,
            "columns_ready": self.columns_ready,
            "indexes_ready": self.indexes_ready,
            "schema_ready": self.schema_ready,
            "path_writes_ready": self.path_writes_ready,
            "check_ready": self.check_ready,
            "reference_writes_ready": self.reference_writes_ready,
            "steps": {s.name: s.status.value for s in self.steps},
            "blocked": {s.name: s.detail for s in self.blocked},
        }


#: Stands in for "this dialect has no collation to set". SQLite compares TEXT
#: byte-for-byte, so it is already case-sensitive and there is nothing to check;
#: a distinct sentinel keeps that apart from None, which means MySQL was asked
#: and did not answer.
_COLLATION_NOT_APPLICABLE: Final[str] = "<not-applicable>"


@dataclasses.dataclass(frozen=True)
class LiveShape:
    exists: bool
    columns: dict[str, dict[str, Any]]
    indexes: dict[str, dict[str, Any]]
    checks: dict[str, str]
    foreign_keys: dict[str, dict[str, Any]]
    #: The connection's own schema/database, so a reflected foreign key that
    #: names a schema can be told apart from one that does not. Defaults to None
    #: for hand-built shapes in tests, which is also what same-schema reflection
    #: reports.
    default_schema: str | None = None
    #: Effective collation of each identity column, read from
    #: `information_schema` because reflection does not carry one. Keyed by
    #: column name; a column absent from the mapping was not asked about.
    #:
    #: Three states per column, and they are NOT interchangeable:
    #:
    #: * a name -- MySQL answered, and `_verify_collation` judges the name;
    #: * `_COLLATION_NOT_APPLICABLE` -- the dialect has no collations to set and
    #:   compares bytes already (SQLite), which is a pass;
    #: * None -- MySQL was asked and could not answer, which is a FAILURE to
    #:   establish the property, not an absence of it.
    #:
    #: A mapping rather than a named field, so a second column earning a
    #: collation requirement does not need a second hand-written field here.
    #: One column carries one today: `schedule_path`.
    #:
    #: It lives on the shape, rather than being queried where it is checked, so
    #: that `verify_schema` stays a pure function of one inspection and every
    #: consumer of it -- including the pod that LOSES the migration lock --
    #: verifies this property too. That is the exact bug this class's history
    #: warns about: an object kind left out of `verify_schema` let the loser
    #: report ready while it was missing.
    collations: Mapping[str, str | None] = dataclasses.field(
        default_factory=lambda: {
            target.column: _COLLATION_NOT_APPLICABLE for target in _COLLATION_TARGETS
        }
    )


@dataclasses.dataclass(frozen=True)
class _ColumnSpecBase:
    """Target definition of one column: exact type, nullable, no server default.

    Exact, not "wide enough". A column of the right name and a merely compatible
    type is the failure mode this module exists to refuse: a `>=` test passes on
    a column too narrow to hold a `pipeline.id` UUID or too wide to keep the
    unique index inside InnoDB's key limit, and either is certified ready at
    startup and discovered later.

    The base exists so a column that is not a VARCHAR can be a target without
    every verifier and emitter growing an `isinstance` branch. Exactly four
    things vary by type, and they are the four members below; nothing else in
    this module asks a spec what kind of column it is.
    """

    name: str

    @property
    def sql_type(self) -> str:
        """The type fragment of a MySQL ALTER, e.g. `VARCHAR(36)`."""
        raise NotImplementedError

    @property
    def alembic_type(self) -> sqlalchemy.types.TypeEngine[Any]:
        """The same type for the Alembic path, which takes an object not text."""
        raise NotImplementedError

    def type_mismatch(self, live_type: Any) -> str | None:
        """`None` when the reflected type is the target; else why it is not."""
        raise NotImplementedError

    def widenable_from(self, live_type: Any) -> bool:
        """Is this live type repairable in place by a widen?

        `False` unless a subclass says otherwise. A type with no notion of
        "too short" has nothing to widen, so the safe default is to report the
        conflict rather than emit a MODIFY that would rewrite a decision.
        """
        return False


@dataclasses.dataclass(frozen=True)
class _ColumnSpec(_ColumnSpecBase):
    """A VARCHAR column of an exact length."""

    length: int

    @property
    def sql_type(self) -> str:
        return f"VARCHAR({self.length})"

    @property
    def alembic_type(self) -> sqlalchemy.types.TypeEngine[Any]:
        return sqlalchemy.String(self.length)

    def type_mismatch(self, live_type: Any) -> str | None:
        # `sqlalchemy.VARCHAR` is the exact family test: `CHAR` and `TEXT`
        # subclass `String` but not `VARCHAR`, so a `TEXT` column is still a
        # conflict rather than being waved through as "a string, near enough".
        if not isinstance(live_type, sqlalchemy.VARCHAR):
            return f"expected a VARCHAR column, found {type(live_type).__name__} ({live_type})"
        length = getattr(live_type, "length", None)
        if length != self.length:
            return f"expected length {self.length}, found {length}"
        return None

    def widenable_from(self, live_type: Any) -> bool:
        if not isinstance(live_type, sqlalchemy.String):
            return False
        length = getattr(live_type, "length", None)
        return length is not None and length < self.length


@dataclasses.dataclass(frozen=True)
class _JsonColumnSpec(_ColumnSpecBase):
    """A JSON column. No length, no collation, and never widened.

    MySQL and SQLite render the type as the bare word `JSON`, and both
    reflect it as something that satisfies `isinstance(t, sqlalchemy.JSON)` --
    `sqlalchemy.dialects.mysql.JSON` on MySQL, `sqlalchemy.dialects.sqlite.JSON`
    on SQLite -- so one family test covers both, the same way `sqlalchemy.VARCHAR`
    does for strings. Checked, not assumed: MySQL 5.7+ stores JSON as a native
    binary type rather than as TEXT, so the reflected type is not a string one.

    There is no length to get wrong, which removes the entire widen path: a JSON
    column that verifies as CONFLICTS is genuinely the wrong type, and repairing
    that in place would be a rewrite, not a widen. `widenable_from` therefore
    keeps the base's `False`.
    """

    @property
    def sql_type(self) -> str:
        return "JSON"

    @property
    def alembic_type(self) -> sqlalchemy.types.TypeEngine[Any]:
        return sqlalchemy.JSON()

    def type_mismatch(self, live_type: Any) -> str | None:
        if not isinstance(live_type, sqlalchemy.JSON):
            return f"expected a JSON column, found {type(live_type).__name__} ({live_type})"
        return None


@dataclasses.dataclass(frozen=True)
class _IndexSpec:
    """Target definition of one index: exact ordered columns and uniqueness."""

    name: str
    columns: tuple[str, ...]
    unique: bool


@dataclasses.dataclass(frozen=True)
class _ForeignKeySpec:
    """Target definition of one foreign key: columns, target, delete behaviour.

    Verified on all four, never on the name alone. A constraint of the right
    symbol pointing at the wrong table, or carrying `ON DELETE CASCADE`, is a
    worse outcome than no constraint at all -- the cascade one deletes a
    customer's schedule as a side effect of deleting a pipeline -- and a
    name-only check certifies exactly that as ready.
    """

    name: str
    columns: tuple[str, ...]
    referred_table: str
    referred_columns: tuple[str, ...]

    @property
    def mysql_clause(self) -> str:
        constrained = ", ".join(self.columns)
        referred = ", ".join(self.referred_columns)
        # RESTRICT on both, stated explicitly even though it is MySQL's default:
        # the statement is the documentation an operator reads in the binlog.
        return (
            f"CONSTRAINT {self.name} FOREIGN KEY ({constrained})"
            f" REFERENCES {self.referred_table} ({referred})"
            " ON DELETE RESTRICT ON UPDATE RESTRICT"
        )


#: THE target schema set. Every consumer — the lock winner, the lock-timeout
#: verifier, startup readiness, the MySQL DDL tests and PR3's gate — resolves
#: through this one tuple and `_verify_*`, so a ready verdict cannot be reached
#: by a weaker path. Adding an object here automatically extends all of them.
#:
#: The columns are nullable with no server default, which is also what keeps a
#: previous application image working: it inserts without naming them, and gets
#: NULL. `pipeline_task_spec_from_user_pipeline_id` is a *widen* (the legacy
#: placeholder String(20) exists and cannot hold a 36-char UUID); the version key
#: and `schedule_path` are *appends*. Which one applies is decided per column at
#: run time by `_widenable`, not assumed here.
_TARGET_COLUMNS: Final[tuple[_ColumnSpecBase, ...]] = (
    _ColumnSpec(name=_PIPELINE_ID_COLUMN, length=db_models.PIPELINE_ID_LENGTH),
    _ColumnSpec(name=_VERSION_KEY_COLUMN, length=db_models.PIPELINE_VERSION_KEY_LENGTH),
    _ColumnSpec(name="schedule_path", length=db_models.SCHEDULE_PATH_LENGTH),
    # Paired with `db_models.ScheduledPipelineRun.settings` in one commit; see the warning
    # on that column. No length and never widenable -- see `_JsonColumnSpec`.
    _JsonColumnSpec(name="settings"),
)

#: The unique index is safe to add before any path exists: every row's
#: schedule_path is NULL and a unique index treats NULLs as distinct. The
#: reference index leads with the pipeline id so a lookup by pipeline alone --
#: "what schedules reference this deployment" -- is served by the left prefix,
#: and pinned-version lookups by the whole key, without a second index.
#:
#: The owner-page index is the access path for the owner-scoped list, and the
#: only member of this tuple that gates nothing: an absent one costs a scan, not
#: a wrong answer, so refusing the endpoint over it would trade a slow list for
#: no list. Its column ORDER is the whole point and is verified exactly --
#: `(created_by, updated_at, id)` and no other permutation serves the equality,
#: the sort and the cursor together. See `db_models.ScheduledPipelineRun` for why
#: it is ascending under a descending ORDER BY.
_TARGET_INDEXES: Final[tuple[_IndexSpec, ...]] = (
    _IndexSpec(
        name=_UQ_SCHEDULE_PATH,
        columns=("created_by", "schedule_path"),
        unique=True,
    ),
    _IndexSpec(
        name=_IX_REFERENCE,
        columns=(_PIPELINE_ID_COLUMN, _VERSION_KEY_COLUMN),
        unique=False,
    ),
    _IndexSpec(
        name=_IX_OWNER_PAGE,
        columns=("created_by", "updated_at", "id"),
        unique=False,
    ),
)

#: Exactly one foreign key, on exactly one column.
#:
#: The saved-pipeline reference gets a real database constraint. The version
#: pair deliberately does not, and cannot usefully: InnoDB implements MATCH
#: SIMPLE, under which a composite foreign key is satisfied whenever ANY of its
#: columns is NULL -- and `version_key IS NULL` is the ordinary track-current
#: mode, so the constraint would skip the common row while looking like it
#: covered it. That is worse than not declaring it, because the next reader
#: assumes the pair is checked.
#:
#: What this constraint does NOT replace is the service-layer resolution. A
#: foreign key proves the parent row exists; pipelines are *soft*-deleted
#: (`deleted_at`), so existence is not liveness, and only the application can
#: refuse a reference to a deleted pipeline. Defence in depth, not a handover:
#: the FK closes the check-then-write race that the service layer cannot, and
#: the service layer closes the liveness gap the FK cannot.
_TARGET_FOREIGN_KEYS: Final[tuple[_ForeignKeySpec, ...]] = (
    _ForeignKeySpec(
        name=_FK_USER_PIPELINE,
        columns=(_PIPELINE_ID_COLUMN,),
        referred_table=_PIPELINE_TABLE,
        referred_columns=("id",),
    ),
)


def _write_tier_steps(
    index_name: str,
    *,
    foreign_keys: tuple[str, ...] = (),
    extra_steps: tuple[str, ...] = (),
) -> list[str]:
    """The steps a write tier depends on: every column, its index, its FKs.

    One definition per tier, consumed by both the readiness property and the
    startup warning, so the warning cannot name a cause the gate ignores.
    """
    return (
        [f"column:{spec.name}" for spec in _TARGET_COLUMNS]
        + list(extra_steps)
        + [f"index:{index_name}"]
        + [f"foreign_key:{name}" for name in foreign_keys]
    )


#: Tier name -> its step names. The two tiers depend on *different* indexes, so
#: one being ready says nothing about the other, and only the reference tier
#: depends on the foreign key -- a path write touches no pipeline reference, so
#: gating it on the constraint would close a feature for an unrelated reason.
#:
#: `path_writes` additionally depends on the collation step, and only that tier
#: does. A case-folding column is not a slow path or a degraded one -- it is a
#: column that cannot hold the identity the API promises, so accepting a path
#: write over it would store something the service cannot honour on read. A
#: reference write touches no path and is unaffected.
_WRITE_TIER_STEPS: Final[dict[str, Callable[[], list[str]]]] = {
    "path_writes": lambda: _write_tier_steps(
        _UQ_SCHEDULE_PATH, extra_steps=_COLLATION_STEPS
    ),
    "reference_writes": lambda: _write_tier_steps(
        _IX_REFERENCE, foreign_keys=(_FK_USER_PIPELINE,)
    ),
}


def _inspect(*, conn: sqlalchemy.Connection) -> LiveShape:
    inspector = sqlalchemy.inspect(conn)
    if not inspector.has_table(_SCHEDULE_TABLE):
        return LiveShape(
            exists=False, columns={}, indexes={}, checks={}, foreign_keys={}
        )
    default_schema = inspector.default_schema_name
    # A named UNIQUE is an index of the same name on MySQL, but SQLite reflects
    # it only through get_unique_constraints. Merging both means one lookup
    # answers "is this uniqueness rule present, over these columns?" on either.
    indexes: dict[str, dict[str, Any]] = {
        i["name"]: i for i in inspector.get_indexes(_SCHEDULE_TABLE) if i.get("name")
    }
    for uc in inspector.get_unique_constraints(_SCHEDULE_TABLE):
        if uc.get("name") and uc["name"] not in indexes:
            indexes[uc["name"]] = {
                "name": uc["name"],
                "column_names": list(uc.get("column_names") or []),
                "unique": True,
            }
    return LiveShape(
        exists=True,
        default_schema=default_schema,
        collations=_read_collations(
            conn=conn,
            present={c["name"] for c in inspector.get_columns(_SCHEDULE_TABLE)},
        ),
        columns={c["name"]: c for c in inspector.get_columns(_SCHEDULE_TABLE)},
        indexes=indexes,
        checks={
            c["name"]: str(c.get("sqltext") or "")
            for c in inspector.get_check_constraints(_SCHEDULE_TABLE)
            if c.get("name")
        },
        # Unnamed constraints are kept under a synthetic key rather than
        # dropped. MySQL always assigns an FK symbol, but SQLite does not, and
        # the only consumer of this mapping reports foreign keys nobody here
        # created -- so silently discarding the anonymous ones would make that
        # report claim an exhaustiveness it does not have.
        foreign_keys={
            fk.get("name") or f"<unnamed:{index}>": fk
            for index, fk in enumerate(inspector.get_foreign_keys(_SCHEDULE_TABLE))
        },
    )


def _read_collations(
    *, conn: sqlalchemy.Connection, present: set[str]
) -> dict[str, str | None]:
    """Effective collation of each identity column on MySQL.

    `information_schema.COLUMNS` reports the value MySQL actually resolved from
    column, table and schema defaults. `SHOW CREATE TABLE` prints `COLLATE` on a
    column only when it overrides its table, so a column silently inheriting a
    case-folding table default looks unremarkable in the DDL -- which is exactly
    the state this has to detect. Reflection does not expose collation at all.

    One statement for every target column. Not an optimisation: a per-column
    query that fails halfway would leave one column judged and another reported
    unreadable, which reads as a partial answer rather than as the failed
    inspection it is. Either all are known or none are.

    A column missing from the table is left out of the mapping rather than
    recorded as unreadable. The column checks own that failure and say so
    precisely; duplicating it here would raise a second, vaguer alarm about the
    same cause.

    Reading a definition, not data: no rows are touched and no statistic is
    consulted.
    """
    wanted = [
        target.column for target in _COLLATION_TARGETS if target.column in present
    ]
    if conn.dialect.name != "mysql" or not wanted:
        return {column: _COLLATION_NOT_APPLICABLE for column in wanted}
    try:
        rows = conn.execute(
            sqlalchemy.text(
                "SELECT COLUMN_NAME, COLLATION_NAME FROM information_schema.COLUMNS"
                " WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table"
                " AND COLUMN_NAME IN :columns"
            ).bindparams(sqlalchemy.bindparam("columns", expanding=True)),
            {"table": _SCHEDULE_TABLE, "columns": wanted},
        ).all()
    except sqlalchemy.exc.SQLAlchemyError:
        _logger.warning(
            "Could not read the identity collations of %s",
            _SCHEDULE_TABLE,
            exc_info=True,
        )
        return dict.fromkeys(wanted)
    answered = {
        str(name): None if collation is None else str(collation)
        for name, collation in rows
    }
    # A column present in the table but absent from the answer is unreadable,
    # not applicable: falling back to the pass value here would report ready on
    # the strength of a row the server never returned.
    return {column: answered.get(column) for column in wanted}


def _normalize_sql(text: str) -> str:
    """Compare CHECK bodies ignoring quoting and whitespace but NOT grouping.

    Internal parentheses are structural. Deleting them made the intended
    `count = 1 AND (vkey IS NULL OR pid IS NOT NULL)` compare equal to the
    weakened `(count = 1 AND vkey IS NULL) OR pid IS NOT NULL`, which permits any
    row with a pipeline reference — so the "exact" verifier would have accepted a
    constraint that does not enforce the invariant. Only a redundant balanced
    wrapper around the whole body is dropped, because MySQL reflects the body
    with one and SQLite does not.
    """
    collapsed = "".join(text.split()).replace("`", "").replace('"', "").lower()
    return _strip_outer_parens(collapsed)


def _strip_outer_parens(text: str) -> str:
    """Drop wrapper parens only while they enclose the entire expression."""
    while text.startswith("(") and text.endswith(")"):
        depth = 0
        for position, char in enumerate(text):
            depth += (char == "(") - (char == ")")
            if depth == 0 and position < len(text) - 1:
                # The opening paren closes before the end, so it is a group such
                # as `(a OR b) AND c` rather than a wrapper. Keep it.
                return text
        text = text[1:-1]
    return text


#: One `CASE WHEN <col> IS NOT NULL THEN 1 ELSE 0 END` term of the source count.
_CASE_TERM: Final[re.Pattern[str]] = re.compile(
    r"case when (\w+) is not null then 1 else 0 end"
)
_IS_NULL: Final[re.Pattern[str]] = re.compile(r"(\w+) is null")
_IS_NOT_NULL: Final[re.Pattern[str]] = re.compile(r"(\w+) is not null")


def _sql_tokens(text: str) -> list[str]:
    """Lowercased word/punctuation tokens, quoting and whitespace discarded.

    Parentheses and operators become their own tokens so that grouping can be
    walked rather than pattern-matched out of a string. Splitting a
    whitespace-collapsed body on the literal `and` would also match inside an
    identifier; tokens cannot.
    """
    cleaned = text.replace("`", " ").replace('"', " ").lower()
    for punctuation in "()+=":
        cleaned = cleaned.replace(punctuation, f" {punctuation} ")
    return cleaned.split()


def _without_wrapping_parens(tokens: list[str]) -> list[str]:
    """Drop parens only while they enclose the whole token sequence."""
    while len(tokens) >= 2 and tokens[0] == "(" and tokens[-1] == ")":
        depth = 0
        for position, token in enumerate(tokens):
            depth += (token == "(") - (token == ")")
            if depth == 0 and position < len(tokens) - 1:
                return tokens
        tokens = tokens[1:-1]
    return tokens


def _split_at_depth_zero(tokens: list[str], keyword: str) -> list[list[str]]:
    """Split on a keyword only where it is not inside parentheses.

    Depth awareness is what keeps grouping meaningful: `(a AND b) OR c` has no
    top-level `AND`, so it cannot be mistaken for `a AND (b OR c)`.
    """
    parts: list[list[str]] = []
    current: list[str] = []
    depth = 0
    for token in tokens:
        depth += (token == "(") - (token == ")")
        if depth == 0 and token == keyword:
            parts.append(current)
            current = []
            continue
        current.append(token)
    parts.append(current)
    return parts


def _phrase(tokens: list[str]) -> str:
    """The token sequence as words, with grouping punctuation removed."""
    return " ".join(
        token for token in _without_wrapping_parens(tokens) if token not in "()"
    )


class _SourceInvariant(typing.NamedTuple):
    """What the source CHECK *means*, independent of how a server prints it."""

    counted_columns: frozenset[str]
    term_count: int
    version_key_column: str
    pipeline_id_column: str


def _source_invariant_fingerprint(sql: str) -> _SourceInvariant | None:
    """Reduce a CHECK body to its meaning, or None if it is not our shape.

    MySQL 8 does not store the expression you wrote. It stores its own rewrite,
    with parentheses around every predicate, so a textual comparison against the
    declared SQL can never match and `check_ready` stays false on every boot
    forever. Comparing structure instead makes the server's rendering irrelevant
    -- the same lesson `_verify_column` learned about collation, one function
    along.

    Grouping is preserved rather than normalized away, because it carries the
    meaning. The weakened `(count = 1 AND vkey IS NULL) OR pid IS NOT NULL`
    permits any row with a pipeline reference; it has no top-level `AND`, so it
    does not parse as this shape and cannot be mistaken for the strict form.

    `term_count` accompanies the column set so that a repeated term is not
    silently deduplicated into agreement.
    """
    tokens = _without_wrapping_parens(_sql_tokens(sql))
    conjuncts = _split_at_depth_zero(tokens, "and")
    if len(conjuncts) != 2:
        return None

    counted, comparison = conjuncts[0], conjuncts[1]

    # `( CASE ... + CASE ... + CASE ... ) = 1`
    counted = _without_wrapping_parens(counted)
    if counted[-2:] != ["=", "1"]:
        return None
    columns: list[str] = []
    for term in _split_at_depth_zero(_without_wrapping_parens(counted[:-2]), "+"):
        match = _CASE_TERM.fullmatch(_phrase(term))
        if match is None:
            return None
        columns.append(match.group(1))
    if not columns:
        return None

    # `vkey IS NULL OR pipeline_id IS NOT NULL`
    alternatives = _split_at_depth_zero(_without_wrapping_parens(comparison), "or")
    if len(alternatives) != 2:
        return None
    nullable = _IS_NULL.fullmatch(_phrase(alternatives[0]))
    required = _IS_NOT_NULL.fullmatch(_phrase(alternatives[1]))
    if nullable is None or required is None:
        return None

    return _SourceInvariant(
        counted_columns=frozenset(columns),
        term_count=len(columns),
        version_key_column=nullable.group(1),
        pipeline_id_column=required.group(1),
    )


def _first_mismatch(checks: list[tuple[bool, str]]) -> tuple[_Verdict, str]:
    """Definition comparison, expressed once and reused by every verifier."""
    for ok, message in checks:
        if not ok:
            return _Verdict.CONFLICTS, message
    return _Verdict.MATCHES, ""


def _verify_column(shape: LiveShape, spec: _ColumnSpec) -> tuple[_Verdict, str]:
    """Structure, not rendered text: family, length, nullability, default.

    This used to compare `str(live["type"]).upper()` against the declared
    `VARCHAR(n)`. That is a defect a reviewer caught before it reached a live
    database, and it was fatal: SQLAlchemy renders a reflected MySQL column that
    carries an explicit collation as `VARCHAR(36) COLLATE "utf8mb4_bin"`, so the
    strings differ, the verdict is CONFLICTS, `_widenable` declines it (the
    length is already right, so there is nothing to widen), and a conflicting
    mapped column is fatal -- every pod fails to boot, on a column that is
    perfectly capable of holding the data.

    Collation is a real constraint, but it is a constraint on the FOREIGN KEY,
    not on whether the ORM can select the column. It is checked where it
    matters, against the parent column, in `_foreign_key_preflight` -- so a
    mismatch closes the reference tier and leaves path writes and inline
    schedules working, instead of taking the service down.

    What counts as the right type is the spec's to answer, not this function's:
    `type_mismatch` is the one place a family and a length are compared, so a
    non-VARCHAR target cannot reach a verdict by a path that never learned
    about it. Nullability and the absent server default are checked here
    because they are the same demand for every column.
    """
    live = shape.columns.get(spec.name)
    if live is None:
        return _Verdict.ABSENT, ""
    type_mismatch = spec.type_mismatch(live["type"])
    return _first_mismatch(
        [
            (type_mismatch is None, type_mismatch or ""),
            # A NOT NULL added column would break a previous image's INSERTs.
            (
                bool(live.get("nullable")),
                f"expected nullable, found NOT NULL on {spec.name}",
            ),
            (
                live.get("default") is None,
                f"unexpected server default {live.get('default')!r}",
            ),
        ]
    )


def _verify_index(
    shape: LiveShape, *, name: str, columns: list[str], unique: bool
) -> tuple[_Verdict, str]:
    live = shape.indexes.get(name)
    if live is None:
        return _Verdict.ABSENT, ""
    live_columns = list(live.get("column_names") or [])
    return _first_mismatch(
        [
            (
                live_columns == columns,
                f"expected columns {columns}, found {live_columns}",
            ),
            (
                bool(live.get("unique")) == unique,
                f"expected unique={unique}, found {live.get('unique')}",
            ),
        ]
    )


#: Delete/update rules that do not destroy or rewrite a schedule row. MySQL's
#: `SHOW CREATE TABLE` -- which is what SQLAlchemy reflects -- OMITS the clause
#: when it is the default, so a constraint created with `ON DELETE RESTRICT`
#: reflects with no `ondelete` at all. Requiring the literal string would
#: therefore report our own freshly installed constraint as conflicting. The
#: property that actually matters is negative: nothing that deletes or nulls a
#: row of ours as a side effect of a parent delete.
_NON_DESTRUCTIVE_FK_ACTIONS: Final[frozenset[str]] = frozenset(
    {"", "RESTRICT", "NO ACTION"}
)


def _fk_action(live: dict[str, Any], key: str) -> str:
    return str((live.get("options") or {}).get(key) or "").upper()


def _verify_foreign_key(
    shape: LiveShape, spec: _ForeignKeySpec
) -> tuple[_Verdict, str]:
    """Columns, target table, target columns and delete behaviour -- not the name.

    A same-named constraint pointing somewhere else is a CONFLICT, not a match:
    the name is ours, so a mismatch means something installed a constraint we do
    not understand, and reporting it ready would gate reference writes on a rule
    nobody has read.
    """
    live = shape.foreign_keys.get(spec.name)
    if live is None:
        return _Verdict.ABSENT, ""
    constrained = tuple(live.get("constrained_columns") or ())
    referred = tuple(live.get("referred_columns") or ())
    # Reflection reports the schema only when the DDL named one, so same-schema
    # is None here and on MySQL. A named schema equal to our own is the same
    # table; anything else is a DIFFERENT table that happens to share a name,
    # and `referred_table` alone cannot tell them apart.
    referred_schema = live.get("referred_schema") or None
    same_schema = referred_schema is None or referred_schema == shape.default_schema
    ondelete = _fk_action(live, "ondelete")
    onupdate = _fk_action(live, "onupdate")
    return _first_mismatch(
        [
            (
                constrained == spec.columns,
                f"expected columns {list(spec.columns)}, found {list(constrained)}",
            ),
            (
                str(live.get("referred_table") or "") == spec.referred_table,
                f"expected to reference {spec.referred_table}, found {live.get('referred_table')!r}",
            ),
            (
                same_schema,
                f"expected to reference {spec.referred_table} in this database"
                f" ({shape.default_schema!r}), found it in {referred_schema!r}",
            ),
            (
                referred == spec.referred_columns,
                f"expected to reference {list(spec.referred_columns)}, found {list(referred)}",
            ),
            # The one that would cost a customer their schedule.
            (
                ondelete in _NON_DESTRUCTIVE_FK_ACTIONS,
                f"ON DELETE {ondelete} would modify a schedule when a pipeline is deleted;"
                " expected RESTRICT or NO ACTION",
            ),
            (
                onupdate in _NON_DESTRUCTIVE_FK_ACTIONS,
                f"ON UPDATE {onupdate} would rewrite a schedule's reference; expected RESTRICT or NO ACTION",
            ),
        ]
    )


def _verify_check(shape: LiveShape) -> tuple[_Verdict, str]:
    """Compare what the constraint MEANS, falling back to text if it will not parse.

    A reviewer found that the textual comparison could never succeed on MySQL,
    which stores its own rewrite of the expression rather than the submitted
    text. The constraint was installed and enforcing correctly the whole time;
    only the verdict was wrong -- permanently, on every boot.

    Structure first, then text. The fallback keeps this no worse than the old
    behaviour for any body neither side can parse, and the structural path fixes
    the case that actually occurs. Note that no readiness tier gates on
    `check_ready` (see `_write_tier_steps`), so the damage was a standing false
    conflict in the boot log rather than a closed feature -- worth fixing
    because an operator who learns to ignore one conflict line ignores the next
    one too.
    """
    if _CK_SOURCE not in shape.checks:
        return _Verdict.ABSENT, ""
    live = shape.checks[_CK_SOURCE]
    if not live:
        # An unverifiable definition must not be reported as satisfied: the
        # readiness gate would then describe an unknown constraint as checked.
        return (
            _Verdict.CONFLICTS,
            "dialect does not expose the check body; cannot verify",
        )

    declared_shape = _source_invariant_fingerprint(db_models.SOURCE_INVARIANT_CHECK_SQL)
    live_shape = _source_invariant_fingerprint(live)
    if declared_shape is not None and live_shape is not None:
        if live_shape == declared_shape:
            return _Verdict.MATCHES, ""
        return _Verdict.CONFLICTS, f"different check semantics: {live}"

    # One of the two did not parse as the source-invariant shape. That is not
    # proof of a different constraint, so fall back to the textual comparison
    # this replaced rather than reporting a conflict we cannot substantiate.
    if _normalize_sql(live) == _normalize_sql(db_models.SOURCE_INVARIANT_CHECK_SQL):
        return _Verdict.MATCHES, ""
    return _Verdict.CONFLICTS, f"different check body: {live}"


def _operations(conn: sqlalchemy.Connection) -> alembic_operations.Operations:
    return alembic_operations.Operations(
        alembic_migration.MigrationContext.configure(conn)
    )


def _apply(
    *,
    conn: sqlalchemy.Connection,
    report: MigrationReport,
    step: str,
    verify: Callable[[LiveShape], tuple[_Verdict, str]],
    emit: Callable[[], None],
    required: bool,
    repairable: Callable[[LiveShape], bool] | None = None,
) -> None:
    """Bring one object to its target definition, deciding by re-inspection.

    The emit/re-inspect ordering is what makes this concurrency-safe without
    interpreting driver error codes: if a peer applied the same DDL first, our
    statement fails, we re-inspect, find an exactly matching object, and report
    ALREADY_PRESENT. An error with a *non*-matching object is reported as
    blocked, carrying the original message.
    """
    shape = _inspect(conn=conn)
    verdict, detail = verify(shape)
    if verdict is _Verdict.MATCHES:
        report.add(step, StepStatus.ALREADY_PRESENT)
        return
    if verdict is _Verdict.CONFLICTS and not (repairable and repairable(shape)):
        report.add(step, StepStatus.BLOCKED, detail)
        if required:
            raise SchedulerSchemaError(f"{step}: {detail}")
        return

    error: Exception | None = None
    try:
        emit()
    except Exception as exc:  # reconciled below by re-inspection, not by code
        error = exc
        # Re-inspection is the entire trust mechanism, so the connection has to
        # be usable before it runs. A failure before MySQL's implicit DDL commit,
        # or SQLAlchemy 2.0 autobegin, can leave an aborted transaction in which
        # the reflection queries themselves fail. The advisory lock is
        # connection-scoped and survives this.
        try:
            conn.rollback()
        except Exception:
            _logger.warning(
                "Rollback after failed DDL for %s also failed",
                step,
                exc_info=True,
            )

    verdict, detail = verify(_inspect(conn=conn))
    if verdict is _Verdict.MATCHES:
        report.add(step, StepStatus.ALREADY_PRESENT if error else StepStatus.APPLIED)
        return
    message = detail or (str(error) if error else "object still absent after DDL")
    report.add(step, StepStatus.BLOCKED, message)
    if required:
        raise SchedulerSchemaError(f"{step}: {message}")


#: What an ABSENT object means per kind, on the read-only verification paths.
#: The install path decides by re-inspection instead -- see `_apply` -- so an
#: index that was attempted and did not land is BLOCKED, not a not-yet.
#:
#: Columns are fatal because the ORM selects them. An absent index means the
#: build has not run yet or another process holds the lock. An absent foreign
#: key means the same, or that the preflight refused it. An absent CHECK may be
#: permanent. None of them is a reason to refuse a boot: the tiers that depend
#: on them close, and inline scheduling keeps working.
_ABSENT_POLICY: Final[tuple[tuple[str, StepStatus], ...]] = (
    ("column:", StepStatus.BLOCKED),
    ("index:", StepStatus.SKIPPED),
    ("foreign_key:", StepStatus.SKIPPED),
    ("check:", StepStatus.SKIPPED),
    # SKIPPED, not BLOCKED: a case-folding column still boots the service and
    # still serves every non-path feature. It closes `path_writes` and nothing
    # else, and a later boot can clear it by converting.
    ("collation:", StepStatus.SKIPPED),
)


def _absence_policy(step: str) -> StepStatus:
    """What ABSENT means for this step kind. Raises rather than guessing.

    A kind with no policy is an exhaustiveness bug in this module, not a live
    schema outcome, and it must not be given a default. Every readiness tier
    names its steps explicitly, so an unrecognised step belongs to no tier: a
    lenient default would leave startup running and every feature gate reading
    true, which is the fail-open behaviour this module exists to prevent.
    """
    for prefix, policy in _ABSENT_POLICY:
        if step.startswith(prefix):
            return policy
    raise SchedulerSchemaError(f"no absence policy for verification step {step!r}")


def verify_columns(shape: LiveShape) -> list[tuple[str, _Verdict, str]]:
    return [
        (f"column:{spec.name}", *_verify_column(shape, spec))
        for spec in _TARGET_COLUMNS
    ]


def verify_indexes(shape: LiveShape) -> list[tuple[str, _Verdict, str]]:
    return [
        (
            f"index:{spec.name}",
            *_verify_index(
                shape,
                name=spec.name,
                columns=list(spec.columns),
                unique=spec.unique,
            ),
        )
        for spec in _TARGET_INDEXES
    ]


def verify_hardening_objects(
    shape: LiveShape,
) -> list[tuple[str, _Verdict, str]]:
    return [(step, *verify(shape)) for step, verify in _hardening_checks()]


def verify_collation(shape: LiveShape) -> list[tuple[str, _Verdict, str]]:
    """One step per identity column, in the shape `record_verification` consumes."""
    return [
        (_collation_step(target.column), *_verify_collation(shape, target))
        for target in _COLLATION_TARGETS
    ]


def verify_foreign_keys(shape: LiveShape) -> list[tuple[str, _Verdict, str]]:
    """The expected foreign key, plus any other one on the reference columns.

    Two different questions, deliberately answered together. The first is
    readiness: is OUR constraint present and exactly right? The second is the
    one this used to answer alone -- is there a constraint here that this
    application did not install? Reporting only the first would make an
    unexplained `ON DELETE CASCADE` on the version key invisible, because no
    target spec names it and every other verifier here ignores foreign keys.

    Extras are reported, never dropped. Removing a constraint nobody can explain
    is an operator decision made with evidence, not a boot-time guess.
    """
    return [
        (f"foreign_key:{spec.name}", *_verify_foreign_key(shape, spec))
        for spec in _TARGET_FOREIGN_KEYS
    ] + _verify_unexpected_foreign_keys(shape)


def _verify_unexpected_foreign_keys(
    shape: LiveShape,
) -> list[tuple[str, _Verdict, str]]:
    """Foreign keys on the reference columns that this application never installs.

    Recorded separately from the target verdicts because the install path emits
    only the target steps -- so without this, a hand-added `ON DELETE CASCADE`
    would be reported by the read-only verify path and invisible on the ordinary
    boot that installs the expected constraint successfully.
    """
    expected = {spec.name for spec in _TARGET_FOREIGN_KEYS}
    reference_columns = {_PIPELINE_ID_COLUMN, _VERSION_KEY_COLUMN}
    return [
        (
            f"foreign_key:unexpected:{name}",
            _Verdict.CONFLICTS,
            f"unexpected foreign key on {sorted(reference_columns.intersection(constrained))}"
            f" referencing {live.get('referred_table')}"
            f" (ON DELETE {_fk_action(live, 'ondelete') or 'UNSPECIFIED'});"
            " this application installs only"
            f" {sorted(expected)}, so it was not installed here",
        )
        for name, live in sorted(shape.foreign_keys.items())
        if name not in expected
        and (constrained := set(live.get("constrained_columns") or []))
        & reference_columns
    ]


def verify_schema(shape: LiveShape) -> list[tuple[str, _Verdict, str]]:
    """Every object in the target set, verified exactly.

    Foreign keys included, so the pod that LOSES the migration lock verifies the
    same set as the winner installs. An earlier version of this function omitted
    an object kind, and the loser reported ready while that object was missing.
    """
    return (
        verify_columns(shape)
        + verify_collation(shape)
        + verify_indexes(shape)
        + verify_foreign_keys(shape)
    )


def record_verification(
    *, report: MigrationReport, results: list[tuple[str, _Verdict, str]]
) -> None:
    """The single place a verdict becomes a status.

    One verification path and one recording path. Every laxer side path the
    reviews found — the lock-timeout branch checking fewer objects, indexes
    aggregated into no tier, SQLite matching constraints by name — existed
    because some consumer decided readiness for itself. Nothing decides it here
    except this function, so a new consumer cannot be laxer than the rest.
    """
    for step, verdict, detail in results:
        if verdict is _Verdict.MATCHES:
            report.add(step, StepStatus.ALREADY_PRESENT)
            continue
        if verdict is _Verdict.CONFLICTS:
            report.add(step, StepStatus.BLOCKED, detail)
            continue
        report.add(
            step,
            _absence_policy(step),
            detail or _absence_hint(step=step, dialect=report.dialect),
        )


#: What an absent index actually costs, per index. Split because the two kinds
#: of consequence are not interchangeable: the tier indexes close a feature,
#: because a write that needs the guarantee is refused rather than performed
#: unguarded; the owner-page index closes nothing and only makes an answer
#: expensive. One shared sentence would tell an operator that listing schedules
#: is disabled when it is merely slow, or -- worse in the other direction -- that
#: a missing uniqueness guarantee is a performance matter.
_INDEX_ABSENCE_CONSEQUENCE: Final[dict[str, str]] = {
    _UQ_SCHEDULE_PATH: "schedule-path writes are disabled",
    _IX_REFERENCE: "pipeline reference writes are disabled",
    _IX_OWNER_PAGE: (
        "no feature is disabled, but the owner-scoped schedule list has no access path and the"
        " server scans rows belonging to other users to fill a page"
    ),
}


def _index_absence_consequence(*, step: str) -> str:
    """The consequence sentence for one `index:<name>` step.

    Raises on an unknown index for the same reason `_absence_policy` does: a new
    index whose consequence nobody stated would otherwise inherit a claim about
    write tiers that may be false of it.
    """
    name = step.removeprefix("index:")
    consequence = _INDEX_ABSENCE_CONSEQUENCE.get(name)
    if consequence is None:
        raise SchedulerSchemaError(
            f"no absence consequence recorded for index {name!r}"
        )
    return consequence


def _absence_hint(*, step: str, dialect: str) -> str:
    """What an absent object of this kind actually means for the operator.

    States what is untrue and what is therefore disabled. Naming a mechanism here
    has been wrong twice: once pointing at a discarded operator CLI, once
    promising a boot-time retry startup did not then perform. Startup does build
    the indexes now, so the index hint may name the retry -- but it still says
    what is disabled first, because that is the part an operator acts on.

    Split by kind because one shared sentence was a reporting regression (pi-29):
    it told a missing *column* that a hardening step had not installed an *index*,
    and told a missing CHECK that writes were disabled when the CHECK gates no
    writes at all. Three kinds, three different consequences.
    """
    if step.startswith("column:"):
        # Startup's own job, and fatal: migrate_db raises when columns_ready is false.
        return "absent; startup schema expansion is incomplete and the application cannot serve"
    if step.startswith("index:"):
        if dialect == "sqlite":
            return "absent; recreate the database"
        return (
            f"absent; the exact index is not installed, so {_index_absence_consequence(step=step)}."
            " The next boot retries the build unless the size gate declined it, and a pod"
            " that lost the migration lock sees this while the winner is still building"
        )
    if step.startswith("collation:"):
        return (
            "absent; schedule_path still compares case-insensitively, so path writes are"
            " disabled rather than allowed to store an identity the column cannot keep"
            " distinct. The next boot retries the conversion unless the size gate declined"
            " it; the conversion rewrites the table, so a large one is left to an operator"
        )
    if step.startswith("foreign_key:"):
        if dialect == "sqlite":
            # No ALTER ... ADD CONSTRAINT here; create_all builds it or nothing does.
            return "absent; recreate the database"
        return (
            "absent; pipeline reference writes are disabled. The next boot retries the add unless"
            " the size gate declined it or the preflight found existing reference rows -- the step"
            " detail says which"
        )
    if step.startswith("check:"):
        # Deliberately silent about writes: no tier gates on check_ready.
        return (
            "absent; installed only on a fresh database. Defence in depth only --"
            " application validation remains primary and no readiness tier depends on it"
        )
    raise SchedulerSchemaError(f"no absence hint for verification step {step!r}")


def _widenable(live: dict[str, Any] | None, spec: _ColumnSpecBase) -> bool:
    """Is the only discrepancy one the spec's own type can repair in place?

    Deliberately narrow. A NOT NULL column, an unexpected server default or a
    type the spec does not recognise as merely too small is a conflict to
    report, not damage to repair: MODIFY would silently overwrite a decision
    someone made on purpose. A type with no length -- JSON -- is never
    widenable, which is the base class's default rather than a check here.
    """
    if live is None:
        return False
    if not live.get("nullable") or live.get("default") is not None:
        return False
    return spec.widenable_from(live.get("type"))


def _expand(*, conn: sqlalchemy.Connection, report: MigrationReport) -> None:
    """Startup DDL: the three ORM-required columns, the indexes, then the FK.

    The order is not incidental. The widen must precede the constraint, because
    before it the child column is too short to hold a `pipeline.id` at all; and
    the reference index must precede it too, or InnoDB creates its own child
    index under a name nothing here verifies.

    ADD INDEX scans and sorts the table, so startup index builds require
    deployment prerequisites that operators must verify:

    * Run a single scheduler instance and keep the previous instance serving
      until the replacement is ready. A slow build then overlaps with a
      working scheduler. During a brief rollout overlap, the loser of the
      advisory lock verifies read-only and boots rather than waiting for the
      build.
    * `ALGORITHM=INPLACE, LOCK=NONE` permits concurrent DML for a secondary
      index add (MySQL 8.0 reference manual, table 17.16), so the build itself
      does not block the per-fire `last_run_at` write. What it needs is a brief
      exclusive metadata lock at each end, and `_bounded_metadata_lock_wait`
      bounds the wait for that.

    Under these prerequisites, a slow build delays one instance's startup
    while the previous instance continues serving.

    Caveats worth keeping in view, because they invalidate the reasoning rather
    than merely weakening it:

    * Revisit startup DDL before running multiple scheduler instances or
      changing rollout availability. The advisory lock prevents concurrent
      migration writes, but it does not itself guarantee serving capacity.

    * `ALGORITHM=INPLACE, LOCK=NONE` stops the build blocking DML; it does not
      make the build quick, and its duration scales with table size. Nothing
      bounds that once the statement is running: the 3-second bound covers
      metadata-lock *acquisition*, and `max_execution_time` applies only to
      SELECTs. So the bound is taken beforehand -- see
      `_refuse_if_table_is_large`, which declines the build outright above
      `_MAX_INDEX_BUILD_ROWS` and reports the tier closed instead.

    * Still unobserved: server lock behaviour under concurrent `last_run_at`
      writes. The size gate reports the threshold it declined against -- it
      measures no count -- so the first boot against a large table says why it
      stopped rather than stalling silently, but no live-MySQL run has confirmed
      the lock behaviour itself.

    Index failures are deliberately **not** fatal. A missing index means the
    write tiers that depend on it stay closed, which is a feature staying off;
    a missing column means the ORM selects a column that is not there, which is
    every request failing. Those do not deserve the same response. A failed
    build is retried on the next boot, and the retry is a no-op if a previous
    attempt actually succeeded.
    """
    dialect = report.dialect
    if dialect not in _DDL_DIALECTS:
        # Checked once, before any emitter runs, because the per-statement
        # guards are easy to forget when adding a new one -- which is exactly
        # what happened: `_widen` refused unknown dialects while `_add_column`
        # still fell through to a generic ALTER TABLE ADD COLUMN.
        #
        # Raised, not reported: columns are startup-fatal, so a dialect whose
        # statements nobody has reviewed for lock behaviour must stop the
        # process rather than quietly emit DDL against it. Verification is
        # read-only and still runs on any dialect.
        raise SchedulerSchemaError(
            f"refusing to emit schema DDL on dialect {dialect!r}: no non-blocking statements are defined for it"
        )

    for spec in _TARGET_COLUMNS:
        live = _inspect(conn=conn).columns.get(spec.name)
        _apply(
            conn=conn,
            report=report,
            step=f"column:{spec.name}",
            verify=lambda s, spec=spec: _verify_column(s, spec),
            emit=lambda spec=spec, live=live: _emit_column(
                conn=conn, dialect=dialect, spec=spec, live=live
            ),
            required=True,
            repairable=lambda s, spec=spec: _widenable(s.columns.get(spec.name), spec),
        )

    # Before the indexes: the unique index must be built over the collation the
    # column is going to keep. See `_expand_collation`.
    _expand_collation(conn=conn, report=report)
    _expand_indexes(conn=conn, report=report)
    _expand_foreign_keys(conn=conn, report=report)


def _expand_indexes(*, conn: sqlalchemy.Connection, report: MigrationReport) -> None:
    """Install the target indexes, independently, non-fatally, idempotently.

    Independently on purpose: the two write tiers gate on one index each, so a
    conflict on one must not withhold the other -- and the owner-page index gates
    nothing at all, so a conflict on it must not withhold either of them.

    A build is declined outright if the table is too large to index during
    startup, or if its size cannot be established -- see
    `_refuse_if_table_is_large`. Only a missing index is subject to that gate.

    No census, no COUNT, no GROUP BY. The unique index is the only object here
    whose build can fail on data, and it cannot fail on this schema today:
    `schedule_path` has no writer anywhere in the codebase, so every value is
    NULL, and InnoDB permits many NULLs in a unique index. Installing it now,
    before the writer that PR3 adds, is what keeps that true -- the ordering is
    enforced by the writer gating on `path_writes_ready`, which is false until
    this index exists. The reference and owner-page indexes are not unique and so
    cannot fail on data at all.

    A conflicting index of the same name is reported and left alone. Dropping
    and recreating an index this code did not create is an operator decision
    made with evidence, not something to infer from a name collision.
    """
    if not report.columns_ready:
        # Unreachable through `_expand`, whose column steps are fatal, but an
        # index over a column that is absent or the wrong width is not an index
        # anyone should be building. Report what is there and stop.
        record_verification(report=report, results=verify_indexes(_inspect(conn=conn)))
        return

    # Only a *missing* index needs a build, so only a missing index is subject to
    # the size gate. Consulting it unconditionally would be worse than useless:
    # an unanswerable probe would report indexes that already exist as skipped
    # and close write tiers that are genuinely open.
    shape = _inspect(conn=conn)
    # Only an ABSENT index is gated. A CONFLICTS verdict must reach `_apply` so it
    # is reported BLOCKED with the exact mismatch: recording "too big, build it
    # deliberately" about an index that already exists under the wrong definition
    # sends the operator to do the one thing that cannot help.
    absent = {
        spec.name
        for spec in _TARGET_INDEXES
        if _verify_index(
            shape,
            name=spec.name,
            columns=list(spec.columns),
            unique=spec.unique,
        )[0]
        is _Verdict.ABSENT
    }
    too_big = (
        _refuse_if_table_is_large(conn=conn, dialect=report.dialect) if absent else None
    )

    for spec in _TARGET_INDEXES:
        if too_big is not None and spec.name in absent:
            # Reported rather than attempted: a build whose duration nothing can
            # bound does not belong on the boot path.
            report.add(f"index:{spec.name}", StepStatus.SKIPPED, detail=too_big)
            continue
        _apply(
            conn=conn,
            report=report,
            step=f"index:{spec.name}",
            verify=lambda s, spec=spec: _verify_index(
                s,
                name=spec.name,
                columns=list(spec.columns),
                unique=spec.unique,
            ),
            emit=lambda spec=spec: _create_index(
                conn=conn, dialect=report.dialect, spec=spec
            ),
            required=False,
        )


def _expand_foreign_keys(
    *, conn: sqlalchemy.Connection, report: MigrationReport
) -> None:
    """Add the saved-pipeline foreign key: preflight, size gate, copy, re-inspect.

    Ordered last on purpose, and dependent on the reference index. InnoDB
    requires an index on the child columns and will silently create one if none
    exists -- which would leave a constraint-owned index alongside, or instead
    of, `_IX_REFERENCE`, with a name nothing here verifies. Installing the index
    first makes the constraint adopt it.

    The preflight is the part that is NOT about performance. A validated add
    fails outright on an orphan row, and the failure would be reported as a
    blocked step with a driver message, on every boot, with no statement of what
    an operator should do. So the orphan question is asked BEFORE the statement
    -- though not first among the checks: on MySQL the bounded size probe runs
    ahead of it, so the anti-join is never reached on a table already judged too
    large to copy. It is read-only either way, and the answer never mutates
    anything: no NULLing, no delete, no remap. A schedule pointing at a pipeline that does not exist is a fact about
    production that a migration must surface, not erase -- and nulling it is not
    even available, because the source CHECK requires exactly one source column
    to be set, so a nulled row becomes a schedule with no source at all.

    Non-fatal throughout, like the indexes: a pod that cannot add the constraint
    boots, serves, and reports `reference_writes` closed.
    """
    if not report.columns_ready:
        record_verification(
            report=report, results=verify_foreign_keys(_inspect(conn=conn))
        )
        return

    shape = _inspect(conn=conn)
    absent = {
        spec.name
        for spec in _TARGET_FOREIGN_KEYS
        if _verify_foreign_key(shape, spec)[0] is _Verdict.ABSENT
    }
    decline = (
        _foreign_key_preflight(conn=conn, dialect=report.dialect, shape=shape)
        if absent
        else None
    )

    for spec in _TARGET_FOREIGN_KEYS:
        if decline is not None and spec.name in absent:
            status, detail = decline
            report.add(f"foreign_key:{spec.name}", status, detail=detail)
            continue
        _apply(
            conn=conn,
            report=report,
            step=f"foreign_key:{spec.name}",
            verify=lambda s, spec=spec: _verify_foreign_key(s, spec),
            emit=lambda spec=spec: _create_foreign_key(
                conn=conn, dialect=report.dialect, spec=spec
            ),
            required=False,
        )

    # After the attempt, not before: a rebuild on SQLite reflects and recreates
    # the table, so the set of constraints present is only final once it is done.
    record_verification(
        report=report,
        results=_verify_unexpected_foreign_keys(_inspect(conn=conn)),
    )


def _foreign_key_preflight(
    *, conn: sqlalchemy.Connection, dialect: str, shape: LiveShape
) -> tuple[StepStatus, str] | None:
    """Everything that must hold before a copying ALTER is worth emitting.

    Returns the (status, detail) to record instead of attempting the add, or
    None to proceed. A status rather than a bare reason because the outcomes are
    not equivalent: an unmet precondition is a SKIPPED not-yet that a later boot
    may clear on its own, while an orphaned row is BLOCKED and needs a person.
    Collapsing the two would report a state nobody is coming to fix as routine.

    Order matters: the cheapest and most certain refusals come first, and the
    two that touch data come last.
    """
    if dialect not in _DDL_DIALECTS:
        return (
            StepStatus.SKIPPED,
            f"skipped: no reviewed statement adds a constraint on dialect {dialect!r}",
        )
    reference_index = _verify_index(
        shape,
        name=_IX_REFERENCE,
        columns=[_PIPELINE_ID_COLUMN, _VERSION_KEY_COLUMN],
        unique=False,
    )
    if reference_index[0] is not _Verdict.MATCHES:
        # Without it InnoDB invents its own index for the constraint, under a
        # name nothing here verifies.
        return (
            StepStatus.SKIPPED,
            f"skipped: {_IX_REFERENCE} is not present, and the constraint must adopt that index"
            " rather than have InnoDB create one",
        )
    if dialect != "mysql":
        # SQLite rebuilds the table rather than copying it under a lock, so the
        # size gate below does not apply -- but two other things do.
        #
        # A rebuild reflects the table and recreates it, and reflection does not
        # faithfully round-trip an UNNAMED foreign key: SQLAlchemy parses it out
        # of the DDL and then cannot match it to PRAGMA foreign_keys, so the
        # recreated table silently loses it. Dropping a constraint nobody can
        # explain is exactly what this module refuses to do, so an unexplained
        # one blocks the rebuild instead of being destroyed by it.
        if _verify_unexpected_foreign_keys(shape):
            return (
                StepStatus.SKIPPED,
                f"skipped: {_SCHEDULE_TABLE} carries a foreign key this application does not install,"
                f" and adding the constraint on dialect {dialect!r} rebuilds the table, which would"
                " drop it. Resolve the unexpected constraint deliberately, then restart.",
            )
        # The orphan preflight still applies: a rebuild with a constraint the
        # data violates fails just the same.
        return _refuse_if_orphans_exist(conn=conn)
    # Metadata, not data: two rows out of information_schema, no table scan. It
    # comes before the probes that touch the table because it is both cheaper
    # and more certain than either.
    mismatched = _refuse_if_collations_differ(conn=conn)
    if mismatched is not None:
        return mismatched
    # Size BEFORE orphans -- the order a reviewer asked for, and the order the
    # costs argue for. The orphan probe is a LEFT JOIN over this table, so
    # running it first made that scan reachable on a table this code was about
    # to reject as too large to copy anyway. `LIMIT 1` bounds only the case
    # where an orphan is found EARLY; with many valid references and no orphan
    # the join runs to completion. Deciding the cheap, certain refusal first
    # means the size gate also bounds the anti-join.
    too_big = _refuse_if_table_is_large(
        conn=conn,
        dialect=dialect,
        limit=_MAX_FK_COPY_ROWS,
        operation="foreign key table copy",
        remedy="Add the constraint deliberately, then restart.",
    )
    if too_big is not None:
        return (StepStatus.SKIPPED, too_big)
    return _refuse_if_orphans_exist(conn=conn)


def _refuse_if_orphans_exist(
    *, conn: sqlalchemy.Connection
) -> tuple[StepStatus, str] | None:
    """Refuse -- never repair -- when a reference names no pipeline."""
    try:
        orphans = _saved_pipeline_references_without_a_pipeline(conn=conn)
    except sqlalchemy.exc.SQLAlchemyError as error:
        return (
            StepStatus.SKIPPED,
            f"skipped: could not establish whether {_SCHEDULE_TABLE} holds unresolvable saved-pipeline"
            f" references ({type(error).__name__}), so a validated add could fail on data."
            " Check the data, add the constraint deliberately, then restart.",
        )
    if orphans:
        return (
            StepStatus.BLOCKED,
            f"{_SCHEDULE_TABLE} holds at least one row whose {_PIPELINE_ID_COLUMN} names no"
            f" {_PIPELINE_TABLE} row (example: {orphans[0]!r}). A validated foreign key add would"
            " fail on it. Nothing was modified -- not nulled, not deleted, not remapped: resolve"
            " the reference deliberately, then restart.",
        )
    return None


class _ColumnCollation(NamedTuple):
    charset: str
    collation: str

    def __str__(self) -> str:
        return f"{self.charset}/{self.collation}"


def _effective_collations(
    *, conn: sqlalchemy.Connection
) -> dict[str, _ColumnCollation]:
    """The RESOLVED charset and collation of the child and parent columns.

    Read from `information_schema.COLUMNS`, deliberately, because that view
    reports the *effective* values: MySQL resolves column-level, table-level and
    schema-level defaults before storing them there. `SHOW CREATE TABLE` does
    not -- it prints `COLLATE` on a column only when the column overrides its
    table -- so a child and parent that inherit DIFFERENT table defaults look
    identical in the DDL and are not. Reflection has the same blind spot, which
    is why this is a separate query rather than something read off `LiveShape`.

    Note what this is not: `information_schema.TABLES.TABLE_ROWS` was rejected
    elsewhere in this module because it is a *statistic* -- approximate, lazily
    recalculated, switchable off. `COLLATION_NAME` is a *definition*. It is
    exact, it is metadata rather than table data, and reading it scans no rows.

    Returned keyed by `"table.column"`, empty for anything the view does not
    report, so the caller distinguishes "differs" from "could not tell".
    """
    rows = conn.execute(
        sqlalchemy.text(
            "SELECT TABLE_NAME, COLUMN_NAME, CHARACTER_SET_NAME, COLLATION_NAME"
            " FROM information_schema.COLUMNS"
            " WHERE TABLE_SCHEMA = DATABASE()"
            "   AND ((TABLE_NAME = :child_table AND COLUMN_NAME = :child_column)"
            "     OR (TABLE_NAME = :parent_table AND COLUMN_NAME = :parent_column))"
        ),
        {
            "child_table": _SCHEDULE_TABLE,
            "child_column": _PIPELINE_ID_COLUMN,
            "parent_table": _PIPELINE_TABLE,
            "parent_column": "id",
        },
    ).all()
    return {
        f"{table}.{column}": _ColumnCollation(
            charset=str(charset), collation=str(collation)
        )
        for table, column, charset, collation in rows
        if charset is not None and collation is not None
    }


def _current_database(*, conn: sqlalchemy.Connection) -> str:
    """Which database the lookup above actually resolved to.

    Named in the diagnostic rather than assumed. `DATABASE()` is session state,
    and the whole design depends on it being the same session state the
    unqualified `ALTER` and `REFERENCES` resolve against; if a proxy ever broke
    that, the symptom would be a constraint that is skipped forever for no
    visible reason. Printing the name turns that into one obvious log line.
    """
    try:
        return str(
            conn.execute(sqlalchemy.text("SELECT DATABASE()")).scalar() or "<none>"
        )
    except sqlalchemy.exc.SQLAlchemyError:
        return "<unreadable>"


#: A collation whose name ends in one of these compares case-sensitively.
#:
#: The PROPERTY is tested, not a specific name. `db_models` names the one this
#: code creates, but a database that already carries a different, equally
#: case-sensitive collation is correct and must not be failed over a spelling --
#: and the whole point of this check is the behaviour, not the label.
#:
#: MySQL's suffix convention is the contract being read: `_bin` compares code
#: points, `_cs` compares case-sensitively with linguistic ordering, and every
#: other suffix -- `_ci`, plus the legacy unsuffixed names -- folds case. There
#: is no server flag that makes a `_ci` collation case-sensitive, so the name is
#: a sound test rather than a heuristic.
_BINARY_SUFFIX: Final[str] = "bin"
_CASE_SENSITIVE_SUFFIX: Final[str] = "cs"
_KANA_SUFFIX: Final[str] = "ks"


@dataclasses.dataclass(frozen=True)
class _CollationTarget:
    """One column whose comparison decides an identity, and its MySQL definition."""

    column: str
    length: int
    nullable: bool
    #: What goes wrong when this column folds, in the caller's terms.
    consequence: str


#: The path half of `uq_scheduled_pipeline_run_created_by_schedule_path`, and
#: only that half.
#:
#: `created_by` was briefly a target here too, on the premise that 'jose' and
#: 'Jose' are two principals who must not share a path slot. The product
#: contract is the opposite -- user identity is not case-sensitive, so they are
#: one principal and one slot is correct -- so the owner column keeps the table
#: default and this tuple has one member again. The visible consequence is that
#: a startup migration performs ONE table copy, not two.
#:
#: A unique key is as strict as each of its columns separately, which is what
#: makes the mixed key coherent rather than half-converted: the path half is
#: byte-exact, so 'Foo' and 'foo' are distinct paths, and the owner half folds,
#: so they are distinct paths belonging to the same person.
_COLLATION_TARGETS: Final[tuple[_CollationTarget, ...]] = (
    _CollationTarget(
        column="schedule_path",
        length=db_models.SCHEDULE_PATH_LENGTH,
        nullable=True,
        # A path is ASCII by construction -- non-ASCII is rejected before
        # validation -- and trimmed, so it has no accents to fold, no canonical
        # equivalents to collapse and no trailing space for a PAD SPACE collation
        # to swallow. Over that repertoire a case-sensitive collation IS
        # byte-exact, so a database already carrying one is correct and must not
        # be failed, or copied, over a spelling.
        consequence=(
            "'Foo/Bar' and 'foo/bar' would be one identity on the unique index and either"
            " could answer a lookup for the other"
        ),
    ),
)


def _is_case_sensitive_collation(name: str) -> bool:
    """Does a MySQL collation NAME promise case-sensitive comparison?

    Parsed from the naming grammar, positionally, and this took three attempts.

    `endswith(("_bin", "_cs"))` refuses `utf8mb4_ja_0900_as_cs_ks`, which is
    accent-, case- AND kana-sensitive: fail-closed failing on a database that was
    already correct. Widening to token membership was much worse, because
    `utf8mb4_cs_0900_ai_ci` is the CZECH collation -- `cs` is the LOCALE and the
    trailing `ai_ci` is the sensitivity. It folds case, and membership called it
    case-sensitive, which would have opened path writes on a database that still
    aliases 'Foo' and 'foo' (found by pi-38 while reviewing the same rule in
    `user_pipelines/schema_readiness.py`).

    The grammar is `<charset>[_<locale>][_<version>]_<accent>_<case>[_ks]`, or
    the flat `<charset>_bin`. So: the sensitivity is TERMINAL, `ks` is the only
    token permitted after the case token, and any other ending is not recognised
    as strict. Enumerating all 91 `utf8mb4_*` collations in MySQL 8.0
    `strings/ctype-uca.cc` yields four terminal shapes and no others -- 56 `_ci`,
    33 `_cs`, 1 `_cs_ks`, 1 `_bin` -- with exactly one case-insensitive name
    carrying a nonterminal `cs`.

    Unrecognised endings fail closed to False. There is no server flag that makes
    a `_ci` collation case-sensitive, so a name that parses IS sound evidence;
    a name that does not parse is merely unfamiliar, and treating unfamiliar as
    strict is how the two earlier versions of this went wrong.
    """
    tokens = name.lower().split("_")
    if not tokens:
        return False
    if tokens[-1] == _BINARY_SUFFIX:
        return True
    if tokens[-1] == _KANA_SUFFIX:
        tokens = tokens[:-1]
    if not tokens:
        return False
    return tokens[-1] == _CASE_SENSITIVE_SUFFIX


def _collation_step(column: str) -> str:
    return f"collation:{column}"


_COLLATION_STEPS: Final[tuple[str, ...]] = tuple(
    _collation_step(t.column) for t in _COLLATION_TARGETS
)


def _verify_collation(
    shape: LiveShape, target: _CollationTarget
) -> tuple[_Verdict, str]:
    """Does this live column compare case-sensitively?

    A pure function of the shape, so it can join `verify_schema` alongside every
    other object kind and be answered identically by the pod that installs the
    schema and the pod that only verifies it.

    Four inputs, and the last two are the ones worth stating:

    * not applicable -- SQLite's `=` on TEXT is byte comparison, so the property
      already holds and there is no collation to set. MATCHES.
    * a case-sensitive name -- MATCHES.
    * a case-folding name -- ABSENT, which is what invites the conversion.
    * unreadable (None on MySQL) -- CONFLICTS, deliberately not ABSENT. ABSENT
      would invite `_apply` to emit a table-copying ALTER against a column whose
      current definition nobody could read.

    A column missing from the mapping is MATCHES here: it is not present on the
    table, the column checks already fail on that, and a second complaint about
    the same cause would only obscure which check found it.
    """
    if target.column not in shape.collations:
        return _Verdict.MATCHES, ""
    collation = shape.collations[target.column]
    if collation == _COLLATION_NOT_APPLICABLE:
        return _Verdict.MATCHES, ""
    if collation is None:
        return (
            _Verdict.CONFLICTS,
            f"the collation of {_SCHEDULE_TABLE}.{target.column} could not be read from"
            " information_schema, so case-sensitive identity cannot be established."
            " Nothing was modified.",
        )
    if _is_case_sensitive_collation(collation):
        return _Verdict.MATCHES, ""
    return (
        _Verdict.ABSENT,
        f"{_SCHEDULE_TABLE}.{target.column} is {collation}, which folds case, so {target.consequence}.",
    )


def _refuse_unexpected_definition(
    shape: LiveShape, target: _CollationTarget
) -> str | None:
    """Refuse to rewrite a definition this module has not verified.

    `MODIFY COLUMN` does not adjust a column, it REPLACES the definition, and
    everything the statement omits is dropped. So the conversion silently
    imposes the model's width, nullability and absence of a default on whatever
    is actually there. Every other object here treats the live shape as a
    fail-closed contract; this statement was the one place that assumed it
    (pi-38).

    Four ways that goes wrong, and none of them announce themselves:

    * a wider live column is TRUNCATED to the model's length, destroying data;
    * a nullable live column is tightened to `NOT NULL`, which either rewrites
      the meaning of existing rows or fails the copy outright on the first NULL
      -- mid-`ALTER`, on a table already being copied;
    * a server default is dropped, so a previous application image that inserted
      without naming the column starts failing;
    * a non-VARCHAR type is rewritten to VARCHAR.

    Returns a reason to refuse, or None to proceed. BLOCKED rather than SKIPPED
    at the call site: no later boot clears a definition drift on its own, and a
    gate that waits for one is a gate that never reports.
    """
    live = shape.columns.get(target.column)
    if live is None:
        # The column checks own an absent column and name it precisely; the
        # collation verdict already treats it as nothing to convert.
        return f"{target.column} is not present on {_SCHEDULE_TABLE}"
    live_type = live["type"]
    length = getattr(live_type, "length", None)
    mismatch = _first_mismatch(
        [
            (
                isinstance(live_type, sqlalchemy.VARCHAR),
                f"expected a VARCHAR column, found {type(live_type).__name__} ({live_type})",
            ),
            (
                length == target.length,
                f"expected length {target.length}, found {length}",
            ),
            (
                bool(live.get("nullable")) == target.nullable,
                f"expected nullable={target.nullable}, found nullable={bool(live.get('nullable'))}",
            ),
            (
                live.get("default") is None,
                f"unexpected server default {live.get('default')!r}",
            ),
        ]
    )
    if mismatch[0] is _Verdict.MATCHES:
        return None
    return (
        f"refusing to convert {_SCHEDULE_TABLE}.{target.column}: {mismatch[1]}."
        " MODIFY COLUMN rewrites the whole definition, so converting it would also"
        " impose the model's width, nullability and default on the live column."
        " Nothing was modified."
    )


def _apply_collation(
    *, conn: sqlalchemy.Connection, dialect: str, target: _CollationTarget
) -> None:
    """Convert the path column to the case-sensitive collation.

    A collation change is not an INPLACE operation: MySQL rebuilds the column
    and every index over it, so this is `ALGORITHM=COPY, LOCK=SHARED` and is
    gated by the same row bound the foreign-key copy uses. Stated explicitly so
    the server refuses rather than silently choosing something worse.

    `CHARACTER SET utf8mb4` is named alongside the collation because `COLLATE`
    alone is an error when the column's current charset is not utf8mb4's, and a
    legacy column could be anything.

    Nullability is restated because MySQL's `MODIFY COLUMN` rewrites the whole
    definition and drops anything the statement omits: leaving the live
    nullability off would quietly rewrite it, which no verification here checks
    for and which the model does not permit.

    The conversion cannot fail on data. Moving a unique index from a
    case-insensitive collation to a case-sensitive one only ever makes
    comparison stricter: two rows distinct under `_ci` stay distinct under
    `_bin`, and two that would collide under `_bin` could never have both existed
    under `_ci`. So the rebuilt unique index cannot discover a duplicate. The
    reverse direction is the dangerous one, and is not what this does.
    """
    if dialect != "mysql":
        raise SchedulerSchemaError(
            f"refusing to change the collation of {target.column} on dialect {dialect!r}:"
            " no statement is defined for it"
        )
    nullability = "NULL" if target.nullable else "NOT NULL"
    conn.execute(
        sqlalchemy.text(
            f"ALTER TABLE {_SCHEDULE_TABLE} MODIFY COLUMN {target.column}"
            f" VARCHAR({target.length}) CHARACTER SET utf8mb4"
            f" COLLATE {db_models.SCHEDULE_PATH_COLLATION} {nullability}, {_MYSQL_COPY}"
        )
    )
    conn.commit()


def _expand_collation(*, conn: sqlalchemy.Connection, report: MigrationReport) -> None:
    """Install the case-sensitive collation on the path column.

    Non-fatal, like the indexes and unlike the columns: a case-folding column
    still stores and returns values, so the service boots and every non-path
    feature works. What it must not do is accept a path write, which is why
    these steps are members of the `path_writes` tier rather than a startup
    error.

    Ordered before the indexes deliberately. The unique index is built over
    whatever collation its columns have, so converting first means the index is
    correct when it is created; converting afterwards would rebuild it anyway,
    for no benefit and one more table copy.

    Written as a loop over targets although there is one target today. The list
    had two members while `created_by` was also being converted, and the loop is
    what kept each column's size gate and verdict separate; collapsing it now
    would only have to be undone if another column ever earns a collation.
    """
    shape = _inspect(conn=conn)
    for target in _COLLATION_TARGETS:
        step = _collation_step(target.column)
        verdict, _ = _verify_collation(shape, target)
        if verdict is _Verdict.ABSENT:
            unexpected = _refuse_unexpected_definition(shape, target)
            if unexpected is not None:
                report.add(step, StepStatus.BLOCKED, detail=unexpected)
                continue
        too_big = (
            _refuse_if_table_is_large(
                conn=conn,
                dialect=report.dialect,
                limit=_MAX_FK_COPY_ROWS,
                operation=f"collation change on {target.column}",
                remedy=f"Convert {target.column} deliberately, then restart.",
            )
            if verdict is _Verdict.ABSENT
            else None
        )
        if too_big is not None:
            report.add(step, StepStatus.SKIPPED, detail=too_big)
            continue
        _apply(
            conn=conn,
            report=report,
            step=step,
            verify=lambda s, target=target: _verify_collation(s, target),
            emit=lambda target=target: _apply_collation(
                conn=conn, dialect=report.dialect, target=target
            ),
            required=False,
        )


def _refuse_if_collations_differ(
    *, conn: sqlalchemy.Connection
) -> tuple[StepStatus, str] | None:
    """InnoDB rejects a foreign key between columns of different collations.

    Checked before the statement rather than discovered from its error, because
    the server's message names neither column and this one can say exactly which
    two values disagree. A mismatch is BLOCKED, not SKIPPED: no later boot clears
    it on its own and no gate should quietly wait for one.
    """
    try:
        collations = _effective_collations(conn=conn)
    except sqlalchemy.exc.SQLAlchemyError as error:
        return (
            StepStatus.SKIPPED,
            f"skipped: could not read the effective collation of {_PIPELINE_ID_COLUMN} and"
            f" {_PIPELINE_TABLE}.id ({type(error).__name__}), and InnoDB refuses a foreign key"
            " across differing collations.",
        )
    child = collations.get(f"{_SCHEDULE_TABLE}.{_PIPELINE_ID_COLUMN}")
    parent = collations.get(f"{_PIPELINE_TABLE}.id")
    if child is None or parent is None:
        missing = (
            f"{_SCHEDULE_TABLE}.{_PIPELINE_ID_COLUMN}"
            if child is None
            else f"{_PIPELINE_TABLE}.id"
        )
        return (
            StepStatus.SKIPPED,
            f"skipped: information_schema reported no charset or collation for {missing}"
            f" in database {_current_database(conn=conn)!r}, so the constraint's compatibility"
            f" with {_PIPELINE_TABLE}.id cannot be established. If that database name is not the"
            " one this service writes to, the lookup and the ALTER disagree about the session's"
            " default schema and the constraint will never install.",
        )
    if child != parent:
        return (
            StepStatus.BLOCKED,
            f"{_PIPELINE_ID_COLUMN} is {child} and {_PIPELINE_TABLE}.id is {parent};"
            " InnoDB refuses a foreign key between columns whose character set or collation"
            " differ. Nothing was modified. Align the two deliberately -- converting a column"
            " rewrites the table and is not something startup may decide -- then restart.",
        )
    return None


def _saved_pipeline_references_without_a_pipeline(
    *, conn: sqlalchemy.Connection
) -> list[str]:
    """At most one orphaned reference, as evidence. Read-only, bounded, no census.

    `LIMIT 1`, because the caller needs one bit and the operator needs one
    example. A count would be a census over exactly the table this module
    refuses to scan, and the second orphan does not change any decision.

    The anti-join is served by `_IX_REFERENCE` on the child side and the primary
    key on the parent, which is why this runs only after the index exists.
    """
    rows = conn.execute(
        sqlalchemy.text(
            f"SELECT /*+ MAX_EXECUTION_TIME({_ROW_PROBE_TIMEOUT_MILLISECONDS}) */ s.{_PIPELINE_ID_COLUMN}"
            f" FROM {_SCHEDULE_TABLE} AS s"
            f" LEFT JOIN {_PIPELINE_TABLE} AS p ON p.id = s.{_PIPELINE_ID_COLUMN}"
            f" WHERE s.{_PIPELINE_ID_COLUMN} IS NOT NULL AND p.id IS NULL"
            " LIMIT 1"
        )
    ).scalars()
    return [str(row) for row in rows]


def _create_foreign_key(
    *, conn: sqlalchemy.Connection, dialect: str, spec: _ForeignKeySpec
) -> None:
    if dialect == "mysql":
        conn.execute(
            sqlalchemy.text(
                f"ALTER TABLE {_SCHEDULE_TABLE} ADD {spec.mysql_clause}, {_MYSQL_COPY}"
            )
        )
    elif dialect == "sqlite":
        # RECREATES the table, exactly as `_widen` does and for the same reason:
        # SQLite has no ALTER ... ADD CONSTRAINT, so `batch_alter_table` builds a
        # new table, copies every row and renames. Reserve this path for small
        # databases without concurrent scheduler traffic, and assess the rebuild
        # cost before using it on a larger database. Existing SQLite databases
        # can then reach the same reference-write readiness as fresh databases.
        with _operations(conn).batch_alter_table(_SCHEDULE_TABLE) as batch_op:
            batch_op.create_foreign_key(
                spec.name,
                spec.referred_table,
                list(spec.columns),
                list(spec.referred_columns),
                ondelete="RESTRICT",
                onupdate="RESTRICT",
            )
    else:
        raise SchedulerSchemaError(
            f"refusing to add {spec.name} on dialect {dialect!r}: no reviewed statement is defined for it"
        )
    conn.commit()


def rollback_foreign_key_statement(spec: _ForeignKeySpec) -> str:
    """The named reverse of `_create_foreign_key`. Never executed here.

    A deliberate rollback path, written down because an image revert is no
    longer the whole story once this constraint exists: DDL is not rolled back
    by deploying an older image, so a previous release runs against a schema
    that still enforces the reference. That is safe -- the constraint only
    rejects writes the service layer already rejects -- but "safe" is not the
    same as "reversible", and the reversal has to exist somewhere an operator
    can find it.

    Consequences of running it, in order:

    * `DROP FOREIGN KEY` is INPLACE and does not copy the table, so unlike the
      add it is cheap at any size.
    * `reference_writes_ready` goes false on the next boot, which closes
      pipeline-reference writes. That is the intended failure mode, not a
      regression: the tier names the constraint as a dependency.
    * InnoDB keeps `_IX_REFERENCE`, because this application created it
      independently rather than letting the constraint create one. Dropping the
      constraint therefore does not silently remove the index the reads use.
    * Startup will try to add the constraint again on the next boot. An operator
      dropping it to keep it dropped must also stop that -- which is a code
      change, deliberately, so a constraint cannot be quietly removed from the
      schema without a review.
    """
    return f"ALTER TABLE {_SCHEDULE_TABLE} DROP FOREIGN KEY {spec.name}"


def _exceeds_row_limit(
    *, conn: sqlalchemy.Connection, limit: int = _MAX_INDEX_BUILD_ROWS
) -> bool:
    """Does the table hold more than `limit` rows?

    Total, not an estimate, and deliberately only this one bit: the caller needs
    a decision, not a count, and a count is the expensive thing. Never returns an
    "unknown" -- an unanswerable probe RAISES, and `_refuse_if_table_is_large`
    turns that into the decline. Encoding unknown as a third return value would
    let a caller forget to handle it; an exception cannot be ignored.

    `LIMIT 1 OFFSET n` is what makes it cheap: the server can stop as soon as it
    has produced n+1 rows on whatever access path it picks, however large the
    table is. Not a census -- no aggregate, no full scan, no COUNT -- and unlike a
    stored statistic, the answer cannot be stale, because it is taken from the
    data at the moment it is asked.

    The access path is the optimizer's choice, though, and MVCC means physical
    entries visited need not equal rows produced, so "n+1 entries" is a shape
    argument and not a guarantee. `MAX_EXECUTION_TIME` is the actual hard bound.
    That hint applies only to SELECTs, which is exactly why it can protect this
    probe and cannot protect the DDL the probe is guarding: a timeout raises, and
    the caller treats any database failure as unknown.

    An earlier version of this trusted `information_schema.TABLES.TABLE_ROWS`
    instead. That was wrong, and both reviewers caught it: MySQL documents the
    InnoDB value as an approximation that "may vary from the actual value by as
    much as 40% to 50%", persistent statistics are recalculated asynchronously,
    and `innodb_stats_auto_recalc` can be disabled globally or per table with
    `STATS_AUTO_RECALC`. A bound that depends on a statistic which is allowed to
    be stale and allowed to be switched off is not a bound.
    """
    row = conn.execute(
        sqlalchemy.text(
            f"SELECT /*+ MAX_EXECUTION_TIME({_ROW_PROBE_TIMEOUT_MILLISECONDS}) */ 1"
            f" FROM {_SCHEDULE_TABLE} LIMIT 1 OFFSET {limit}"
        )
    ).scalar()
    return row is not None


def _refuse_if_table_is_large(
    *,
    conn: sqlalchemy.Connection,
    dialect: str,
    limit: int = _MAX_INDEX_BUILD_ROWS,
    operation: str = "index build",
    remedy: str = "Build the index deliberately, then restart.",
) -> str | None:
    """Decline unbounded startup work when the table is too large for it.

    Parameterised by limit and operation because the two statements it guards
    have different costs and therefore different thresholds. An `ADD INDEX` is
    INPLACE and never blocks DML, so the cost of being wrong is a slow boot; the
    foreign key add is a table copy holding LOCK=SHARED, so the cost of being
    wrong is blocked schedule writes. The second tolerates far less.

    `ALGORITHM=INPLACE, LOCK=NONE` keeps the build from blocking concurrent DML,
    but it does not make the build quick: adding a secondary index scans and
    sorts the table, so its duration scales with table size. Nothing can bound
    that once it has started -- `lock_wait_timeout` bounds only the metadata-lock
    wait, and `max_execution_time` applies only to SELECTs -- so the bound has to
    be a decision made *before* emitting the statement.

    Declining is safe in a way that a slow build is not: index steps are
    non-fatal, so the pod boots, serves, and reports the tier closed. The
    consequence is a feature that stays gated off until someone builds the index
    deliberately, with a maintenance window and a plan.

    The refusal names the LIMIT rather than a size, because the probe measures
    no size: it answers "more than n?" and nothing else. Reporting a count would
    mean either an aggregate this exists to avoid, or the stale statistic it
    replaced. What the operator gets is the decision and its threshold, in the
    step detail and the startup warning, instead of a boot that hangs while
    nobody knows why.

    Returns a reason to skip, or None to proceed.
    """
    if dialect != "mysql":
        # SQLite reaches this only on a fresh or developer database, where
        # `create_all` has already built every index and there is nothing to
        # scan. No other dialect can get here; `_expand` refuses them outright.
        return None
    try:
        exceeds = _exceeds_row_limit(conn=conn, limit=limit)
    except sqlalchemy.exc.SQLAlchemyError as error:
        # Every way the probe can fail -- denied permission, an unsupported hint
        # at a proxy, the MAX_EXECUTION_TIME timeout firing on a table too large
        # to skip through in time -- means the size is unknown. Unknown declines.
        # Letting this propagate would make startup fatal, which is the opposite
        # of the intended behaviour: index steps are non-fatal precisely so a pod
        # boots and serves with the tier closed. Deliberately catches
        # SQLAlchemyError rather than Exception, so a genuine process-level fault
        # is not silently reinterpreted as a large table.
        return (
            f"skipped: the size of {_SCHEDULE_TABLE} could not be established"
            f" ({type(error).__name__}), so the {operation} time is unknown. {remedy}"
        )
    if exceeds:
        return (
            f"skipped: {_SCHEDULE_TABLE} holds more than {limit} rows, too many for the"
            f" {operation} to run during startup. {remedy}"
        )
    return None


def _create_index(
    *, conn: sqlalchemy.Connection, dialect: str, spec: _IndexSpec
) -> None:
    unique = "UNIQUE " if spec.unique else ""
    columns = ", ".join(spec.columns)
    if dialect == "mysql":
        # Stated explicitly so the server refuses rather than silently choosing
        # a copying algorithm: INPLACE with LOCK=NONE permits concurrent DML
        # for a secondary index add, and a COPY fallback would not.
        conn.execute(
            sqlalchemy.text(
                f"ALTER TABLE {_SCHEDULE_TABLE} ADD {unique}INDEX {spec.name} ({columns}), {_MYSQL_INPLACE}"
            )
        )
    elif dialect == "sqlite":
        # CREATE INDEX does not recreate the table on SQLite, unlike the widen.
        _operations(conn).create_index(
            spec.name, _SCHEDULE_TABLE, list(spec.columns), unique=spec.unique
        )
    else:
        raise SchedulerSchemaError(
            f"refusing to create {spec.name} on dialect {dialect!r}: no non-blocking statement is defined for it"
        )
    conn.commit()


def _emit_column(
    *,
    conn: sqlalchemy.Connection,
    dialect: str,
    spec: _ColumnSpecBase,
    live: dict[str, Any] | None,
) -> None:
    """Append the column, or widen it if it exists and is merely too short."""
    if live is None:
        _add_column(conn=conn, dialect=dialect, spec=spec)
    else:
        _widen(conn=conn, dialect=dialect, spec=spec, live=live)


def _widen(
    *,
    conn: sqlalchemy.Connection,
    dialect: str,
    spec: _ColumnSpecBase,
    live: dict[str, Any],
) -> None:
    if dialect == "mysql":
        conn.execute(
            sqlalchemy.text(
                f"ALTER TABLE {_SCHEDULE_TABLE} MODIFY COLUMN {spec.name} {spec.sql_type} NULL, {_MYSQL_INPLACE}"
            )
        )
    elif dialect == "sqlite":
        # This RECREATES the table: SQLite cannot change a column type in place,
        # so `batch_alter_table` builds a new table, copies every row, and
        # renames. That is exactly the unbounded rewrite the rest of this module
        # refuses to do at boot. Reserve the SQLite path for small local or
        # test databases without concurrent scheduler traffic; operators must
        # assess the rebuild cost before using it on a larger database.
        #
        # It is also nearly unreachable: `create_all` gives a fresh local
        # database the target types, so this runs only for a local file that
        # predates the column change.
        with _operations(conn).batch_alter_table(_SCHEDULE_TABLE) as batch_op:
            # Every existing_* passed explicitly, per the
            # migrate_secret_value_column precedent: omitting them lets a CHANGE
            # COLUMN silently reset nullability or the default.
            batch_op.alter_column(
                spec.name,
                type_=spec.alembic_type,
                existing_type=live["type"],
                existing_nullable=True,
                existing_server_default=None,
            )
    else:
        # No fall-through to whatever Alembic emits by default. On PostgreSQL an
        # ALTER TYPE rewrites the table under an ACCESS EXCLUSIVE lock, which
        # would block the per-fire `last_run_at` write; `create_db_engine`
        # accepts any URI, so this is about what the process might be handed.
        # Raised, not reported: `_apply` turns this into a BLOCKED column, and a
        # blocked column is startup-fatal, which is the correct fail-closed
        # outcome for "the schema is wrong and I must not touch it".
        raise SchedulerSchemaError(
            f"refusing to widen {spec.name} on dialect {dialect!r}: no non-blocking statement is defined for it"
        )
    conn.commit()


def _add_column(
    *, conn: sqlalchemy.Connection, dialect: str, spec: _ColumnSpecBase
) -> None:
    """Append one nullable column. Mirrors `_widen`'s dialect handling exactly.

    `_expand` already refuses unknown dialects, so the `else` here is
    unreachable through the startup path. It is still explicit, because the
    previous version's silent fall-through to a generic ALTER was reachable and
    nobody noticed until a reviewer read the two emitters side by side.
    """
    if dialect == "mysql":
        # ALGORITHM is stated explicitly so the server errors instead of
        # silently falling back to a blocking table copy.
        conn.execute(
            sqlalchemy.text(
                f"ALTER TABLE {_SCHEDULE_TABLE} ADD COLUMN {spec.name} {spec.sql_type} NULL, {_MYSQL_INSTANT}"
            )
        )
    elif dialect == "sqlite":
        # Appending a column is cheap on SQLite and does not recreate the table.
        _operations(conn).add_column(
            _SCHEDULE_TABLE,
            sqlalchemy.Column(spec.name, spec.alembic_type, nullable=True),
        )
    else:
        raise SchedulerSchemaError(
            f"refusing to add {spec.name} on dialect {dialect!r}: no non-blocking statement is defined for it"
        )
    conn.commit()


def _verify_hardening(*, conn: sqlalchemy.Connection, report: MigrationReport) -> None:
    """Read-only: does the hardening exist, and does it match? No data scans."""
    # Same verifiers on every dialect. SQLite used to be matched by name only,
    # which meant a same-named CHECK(1=1) read as ready. SQLite reflection does
    # expose the check body exactly — I confirmed it round-trips — so there was
    # never a reason to accept less.
    record_verification(
        report=report, results=verify_hardening_objects(_inspect(conn=conn))
    )


def _hardening_checks() -> (
    list[tuple[str, Callable[[LiveShape], tuple[_Verdict, str]]]]
):
    """(step name, verifier) for objects startup verifies but never installs.

    Only the source CHECK, and only because nothing can add it to an existing
    table: MySQL has no non-copying `ADD CHECK`, and unlike the foreign key this
    one has no bounded-copy story worth taking -- the invariant it encodes is
    already enforced on every write, so the constraint buys defence in depth and
    nothing a gate should wait for. The foreign key is NOT here: startup installs
    it, under the size gate and the preflight, so it is a target object with a
    readiness tier rather than a report.
    """
    return [(f"check:{_CK_SOURCE}", _verify_check)]


@contextlib.contextmanager
def _bounded_metadata_lock_wait(
    *, conn: sqlalchemy.Connection, dialect: str
) -> Iterator[None]:
    """Make statements on this connection fail fast rather than queue on the MDL.

    `ALGORITHM=INSTANT` and `LOCK=NONE` describe the *table rebuild*, not the
    metadata lock. Every ALTER still needs a brief exclusive MDL, and MySQL
    grants metadata locks in request order: a long-running transaction that
    still holds a shared MDL on this table makes the ALTER wait, and every
    schedule write that arrives afterwards queues behind the ALTER's pending
    exclusive request. That is how an "instant" DDL stalls the per-fire
    `last_run_at` write, and it is a pileup this application must never cause,
    because schedule runs cannot be paused while it clears.

    Not only DDL. `lock_wait_timeout` bounds every statement that takes a
    metadata lock, which includes the *reads* the inspector issues: a
    `SHOW CREATE TABLE` arriving behind a pending exclusive request waits with
    the ALTER, for the server default rather than for anything this application
    chose. So the readiness re-check wraps its metadata reads in this too --
    see `SchemaReadiness._verify_read_only`, which also owns the pooling
    consequence described below.

    A small `lock_wait_timeout` converts that from an unbounded stall into a
    quick, loud failure. The pod then reports the column as blocked and refuses
    to boot, which is the better trade: one replica failing its readiness check
    is recoverable and visible, whereas a blocked MDL queue degrades every
    schedule fire and every API write until the offending transaction ends.
    Retrying is free -- the expansion is idempotent -- so the next pod start, or
    the next deploy attempt, picks it up once the table is quiet.

    **This connection does not go back to the pool.** The bound is session
    scoped, and the statements that install it name a session variable, which
    ProxySQL may take as a reason to stop multiplexing this frontend connection
    -- permanently, and without any signal the application can observe. So the
    contract is not "restore the value", it is "destroy the session": the
    connection is discarded in a `finally`, on every exit, success or failure.
    That subsumes what the restore was for, because a session that no longer
    exists cannot hand a 3s `lock_wait_timeout` to the API and scheduler traffic
    that borrows the connection next (pi-41).

    The restore is still attempted first, purely as depth: it is the only thing
    that keeps a *failed* discard from leaking the bound as well as the pin, and
    a failed discard raises loudly on its own terms. A failed restore does not,
    for the same reason -- the discard behind it is the guarantee.

    One connection per call is the price, which is why the caller decides what
    to wrap: `expand_schema` wraps a whole boot, and `SchemaReadiness` wraps a
    re-check that happens at most once per interval and stops entirely once
    every tier is open.
    """
    if not _touches_session_state(dialect):
        yield
        return
    previous: int | None = None
    try:
        previous = conn.execute(
            sqlalchemy.text("SELECT @@SESSION.lock_wait_timeout")
        ).scalar()
        conn.execute(
            sqlalchemy.text(
                f"SET SESSION lock_wait_timeout = {_MYSQL_LOCK_WAIT_TIMEOUT_SECONDS}"
            )
        )
        yield
    finally:
        if previous is not None:
            try:
                # Interpolated as an int, never as text: this is a value MySQL
                # just gave us, and `SET` does not accept a placeholder for it.
                conn.execute(
                    sqlalchemy.text(f"SET SESSION lock_wait_timeout = {int(previous)}")
                )
            except Exception:
                # Not fatal, and deliberately not re-raised: the discard below
                # is what guarantees this session never serves another request,
                # so a failed restore only matters if the discard also fails --
                # and that raises on its own terms.
                _logger.warning(
                    "could not restore lock_wait_timeout before discarding the connection",
                    exc_info=True,
                )
        _discard_connection(conn=conn)


def _touches_session_state(dialect: str) -> bool:
    """Single source for "does the lock-wait bound run session statements here".

    Separate from `_uses_advisory_lock` because it answers a different question
    about a different pin: the bound is a no-op outside MySQL, the advisory lock
    need not be, and the readiness re-check runs the bound without ever going
    near GET_LOCK. Named rather than inlined for the same reason as its
    neighbour -- `_bounded_metadata_lock_wait` and the one caller that still has
    a recycle decision to make must not drift apart.
    """
    return dialect == "mysql"


def _uses_advisory_lock(dialect: str) -> bool:
    """Single source for "does this dialect get a GET_LOCK on the connection".

    `_acquire_lock`, `_release_lock` and `_discard_connection` must agree. Three
    independent `== "mysql"` tests is precisely how one of them silently stops
    matching -- the shape of the `_add_column` fall-through bug found earlier.
    """
    return dialect == "mysql"


def _discard_connection(*, conn: sqlalchemy.Connection) -> None:
    """Drop this connection physically instead of returning it to the pool.

    ProxySQL disables connection multiplexing for a frontend connection as soon
    as it sees `GET_LOCK`, and does **not** re-enable it on `RELEASE_LOCK`. This
    connection is checked out of the application's own engine, so returning it to
    the pool would hand the app a permanently de-multiplexed connection for the
    life of the process: a capacity leak nothing downstream could detect or undo.

    `invalidate()` discards the DBAPI connection, so the pool opens a fresh one
    on next checkout.

    GET_LOCK is not the only way to pin one. ProxySQL may also stop multiplexing
    a frontend connection that runs session-variable statements -- which is what
    `_bounded_metadata_lock_wait` does, `SELECT @@SESSION.lock_wait_timeout` and
    `SET SESSION ...` -- unless an explicit `multiplex: 2` query rule covers
    them, and restoring the *value* does not restore multiplexing. Whether such a
    rule exists is a property of the database proxy configuration, not of this repository,
    and application correctness must not depend on an externally managed rule
    staying in place (pi-41).

    So this is called on any MySQL path that took the advisory lock OR ran the
    timeout bound, deliberately wider than "GET_LOCK succeeded" in three
    directions: a *failed* GET_LOCK is still a GET_LOCK to ProxySQL, a failure
    while reading or setting `lock_wait_timeout` happens on a connection that was
    already touched, and a clean bounded read must be treated as pinned exactly
    like a dirty one -- success is not evidence the connection was left
    multiplexable. The conservative side of that boundary costs one extra
    connection open; the other side leaks a pinned one.

    **Fails closed.** An earlier version logged and continued, which knowingly
    returned the pinned frontend to the application pool -- the exact state this
    function exists to prevent (pi-29). Logging is not recycling. Two further
    reasons the swallow was wrong: SQLAlchemy's `invalidate()` already handles
    DBAPI `close()` failures internally and discards the pool record anyway, so
    an exception escaping it signals a higher-level fault where continuing is
    least justified; and raising is the *safer* rollout failure here, because the
    old singleton pod stays Available and the next boot retries idempotently. A
    silently pinned connection degrades the pool for the life of the process and
    nothing downstream can detect or undo it.
    """
    try:
        conn.invalidate()
        return
    except Exception as invalidate_error:
        # Last attempt to guarantee the recycle: detaching removes the DBAPI
        # connection from the pool's bookkeeping, so closing it cannot return it.
        try:
            conn.detach()
            conn.close()
        except Exception as close_error:
            # Chained from close_error, the proximate failure, so its traceback is
            # not suppressed by the earlier one (pi-29). invalidate_error is named
            # in the message and logged, because knowing which of the two paths
            # failed first is what tells an operator whether the pool or the
            # driver is unhealthy.
            _logger.warning(
                "invalidate() failed before the recycle fallback also failed",
                exc_info=invalidate_error,
            )
            raise SchedulerSchemaError(
                "could not recycle the migration connection, so it cannot be proven unpinned; "
                f"refusing to return it to the application pool (invalidate raised "
                f"{type(invalidate_error).__name__}, detach/close raised {type(close_error).__name__})"
            ) from close_error
        _logger.warning(
            "invalidate() failed; the migration connection was detached and closed instead",
            exc_info=True,
        )


def _acquire_lock(
    *, conn: sqlalchemy.Connection, dialect: str, migration_lock_name: str = _LOCK_NAME
) -> bool:
    if not _uses_advisory_lock(dialect):
        return True
    acquired = conn.execute(
        sqlalchemy.text("SELECT GET_LOCK(:name, :timeout)"),
        {"name": migration_lock_name, "timeout": _LOCK_TIMEOUT_SECONDS},
    ).scalar()
    # The lock's lifetime is this connection's lifetime, not a transaction's, so
    # it survives the implicit commits that every DDL statement performs.
    return acquired == 1


def _release_lock(
    *, conn: sqlalchemy.Connection, dialect: str, migration_lock_name: str = _LOCK_NAME
) -> None:
    if not _uses_advisory_lock(dialect):
        return
    try:
        conn.execute(
            sqlalchemy.text("SELECT RELEASE_LOCK(:name)"), {"name": migration_lock_name}
        )
    except Exception:
        # The lock is released when the connection closes regardless.
        _logger.warning(
            "Could not release %s explicitly", migration_lock_name, exc_info=True
        )


def migrate_db(
    *, db_engine: sqlalchemy.Engine, migration_lock_name: str = _LOCK_NAME
) -> MigrationReport:
    """Startup entry point: schema expansion, fatal only on column drift.

    No census queries over table data. Two things the server does still touch
    it: `ADD INDEX` scans and sorts the table, and the SQLite widen copies it
    (see `_widen` -- reserve this rebuild for small databases without concurrent
    scheduler traffic).
    """
    report = expand_schema(db_engine=db_engine, migration_lock_name=migration_lock_name)
    if report.status_of("table") is StepStatus.SKIPPED:
        # create_all runs before this hook, so an absent table at startup means
        # the model was never registered or the database is not the one we think.
        raise SchedulerSchemaError(
            f"{_SCHEDULE_TABLE} does not exist; create_all must run before migrate_db"
        )
    if not report.columns_ready:
        raise SchedulerSchemaError(
            f"{_SCHEDULE_TABLE} cannot support the mapped model: {report.summary()['blocked']}"
        )
    # Both tiers, checked separately: they depend on different indexes, so one
    # being ready says nothing about the other and a single combined condition
    # would silently suppress the other's warning.
    unready_tiers = {
        name: report.tier_causes(name)
        for name, ready in (
            ("path_writes", report.path_writes_ready),
            ("reference_writes", report.reference_writes_ready),
        )
        if not ready
    }
    if unready_tiers:
        # Deliberately not fatal and deliberately not silent: the named write
        # kinds must stay closed until the objects they depend on exist.
        # Reaching here means an install failed, was declined or was blocked, or
        # that this pod lost the migration lock and the winner has not finished
        # -- all states the next boot retries.
        #
        # Note for operators: while `path_writes` is unready, schedule CREATION
        # is refused outright -- including inline creates, which now derive a
        # path and so depend on this tier. An earlier version of this comment
        # said inline schedules remained writable; that stopped being true when
        # the path became universal.
        _logger.warning(
            "scheduled_pipeline_run hardening incomplete; %s not yet safe to enable. Causes: %s",
            ", ".join(unready_tiers),
            {
                tier: [(s.name, s.status.value, s.detail) for s in causes]
                for tier, causes in unready_tiers.items()
            },
        )
    unexpected_fks = [
        s for s in report.steps if s.name.startswith("foreign_key:unexpected:")
    ]
    if unexpected_fks:
        # Separate from the tier warning because it is not a not-yet: this
        # application installs exactly one foreign key, so any *other* one on the
        # reference columns is unexplained, and an ON DELETE CASCADE one could
        # delete a schedule. Reported, not dropped: see verify_foreign_keys.
        _logger.warning(
            "scheduled_pipeline_run has foreign keys this application does not install: %s",
            [(s.name, s.detail) for s in unexpected_fks],
        )
    _logger.info("scheduled_pipeline_run schema ready: %s", report.summary())
    return report


def _already_final(*, conn: sqlalchemy.Connection, report: MigrationReport) -> bool:
    """Read-only: is every target object already exactly right?

    Called BEFORE the advisory lock, because in steady state -- which is almost
    every boot, forever -- there is no work to serialize and no reason to make
    each pod queue for a lock it will immediately release. A reviewer pointed
    out that waiting up to `_LOCK_TIMEOUT_SECONDS` for permission to discover
    there is nothing to do puts an entire rollout behind one lock, and turns a
    restart during unrelated lock contention into a stall before the app can
    even decline.

    Safe against a racing writer precisely because it claims nothing. It returns
    True only when every INSTALLABLE target already matches, which is terminal
    for cooperating instances of THIS target version: the emitters here only add
    or widen absent objects, so none of them can turn a matching object back
    into an absent or conflicting one. It is not terminal against arbitrary
    actors -- an older add-only migrator could still install the now-unexpected
    composite version foreign key, and a person can drop anything -- but that is
    the pre-existing time-of-check limit of a report read once at boot, not
    something this fast path introduces: a lock released before the report is
    ever used could not prevent later DDL either. Anything less falls
    through to the locked path, where `_expand` re-inspects per object before
    emitting -- so the decision to act is still made under the lock, and this
    check can only ever skip work, never authorize it.

    Only `verify_schema` gates the decision, and that distinction is the whole
    correctness of the thing. The source CHECK is report-only: nothing here can
    install it on an existing table, so on every database that reached its
    current shape by UPGRADE rather than by `create_all` it is ABSENT and stays
    ABSENT forever. Requiring it would have made this return False on exactly
    the deployed steady state the change exists to speed up -- a fast path that
    never fires. Unexpected foreign keys need no special handling: they are
    already part of `verify_schema` and report CONFLICTS, so an unexplained
    constraint keeps the slow path, which is what it should do.

    Both sets are RECORDED either way, so the report a caller receives is the
    same whether or not the lock was taken. Only the gate is narrower.
    """
    shape = _inspect(conn=conn)
    installable = verify_schema(shape)
    if not all(verdict is _Verdict.MATCHES for _, verdict, _ in installable):
        return False
    record_verification(
        report=report, results=installable + verify_hardening_objects(shape)
    )
    return True


def expand_schema(
    *, db_engine: sqlalchemy.Engine, migration_lock_name: str = _LOCK_NAME
) -> MigrationReport:
    """The migration proper: `migrate_db` minus the raise, returning the report.

    `migrate_db` adds only the fatality check on top of this, so tests can drive
    a blocked or partial outcome and inspect it. Note that this still raises on a
    column whose live definition conflicts irreconcilably with the target — that
    is a bug report, not a migration outcome — so it is not exception-free.
    """
    with db_engine.connect() as conn:
        report = MigrationReport(dialect=conn.dialect.name)
        # Whether GET_LOCK was ever issued on this connection -- set before the
        # attempt, so a raising `_acquire_lock` still counts. It scopes the
        # advisory-lock quarantine at the bottom, and nothing else: a connection
        # that never took the lock has no LOCK to quarantine, so recycling it for
        # that reason would cost a pool slot on every boot and could fail startup
        # inside `_discard_connection` for nothing. It says nothing about the
        # OTHER pin. On MySQL every path below runs session statements and is
        # recycled by the bounded block regardless of this flag -- including the
        # no-lock fast paths, which is the point of that block owning it.
        lock_attempted = False
        try:
            # Wraps everything, so it also bounds the verify-only path's
            # statements and anything a later edit adds to this connection -- and
            # so the restore happens before the connection leaves this block.
            #
            # Including the existence check. `has_table` is a metadata read like
            # any other and queues on the same MDL, so leaving it outside left
            # startup's very first statement waiting at the server default while
            # the comment above claimed everything was bounded (pi-41). A fresh
            # database now pays one recycled connection for that, once, at the
            # only boot where the table does not exist.
            with _bounded_metadata_lock_wait(conn=conn, dialect=report.dialect):
                if not _inspect(conn=conn).exists:
                    # create_all owns table creation; a fresh database is already
                    # final, and no GET_LOCK was issued to find that out.
                    report.add(
                        "table",
                        StepStatus.SKIPPED,
                        "table absent; create_all owns it",
                    )
                    return report
                if _already_final(conn=conn, report=report):
                    # Nothing to do, and nothing was locked to find that out.
                    return report
                lock_attempted = True
                acquired = _acquire_lock(
                    conn=conn,
                    dialect=report.dialect,
                    migration_lock_name=migration_lock_name,
                )
                try:
                    if acquired:
                        _expand(conn=conn, report=report)
                    else:
                        # Verify-only: proceed if a peer finished, fail closed
                        # otherwise. Runs the SAME verifiers as the winner over the
                        # SAME target set; an earlier version checked only the
                        # appended columns, so a loser could report ready with the
                        # reference column still VARCHAR(20). An index the winner
                        # is still building reads as absent here, which is why
                        # absence is reported and never fatal: this pod boots and
                        # its write tiers stay closed until a later boot sees them.
                        _logger.warning(
                            "Could not acquire %s; verifying read-only",
                            migration_lock_name,
                        )
                        record_verification(
                            report=report,
                            results=verify_schema(_inspect(conn=conn)),
                        )
                    _verify_hardening(conn=conn, report=report)
                finally:
                    _release_lock(
                        conn=conn,
                        dialect=report.dialect,
                        migration_lock_name=migration_lock_name,
                    )
        finally:
            # Ordered deliberately: after the lock-wait restore, so that runs on
            # a live connection, and before the enclosing `with` hands this
            # connection back to the application pool. See _discard_connection.
            #
            # This is the GET_LOCK quarantine and nothing else. The bounded
            # block above already recycles any connection it ran session
            # statements on, so on MySQL there is nothing left to do here and
            # calling it again would only add a second way for a healthy
            # migration to fail (pi-41). What the clause still covers is a
            # dialect that takes the advisory lock WITHOUT the timeout bound --
            # the bound is a no-op outside MySQL, the lock need not be -- which
            # is also the shape these tests can drive against a real pool,
            # SQLite being unable to execute the session statements the other
            # recycle is gated behind.
            if (
                lock_attempted
                and _uses_advisory_lock(report.dialect)
                and not _touches_session_state(report.dialect)
            ):
                _discard_connection(conn=conn)
    return report


#: How long a closed tier waits before asking the database again. Short enough
#: that a pod recovers within a deploy's normal noise, long enough that a fleet
#: sitting behind a blocked migration does not turn a closed tier into a steady
#: metadata query load.
_READINESS_RECHECK_SECONDS: Final[float] = 30.0


class SchemaReadiness:
    """The boot report, plus a way for a closed tier to notice it opened.

    The problem this solves: `MigrationReport` is computed once during import and
    then frozen for the pod's lifetime. A pod that boots while the index build is
    still running records `path_writes_ready = False` and serves 503 for that
    feature **forever** -- including long after the index exists. The design note
    admitted the consequence and asked operators to guarantee "every serving pod
    booted after the migration succeeded", which nothing in the deployment
    enforces and which a routine restart, autoscale event or crash-loop violates.

    Three properties make re-checking safe:

    * **Read-only.** It re-inspects and verifies. It never acquires the advisory
      lock, never emits DDL, and never repairs anything -- so it cannot race the
      migration it is observing. Read-only is not the same as cheap or bounded,
      though: the metadata reads still queue on the table's metadata lock, so
      `_verify_read_only` bounds them and recycles the connection afterwards.
    * **One-way.** A tier may go closed -> open, never open -> closed. A verdict
      already acted on does not get withdrawn under a request, and a transient
      failure to read the schema cannot close a working feature. Enforced by
      accepting only a report that regresses NO tier -- an earlier version asked
      only whether *some* tier opened and then swapped the whole report in, so a
      refresh reading (path closed, reference open) over (path open, reference
      closed) silently closed the path tier. Tiers do not close in reality, so
      refusing such a report costs nothing and removes the failure mode.
    * **Fail closed.** Any error, and the previous answer stands. Since the
      previous answer for a closed tier is "closed", an unreachable database
      keeps refusing rather than guessing.

    Without a `db_engine` this is exactly the old behaviour: the boot report,
    unchanged, forever. Tests that inject a constructed report keep working, and
    a caller has to opt in to the re-check.
    """

    def __init__(
        self,
        *,
        report: MigrationReport,
        db_engine: sqlalchemy.Engine | None = None,
        recheck_seconds: float = _READINESS_RECHECK_SECONDS,
        clock: collections.abc.Callable[[], float] = time.monotonic,
    ) -> None:
        self._report = report
        self._db_engine = db_engine
        self._recheck_seconds = recheck_seconds
        self._clock = clock
        self._last_checked: float | None = None
        # `current()` runs on request threads for one process-wide instance.
        # Without this, two refreshes interleave: both pass the interval check,
        # both issue metadata reads, and the loser's staler report can land last.
        self._lock = threading.Lock()

    def current(self) -> MigrationReport:
        """The freshest report this may safely report, never a worse one."""
        if self._db_engine is None or self._is_fully_open(self._report):
            # Nothing to gain: no way to look, or nothing left to open.
            return self._report
        # Non-blocking on purpose. If another request is already doing the
        # metadata read, this one returns the previous conservative answer
        # immediately rather than queueing behind someone else's I/O. The
        # previous answer is the one we would most likely have returned anyway,
        # and a request path is the wrong place to wait for a lock.
        if not self._lock.acquire(blocking=False):
            return self._report
        try:
            now = self._clock()
            if (
                self._last_checked is not None
                and now - self._last_checked < self._recheck_seconds
            ):
                return self._report
            self._last_checked = now
            refreshed = self._verify_read_only()
            if refreshed is not None and self._is_an_improvement(refreshed):
                self._report = refreshed
            return self._report
        finally:
            self._lock.release()

    @staticmethod
    def _is_fully_open(report: MigrationReport) -> bool:
        return report.path_writes_ready and report.reference_writes_ready

    def _is_an_improvement(self, refreshed: MigrationReport) -> bool:
        """Strictly better on at least one tier, and worse on none.

        Both halves matter. Requiring an improvement stops a no-op refresh from
        churning the stored report; requiring no regression is what actually
        enforces the one-way rule, because the report is swapped in WHOLE. Asking
        only "did some tier open?" is not enough: a refresh can open one tier and
        close another in the same object, and the opened one would then smuggle
        the closed one past the check.
        """
        tiers = (
            (self._report.path_writes_ready, refreshed.path_writes_ready),
            (
                self._report.reference_writes_ready,
                refreshed.reference_writes_ready,
            ),
        )
        if any(was and not now for was, now in tiers):
            return False
        return any(now and not was for was, now in tiers)

    def _verify_read_only(self) -> MigrationReport | None:
        assert self._db_engine is not None
        # Set when the connection could not be proven unpinned. Tracked as state
        # rather than as an exception type because unwinding can replace the
        # exception -- `Connection.__exit__` may raise its own error on the way
        # out -- and this must not be quietly demoted to "no answer" (pi-41).
        unrecycled = False
        try:
            with self._db_engine.connect() as conn:
                fresh = MigrationReport(dialect=conn.dialect.name)
                try:
                    # Bounded for the same reason startup's reads are, and it is
                    # the request path that needs it most. The inspector's reads
                    # take a shared metadata lock on the table, so arriving behind
                    # the migration's pending exclusive request makes them wait --
                    # at the server default, which is effectively forever.
                    # Unbounded, one request thread stalls for the duration of the
                    # migration it is trying to observe. The non-blocking latch in
                    # `current()` bounds how MANY threads that happens to; only
                    # this bounds how long.
                    #
                    # Per-acquisition, not end-to-end: `lock_wait_timeout` bounds
                    # each attempt to take a metadata lock and this re-check issues
                    # several statements, so the claim is "no MDL wait here is
                    # unbounded", not "this returns within 3 seconds".
                    # `MAX_EXECUTION_TIME` would not sharpen it: it bounds
                    # read-only SELECTs, much of the reflection is `SHOW`, and it
                    # is not a metadata-lock bound at all.
                    #
                    # The context manager also recycles the connection on the way
                    # out, which is the whole cost of this: one discarded
                    # connection per re-check interval, only while a tier is still
                    # closed, and none at all once every tier is open.
                    with _bounded_metadata_lock_wait(conn=conn, dialect=fresh.dialect):
                        shape = _inspect(conn=conn)
                        if not shape.exists:
                            return None
                        record_verification(
                            report=fresh,
                            results=verify_schema(shape)
                            + verify_hardening_objects(shape),
                        )
                except SchedulerSchemaError:
                    # The only thing that raises this here is a connection
                    # `_discard_connection` could not recycle. Caught INSIDE the
                    # connection context so the pool is replaced before this one
                    # is handed back by the exit, and so a raising exit cannot
                    # lose the reason.
                    unrecycled = True
                    _logger.error(
                        "scheduler readiness re-check could not recycle its connection",
                        exc_info=True,
                    )
                    try:
                        self._db_engine.dispose()
                    except Exception:
                        _logger.error(
                            "could not dispose the engine after a failed recycle",
                            exc_info=True,
                        )
                    raise
            return fresh
        except Exception:
            if unrecycled:
                # Fail-closed covers the schema verdict, not the pool. A
                # connection that may be pinned and could not be recycled is a
                # different class of problem from "the database did not answer",
                # and answering it with the warning below is how it would go
                # unnoticed for the life of the process.
                raise
            # Fail closed and stay quiet about the details: this runs on a request
            # path, and the previous verdict is already the conservative one.
            _logger.warning(
                "scheduler readiness re-check failed; keeping the previous verdict"
            )
            return None
