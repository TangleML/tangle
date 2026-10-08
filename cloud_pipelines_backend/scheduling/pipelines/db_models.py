import datetime
import enum
import logging
from typing import Any, Final

import sqlalchemy as sql
from sqlalchemy import orm
from sqlalchemy.dialects import mysql
from sqlalchemy.ext import mutable

from cloud_pipelines_backend import backend_types_sql as bts

# Imported for the reference column lengths (`DIGEST_LENGTH`) and to keep the
# saved-pipeline tables registered in the shared metadata alongside this one.
from cloud_pipelines_backend.user_pipelines import db_models as user_pipeline_db_models
from cloud_pipelines_backend.utils import db as db_utils

DEFAULT_TIMEZONE_UTC: Final[str] = "UTC"

#: `pipeline.id` is a 36-char UUID; the legacy placeholder column was String(20).
PIPELINE_ID_LENGTH: Final[int] = 36
#: `pipeline_version.version_key` is a 64-char content digest (or the reserved
#: `current` sentinel, which a pin can never hold).
PIPELINE_VERSION_KEY_LENGTH: Final[int] = user_pipeline_db_models.DIGEST_LENGTH
#: `schedule_path` cap. 255 + 255 chars x 4 bytes utf8mb4 = 2040 bytes for the
#: unique index, inside InnoDB's 3072-byte limit. Widening either column past
#: ~380 chars breaks that index, and widening past 255 *bytes* would also cost
#: the in-place VARCHAR length-prefix trick — so neither should ever grow.
SCHEDULE_PATH_LENGTH: Final[int] = bts._STR_MAX_LENGTH

#: The collation `schedule_path` is CREATED with on MySQL.
#:
#: A path is a user-chosen identity and its case is preserved, so the column it
#: lives in has to compare case-sensitively -- otherwise the API accepts
#: `Foo/Bar`, the unique index treats it as `foo/bar`, and one of two things
#: happens: a legitimate second path is refused as a duplicate, or a lookup
#: returns a row the caller did not ask for. The table default is inherited
#: otherwise, and MySQL's default folds both case and accents.
#:
#: Binary rather than a `_cs` collation because the value is ASCII by
#: construction (`schedule_paths.canonicalize_schedule_path` rejects everything
#: else), so there is no linguistic ordering left for a collation to get right,
#: and byte comparison is the one rule that is identical on MySQL and on the
#: SQLite the unit suite runs against.
#:
#: `..._0900_bin` is the utf8mb4 binary collation of MySQL 8.0 and later, which
#: is NO PAD -- trailing spaces are significant. That is safe here only because
#: the canonicalizer trims, and it is strictly better than PAD SPACE: `a` and
#: `a ` cannot become one identity.
#:
#: `database_migrations` does NOT require this exact name on a live column. It
#: requires the *property* -- see `_verify_collation` -- so a database already
#: carrying an equally case-sensitive collation is not failed over a spelling.
#:
#: This applies to `schedule_path` and to nothing else. `created_by` is
#: deliberately left on the table default -- see the column below.
SCHEDULE_PATH_COLLATION: Final[str] = "utf8mb4_0900_bin"

logger = logging.getLogger(__name__)


class SubmissionResult(str, enum.Enum):
    SUCCESS = "Success"
    ERROR = "Error"


#: The transitional body-source invariant: exactly one of the three source
#: columns is populated, and a version key only means something alongside a
#: pipeline reference.
#:
#: `current` versus `pinned` is *derived* from the version key's null-ness rather
#: than persisted, so there is no mode column to keep in agreement with the data.
#: `pipeline_task_spec_from_pipeline_run_id` is a first-class source here: it has
#: no writer today, but existing rows may use it and must stay legal.
#:
#: This is a *secondary* defence. The invariant is enforced primarily in the
#: application, on write, because the database constraint cannot be relied upon:
#: it is declared here so `create_all` applies it to a fresh database, but on an
#: existing table it can only be added if MySQL accepts the ALTER online and no
#: pre-existing row violates it. Neither is guaranteed, so nothing may assume
#: the CHECK is present. It catches writers that bypass the service layer.
#:
#: One shape it cannot catch: a `pipeline_task_spec` holding the JSON literal
#: `null` is non-NULL to SQL, so the CHECK counts it as a valid inline source.
#: `JSON(none_as_null=True)` below is what keeps current code from producing it.
#:
#: Written as portable SQL (CASE rather than MySQL's boolean-to-int coercion)
#: because `create_all` emits it on a fresh database and the expand migration
#: emits the identical text on a live one.
SOURCE_INVARIANT_CHECK_SQL: Final[str] = """
(CASE WHEN pipeline_task_spec IS NOT NULL THEN 1 ELSE 0 END
    + CASE WHEN pipeline_task_spec_from_pipeline_run_id IS NOT NULL THEN 1 ELSE 0 END
    + CASE WHEN pipeline_task_spec_from_user_pipeline_id IS NOT NULL THEN 1 ELSE 0 END) = 1
AND (pipeline_task_spec_from_user_pipeline_version_key IS NULL
    OR pipeline_task_spec_from_user_pipeline_id IS NOT NULL)
""".strip()


class ScheduledPipelineRun(bts._TableBase):
    __tablename__ = "scheduled_pipeline_run"

    # Column types inherited from _TableBase.type_annotation_map:
    #   str -> String(255), datetime -> UtcDateTime, dict -> MutableDict(JSON)
    # Explicit types below only where overriding the default (e.g. String(20), Text, ForeignKey).

    id: orm.Mapped[bts.IdType] = orm.mapped_column(
        sql.String(db_utils.ID_LENGTH),
        primary_key=True,
        init=False,
        insert_default=bts.generate_unique_id,
    )
    name: orm.Mapped[str] = orm.mapped_column()
    cron_expression: orm.Mapped[str] = orm.mapped_column()
    timezone: orm.Mapped[str] = orm.mapped_column(default=DEFAULT_TIMEZONE_UTC)
    # none_as_null=True is load-bearing, not a style choice. SQLAlchemy's JSON
    # type defaults to none_as_null=False, which persists Python ``None`` as the
    # JSON literal ``null`` — a value that is *not* SQL NULL. Every
    # ``pipeline_task_spec IS NULL`` predicate in
    # ``SOURCE_INVARIANT_CHECK_SQL`` would then be false for a cleared spec, so
    # the exactly-one-source CHECK would reject every reference row while
    # silently accepting a row with no source at all. Mapping None to SQL NULL is
    # what makes the invariant expressible in the database.
    #
    # Reads are unaffected either way: a stored JSON ``null`` still deserializes
    # to Python None, so pre-existing rows keep behaving as they do today.
    pipeline_task_spec: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(
        mutable.MutableDict.as_mutable(sql.JSON(none_as_null=True)),
        default=None,
    )
    pipeline_task_spec_from_pipeline_run_id: orm.Mapped[str | None] = orm.mapped_column(
        sql.ForeignKey(bts.PipelineRun.id),
        default=None,
    )
    # Widened from String(20) to String(36): the placeholder could not physically
    # hold a `pipeline.id` UUID -- and could not have been constrained before the
    # widen either, since the types were incompatible. The FK is declared in
    # __table_args__; the ordering that makes it installable is explained there.
    pipeline_task_spec_from_user_pipeline_id: orm.Mapped[str | None] = (
        orm.mapped_column(
            sql.String(PIPELINE_ID_LENGTH),
            default=None,
        )
    )
    # Non-NULL only for a pinned reference; NULL means "track current". Deriving
    # the distinction from this column is what removes the need for a persisted
    # mode that could disagree with the data.
    pipeline_task_spec_from_user_pipeline_version_key: orm.Mapped[str | None] = (
        orm.mapped_column(
            sql.String(PIPELINE_VERSION_KEY_LENGTH),
            default=None,
        )
    )
    # Stable, user-owned alternate identity, unique per owner.
    #
    # Permanently nullable: legacy rows have no path and there is no NOT NULL
    # contract step planned. Requiring a canonical path on create is an
    # API-level guarantee, and the unique index treats NULLs as distinct, so any
    # number of path-less rows coexist. The CHECK deliberately says nothing
    # about this column — path adoption and inline reconciliation are
    # independent concerns.
    #
    # The MySQL variant carries an explicit case-sensitive collation; see
    # `SCHEDULE_PATH_COLLATION`. `with_variant` and not a bare `String`, because
    # SQLite has no `utf8mb4_*` collation at all and would fail to create the
    # table -- its `=` is already byte comparison, so the two agree.
    schedule_path: orm.Mapped[str | None] = orm.mapped_column(
        sql.String(SCHEDULE_PATH_LENGTH).with_variant(
            mysql.VARCHAR(SCHEDULE_PATH_LENGTH, collation=SCHEDULE_PATH_COLLATION),
            "mysql",
        ),
        default=None,
    )
    paused: orm.Mapped[bool] = orm.mapped_column(default=False)
    # NO explicit collation, deliberately, and this is the half of the identity
    # that is NOT case-sensitive.
    #
    # An earlier revision of this file pinned this column to the same binary
    # collation as the path, on the premise that 'jose' and 'Jose' are two
    # principals contending for one `(owner, path)` slot. That premise is wrong
    # as a product matter: user identity here is not case-sensitive, so those
    # spellings are ONE principal and sharing a slot is the intended behaviour,
    # not a collision. The pin has been reverted along with the application-side
    # byte-exact residuals it was introduced to support.
    #
    # So the composite unique key is deliberately MIXED: case-insensitive owner,
    # byte-exact path. See the constraint below.
    #
    # Owner equality is therefore whatever this column's inherited collation
    # says, evaluated by the database. Nothing in the request path lowercases,
    # casefolds or Unicode-normalizes an owner name, and nothing should start
    # without a stated product contract to implement -- the equality rule here
    # is the deployment's, not this file's. The dialect consequence is real and
    # is documented in SCHEDULER_DESIGN.md: the SQLite the unit suite runs
    # against compares byte-for-byte, so ownership looks case-SENSITIVE locally
    # and case-insensitive on MySQL.
    created_by: orm.Mapped[str] = orm.mapped_column()
    created_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        init=False,
        insert_default=db_utils.utc_now,
    )
    updated_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        init=False,
        insert_default=db_utils.utc_now,
    )
    last_run_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        default=None,
    )
    # sql.Text: 64KB on MySQL, unlimited on PostgreSQL and SQLite
    last_run_submission_result: orm.Mapped[str | None] = orm.mapped_column(
        sql.Text,
        default=None,
    )
    extra_data: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)
    # Per-feature settings. Nullable with no server default -- MySQL permits neither a
    # literal DEFAULT on JSON nor a NOT NULL added column the previous image's INSERTs
    # do not name.
    #
    # Three states reach a reader -- NULL (row predates the feature), `{}` (set and
    # empty) and a populated dict -- and the first two mean the same thing.
    #
    # Paired with its `_TARGET_COLUMNS` entry in one commit; see migration.md.
    settings: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)

    __table_args__ = (
        # Composite index for cursor-based pagination
        # (ORDER BY updated_at DESC, id DESC)
        #
        # Kept, and no longer the one the list endpoint wants: every list read is
        # owner-scoped now, so this index orders the whole table and offers no way
        # to skip the rows belonging to other people. See the owner-scoped index
        # below for what supersedes it.
        #
        # Not dropped, deliberately. Its remaining cost is write amplification on
        # a table of user-created cron entries, and the migration installs objects
        # but never removes them: a DROP is irreversible online, it is the one
        # statement whose rollback is a rebuild, and nothing here can prove no
        # other reader depends on it. Retiring it is an operator decision made
        # with an EXPLAIN, not something to infer from this file.
        sql.Index(
            "ix_scheduled_pipeline_run_updated_at_desc_id_desc",
            updated_at.desc(),
            id.desc(),
        ),
        # Owner-scoped pagination: the access path for
        # `WHERE created_by = ? ORDER BY updated_at DESC, id DESC LIMIT n`.
        #
        # Without it the server can only walk the index above newest-first and
        # throw away every row it reads that belongs to someone else. The LIMIT
        # bounds the ANSWER, not the WORK: a user whose newest schedule is the
        # oldest row in the table is served by reading the table. That is the
        # ordered-scan failure -- an ordered index scan under a low-selectivity
        # filter -- and it arrived with owner scoping , which
        # added the filter without adding a path for it.
        #
        # Leading with `created_by` turns the equality into a range and leaves
        # the remaining key ordered WITHIN it, so the LIMIT stops the scan, the
        # cursor predicate `(updated_at, id) < (:u, :i)` narrows the same range
        # rather than filtering after the fact, and `COUNT(id)` over one owner is
        # served by the leading column alone.
        #
        # Ascending, though the ORDER BY is descending on both keys. The
        # requested order is the exact reverse of this index's, which a backward
        # index scan satisfies with no filesort -- the property that matters is
        # that the sort follows the key, not which end it starts from. A
        # descending index would state the intent more literally at a real cost:
        # key direction never reaches the startup verifier at all. SQLAlchemy's
        # MySQL Inspector does parse ASC/DESC while reflecting, and then returns
        # `column_names` only -- so `_verify_index`, which compares exactly that
        # list, could not tell a correct descending index from a silently
        # ascending one, and an object it cannot check exactly is the thing
        # `database_migrations` exists to refuse.
        #
        # Nothing is left to filter after access: the owner predicate is a plain
        # equality the database evaluates under this column's collation, so the
        # range this index produces IS the answer.
        sql.Index(
            "ix_scheduled_pipeline_run_created_by_updated_at_id",
            "created_by",
            "updated_at",
            "id",
        ),
        # 255 + 255 chars x 4 bytes utf8mb4 = 2040 bytes, inside InnoDB's 3072
        # byte index limit. Widening either column past ~380 chars breaks this.
        #
        # Intentionally mixed, and a unique key IS as strict as each of its
        # columns separately: `created_by` folds under the table default, so
        # 'Jose' and 'jose' address one owner namespace, while `schedule_path`
        # is byte-exact, so 'Foo' and 'foo' are two distinct paths WITHIN that
        # namespace. Both halves are the product contract. Converting the owner
        # half to match the path half is the change that was reverted; do not
        # reintroduce it in the name of making the key uniform.
        sql.UniqueConstraint(
            "created_by",
            "schedule_path",
            name="uq_scheduled_pipeline_run_created_by_schedule_path",
        ),
        # A database foreign key on the saved-pipeline column, and deliberately
        # nothing on the version pair.
        #
        # An earlier revision of this comment claimed no code path could ever
        # install this constraint on the live table. That was wrong, and a
        # reviewer disproved it by running the exact statement: a validated add
        # is `ALGORITHM=COPY` (MySQL permits INPLACE only with
        # `foreign_key_checks` disabled, which skips the validation the
        # constraint exists for) and a copy holds at least `LOCK=SHARED`, but
        # *duration* is what makes a shared lock an outage. The copy cost grows
        # with table size, so the migration installs it under an explicit size
        # gate and fails closed above it rather than stalling schedule writes. See
        # `database_migrations._expand_foreign_keys`.
        #
        # The version pair gets no constraint and could not usefully have one:
        # InnoDB implements MATCH SIMPLE, so a composite foreign key is
        # satisfied whenever any of its columns is NULL, and
        # `pipeline_task_spec_from_user_pipeline_version_key IS NULL` is the
        # ordinary track-current mode. It would skip the common row.
        #
        # The constraint does not replace the service layer, which still
        # resolves a pipeline and version before writing either column. A
        # foreign key proves the parent row exists; pipelines are soft-deleted,
        # so existence is not liveness. What it adds is atomicity: the service
        # check is check-then-write, and only the engine can enforce the
        # reference on every write path, including ones that skip the service.
        #
        # RESTRICT in both directions, never CASCADE: a cascade would delete a
        # customer's schedule as a side effect of deleting a pipeline. Nothing
        # hard-deletes a pipeline today, so RESTRICT blocks nothing that
        # currently happens.
        sql.ForeignKeyConstraint(
            ["pipeline_task_spec_from_user_pipeline_id"],
            ["pipeline.id"],
            name="fk_scheduled_pipeline_run_user_pipeline_id",
            ondelete="RESTRICT",
            onupdate="RESTRICT",
        ),
        #
        # Reverse lookup: "what schedules reference this deployment", and
        # "which of them are pinned" without a second index.
        sql.Index(
            "ix_scheduled_pipeline_run_user_pipeline_id_version_key",
            pipeline_task_spec_from_user_pipeline_id,
            pipeline_task_spec_from_user_pipeline_version_key,
        ),
        sql.CheckConstraint(
            SOURCE_INVARIANT_CHECK_SQL,
            name="ck_scheduled_pipeline_run_source",
        ),
    )


def register_db_tables() -> None:
    """Explicitly import this module so ScheduledPipelineRun is registered
    with _TableBase.metadata before create_all() runs."""
    logger.info(
        f"Native scheduler tables registered: {ScheduledPipelineRun.__tablename__}"
    )
