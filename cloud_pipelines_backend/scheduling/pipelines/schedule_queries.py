"""Reads and row locks for schedule rows, kept out of the HTTP layer.

Thread 3898719409 asked that SQL stop accumulating in `api_routes`. It had a
concrete reason: a handler that builds its own statements is a handler nobody can
test without a request, and the query rules end up restated per endpoint.

These functions take a `Session` and return data or `None`. They raise no
`HTTPException` and know no status codes -- deciding what a missing row *means*
is the API layer's job, and the same query answers differently on a create
(409), a lookup (404) and a PATCH adoption check (409 or 404). Splitting it that
way is what makes the SQL reusable rather than merely relocated.

They are not on `SchedulerService`: that wraps APScheduler -- jobs, triggers,
next-run times -- and does not otherwise touch this table. Hanging row queries
off it would mean one class owning both the job store and the schedule table,
which is the coupling the request was trying to avoid, not a smaller version of
it.
"""

from typing import Final, NamedTuple

import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines_backend.scheduling.pipelines import db_models

#: The columns a LIST response never reads, and the only two on this table whose
#: size is unbounded by the schema: `pipeline_task_spec` is a whole pipeline
#: definition and `extra_data` appears in no response at all. Everything else is
#: a short scalar the list serializes, with `last_run_submission_result` -- TEXT,
#: but a single submission message -- the largest of them.
#:
#: Named as an EXCLUSION rather than a list of wanted columns, unlike the
#: projections in `user_pipelines.services`. Those exist for callers that read
#: two or three fields and discard the object; this one exists for a caller that
#: serializes nearly the whole row. An inclusion list would have to be edited
#: every time a scalar column is added, and forgetting would raise on a field the
#: response is supposed to carry. Stating what must NOT be hydrated puts the
#: maintenance burden on adding another payload column, which is the rarer and
#: more deliberate act.
_UNPROJECTED_LIST_COLUMNS: Final[frozenset[str]] = frozenset(
    {"pipeline_task_spec", "extra_data"}
)

#: Deferred with `raiseload=True`, not merely deferred: a plain defer answers a
#: stray attribute access with a second SELECT per row, which is the exact cost
#: this removes and the kind of regression a green test suite hides. A list that
#: reads a payload column should fail loudly .
#:
#: Safe for `_schedule_to_response(include_spec=False)` because that call never
#: evaluates `schedule.pipeline_task_spec`: the conditional short-circuits. A
#: caller that wants the spec must load the entity, which is what the by-id route
#: does.
#:
#: Same identity-map caveat as `owned_schedule_stub_by_path`: these ARE persistent
#: instances, so a later `session.get` in the same session would hand back the
#: partial object rather than reloading it. The list handler is the whole of its
#: request, so nothing here reads these rows again -- which is why this is a
#: constant used by that one handler and not a general-purpose loader option.
LIST_PROJECTION: Final = (
    orm.load_only(
        *(
            getattr(db_models.ScheduledPipelineRun, attribute.key)
            for attribute in sqlalchemy.inspect(
                db_models.ScheduledPipelineRun
            ).column_attrs
            if attribute.key not in _UNPROJECTED_LIST_COLUMNS
        ),
        raiseload=True,
    ),
)


class PathOwner(NamedTuple):
    """Just enough of a row to decide an adoption, without loading the entity."""

    id: str
    schedule_path: str | None


class ScheduleIdentity(NamedTuple):
    """Just enough of a row to authorize a request and route it.

    `schedule_path` is here because the caller has to re-compare it in Python --
    see `owned_schedule_identity_by_path` -- `created_by` because the
    id-addressed routes authorize against it, and `paused` because a trigger's
    409 is the only other thing a locator's callers decide before they either
    reload the row or hand an id to another component.

    Not a `ScheduledPipelineRun`: the entity carries `pipeline_task_spec` (JSON)
    and `last_run_submission_result` (TEXT), and a request that is about to be
    refused -- wrong owner, absent path, paused -- has no business paying to
    hydrate them first .
    """

    id: str
    created_by: str
    schedule_path: str | None
    paused: bool


def owned_by(*, created_by: str) -> sqlalchemy.ColumnElement[bool]:
    """A `created_by` predicate, compared the way the deployment compares owners.

    A plain equality, and that is the whole design. Owner identity is not
    case-sensitive, so this delegates the comparison to the column's collation
    and states no rule of its own.

    It did not always. An earlier revision wrapped this in a byte-exact residual
    -- `CAST(CAST(created_by AS CHAR CHARACTER SET utf8mb4) AS BINARY)` on both
    sides -- so that 'Jose' and 'jose' could never answer for one another. The
    reasoning was sound given its premise and the premise was wrong: those are
    the same principal, so the residual was excluding callers from their own
    rows on any folding deployment. It is gone rather than relaxed, because a
    predicate that half-applies a rejected rule is worse than one that applies
    none.

    What is left delegates to the database, which also keeps the leading
    `created_by` column of `ix_scheduled_pipeline_run_created_by_updated_at_id`
    eligible for range access -- a cast was never indexable, so the residual had
    been paying that cost as well.

    One definition, because two would drift: the list endpoint and the conflict
    classifier must agree about who a row belongs to, or a 409 names a path the
    caller cannot see.

    A function of the owner name only. It takes no `Session` because there is
    no longer anything dialect-specific to decide -- which is itself the point.
    """
    return db_models.ScheduledPipelineRun.created_by == created_by


def is_owned_by(*, session: orm.Session, schedule_id: str, created_by: str) -> bool:
    """Does this row belong to this caller, under the deployment's comparator?

    The id-addressed authorization probe. It exists so that "who owns this row"
    has ONE answer across the API rather than one for path routes and another
    for id routes.

    It used to have two. The id routes compared `user_details.name` to the
    loaded `created_by` in Python, which does not fold anything, while the path
    routes scoped their SELECT and let the column's collation decide. On a
    folding deployment that made the same caller's ownership verb-dependent: a
    caller whose identity provider spelled their address `Jose@example.com`
    could list and PATCH their schedule BY PATH and be 403'd on the same row BY
    ID. Not a hardening -- the row was theirs under the only comparator the
    product recognises, and the 403 even named them as a stranger to themselves.

    A separate SELECT rather than a comparison against the row already in hand,
    because the comparison is precisely the thing this must not perform. Python
    cannot reproduce a MySQL collation, and approximating one here would be a
    third rule rather than a shared one.

    The cost is one indexed primary-key probe per id-addressed request, which is
    the price of the comparator living in exactly one place.

    `limit(1)` is defensive only; `id` is the primary key.

    Ownership ALONE. Admin bypass, liveness and the 404-vs-403 split are the
    caller's, because they differ per route and this must stay a single
    question with a single answer.
    """
    return (
        session.scalar(
            sqlalchemy.select(sqlalchemy.literal(1))
            .where(
                db_models.ScheduledPipelineRun.id == schedule_id,
                owned_by(created_by=created_by),
            )
            .limit(1)
        )
        is not None
    )


def path_is_taken(
    *, session: orm.Session, created_by: str, canonical_path: str
) -> bool:
    """Is this owner's path already used by some schedule?

    Owner-scoped under the deployment's own comparator, and byte-exact on the
    path. That asymmetry is the contract, not an oversight: a case variant of
    the caller's name IS the caller, so a row it returns is the caller's own and
    a 409 naming it is honest; a case variant of the path is a DIFFERENT path,
    so treating it as taken would refuse a name that is genuinely free.

    The path residual is Python-side because the unique key bounds this select
    to a single row per owner and path, so re-comparing afterwards is sound. It
    is still needed: on a database where the path conversion has not run yet the
    column folds, and the row that comes back may sit at a neighbouring
    spelling.
    """
    row_path = session.scalar(
        sqlalchemy.select(db_models.ScheduledPipelineRun.schedule_path).where(
            owned_by(created_by=created_by),
            db_models.ScheduledPipelineRun.schedule_path == canonical_path,
        )
    )
    # A folding column can match a case neighbour; only a byte-identical path is
    # the path the caller named.
    return row_path == canonical_path


def row_exists(*, session: orm.Session, model: type, identifier: str) -> bool:
    """Does a row with this primary key exist, ignoring every other predicate?

    Used to classify an `IntegrityError`: a foreign key can only have failed
    because a parent row was absent, so liveness, ownership and version rules --
    which the preflight validators also apply -- would answer a question the
    constraint never asked. A soft-deleted row is present.
    """
    return (
        session.scalar(
            sqlalchemy.select(sqlalchemy.literal(1))
            .where(model.id == identifier)
            .limit(1)
        )
        is not None
    )


_IDENTITY_COLUMNS = (
    db_models.ScheduledPipelineRun.id,
    db_models.ScheduledPipelineRun.created_by,
    db_models.ScheduledPipelineRun.schedule_path,
    db_models.ScheduledPipelineRun.paused,
)


def _identity(
    row: sqlalchemy.Row[tuple[str, str, str | None, bool]] | None,
) -> ScheduleIdentity | None:
    return (
        None
        if row is None
        else ScheduleIdentity(
            id=row.id,
            created_by=row.created_by,
            schedule_path=row.schedule_path,
            paused=row.paused,
        )
    )


def owned_schedule_identity_by_path(
    *,
    session: orm.Session,
    created_by: str,
    canonical_path: str,
) -> ScheduleIdentity | None:
    """One owner's schedule at a canonical path, as identity columns only.

    The owner predicate is the database's own comparison and needs no residual:
    a row it matches belongs to the caller by definition, because owner identity
    is not case-sensitive. The PATH predicate does need one, and the caller MUST
    apply it -- this function cannot, because it does not know whether a
    mismatch should be a 404 or a 409.

    Why the path half is not optional: on a pre-conversion database the column
    folds, so a lookup for 'Team/Nightly' resolves the row stored at
    'team/nightly'. The caller asked for a path that does not exist and got a
    different schedule OF THEIR OWN -- undetectable from the response, and
    irreversible through DELETE. The conversion closes that, and the residual is
    what holds until it has run.

    The database predicate stays a plain equality on both columns. That is what
    keeps the unique index eligible for lookup, and the unique key bounds the
    result to one row per owner and path, so a path neighbour returned here can
    be recognised and refused in Python without any risk that the row the caller
    actually named was filtered out first.

    That re-comparison is the reason this reads columns rather than the entity:
    the row it returns may sit at a *different* path, and the whole point is to
    reject it before loading anything expensive.

    Columns also keep the identity map clean, exactly as `lock_path_owner`
    documents, so a caller that genuinely needs the whole row can still load it
    with an ordinary `session.get` and get a real, fully populated entity rather
    than this partial one back out of the map.

    `one_or_none` and not `first`: two rows for one owner and path would mean the
    unique index is absent, and that is a broken invariant rather than something
    to pick a winner from. The API layer only reaches this after the
    SCHEDULE_PATH tier gate, which is what guarantees the index exists, so the
    raised `MultipleResultsFound` is unreachable in a correctly gated request --
    it is here to be loud if that ever stops being true.
    """
    return _identity(
        session.execute(
            sqlalchemy.select(*_IDENTITY_COLUMNS).where(
                owned_by(created_by=created_by),
                db_models.ScheduledPipelineRun.schedule_path == canonical_path,
            )
        ).one_or_none()
    )


def schedule_identity_by_id(
    *, session: orm.Session, schedule_id: str
) -> ScheduleIdentity | None:
    """The same identity columns, addressed by primary key.

    Exists so the id-addressed and path-addressed forms of an operation read the
    same shape. Ownership is NOT filtered here -- the id routes answer 403 for
    another user's schedule and must be able to tell it apart from a missing one,
    which a scoped read cannot do. Authorization is the separate
    `is_owned_by` probe, so that the distinction and the comparison stay
    independent decisions.
    """
    return _identity(
        session.execute(
            sqlalchemy.select(*_IDENTITY_COLUMNS).where(
                db_models.ScheduledPipelineRun.id == schedule_id
            )
        ).one_or_none()
    )


def owned_schedule_stub_by_path(
    *,
    session: orm.Session,
    created_by: str,
    canonical_path: str,
) -> db_models.ScheduledPipelineRun | None:
    """The same lookup, as a persistent entity carrying no payload.

    For the one caller that needs an INSTANCE rather than values: `session.delete`
    is the ORM's unit of work, and it takes an object. It does not need a loaded
    object -- the DELETE it emits is by primary key -- so this loads the two
    columns the delete path reads and refuses the rest.

    `raiseload=True` rather than a plain defer, deliberately: a deferred column
    would emit a silent extra SELECT if anything ever touched it, which is the
    cost this exists to remove. Reading anything else off this object is a bug,
    so it raises and says so.

    Same collation caveat as `owned_schedule_identity_by_path`: the caller
    re-compares `schedule_path` exactly. Unlike that function this DOES place a
    partial instance in the identity map, which is why it is scoped to the
    delete path rather than offered as the general locator.
    """
    return session.scalar(
        sqlalchemy.select(db_models.ScheduledPipelineRun)
        .where(
            owned_by(created_by=created_by),
            db_models.ScheduledPipelineRun.schedule_path == canonical_path,
        )
        .options(
            orm.load_only(
                db_models.ScheduledPipelineRun.created_by,
                # Loaded because the caller re-compares it: a folding PATH column
                # can return a case neighbour, and DELETE is the verb where
                # acting on the wrong one cannot be undone.
                db_models.ScheduledPipelineRun.schedule_path,
                raiseload=True,
            )
        )
    )


def lock_path_owner(*, session: orm.Session, schedule_id: str) -> PathOwner | None:
    """Lock one schedule row and read only its id and path.

    Columns, deliberately, not the entity: loading the ORM object would route
    this row through the identity map, and `populate_existing=True` -- the
    obvious way to defeat the stale cache -- overwrites the loaded attributes of
    the instance the caller is still mutating. Sessions here use
    `autoflush=False`, so a PATCH's other pending field changes would be
    silently discarded. A column read cannot touch the identity map.
    """
    row = session.execute(
        sqlalchemy.select(
            db_models.ScheduledPipelineRun.id,
            db_models.ScheduledPipelineRun.schedule_path,
        )
        .where(db_models.ScheduledPipelineRun.id == schedule_id)
        .with_for_update()
    ).one_or_none()
    return (
        None if row is None else PathOwner(id=row.id, schedule_path=row.schedule_path)
    )
