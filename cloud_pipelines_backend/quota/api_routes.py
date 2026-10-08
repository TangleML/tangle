"""The quota group HTTP surface, under `/api/quota_groups`.

| method | path | handler |
| --- | --- | --- |
| POST | `/` | `create_quota_group` |
| GET | `/` | `list_quota_groups` |
| GET | `/{key_kind}/{key}` | `get_quota_group` |
| PATCH | `/{key_kind}/{key}` | `update_quota_group` |
| DELETE | `/{key_kind}/{key}` | `delete_quota_group` |
| GET | `/{key_kind}/{key}/claims` | `list_quota_group_claims` |
| DELETE | `/{key_kind}/{key}/claims/{execution_node_id}` | `release_quota_group_claim` |
| POST | `/{key_kind}/{key}/promote` | `promote_quota_group` |

Everything an operator can do to a group from outside the orchestrator lives here. The module
holds no quota logic of its own -- admission is `quota/interceptor.py`, un-parking is
`quota/promotion.py`, and counting is `quota/occupancy.py`. What it adds is the request/response
shapes, the ownership checks, and the two places where an edit has to *trigger* something:

- **PATCH** bumps `version` and then runs a promotion pass, because raising capacity produces no
  completion event and the sink is edge-triggered. Without that call, raising a cap from 0 to 10
  promotes nobody.
- **DELETE** un-parks every waiter first, in the same transaction, because the cascade would
  otherwise take the claim rows away and leave parked nodes invisible forever.

Pagination copies `scheduling/pipelines/api_routes.py:309` rather than inventing a second
convention -- same query parameters, same envelope, same opaque keyset cursor.
"""

import collections.abc
import datetime
import logging
from typing import Annotated, Any, Final, Literal, NamedTuple

import fastapi
import pydantic
import sqlalchemy as sql
from sqlalchemy import exc, orm
from starlette import status

from cloud_pipelines_backend import api_router
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.quota import db_models, occupancy, promotion
from cloud_pipelines_backend.quota.observability import metrics as quota_metrics
from cloud_pipelines_backend.quota.observability import promotion_observer

logger = logging.getLogger(__name__)

_API_BASE: Final[str] = "/api/quota_groups"
_INSTANCE = f"{_API_BASE}/{{key_kind}}/{{key}}"

QuotaGroupKeyKind = Literal["id", "name"]
"""Which column the key in the path names.

An explicit segment rather than sniffing the key's shape: an id is 20 hex characters and a name
is kebab-case, and `[0-9a-f]` is a subset of `[a-z0-9-]`, so `deadbeef` is a legal value of
both. The kind is in the URL, the router validates it, and no string-shape guessing survives
anywhere in the module.
"""

_KEY_COLUMN: Final[dict[str, orm.Mapped[str]]] = {
    "id": db_models.QuotaGroup.id,
    "name": db_models.QuotaGroup.name,
}
"""The one place `key_kind` turns into a column, so a new kind is a one-line change."""
_TAG: Final[str] = "quota-groups"

# Same separator and the same cursor shape as the scheduling routes. Both halves of a cursor
# are opaque to the client; the only contract is that it round-trips.
_CURSOR_SEPARATOR: Final[str] = "~"


def _encode_cursor(
    *,
    sort_value: datetime.datetime,
    tiebreak: str,
) -> str:
    """Build the opaque page token from the two columns the query orders on.

    Both list endpoints sort on a timestamp plus an id, so one encoder serves both: groups
    paginate on `(updated_at, id)` and claims on `(created_at, execution_node_id)`.

    Args:
        sort_value: The row's timestamp. Naive values are read as UTC, matching
            `scheduling/pipelines/api_routes.py:31`.
        tiebreak: The row's id, making the sort total.

    Returns:
        The token to hand back as `next_page_token`.
    """
    if sort_value.tzinfo is None:
        sort_value = sort_value.replace(tzinfo=datetime.timezone.utc)
    # Normalised to UTC and emitted without the offset, which is the one deliberate difference
    # from the scheduling cursor. `isoformat()` on an aware datetime writes "+00:00", and a bare
    # "+" in a query string decodes to a space -- so that token only survives a client that
    # percent-encodes it. Dropping the offset costs nothing here because both halves are already
    # UTC, and it makes the token safe to paste.
    naive_utc = sort_value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return f"{naive_utc.isoformat()}{_CURSOR_SEPARATOR}{tiebreak}"


def _decode_cursor(
    *,
    cursor: str,
) -> tuple[datetime.datetime, str]:
    """Split a page token back into its timestamp and its id.

    A malformed token is 422 rather than 400, matching `api_routes.py:38`: the token is a
    request parameter that failed validation, not a malformed request.

    Args:
        cursor: The `page_token` as the client sent it.

    Returns:
        The timestamp and id to resume after.

    Raises:
        fastapi.HTTPException: 422 when the token does not carry the separator.
    """
    if _CURSOR_SEPARATOR not in cursor:
        raise fastapi.HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Unrecognized page_token format: '{cursor}'. Expected 'timestamp~id' cursor.",
        )
    sort_value_str, tiebreak = cursor.split(_CURSOR_SEPARATOR, 1)
    try:
        sort_value = datetime.datetime.fromisoformat(sort_value_str)
    except ValueError:
        raise fastapi.HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Unrecognized page_token timestamp: '{sort_value_str}'.",
        )
    # Returned aware, whatever the token carried. Rows are written with `utils/db.py:8`, which
    # is aware UTC, so the comparison literal has to be aware too or the two are not comparable
    # on a driver that cares.
    if sort_value.tzinfo is None:
        sort_value = sort_value.replace(tzinfo=datetime.timezone.utc)
    return sort_value.astimezone(datetime.timezone.utc), tiebreak


_NAME_PATTERN: Final[str] = r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?$"

QuotaGroupName = Annotated[
    str,
    pydantic.Field(min_length=1, max_length=63, pattern=_NAME_PATTERN),
]
"""A name that is safe to put in a URL path, unescaped, forever.

The RFC 1123 label rule -- lowercase alphanumerics and interior hyphens -- which is what
Kubernetes uses for the same reason: the name is an addressing key (`/name/{key}`), so anything
needing percent-encoding turns every client into a URL-escaping exercise, and a name differing
from another only by case would be two rows that look like one.

63, not the 255 the column allows: validation stricter than storage is free to tighten later,
and the DB column is deliberately left alone. There is no CHECK constraint for the pattern
either -- SQLite has no REGEXP -- so this annotation is the only enforcement, which is safe
because nothing outside this module ever writes a name (quota/groups.py only reads them).

`pattern` reaches the OpenAPI schema, so generated clients reject a bad name before the call.
"""


class QuotaGroupCreateRequest(pydantic.BaseModel):
    name: QuotaGroupName
    # Zero is valid and means "pause": nothing new is admitted, and nothing already running is
    # touched. The floor is enforced again by the table's CHECK constraint.
    capacity: int = pydantic.Field(ge=0)


class QuotaGroupUpdateRequest(pydantic.BaseModel):
    """The editable surface of a group, which is capacity and nothing else.

    There is no rename. A group's name is resolved by the annotation on a node
    (quota/groups.py) while its claims are keyed by id, so a rename moves every future node
    to a different group while the nodes already parked stay behind on the old one, waiting
    for a capacity that is no longer theirs. Freezing the name removes the failure rather
    than detecting it.

    `extra="forbid"` so a client still sending `{"name": ...}` gets a 422 naming the field
    instead of a 200 that silently ignored it.
    """

    model_config = pydantic.ConfigDict(extra="forbid")

    capacity: int | None = pydantic.Field(default=None, ge=0)


class QuotaGroupResponse(pydantic.BaseModel):
    id: str
    name: str
    capacity: int
    # Claim-state counts, not the gate's occupancy. `active_count` is how many claims say
    # ACTIVE; a member whose node has finished but whose claim the sink has not yet marked
    # DONE is counted here while `occupancy` no longer counts it. Both numbers are shown so
    # that gap is visible rather than hidden behind one that looks authoritative. Claims in
    # DONE -- the ledger, eventually most of the table -- are counted by neither.
    active_count: int
    waiting_count: int
    # The number the gate actually admits on, read through quota/occupancy.py so a dashboard
    # and an admission decision cannot disagree about what "occupied" means.
    occupancy: int
    version: int
    created_by: str
    created_at: datetime.datetime
    updated_at: datetime.datetime


class QuotaGroupClaimResponse(pydantic.BaseModel):
    quota_group_id: str
    execution_node_id: str
    state: db_models.ClaimState
    # Immutable, and the whole FIFO order rests on it: re-parking keeps `created_at` so a node
    # that loses the gate does not go to the back of the queue.
    created_at: datetime.datetime
    updated_at: datetime.datetime


class QuotaGroupDetailResponse(QuotaGroupResponse):
    """One group with its **live** claims inlined -- WAITING and ACTIVE, never DONE.

    Unpaginated, which is only safe because of that filter: live claims are bounded by the
    group's capacity plus however many nodes are queued behind it, while DONE rows accumulate
    for the life of the group and have no bound at all. `GET /{key}/claims?state=DONE` is the
    paginated way to read those.
    """

    claims: list[QuotaGroupClaimResponse]


class QuotaGroupListResponse(pydantic.BaseModel):
    quota_groups: list[QuotaGroupResponse]
    # Across all pages, with no WHERE, so the client sees the size of the collection rather
    # than the size of the page.
    total_count: int
    next_page_token: str | None = None


class QuotaGroupClaimListResponse(pydantic.BaseModel):
    claims: list[QuotaGroupClaimResponse]
    total_count: int
    next_page_token: str | None = None


class QuotaGroupPromoteNode(pydantic.BaseModel):
    """One waiter's line in the promotion report."""

    execution_node_id: str
    outcome: promotion.PromotionOutcome


class QuotaGroupPromoteResponse(pydantic.BaseModel):
    """What a promotion pass did, waiter by waiter.

    A count would answer "how many moved" when the question an operator actually has is "why is
    this group stuck", so every waiter gets a line and only one of the outcomes is good news.

    There is no `occupancy_after`: promotion does not change occupancy. A promoted node sits at
    `QUEUED` with its claim still `WAITING`, which quota/occupancy.py does not count, so the
    two readings are always equal.

    `nodes` is bounded: a pass examines at most `promotion._MAX_REPORT_WAITERS` waiters, so on
    a backlogged group -- the one this endpoint exists for -- the body stays finite.
    `waiters_unexamined` says how many were left, and `0` means the pass saw the whole queue.
    """

    quota_group_id: str
    quota_group: str
    capacity: int
    occupancy_before: int
    promoted: int
    waiters_examined: int
    waiters_unexamined: int
    nodes: list[QuotaGroupPromoteNode]


class QuotaGroupDeleteResponse(pydantic.BaseModel):
    quota_group_id: str
    released: int


def _count_claims(
    *,
    session: orm.Session,
    group_id: str,
    state: db_models.ClaimState,
) -> int:
    """Count one group's claims in one state.

    Args:
        session: The request's session.
        group_id: The `quota_group.id` to count within.
        state: The claim state to match.

    Returns:
        The number of matching claim rows.
    """
    return (
        session.scalar(
            sql.select(sql.func.count())
            .select_from(db_models.QuotaGroupClaim)
            .where(
                db_models.QuotaGroupClaim.quota_group_id == group_id,
                db_models.QuotaGroupClaim.state == state,
            )
        )
        or 0
    )


class _GroupCounts(NamedTuple):
    """The three numbers a group response carries beyond its own columns.

    Two of them count claim rows, the third counts nodes, and they are not a partition --
    `occupancy` is not `active + waiting`, nor is it the whole table::

        claim_state:   WAITING       ACTIVE        DONE
                          |             |            |
                       waiting        active     counted by neither
                                                 (the ledger, eventually
                                                  most of the table)

        occupancy:     <- a different axis: quota/occupancy.py joins each live claim
                          to its node and counts the node's *status*, not the claim's --
                          running container, or ACTIVE claim whose node is still QUEUED

    So a node that finished a second ago is still in `active` until the sink marks its claim
    DONE, but has already left `occupancy`. Both are reported so that lag is visible.
    """

    #: Claims whose state is ACTIVE. Admitted by the gate; says nothing about the node.
    active: int
    #: Claims whose state is WAITING. Parked at the cap, in `created_at` order.
    waiting: int
    #: What the gate admits on -- `occupancy.count_occupancy`, read from node status.
    occupancy: int


def _counts_for_groups(
    *,
    session: orm.Session,
    group_ids: collections.abc.Sequence[str],
) -> dict[str, _GroupCounts]:
    """Count claims and occupancy for a whole page of groups in two queries.

    The single-group path costs three queries; doing that per row made a 100-group page 300
    queries. Both queries here are GROUP BYs over the same indexes, so the page costs two
    round trips whatever the page size.

    Neither GROUP BY emits a row for a group it found nothing for, so every group id is
    seeded with zeros first rather than defaulted at the call site.

    Args:
        session: The request's session.
        group_ids: The groups on this page.

    Returns:
        One entry per requested id, always present, zero-filled where there was nothing.
    """
    if not group_ids:
        return {}

    active: dict[str, int] = {}
    waiting: dict[str, int] = {}
    claim_counts = session.execute(
        sql.select(
            db_models.QuotaGroupClaim.quota_group_id,
            db_models.QuotaGroupClaim.state,
            sql.func.count().label("n"),
        )
        .where(
            db_models.QuotaGroupClaim.quota_group_id.in_(group_ids),
            # DONE is excluded rather than counted and discarded: it is the slice that grows
            # without bound, and no response field reports it.
            db_models.QuotaGroupClaim.state.in_(db_models.LIVE_CLAIM_STATES),
        )
        .group_by(
            db_models.QuotaGroupClaim.quota_group_id,
            db_models.QuotaGroupClaim.state,
        )
    ).all()
    for group_id, state, count in claim_counts:
        if state is db_models.ClaimState.ACTIVE:
            active[group_id] = count
        else:
            waiting[group_id] = count

    occupancies = dict(
        session.execute(occupancy.occupancy_by_group_query(group_ids=group_ids)).all()
    )

    return {
        group_id: _GroupCounts(
            active=active.get(group_id, 0),
            waiting=waiting.get(group_id, 0),
            occupancy=occupancies.get(group_id, 0),
        )
        for group_id in group_ids
    }


def _group_to_response(
    *,
    session: orm.Session,
    group: db_models.QuotaGroup,
    counts: _GroupCounts | None = None,
) -> QuotaGroupResponse:
    """Build the wire shape for one group, counts included.

    Args:
        session: The request's session. Unused when `counts` is supplied.
        group: The group to render.
        counts: Pre-computed counts from `_counts_for_groups`, for callers rendering a page.
            Omitted, the three counts are queried here -- fine for a single-group endpoint.

    Returns:
        The response model.
    """
    if counts is None:
        counts = _GroupCounts(
            active=_count_claims(
                session=session,
                group_id=group.id,
                state=db_models.ClaimState.ACTIVE,
            ),
            waiting=_count_claims(
                session=session,
                group_id=group.id,
                state=db_models.ClaimState.WAITING,
            ),
            occupancy=occupancy.count_occupancy(session=session, group_id=group.id),
        )
    return QuotaGroupResponse(
        id=group.id,
        name=group.name,
        capacity=group.capacity,
        active_count=counts.active,
        waiting_count=counts.waiting,
        occupancy=counts.occupancy,
        version=group.version,
        created_by=group.created_by,
        created_at=group.created_at,
        updated_at=group.updated_at,
    )


def _claim_to_response(
    *,
    claim: db_models.QuotaGroupClaim,
) -> QuotaGroupClaimResponse:
    """Build the wire shape for one claim row.

    Args:
        claim: The claim to render.

    Returns:
        The response model.
    """
    return QuotaGroupClaimResponse(
        quota_group_id=claim.quota_group_id,
        execution_node_id=claim.execution_node_id,
        state=claim.state,
        created_at=claim.created_at,
        updated_at=claim.updated_at,
    )


def _get_group_or_404(
    *,
    session: orm.Session,
    key_kind: QuotaGroupKeyKind,
    key: str,
    for_update: bool = False,
) -> db_models.QuotaGroup:
    """Load a group by whichever key the caller addressed it with, or fail the request.

    Not `session.get`: that is primary-key-only, so it silently cannot serve the name form.

    Args:
        session: The request's session.
        key_kind: Which column the key names, taken from the path and already narrowed to
            `id` or `name` by FastAPI -- an unknown kind is a 422 before this is reached.
        key: The id or the name.
        for_update: Take an exclusive row lock on the group. Admission locks the same row with
            `session.get(..., with_for_update=True)` at `quota/interceptor.py:118`, so only a
            caller that takes it too is ordered against a node arriving mid-request. Every
            read-only handler leaves it False: the lock buys them nothing and would serialise
            each one against every admission in the group.

    Returns:
        The group.

    Raises:
        fastapi.HTTPException: 404 when no group carries that key.
    """
    query = sql.select(db_models.QuotaGroup).where(_KEY_COLUMN[key_kind] == key)
    if for_update:
        query = query.with_for_update()
    group = session.scalars(query).one_or_none()
    if group is None:
        raise fastapi.HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Quota group with {key_kind} '{key}' not found",
        )
    return group


def _is_owned_by(*, session: orm.Session, group_id: str, created_by: str) -> bool:
    """Does this group belong to this caller, under the deployment's comparator?

    A separate SELECT rather than a comparison against the row already in hand, because the
    comparison is precisely the thing this must not perform in Python. `created_by` is
    `utf8mb4_0900_ai_ci` -- the table default, which folds case and accents -- so the database
    considers `Jose@example.com` and `jose@example.com` the same principal and Python's `!=`
    does not. Approximating a MySQL collation here would be a second rule to drift.

    Quota's symptom is not scheduling's. There, two rules already disagreed: path routes scoped
    their SELECT and let the collation decide while id routes compared in Python, so ownership
    depended on how the caller addressed the row. Quota has no owner-scoped read at all, so it
    is uniformly case-sensitive rather than inconsistent -- the caller is simply locked out of
    their own group by every route equally.

    `limit(1)` is defensive only; `id` is the primary key.

    Ownership ALONE. The admin bypass and the 404-vs-403 split stay with the caller, because a
    scoped read cannot tell "absent" from "foreign" and those must keep their separate codes.
    """
    return (
        session.scalar(
            sql.select(sql.literal(1))
            .where(
                db_models.QuotaGroup.id == group_id,
                db_models.QuotaGroup.created_by == created_by,
            )
            .limit(1)
        )
        is not None
    )


def _check_ownership(
    *,
    session: orm.Session,
    group: db_models.QuotaGroup,
    user_details: api_router.UserDetails,
    action: str,
) -> None:
    """Allow the creator and any admin to mutate a group; refuse everyone else.

    Mirrors `_check_ownership` at `scheduling/pipelines/api_routes.py:311` so the two surfaces
    answer "who may edit this" the same way -- including that the comparison is the database's,
    not Python's. See `_is_owned_by`.

    Admin short-circuits FIRST, so an admin costs no probe and the bypass cannot be narrowed by
    the ownership rule changing underneath it.

    `group` is still taken, and `group.created_by` is now used ONLY to phrase the refusal. It is
    deliberately not compared -- if it were, this would be back to two rules.

    Args:
        session: The request's session, for the ownership probe.
        group: The group being acted on.
        user_details: The caller.
        action: The verb to name in the refusal, e.g. "UPDATE".

    Raises:
        fastapi.HTTPException: 403 when the caller is neither an admin nor the creator.
    """
    if user_details.permissions.get("admin"):
        return
    if not _is_owned_by(
        session=session, group_id=group.id, created_by=user_details.name
    ):
        raise fastapi.HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                f"{action} denied: quota group '{group.id}' was created by {group.created_by}, not {user_details.name}"
            ),
        )


def setup_quota_group_routes(
    *,
    app: fastapi.FastAPI,
    get_session: (
        collections.abc.Callable[..., orm.Session]
        | collections.abc.Callable[..., collections.abc.Iterator[orm.Session]]
    ),
    user_details_getter: collections.abc.Callable[..., api_router.UserDetails],
) -> None:
    """Register the eight quota group endpoints on the app.

    Args:
        app: The FastAPI application to mount the router on.
        get_session: The request-scoped session dependency.
        user_details_getter: The dependency yielding the authenticated caller.
    """
    router = fastapi.APIRouter()

    @router.post(
        _API_BASE,
        status_code=status.HTTP_201_CREATED,
        tags=[_TAG],
    )
    def create_quota_group(
        request: QuotaGroupCreateRequest,
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> QuotaGroupResponse:
        existing = session.scalars(
            sql.select(db_models.QuotaGroup).where(
                db_models.QuotaGroup.name == request.name
            )
        ).one_or_none()
        if existing is not None:
            # Caught here as well as by uq_quota_group_name, so the client gets a 409 naming
            # the collision instead of a 500 from the integrity error.
            raise fastapi.HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Quota group '{request.name}' already exists",
            )

        group = db_models.QuotaGroup(
            name=request.name,
            capacity=request.capacity,
            created_by=user_details.name or "",
        )
        session.add(group)
        try:
            session.commit()
        except exc.IntegrityError:
            # The check above loses to a request that committed between it and this one.
            # uq_quota_group_name is what actually decides; the check is only there to make
            # the common case a clean 409 without a rollback. Same status and same wording,
            # so a client cannot tell which path it took.
            session.rollback()
            raise fastapi.HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Quota group '{request.name}' already exists",
            ) from None
        session.refresh(group)
        logger.info(
            f"Created quota group {group.id} ('{group.name}') with capacity {group.capacity}"
        )
        return _group_to_response(session=session, group=group)

    @router.get(
        _API_BASE,
        tags=[_TAG],
    )
    def list_quota_groups(
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
        page_size: int = fastapi.Query(default=10, ge=1, le=100),
        page_token: str | None = fastapi.Query(default=None),
    ) -> QuotaGroupListResponse:
        query = sql.select(db_models.QuotaGroup)

        if page_token:
            cursor_updated_at, cursor_id = _decode_cursor(cursor=page_token)
            # The filter tuple and the ORDER BY tuple must be the same columns in the same
            # order, or rows are skipped when a concurrent write moves a row's updated_at.
            query = query.where(
                sql.tuple_(db_models.QuotaGroup.updated_at, db_models.QuotaGroup.id)
                < sql.tuple_(sql.literal(cursor_updated_at), sql.literal(cursor_id))
            )

        query = query.order_by(
            db_models.QuotaGroup.updated_at.desc(),
            db_models.QuotaGroup.id.desc(),
        ).limit(page_size)

        groups = list(session.scalars(query).all())
        total_count = session.scalar(
            sql.select(sql.func.count(db_models.QuotaGroup.id))
        )

        next_page_token: str | None = None
        if len(groups) >= page_size:
            # Set on a full page rather than on a known-next row: one extra empty page is
            # cheaper than a second count query on every request.
            next_page_token = _encode_cursor(
                sort_value=groups[-1].updated_at, tiebreak=groups[-1].id
            )

        counts = _counts_for_groups(session=session, group_ids=[g.id for g in groups])
        return QuotaGroupListResponse(
            quota_groups=[
                _group_to_response(session=session, group=g, counts=counts[g.id])
                for g in groups
            ],
            total_count=total_count or 0,
            next_page_token=next_page_token,
        )

    @router.get(
        _INSTANCE,
        tags=[_TAG],
    )
    def get_quota_group(
        key_kind: QuotaGroupKeyKind,
        key: str,
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> QuotaGroupDetailResponse:
        group = _get_group_or_404(session=session, key_kind=key_kind, key=key)
        claims = list(
            session.scalars(
                sql.select(db_models.QuotaGroupClaim)
                .where(
                    db_models.QuotaGroupClaim.quota_group_id == group.id,
                    # Unpaginated, so it must be bounded. DONE rows are the group's whole
                    # history and are read through the paginated claims endpoint instead.
                    db_models.QuotaGroupClaim.state.in_(db_models.LIVE_CLAIM_STATES),
                )
                .order_by(
                    db_models.QuotaGroupClaim.created_at.asc(),
                    db_models.QuotaGroupClaim.execution_node_id.asc(),
                )
            ).all()
        )
        summary = _group_to_response(session=session, group=group)
        return QuotaGroupDetailResponse(
            **summary.model_dump(),
            claims=[_claim_to_response(claim=c) for c in claims],
        )

    @router.patch(
        _INSTANCE,
        tags=[_TAG],
    )
    def update_quota_group(
        key_kind: QuotaGroupKeyKind,
        key: str,
        request: QuotaGroupUpdateRequest,
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> QuotaGroupResponse:
        group = _get_group_or_404(session=session, key_kind=key_kind, key=key)
        _check_ownership(
            session=session,
            group=group,
            user_details=user_details,
            action="UPDATE",
        )

        # `db_models.QuotaGroup.version` is the *class* attribute, not `group.version`. On the
        # class, SQLAlchemy's mapper returns a column expression, so `+ 1` builds SQL rather
        # than doing arithmetic:
        #
        #     group.version + 1              ->  4              a Python int, read at load time
        #     db_models.QuotaGroup.version+1 ->  "version + 1"  SQL, evaluated at UPDATE time
        #
        # That difference is the whole reason the read-modify-write was replaced. The version
        # is a CAS token: admissions do `UPDATE ... WHERE version = N` and lose if it moved.
        #
        #     with `group.version += 1`               with `QuotaGroup.version + 1`
        #     ----------------------------            -----------------------------
        #     PATCH loads group, version=3            PATCH loads group, version=3
        #     admission CASes 3 -> 4, wins            admission CASes 3 -> 4, wins
        #     PATCH writes literal 4  <-- clobber     PATCH writes "version + 1" -> 5
        #     next admission WHERE version=4          next admission WHERE version=4
        #       matches -- but it read 4 from a         misses, re-reads 5, retries
        #       row that no longer exists
        #
        # The left column loses an admission silently; the right one makes it retry.
        #
        # Bumped on every edit, not only when capacity moves: an admission that read this
        # group before the edit must lose and re-read either way.
        values: dict[str, Any] = {"version": db_models.QuotaGroup.version + 1}
        if request.capacity is not None:
            values["capacity"] = request.capacity
        session.execute(
            sql.update(db_models.QuotaGroup)
            .where(db_models.QuotaGroup.id == group.id)
            .values(**values)
        )
        # `expire()` marks the instance's loaded columns unknown -- it does not reload, and it
        # does not discard the object. The next attribute read on it emits a fresh SELECT.
        #
        # Needed because the UPDATE above is Core SQL: it goes to the database directly and
        # never passes through the identity map, so `group` still holds the pre-PATCH row.
        #
        #     session.execute(UPDATE ... capacity=10)   db: capacity=10   group.capacity: 2  <- stale
        #     session.expire(group)                     db: capacity=10   group.capacity: ?  <- unknown
        #     promote() -> session.get(...)             identity map hit, columns unknown
        #                                                 -> SELECT               -> 10
        #
        # Without it `session.get()` returns this same instance straight from the identity map
        # with no SELECT at all, and the promotion is sized against the old capacity.
        session.expire(group)

        # Then promote, in the same transaction. The sink is edge-triggered on a member ending,
        # so a group whose capacity just went from 0 to 10 produces no edge and would otherwise
        # promote nobody until something happened to finish. Flushed first for the same reason
        # as in the release handler: autoflush is off everywhere in this service.
        session.flush()
        with promotion_observer.promoting(
            quota_group=group.name,
            trigger=quota_metrics.PromotionTrigger.PATCH,
        ) as promotion_pass:
            promoted = promotion.promote(session=session, group_id=group.id)
            promotion_pass.promoted(count=promoted)
            # Committed inside the measured block: a commit that fails out here would
            # otherwise be a promotion already counted and then rolled back.
            session.commit()
        session.refresh(group)
        logger.info(
            f"Updated quota group {group.id} to capacity {group.capacity} (version {group.version}),"
            f" promoted {promoted} waiter(s)"
        )
        return _group_to_response(session=session, group=group)

    @router.delete(
        _INSTANCE,
        tags=[_TAG],
    )
    def delete_quota_group(
        key_kind: QuotaGroupKeyKind,
        key: str,
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
        force: bool = fastapi.Query(default=False),
    ) -> QuotaGroupDeleteResponse:
        # Locked, alone among the handlers: this transaction releases the waiters and then
        # deletes the group, and admission takes the same row lock at `quota/interceptor.py:118`
        # before it claims a slot. Unlocked, a node admitted between the release scan below and
        # the DELETE has its claim cascaded away and is left parked at UNINITIALIZED with no
        # claim row -- which is precisely the invisible state the comment below says the
        # un-parking exists to prevent, reached by the one path that comment does not cover.
        group = _get_group_or_404(
            session=session, key_kind=key_kind, key=key, for_update=True
        )
        _check_ownership(
            session=session,
            group=group,
            user_details=user_details,
            action="DELETE",
        )

        # A group with live members is a cap that is still doing its job. Deleting it does not
        # stop them -- their claims cascade away and the gate loses the ability to see them --
        # so a same-name recreate admits a second full capacity against nodes that are still
        # running. Refusing by default turns that from a silent doubling into a message; the
        # override is what keeps DELETE usable on the long-running group you most want it for.
        #
        # 409 and not 412: the precondition is on server state the client never asserted, so
        # there is no `If-Match` for it to have failed. 409 is "the resource is in a state that
        # forbids this".
        if not force:
            live = occupancy.count_occupancy(session=session, group_id=group.id)
            if live:
                raise fastapi.HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=(
                        f"quota group {group.name!r} has {live} active claim(s);"
                        " retry with ?force=true to delete anyway"
                    ),
                )

        # Unconditional, and un-parking comes first. ON DELETE CASCADE removes the claims, and a
        # node parked at UNINITIALIZED with no claim row is invisible to everything: the queued
        # sweep does not select it and no promotion pass can find it. One transaction, so a
        # rollback leaves every node still parked and the group still there.
        #
        # Unconditional *once past the guard above*, and the three consequences are accepted
        # rather than overlooked:
        #   1. Running nodes keep running. Their claims cascade away and the terminal-status
        #      sink finds nothing, which it treats as `no_claim` and ignores -- not an error.
        #   2. The cap stops existing. `quota/groups.py:110` returns `group is None` for the
        #      name from here on, so every later node declaring it launches ungated with a
        #      `quota_group_missing` marker on `extra_data`.
        #   3. Delete-then-recreate under the same name can exceed the new capacity. Occupancy
        #      is COUNT(claim JOIN node) and the old claims are gone, so the new group reads 0
        #      while the old members still run. The window closes as they finish; it is not
        #      repaired, only refused by default.
        # Read the id before the row goes: the response always identifies the group by id, even
        # when the caller addressed it by name, and `group` is expired after the commit.
        quota_group_id = group.id
        released = promotion.release_waiters_in_group(
            session=session, group_id=group.id
        )
        session.delete(group)
        session.commit()
        logger.info(
            f"Deleted quota group {quota_group_id}, released {released} parked node(s)"
        )
        return QuotaGroupDeleteResponse(
            quota_group_id=quota_group_id, released=released
        )

    @router.get(
        f"{_INSTANCE}/claims",
        tags=[_TAG],
    )
    def list_quota_group_claims(
        key_kind: QuotaGroupKeyKind,
        key: str,
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
        page_size: int = fastapi.Query(default=10, ge=1, le=100),
        page_token: str | None = fastapi.Query(default=None),
        state: db_models.ClaimState | None = fastapi.Query(default=None),
    ) -> QuotaGroupClaimListResponse:
        group = _get_group_or_404(session=session, key_kind=key_kind, key=key)

        # One state, or the live ones. There is deliberately no "everything" value: the whole
        # point of the filter is that a page must never be cut from the group's full history,
        # and `state=DONE` is how that history is read, a page at a time.
        #
        # Typed as the enum, so FastAPI rejects an unknown value from the OpenAPI schema and
        # the endpoint never sees it.
        state_filter = (
            db_models.QuotaGroupClaim.state == state
            if state is not None
            else db_models.QuotaGroupClaim.state.in_(db_models.LIVE_CLAIM_STATES)
        )

        query = (
            sql.select(db_models.QuotaGroupClaim)
            .options(
                # An allow-list, not `defer(extra_data)`. `defer` names the column to leave
                # behind, so the next wide column rejoins the payload silently; this way a
                # column has to be named here to reach the page. `parked_at` (added after
                # this list was written) is the first one that proves the point -- it stays
                # out without anyone touching this line.
                #
                # What is being kept out is `extra_data`, the unbounded `{"history": [...]}`
                # blob at `quota/db_models.py:154`, which no caller ever receives: the
                # serializer at `_claim_to_response` reads five columns and none of them is
                # it. It matters because of the ORDER BY below -- MySQL's filesort carries
                # the whole selected row, so the blob was being sorted, per row, to be
                # discarded. Without it the payload is at most 72 fixed bytes a row, and a
                # page is ~3.6 KB against a 256 KB default `sort_buffer_size`: one in-memory
                # pass, no merge file. The filesort is not worth restructuring the read for.
                #
                # `raiseload` because the alternative to raising is worse than the blob: a
                # deferred attribute touched later emits a lazy SELECT *per row*, correct
                # and invisible, turning one page query into fifty-one. This makes that a
                # red test instead of a latency graph.
                orm.load_only(
                    db_models.QuotaGroupClaim.state,
                    db_models.QuotaGroupClaim.created_at,
                    db_models.QuotaGroupClaim.updated_at,
                    raiseload=True,
                )
            )
            .where(
                db_models.QuotaGroupClaim.quota_group_id == group.id,
                state_filter,
            )
        )

        if page_token:
            cursor_created_at, cursor_node_id = _decode_cursor(cursor=page_token)
            # Ascending here, unlike the group list: claims are read in FIFO admission order,
            # which is the order promotion walks them in.
            query = query.where(
                sql.tuple_(
                    db_models.QuotaGroupClaim.created_at,
                    db_models.QuotaGroupClaim.execution_node_id,
                )
                > sql.tuple_(
                    sql.literal(cursor_created_at), sql.literal(cursor_node_id)
                )
            )

        query = query.order_by(
            db_models.QuotaGroupClaim.created_at.asc(),
            db_models.QuotaGroupClaim.execution_node_id.asc(),
        ).limit(page_size)

        claims = list(session.scalars(query).all())
        total_count = session.scalar(
            # Same filter as the page, or the count describes a different set than the rows
            # under it -- 4,000 for a page of three waiters.
            sql.select(sql.func.count())
            .select_from(db_models.QuotaGroupClaim)
            .where(
                db_models.QuotaGroupClaim.quota_group_id == group.id,
                state_filter,
            )
        )

        next_page_token: str | None = None
        if len(claims) >= page_size:
            next_page_token = _encode_cursor(
                sort_value=claims[-1].created_at,
                tiebreak=claims[-1].execution_node_id,
            )

        return QuotaGroupClaimListResponse(
            claims=[_claim_to_response(claim=c) for c in claims],
            total_count=total_count or 0,
            next_page_token=next_page_token,
        )

    @router.delete(
        f"{_INSTANCE}/claims/{{execution_node_id}}",
        tags=[_TAG],
    )
    def release_quota_group_claim(
        key_kind: QuotaGroupKeyKind,
        key: str,
        execution_node_id: str,
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> QuotaGroupClaimResponse:
        group = _get_group_or_404(session=session, key_kind=key_kind, key=key)
        _check_ownership(
            session=session,
            group=group,
            user_details=user_details,
            action="RELEASE",
        )

        claim = session.get(db_models.QuotaGroupClaim, (group.id, execution_node_id))
        if claim is None:
            raise fastapi.HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Node '{execution_node_id}' holds no claim in quota group '{group.id}'",
            )

        released = _claim_to_response(claim=claim)

        # Deletes the row rather than moving it to DONE, which is the one place a ledger entry
        # is lost -- deliberately. This is the operator hatch for a claim that should not have
        # existed, and a released WAITING node goes straight back through the gate, which would
        # have to overwrite a DONE row to re-claim (see _claim_impl). DONE is for the normal
        # lifecycle: the node ran, the node ended.
        #
        # The node is un-parked in the same breath for the same reason DELETE un-parks: without
        # its claim, a parked node can never be found again.
        node = session.get(bts.ExecutionNode, execution_node_id)
        # STATES.md row 1, parked -- the Python reading of
        # `occupancy.is_parked_for_quota_group_slot`. Evaluated on loaded objects here, so it
        # cannot import the predicate. If that one changes, change this.
        if (
            claim.state == db_models.ClaimState.WAITING
            and node is not None
            and node.container_execution_status
            == bts.ContainerExecutionStatus.UNINITIALIZED
        ):
            node.container_execution_status = bts.ContainerExecutionStatus.QUEUED

        session.delete(claim)
        # Flushed, not just pending. Every session in this service is autoflush=False, and
        # promotion counts occupancy with a fresh SELECT -- so without this the query still sees
        # the claim that was just deleted, reads the slot as taken, and promotes nobody.
        session.flush()
        # Read before the commit, not after. Sessions here are `expire_on_commit=True`, so
        # `group.name` past the commit is a fresh SELECT -- and if the group were deleted in
        # that window it would raise, turning a write that already succeeded into a 500.
        group_name = group.name
        # Freeing a slot is exactly the edge the sink acts on, so hand it to the next waiter
        # rather than waiting for something to finish.
        with promotion_observer.promoting(
            quota_group=group_name,
            trigger=quota_metrics.PromotionTrigger.CLAIM_RELEASE_API,
        ) as promotion_pass:
            promoted = promotion.promote(session=session, group_id=group.id)
            promotion_pass.promoted(count=promoted)
            # Inside the block, so a failed commit is not a promotion already counted.
            session.commit()
        # Every increment here is a human working around the system, so it is worth a panel
        # even though it should sit at zero.
        quota_metrics.increment(
            counter=quota_metrics.claims_force_deleted,
            attributes={quota_metrics.QUOTA_GROUP_LABEL: group_name},
        )
        logger.info(
            f"Released claim on node {execution_node_id} in quota group {group_name}, "
            f"promoted {promoted} waiter(s)"
        )
        return released

    @router.post(
        f"{_INSTANCE}/promote",
        tags=[_TAG],
    )
    def promote_quota_group(
        key_kind: QuotaGroupKeyKind,
        key: str,
        session: orm.Session = fastapi.Depends(get_session),
        user_details: api_router.UserDetails = fastapi.Depends(user_details_getter),
    ) -> QuotaGroupPromoteResponse:
        group = _get_group_or_404(session=session, key_kind=key_kind, key=key)
        _check_ownership(
            session=session,
            group=group,
            user_details=user_details,
            action="PROMOTE",
        )

        # The level-triggered escape hatch for the edge-triggered sink: a group with nothing
        # running produces no completion event and so promotes nobody, however many waiters it
        # has. This runs the pass the sink would have run, and reports on every waiter rather
        # than only the ones it moved.
        with promotion_observer.promoting(
            quota_group=group.name,
            trigger=quota_metrics.PromotionTrigger.PROMOTE_API,
        ) as promotion_pass:
            report = promotion.promote_with_report(session=session, group=group)
            promotion_pass.promoted(count=report.promoted)
            # Inside the block, so a failed commit is not a promotion already counted.
            session.commit()
        # From the report, not from `group`: the commit above expired the object, so reading
        # an attribute off it here would be a fresh SELECT purely to write a log line.
        logger.info(
            f"Manual promotion pass on quota group {report.quota_group}"
            f" ({report.quota_group_id}) promoted {report.promoted} waiter(s),"
            f" examined {report.waiters_examined},"
            f" left {report.waiters_unexamined} unexamined"
        )
        return QuotaGroupPromoteResponse(
            quota_group_id=report.quota_group_id,
            quota_group=report.quota_group,
            capacity=report.capacity,
            occupancy_before=report.occupancy_before,
            promoted=report.promoted,
            waiters_examined=report.waiters_examined,
            waiters_unexamined=report.waiters_unexamined,
            nodes=[
                QuotaGroupPromoteNode(
                    execution_node_id=node.execution_node_id,
                    outcome=node.outcome,
                )
                for node in report.nodes
            ],
        )

    app.include_router(router)
