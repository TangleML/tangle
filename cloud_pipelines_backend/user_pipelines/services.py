import copy
import dataclasses
import datetime
import hashlib
import json
import logging
import uuid
from collections import abc
from typing import Any, Final, Protocol, cast

import pydantic
import sqlalchemy as sql
from cloud_pipelines_backend import api_server_sql, component_structures
from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.user_pipelines import (
    database_ops,
    db_models,
    pipeline_structure_validation,
)
from cloud_pipelines_backend.user_pipelines import (
    pipeline_run_annotations as pipeline_annotation_validation,
)
from cloud_pipelines_backend.user_pipelines.errors import (
    PipelineNotFoundError,
    PipelineValidationError,
    VersionNotFoundError,
    VersionNotPinnableError,
)
from sqlalchemy import orm

logger = logging.getLogger(__name__)


@dataclasses.dataclass(kw_only=True)
class PipelineContent:
    root_pipeline_task: dict[str, Any]
    pipeline_run_annotations: dict[str, str]
    pipeline_name: str | None
    digest: str


@dataclasses.dataclass(frozen=True, kw_only=True)
class PipelineDigest:
    root_pipeline_task: dict[str, Any]
    digest: str


@dataclasses.dataclass(kw_only=True)
class PipelineWriteResult:
    pipeline: db_models.UserPipeline
    version: db_models.UserPipelineVersion
    message: str
    updated: bool
    reused_version: bool


def normalize_file_path(file_path: str) -> str:
    normalized = file_path.strip()
    if not (0 < len(normalized) <= db_models.MAX_FILE_PATH_LENGTH):
        raise PipelineValidationError(
            f"Pipeline file path must be between 1 and {db_models.MAX_FILE_PATH_LENGTH} characters."
        )
    return normalized


def normalize_pipeline_id(pipeline_id: str) -> str:
    try:
        return str(uuid.UUID(pipeline_id))
    except ValueError as exc:
        raise PipelineValidationError(
            f"pipeline_id must be a valid UUID; received {pipeline_id!r}."
        ) from exc


def live_pipeline_ids(
    *, session: orm.Session, pipeline_ids: abc.Iterable[str]
) -> set[str]:
    """Which of these pipeline ids exist and are not soft-deleted, answered in one query.

    Deliberately says nothing about ownership: this is for read paths that report whether a
    stored reference still points at something, where hiding another tenant's liveness behind
    a false would misreport the row rather than protect anything -- the id is already in the
    response that is asking.

    Ids that are not valid UUIDs are simply absent from the result, because a reference the
    database cannot hold is not live either; the alternative is a read route raising over a
    column a write route could not have written.
    """
    normalized: dict[str, str] = {}
    for pipeline_id in pipeline_ids:
        try:
            normalized[normalize_pipeline_id(pipeline_id)] = pipeline_id
        except PipelineValidationError:
            continue
    if not normalized:
        return set()
    # Batched on purpose: the listing route asks about a whole page at once, and a per-row
    # lookup would make the response cost grow with `page_size`.
    found = session.scalars(
        sql.select(db_models.UserPipeline.id).where(
            db_models.UserPipeline.id.in_(normalized),
            db_models.UserPipeline.deleted_at.is_(None),
        )
    ).all()
    # Keyed back to the caller's spelling, so a caller comparing against the id it passed in
    # does not have to normalize first.
    return {normalized[found_id] for found_id in found}


def get_live_owned_pipeline(
    *,
    session: orm.Session,
    pipeline_id: str,
    user_id: str | None,
    for_share: bool = False,
    load_options: abc.Sequence[orm.interfaces.ORMOption] = (),
) -> db_models.UserPipeline:
    """The pipeline with this id, if it exists, is not soft-deleted, and belongs to `user_id` (None skips ownership).

    `for_share` takes a SHARED row lock, for a caller that has to decide something
    from this row and needs a concurrent mode writer to serialize against that
    decision rather than land in the middle of it. Shared rather than exclusive so
    that two such readers do not block each other; see `_get_pipeline`, which is
    where the reasoning for that choice lives. Off by default: the read-only
    callers that made this the shared door do not want a lock, and taking one for
    them would turn every reference check into a lock holder.

    `load_options` narrows the SELECT for a caller that reads only a few columns.

    Ownership is decided by the `user_id ==` predicate alone, evaluated by the
    database under the column's own collation. That comparator is the
    authoritative one for owner identity across this service, and this function
    deliberately does not hold a second opinion about it in Python.
    """
    normalized_id = normalize_pipeline_id(pipeline_id)
    filters = [
        db_models.UserPipeline.id == normalized_id,
        db_models.UserPipeline.deleted_at.is_(None),
    ]
    if user_id is not None:
        filters.append(db_models.UserPipeline.user_id == user_id)
    statement = sql.select(db_models.UserPipeline).where(*filters)
    if for_share:
        statement = statement.with_for_update(read=True)
    if load_options:
        statement = statement.options(*load_options)
    pipeline = session.scalar(statement)
    # No Python re-comparison of `user_id` here. An earlier version re-compared it
    # byte-exactly after the query, which made this function a SECOND owner
    # comparator competing with the database's. Owner identity has one authority --
    # the deployment's column collation -- and a caller who matches it is the
    # owner. Reinstating a Python comparison would also fail CLOSED for a caller
    # whose identity differs from the stored spelling only in case, locking them
    # out of their own pipeline.
    if pipeline is None:
        # One answer for missing, deleted, and someone else's: distinguishing them would be an
        # existence oracle over other tenants' pipelines.
        raise PipelineNotFoundError(
            f"Pipeline with id {normalized_id!r} was not found."
        )
    return pipeline


def resolve_pinnable_version_key(
    *,
    session: orm.Session,
    pipeline: db_models.UserPipeline,
    content_digest: str,
) -> str:
    """The `version_key` to store for a pinned `content_digest`, refusing anything this pipeline cannot pin."""
    # The mutable head is excluded, so the pinnable set is exactly what `list_versions` returns:
    # its content is overwritten in place, and it is the row a DISABLED -> FULL switch deletes.
    # Takes the loaded pipeline rather than an id so the caller has already passed
    # `get_live_owned_pipeline` and neither failure below is observable cross-tenant.
    # Matched on `version_key`, not `content_digest`: every immutable row is written with the
    # two equal -- both insert sites in this module pass the same digest for each -- so the
    # digest *is* the second half of the primary key. Filtering on the copy seeks to this
    # pipeline and then walks its whole version history; filtering on the key is one PK seek.
    key = session.scalar(
        sql.select(db_models.UserPipelineVersion.version_key).where(
            db_models.UserPipelineVersion.pipeline_id == pipeline.id,
            db_models.UserPipelineVersion.version_key == content_digest,
            db_models.UserPipelineVersion.version_key != db_models.CURRENT_VERSION_KEY,
        )
    )
    if key is not None:
        return key
    # Distinguish "no such content" from "this pipeline keeps no history": the caller can act on
    # the second and not on the first. The sentinel is excluded above rather than tie-broken,
    # because a DISABLED -> FULL switch leaves both rows carrying the same digest until the
    # delete lands, and the immutable one is the answer in that window.
    if session.get(
        db_models.UserPipelineVersion,
        (pipeline.id, db_models.CURRENT_VERSION_KEY),
    ):
        raise VersionNotPinnableError(
            f"Pipeline {pipeline.id!r} does not retain version history; enable full versioning to pin a version."
        )
    raise VersionNotFoundError(
        f"No version {content_digest!r} for pipeline {pipeline.id!r}."
    )


def calculate_pipeline_digest(
    *,
    root_pipeline_task: component_structures.TaskSpec,
    pipeline_run_annotations: dict[str, str],
) -> PipelineDigest:
    canonical_task = root_pipeline_task.to_json_dict()
    digest_payload = {
        "pipeline_run_annotations": pipeline_run_annotations,
        "root_pipeline_task": canonical_task,
    }
    canonical_json = json.dumps(
        digest_payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return PipelineDigest(
        root_pipeline_task=canonical_task,
        digest=hashlib.sha256(canonical_json.encode("utf-8")).hexdigest(),
    )


def prepare_pipeline_content(
    *,
    root_pipeline_task: dict[str, Any],
    pipeline_run_annotations: dict[str, str] | None,
) -> PipelineContent:
    try:
        task_spec = pydantic.TypeAdapter(component_structures.TaskSpec).validate_python(
            root_pipeline_task,
            extra="forbid",
        )
    except Exception as exc:
        raise PipelineValidationError(
            "root_pipeline_task is not a valid TaskSpec: "
            f"{pipeline_structure_validation.describe_parse_failure(exc)}"
        ) from exc

    pipeline_structure_validation.validate_task_output_references(task_spec)

    canonical_annotations = pipeline_run_annotations or {}
    pipeline_annotation_validation.validate_pipeline_run_annotations(
        canonical_annotations,
        # The only caller whose annotations reach `calculate_pipeline_digest` below, where a
        # project id would become part of the pipeline's version identity.
        allow_submission_scoped=False,
    )
    calculated_digest = calculate_pipeline_digest(
        root_pipeline_task=task_spec,
        pipeline_run_annotations=canonical_annotations,
    )

    component_spec = task_spec.component_ref.spec
    pipeline_name = component_spec.name if component_spec else None
    return PipelineContent(
        root_pipeline_task=calculated_digest.root_pipeline_task,
        pipeline_run_annotations=canonical_annotations,
        pipeline_name=pipeline_name,
        digest=calculated_digest.digest,
    )


#: Columns a caller needs to VALIDATE a saved-pipeline reference, as opposed to
#: submitting one. `get_pipeline_and_version(validation_only=True)` selects these instead of
#: the whole row.
#:
#: `raiseload=True` rather than a plain defer: an unloaded column would otherwise
#: emit a silent extra SELECT on first access, which is precisely the cost this
#: projection exists to remove and precisely the kind of regression a passing
#: test suite would hide. Loud beats invisible, and the only caller discards
#: these objects (Binks 3906749295, pi-38/pi-40/pi-41).
_VALIDATION_PIPELINE_LOAD: Final = (
    orm.load_only(
        # `user_id` and `deleted_at` are both enforced by the WHERE clause alone.
        # `user_id` is nonetheless kept in the projection: `raiseload=True` turns
        # any attribute left out of this list into an exception on access, so
        # narrowing it further is a change to what callers may touch, not a free
        # saving. It is deliberately NOT here to support a Python owner
        # comparison -- there is no longer one to support.
        #
        # `id` is absent from this list and is still loaded: `load_only` always
        # keeps the primary key, because SQLAlchemy needs it for identity. That is
        # a guarantee callers may rely on, not an accident of this list, and one
        # does -- the schedule writer's create-time preflight RETURNS the loaded
        # `id` and stores it. It has to: the lookup normalizes, so a caller may
        # have spelled the id in a form the primary key does not hold, and
        # persisting the caller's spelling breaks the foreign key onto
        # `pipeline.id`. Anyone tightening this projection should know that a
        # reader outside this module depends on the PK surviving it.
        db_models.UserPipeline.user_id,
        db_models.UserPipeline.current_version_key,
        db_models.UserPipeline.versioning_mode,
        raiseload=True,
    ),
)

#: Same idea for a version row. `content_digest` is here because the
#: current-following comparison reads it; `root_pipeline_task`,
#: `pipeline_run_annotations` and `extra_data` are the JSON this exists to avoid.
_VALIDATION_VERSION_LOAD: Final = (
    orm.load_only(
        db_models.UserPipelineVersion.content_digest,
        raiseload=True,
    ),
)


def require_pinnable_versioning(pipeline: db_models.UserPipeline) -> None:
    """Refuse an explicit version pin unless the pipeline is in FULL mode.

    Excluding the reserved ``current`` key is not sufficient on its own, which is
    the trap this exists to close. Switching a pipeline to ``DISABLED`` does not
    delete its historical immutable rows -- ``set_versioning_mode`` only writes
    the ``current`` sentinel and repoints ``current_version_key`` -- so a
    now-``DISABLED`` pipeline can still hold old version rows that a lookup would
    happily resolve. Under ``DISABLED`` there is no versioning contract to pin
    against: the pinned row is unreachable through the versioning APIs and cannot
    be reasoned about by its owner, so resolving it would honour a promise the
    pipeline no longer makes.

    Current-following is unaffected. Following the mutable ``current`` head is
    exactly what a caller asks for when it supplies no version.

    Lives here rather than in any one caller because both the scheduler executor
    and the schedule writer have to agree on it; two copies would be two answers.
    """
    if pipeline.versioning_mode is not db_models.PipelineVersioningMode.FULL:
        raise PipelineValidationError(
            f"Pipeline {pipeline.id!r} has versioning mode"
            f" {pipeline.versioning_mode.value!r}, so a specific version cannot be"
            " pinned. Enable full versioning, or reference the pipeline without a"
            " version to follow its current content."
        )


class RunHooks(Protocol):
    """Optional deployment behavior around creation of a saved-pipeline run."""

    def prepare_saved_run(self, task_json: dict[str, Any]) -> Any:
        """Prepare the copied task graph and return context for the created run."""
        ...

    def run_created(self, run: bts.PipelineRun, context: Any) -> None:
        """Apply deployment metadata inside the caller's transaction."""
        ...


class UserPipelineService:
    def __init__(self, *, hooks: RunHooks | None = None) -> None:
        self._hooks = hooks

    def set_pipeline(
        self,
        *,
        session: orm.Session,
        user_id: str,
        file_path: str,
        root_pipeline_task: dict[str, Any],
        pipeline_run_annotations: dict[str, str] | None,
        versioning_mode: db_models.PipelineVersioningMode | None = None,
    ) -> PipelineWriteResult:
        file_path = normalize_file_path(file_path)
        content = prepare_pipeline_content(
            root_pipeline_task=root_pipeline_task,
            pipeline_run_annotations=pipeline_run_annotations,
        )

        with session.begin():
            pipeline = session.scalar(
                sql.select(db_models.UserPipeline)
                .where(
                    db_models.UserPipeline.user_id == user_id,
                    db_models.UserPipeline.file_path == file_path,
                )
                .with_for_update()
            )
            if pipeline is None:
                target_mode = (
                    versioning_mode or db_models.PipelineVersioningMode.DISABLED
                )
                now = datetime.datetime.now(datetime.timezone.utc)
                candidate = db_models.UserPipeline(
                    user_id=user_id,
                    file_path=file_path,
                )
                pipeline_table = cast(sql.Table, db_models.UserPipeline.__table__)
                insert_error = database_ops.insert_with_integrity_fallback(
                    session=session,
                    table=pipeline_table,
                    values={
                        "id": candidate.id,
                        "user_id": user_id,
                        "file_path": file_path,
                        "created_at": now,
                        "updated_at": now,
                        "current_version_key": None,
                        "versioning_mode": target_mode,
                        "deleted_at": None,
                    },
                )
                # The unique constraint resolves concurrent first writers; the
                # savepoint keeps this transaction usable after the losing insert.
                pipeline = session.scalar(
                    sql.select(db_models.UserPipeline)
                    .where(
                        db_models.UserPipeline.user_id == user_id,
                        db_models.UserPipeline.file_path == file_path,
                    )
                    .with_for_update()
                )
                if pipeline is None:
                    if insert_error is not None:
                        raise insert_error
                    raise RuntimeError(
                        f"Failed to create pipeline {user_id!r}/{file_path!r}."
                    )
                if insert_error is not None and versioning_mode is None:
                    # This request lost a concurrent first-write race. Omission now
                    # applies to the winning existing row, so preserve its mode.
                    target_mode = pipeline.versioning_mode
            else:
                target_mode = versioning_mode or pipeline.versioning_mode

            was_deleted = pipeline.deleted_at is not None
            mode_changed = pipeline.versioning_mode is not target_mode
            pipeline.deleted_at = None
            now = datetime.datetime.now(datetime.timezone.utc)
            version_table = cast(sql.Table, db_models.UserPipelineVersion.__table__)

            if target_mode is db_models.PipelineVersioningMode.DISABLED:
                version = session.get(
                    db_models.UserPipelineVersion,
                    (pipeline.id, db_models.CURRENT_VERSION_KEY),
                )
                same_content = (
                    version is not None and version.content_digest == content.digest
                )
                if version is None:
                    insert_error = database_ops.insert_with_integrity_fallback(
                        session=session,
                        table=version_table,
                        values={
                            "pipeline_id": pipeline.id,
                            "version_key": db_models.CURRENT_VERSION_KEY,
                            "content_digest": content.digest,
                            "root_pipeline_task": content.root_pipeline_task,
                            "pipeline_run_annotations": content.pipeline_run_annotations,
                            "created_at": now,
                            "extra_data": (
                                {"pipeline_name": content.pipeline_name}
                                if content.pipeline_name
                                else None
                            ),
                        },
                    )
                    version = session.get(
                        db_models.UserPipelineVersion,
                        (pipeline.id, db_models.CURRENT_VERSION_KEY),
                    )
                    if version is None:
                        if insert_error is not None:
                            raise insert_error
                        raise RuntimeError(
                            f"Failed to create mutable head for pipeline {pipeline.id!r}."
                        )
                elif not same_content:
                    version.content_digest = content.digest
                    version.root_pipeline_task = content.root_pipeline_task
                    version.pipeline_run_annotations = content.pipeline_run_annotations
                    version.created_at = now
                    version.extra_data = (
                        {"pipeline_name": content.pipeline_name}
                        if content.pipeline_name
                        else None
                    )

                updated = was_deleted or mode_changed or not same_content
                pipeline.current_version_key = db_models.CURRENT_VERSION_KEY
                pipeline.versioning_mode = target_mode
                if updated:
                    pipeline.updated_at = now
                return PipelineWriteResult(
                    pipeline=pipeline,
                    version=version,
                    message=(
                        "Pipeline is already at this version; no changes were made."
                        if not updated
                        else "Pipeline saved to its mutable current version."
                    ),
                    updated=updated,
                    reused_version=same_content,
                )

            version = session.get(
                db_models.UserPipelineVersion,
                (pipeline.id, content.digest),
            )
            reused_version = version is not None
            if version is None:
                insert_error = database_ops.insert_with_integrity_fallback(
                    session=session,
                    table=version_table,
                    values={
                        "pipeline_id": pipeline.id,
                        "version_key": content.digest,
                        "content_digest": content.digest,
                        "root_pipeline_task": content.root_pipeline_task,
                        "pipeline_run_annotations": content.pipeline_run_annotations,
                        "created_at": now,
                        "extra_data": (
                            {"pipeline_name": content.pipeline_name}
                            if content.pipeline_name
                            else None
                        ),
                    },
                )
                version = session.get(
                    db_models.UserPipelineVersion,
                    (pipeline.id, content.digest),
                )
                if version is None:
                    if insert_error is not None:
                        raise insert_error
                    raise RuntimeError(
                        f"Failed to create version {content.digest!r} for pipeline {pipeline.id!r}."
                    )

            updated = (
                was_deleted
                or mode_changed
                or pipeline.current_version_key != version.version_key
            )
            pipeline.current_version_key = version.version_key
            pipeline.versioning_mode = target_mode
            if updated:
                pipeline.updated_at = now

            sentinel = session.get(
                db_models.UserPipelineVersion,
                (pipeline.id, db_models.CURRENT_VERSION_KEY),
            )
            if sentinel is not None:
                session.flush()
                session.delete(sentinel)

            return PipelineWriteResult(
                pipeline=pipeline,
                version=version,
                message=(
                    "Pipeline is already at this version; no changes were made."
                    if not updated
                    else (
                        "Pipeline reactivated with full version history."
                        if was_deleted
                        else (
                            "Pipeline updated by restoring an existing version."
                            if reused_version
                            else "Pipeline saved with full version history."
                        )
                    )
                ),
                updated=updated,
                reused_version=reused_version,
            )

    def patch_pipeline_properties(
        self,
        *,
        session: orm.Session,
        user_id: str,
        pipeline_id: str,
        versioning_mode: db_models.PipelineVersioningMode,
    ) -> PipelineWriteResult:
        normalized_id = normalize_pipeline_id(pipeline_id)
        with session.begin():
            # Exclusive, so a mode transition serializes against a pin being
            # resolved -- see `_get_pipeline(for_share=...)`. This lock was
            # already here; what changed is that the pin-resolving reader now
            # takes a SHARED lock on the same row, and `FOR SHARE` blocks
            # `FOR UPDATE`, so the two contend instead of only this side locking.
            pipeline = session.scalar(
                sql.select(db_models.UserPipeline)
                .where(
                    db_models.UserPipeline.id == normalized_id,
                    db_models.UserPipeline.user_id == user_id,
                    db_models.UserPipeline.deleted_at.is_(None),
                )
                .with_for_update()
            )
            if pipeline is None:
                raise PipelineNotFoundError(
                    f"Pipeline with id {normalized_id!r} was not found."
                )
            if pipeline.current_version_key is None:
                raise PipelineNotFoundError(
                    f"Pipeline {pipeline.id!r} has no current version."
                )
            current = session.get(
                db_models.UserPipelineVersion,
                (pipeline.id, pipeline.current_version_key),
            )
            if current is None:
                raise PipelineNotFoundError(
                    f"Pipeline {pipeline.id!r} has no current version."
                )

            if pipeline.versioning_mode is versioning_mode:
                return PipelineWriteResult(
                    pipeline=pipeline,
                    version=current,
                    message=(
                        "Pipeline already uses the requested versioning mode; no changes were made."
                    ),
                    updated=False,
                    reused_version=True,
                )

            now = datetime.datetime.now(datetime.timezone.utc)
            version_table = cast(
                sql.Table,
                db_models.UserPipelineVersion.__table__,
            )
            if versioning_mode is db_models.PipelineVersioningMode.DISABLED:
                sentinel = session.get(
                    db_models.UserPipelineVersion,
                    (pipeline.id, db_models.CURRENT_VERSION_KEY),
                )
                reused_version = (
                    sentinel is not None
                    and sentinel.content_digest == current.content_digest
                )
                if sentinel is None:
                    insert_error = database_ops.insert_with_integrity_fallback(
                        session=session,
                        table=version_table,
                        values={
                            "pipeline_id": pipeline.id,
                            "version_key": db_models.CURRENT_VERSION_KEY,
                            "content_digest": current.content_digest,
                            "root_pipeline_task": copy.deepcopy(
                                current.root_pipeline_task
                            ),
                            "pipeline_run_annotations": copy.deepcopy(
                                current.pipeline_run_annotations
                            ),
                            "created_at": now,
                            "extra_data": copy.deepcopy(current.extra_data),
                        },
                    )
                    sentinel = session.get(
                        db_models.UserPipelineVersion,
                        (pipeline.id, db_models.CURRENT_VERSION_KEY),
                    )
                    if sentinel is None:
                        if insert_error is not None:
                            raise insert_error
                        raise RuntimeError(
                            f"Failed to create mutable head for pipeline {pipeline.id!r}."
                        )
                elif not reused_version:
                    sentinel.content_digest = current.content_digest
                    sentinel.root_pipeline_task = copy.deepcopy(
                        current.root_pipeline_task
                    )
                    sentinel.pipeline_run_annotations = copy.deepcopy(
                        current.pipeline_run_annotations
                    )
                    sentinel.created_at = now
                    sentinel.extra_data = copy.deepcopy(current.extra_data)

                pipeline.current_version_key = db_models.CURRENT_VERSION_KEY
                pipeline.versioning_mode = versioning_mode
                pipeline.updated_at = now
                return PipelineWriteResult(
                    pipeline=pipeline,
                    version=sentinel,
                    message="Pipeline versioning mode updated to disabled.",
                    updated=True,
                    reused_version=reused_version,
                )

            version = session.get(
                db_models.UserPipelineVersion,
                (pipeline.id, current.content_digest),
            )
            reused_version = version is not None
            if version is None:
                insert_error = database_ops.insert_with_integrity_fallback(
                    session=session,
                    table=version_table,
                    values={
                        "pipeline_id": pipeline.id,
                        "version_key": current.content_digest,
                        "content_digest": current.content_digest,
                        "root_pipeline_task": copy.deepcopy(current.root_pipeline_task),
                        "pipeline_run_annotations": copy.deepcopy(
                            current.pipeline_run_annotations
                        ),
                        "created_at": now,
                        "extra_data": copy.deepcopy(current.extra_data),
                    },
                )
                version = session.get(
                    db_models.UserPipelineVersion,
                    (pipeline.id, current.content_digest),
                )
                if version is None:
                    if insert_error is not None:
                        raise insert_error
                    raise RuntimeError(
                        f"Failed to create version {current.content_digest!r} for pipeline {pipeline.id!r}."
                    )

            pipeline.current_version_key = version.version_key
            pipeline.versioning_mode = versioning_mode
            pipeline.updated_at = now
            session.flush()
            sentinel = session.get(
                db_models.UserPipelineVersion,
                (pipeline.id, db_models.CURRENT_VERSION_KEY),
            )
            if sentinel is not None:
                session.delete(sentinel)

            return PipelineWriteResult(
                pipeline=pipeline,
                version=version,
                message="Pipeline versioning mode updated to full.",
                updated=True,
                reused_version=reused_version,
            )

    def _get_pipeline(
        self,
        *,
        session: orm.Session,
        pipeline_id: str | None,
        user_id: str | None,
        file_path: str | None,
        for_share: bool = False,
        validation_only: bool = False,
    ) -> db_models.UserPipeline:
        """Load one owned, non-deleted pipeline.

        `for_share` takes a SHARED row lock so a concurrent versioning-mode
        transition cannot land between this read and what the caller decides
        from it. The mode writer (`patch_pipeline_properties`) takes an
        EXCLUSIVE lock on the same row, and `FOR SHARE` blocks `FOR UPDATE`, so
        the two still serialize -- while two pins being resolved concurrently no
        longer block each other, which is all this path ever needed.

        On MySQL `read=True` compiles to `LOCK IN SHARE MODE`, not `FOR SHARE`;
        worth knowing before grepping a slow query log for the wrong string.

        Shared is sufficient precisely because this path never writes the row it
        locks: it checks pinnability and selects an immutable version row. A
        reader that later wanted to write would have to upgrade S to X, and two
        such readers would deadlock; that is the thing to check before adding a
        write here.

        SQLite ignores both (it has no row locks), so tests exercise the
        ordering rather than the lock. The lock is also only meaningful if a
        transaction's statements reach one backend connection; that is **not yet
        verified** against the live connection layer. See SCHEDULER_DESIGN.md
        ("Verification limits") rather than restating it here.

        `validation_only` narrows the SELECT to the columns a validation-only
        caller reads; see `get_pipeline_and_version`. The WHERE clause is
        untouched either way.

        Both `deleted_at` and `user_id` are enforced by their predicates alone.
        `IS NULL` has no collation; `user_id ==` is evaluated under the column's
        collation, which is the authoritative comparator for owner identity here.
        Neither is second-guessed in Python after the query.
        """

        def _locked(statement: "sql.Select[Any]") -> "sql.Select[Any]":
            return statement.with_for_update(read=True) if for_share else statement

        def _projected(statement: "sql.Select[Any]") -> "sql.Select[Any]":
            # Order matters only for readability: the lock and the projection are
            # independent, and the lock is applied by `_locked` in both branches
            # exactly as before.
            return (
                statement.options(*_VALIDATION_PIPELINE_LOAD)
                if validation_only
                else statement
            )

        if pipeline_id is not None:
            # Delegated rather than repeated: the same three filters are the public door other
            # domains check a pipeline reference through, and two copies of "alive and owned"
            # is exactly the pair that drifts. Both the lock and the projection travel with the
            # delegation rather than being reasons to keep a second copy of the query alive here.
            #
            # Ownership travels with the delegation as a single `user_id ==`
            # predicate the database evaluates. There is no second comparison on
            # either side of the door to keep in step.
            return get_live_owned_pipeline(
                session=session,
                pipeline_id=pipeline_id,
                user_id=user_id,
                for_share=for_share,
                load_options=_VALIDATION_PIPELINE_LOAD if validation_only else (),
            )
        if user_id is not None and file_path is not None:
            normalized_path = normalize_file_path(file_path)
            pipeline = session.scalar(
                _projected(
                    _locked(
                        sql.select(db_models.UserPipeline).where(
                            db_models.UserPipeline.user_id == user_id,
                            db_models.UserPipeline.file_path == normalized_path,
                            db_models.UserPipeline.deleted_at.is_(None),
                        )
                    )
                )
            )
            not_found_message = f"Pipeline with file path {normalized_path!r} was not found for user {user_id!r}."
        else:
            raise PipelineValidationError(
                "A pipeline lookup requires pipeline_id or user_id with file_path."
            )

        # No Python re-comparison of `user_id` on this branch either, for the same
        # reason as the id branch: the `user_id ==` predicate in the SELECT above
        # is the owner rule, and a second one here would compete with it. The
        # schedule writer's preflight and the executor's fire path -- the pair that
        # has to agree -- agree because they resolve through this one predicate,
        # not because two copies of a Python comparison happen to match.
        if pipeline is None:
            raise PipelineNotFoundError(not_found_message)
        return pipeline

    def get_pipeline_and_version(
        self,
        *,
        session: orm.Session,
        pipeline_id: str | None,
        user_id: str | None,
        file_path: str | None,
        version: str | None,
        require_pinnable: bool = False,
        validation_only: bool = False,
    ) -> tuple[
        db_models.UserPipeline,
        db_models.UserPipelineVersion,
        db_models.UserPipelineVersion,
    ]:
        """Resolve a pipeline and the version a caller would run.

        `validation_only` is a PROJECTION, not a different rule. Every query, every filter,
        the shared row lock, its position before `require_pinnable_versioning`,
        and every error stay exactly as they are; only the column list narrows,
        to what validation reads. It exists because the schedule writer's
        create-time preflight calls this to be told "yes, that reference resolves"
        and then discards the PAYLOAD -- and the rows carry `root_pipeline_task`,
        so a validation was paying for a body it never looked at
        (Binks 3906749295).

        Not quite everything is discarded: that caller reads `pipeline.id` off the
        returned row and stores it, which `load_only` guarantees is present because
        the primary key is always selected. Said explicitly here because this is
        the projection's own documentation, and a reader deciding what
        `validation_only` may safely drop should not have to discover that
        dependency in another package.

        Callers that SUBMIT the version -- the executor's fire path above all --
        must leave it False, which is why it defaults that way: the payload is
        the whole point for them. The projected columns are documented on
        `_VALIDATION_PIPELINE_LOAD` / `_VALIDATION_VERSION_LOAD`, and reading anything else
        off a projected row raises rather than quietly emitting another SELECT.
        """
        pinned = require_pinnable and version is not None
        pipeline = self._get_pipeline(
            session=session,
            pipeline_id=pipeline_id,
            user_id=user_id,
            file_path=file_path,
            # A pin is checked under a shared row lock so the mode cannot change
            # between the check and the version selection below. Shared, not
            # exclusive: this path only reads, and concurrent pin resolutions
            # should not serialize against each other.
            for_share=pinned,
            validation_only=validation_only,
        )
        if pinned:
            # Inside the locked read, and before the version row is selected.
            # A caller-side preflight is not sufficient: it would be a separate
            # read, so the mode could change between it and the selection. What
            # has to hold is that the content submitted was selected while the
            # pipeline was pinnable -- and a selected version row is immutable, so
            # a later mode change cannot alter what runs.
            #
            # The lock is held until the caller ends this transaction. Callers
            # that can prove they have nothing pending should end it as soon as
            # the selected content has been copied out; see
            # `create_from_pipeline_no_commit(end_lookup_transaction=...)`.
            require_pinnable_versioning(pipeline)
        if pipeline.current_version_key is None:
            raise PipelineNotFoundError(
                f"Pipeline {pipeline.id!r} has no current version."
            )
        current_row = session.get(
            db_models.UserPipelineVersion,
            (pipeline.id, pipeline.current_version_key),
            options=_VALIDATION_VERSION_LOAD if validation_only else None,
        )
        if current_row is None:
            raise PipelineNotFoundError(
                f"Pipeline {pipeline.id!r} has no current version."
            )

        if version is None or version == current_row.content_digest:
            version_row = current_row
        else:
            version_statement = sql.select(db_models.UserPipelineVersion).where(
                db_models.UserPipelineVersion.pipeline_id == pipeline.id,
                db_models.UserPipelineVersion.version_key == version,
                db_models.UserPipelineVersion.version_key
                != db_models.CURRENT_VERSION_KEY,
            )
            if validation_only:
                version_statement = version_statement.options(*_VALIDATION_VERSION_LOAD)
            version_row = session.scalar(version_statement)

        if version_row is None:
            raise PipelineNotFoundError(
                f"Version {version!r} was not found for pipeline {pipeline.id!r}."
            )
        return pipeline, version_row, current_row

    def list_pipelines(
        self,
        *,
        session: orm.Session,
        user_id: str,
        page_size: int,
        cursor: tuple[datetime.datetime, str] | None,
        file_path_prefix: str | None,
    ) -> tuple[
        list[
            tuple[
                db_models.UserPipeline,
                str,
                dict[str, Any] | None,
            ]
        ],
        int,
        bool,
    ]:
        filters = [
            db_models.UserPipeline.user_id == user_id,
            db_models.UserPipeline.deleted_at.is_(None),
        ]
        if file_path_prefix is not None:
            filters.append(
                db_models.UserPipeline.file_path.startswith(
                    file_path_prefix,
                    autoescape=True,
                )
            )

        total_count = session.scalar(
            sql.select(sql.func.count(db_models.UserPipeline.id)).where(*filters)
        )

        query = (
            sql.select(
                db_models.UserPipeline,
                db_models.UserPipelineVersion.content_digest,
                db_models.UserPipelineVersion.extra_data,
            )
            .join(
                db_models.UserPipelineVersion,
                sql.and_(
                    db_models.UserPipelineVersion.pipeline_id
                    == db_models.UserPipeline.id,
                    db_models.UserPipelineVersion.version_key
                    == db_models.UserPipeline.current_version_key,
                ),
            )
            .where(*filters)
        )
        if cursor is not None:
            cursor_updated_at, cursor_id = cursor
            query = query.where(
                sql.tuple_(
                    db_models.UserPipeline.updated_at,
                    db_models.UserPipeline.id,
                )
                < sql.tuple_(
                    sql.literal(cursor_updated_at),
                    sql.literal(cursor_id),
                )
            )

        rows = list(
            session.execute(
                query.order_by(
                    db_models.UserPipeline.updated_at.desc(),
                    db_models.UserPipeline.id.desc(),
                ).limit(page_size + 1)
            ).tuples()
        )
        has_more = len(rows) > page_size
        return rows[:page_size], total_count or 0, has_more

    def list_versions(
        self,
        *,
        session: orm.Session,
        pipeline_id: str | None,
        user_id: str | None,
        file_path: str | None,
        page_size: int,
        cursor: tuple[datetime.datetime, str] | None,
    ) -> tuple[
        db_models.UserPipeline,
        str,
        list[tuple[str, datetime.datetime]],
        int,
        bool,
    ]:
        pipeline = self._get_pipeline(
            session=session,
            pipeline_id=pipeline_id,
            user_id=user_id,
            file_path=file_path,
        )
        if pipeline.current_version_key is None:
            raise PipelineNotFoundError(
                f"Pipeline {pipeline.id!r} has no current version."
            )
        current_row = session.get(
            db_models.UserPipelineVersion,
            (pipeline.id, pipeline.current_version_key),
        )
        if current_row is None:
            raise PipelineNotFoundError(
                f"Pipeline {pipeline.id!r} has no current version."
            )
        immutable_filter = (
            db_models.UserPipelineVersion.version_key != db_models.CURRENT_VERSION_KEY
        )
        total_count = session.scalar(
            sql.select(sql.func.count(db_models.UserPipelineVersion.version_key)).where(
                db_models.UserPipelineVersion.pipeline_id == pipeline.id,
                immutable_filter,
            )
        )
        query = sql.select(
            db_models.UserPipelineVersion.content_digest,
            db_models.UserPipelineVersion.created_at,
        ).where(
            db_models.UserPipelineVersion.pipeline_id == pipeline.id,
            immutable_filter,
        )
        if cursor is not None:
            cursor_created_at, cursor_digest = cursor
            query = query.where(
                sql.tuple_(
                    db_models.UserPipelineVersion.created_at,
                    db_models.UserPipelineVersion.version_key,
                )
                < sql.tuple_(
                    sql.literal(cursor_created_at),
                    sql.literal(cursor_digest),
                )
            )
        versions = list(
            session.execute(
                query.order_by(
                    db_models.UserPipelineVersion.created_at.desc(),
                    db_models.UserPipelineVersion.version_key.desc(),
                ).limit(page_size + 1)
            ).tuples()
        )
        has_more = len(versions) > page_size
        return (
            pipeline,
            current_row.content_digest,
            versions[:page_size],
            total_count or 0,
            has_more,
        )

    def delete_pipeline(
        self,
        *,
        session: orm.Session,
        user_id: str,
        file_path: str,
    ) -> None:
        file_path = normalize_file_path(file_path)
        with session.begin():
            pipeline = session.scalar(
                sql.select(db_models.UserPipeline)
                .where(
                    db_models.UserPipeline.user_id == user_id,
                    db_models.UserPipeline.file_path == file_path,
                )
                .with_for_update()
            )
            if pipeline is None:
                raise PipelineNotFoundError(
                    f"Pipeline with file path {file_path!r} was not found for user {user_id!r}."
                )
            if pipeline.deleted_at is None:
                now = datetime.datetime.now(datetime.timezone.utc)
                pipeline.deleted_at = now
                pipeline.updated_at = now

    def create_from_pipeline_no_commit(
        self,
        *,
        session: orm.Session,
        pipeline_id: str | None,
        user_id: str | None,
        file_path: str | None,
        version: str | None,
        run_arguments: dict[str, component_structures.ArgumentType] | None,
        pipeline_run_annotations: dict[str, str] | None,
        created_by: str,
        require_pinnable_versioning: bool = False,
        end_lookup_transaction: bool = False,
    ) -> bts.PipelineRun:
        """Builds a pipeline run from a saved pipeline without committing.

        Flushes, so the returned run has its ID, but leaves the transaction open so
        the caller can write the run atomically with its own rows. Callers that only
        want a run created should use `create_from_pipeline`, which commits.

        `require_pinnable_versioning` refuses an explicit `version` unless the
        pipeline is in FULL mode *at the moment the version is selected*, under a
        shared row lock. It is a parameter rather than a separate call because the
        check and the selection have to happen in one read: a caller-side preflight
        can be stale by the time the version is chosen.

        `end_lookup_transaction` rolls this session back once the selected content
        has been copied out, which releases that lock before the run is built.
        **It discards anything the caller had pending on this session**, so it is
        off by default and is only correct for a caller that owns the session and
        knows it is clean -- the scheduler executor's dedicated `run_session` is
        the one such caller. It is not a micro-optimisation: without it the shared
        lock lives until the caller commits, which for a large graph spans
        annotation validation, `TaskSpec` parsing, recursive execution and
        artifact insertion, and the flush. A shared lock blocks every exclusive
        one, so that window blocks `set_pipeline` and `delete_pipeline` on the
        same row, not merely a versioning-mode change.

        Releasing it early is safe for the same reason the check can precede
        submission at all: a FULL-mode pin resolves a digest-keyed row that no
        supported path mutates in place.

        A run reaches a project only through a `project_run_key` in
        `pipeline_run_annotations`; there is no parameter for it. That keeps this route
        interchangeable with `POST /api/pipeline_runs/`, which passes annotations
        straight to the run service. What diverges is what this route rewrites and
        refuses: the membership value is normalized to the marker, a key with no id
        after the prefix is refused, and so is one whose prefix differs from the
        canonical spelling only in case. The id itself is not checked -- any non-empty
        suffix is taken as given. The generic route stores what it was sent; the seam
        to fix that lives in the core run service (see
        `_reject_more_than_one_project`).

        The key is *not* inherited from the pipeline's stored annotations, which the
        pipeline write path refuses outright; the pop below guards rows written
        before that rule. Nor is it checked against the `project` table -- a run
        annotated with a project that does not exist is simply a run the project feed
        never returns.
        """
        # Canonicalized before validation, so two spellings of one project read as one
        # membership rather than tripping the one-project guard.
        request_annotations = (
            pipeline_annotation_validation.canonicalized_project_run_annotations(
                pipeline_run_annotations or {}
            )
        )
        pipeline_annotation_validation.validate_pipeline_run_annotations(
            request_annotations,
        )

        pipeline, version_row, _current_row = self.get_pipeline_and_version(
            session=session,
            pipeline_id=pipeline_id,
            user_id=user_id,
            file_path=file_path,
            version=version,
            require_pinnable=require_pinnable_versioning,
        )

        # Copied out before the transaction can be ended under us.
        #
        # This function used to roll back unconditionally here, to end the implicit
        # lookup transaction before delegating -- necessary when it delegated to
        # `PipelineRunsApiService_Sql.create`, which opens its own `session.begin()`
        # and raises if one is already active. The run is now built with
        # `_create_in_transaction`, inside the caller's transaction, so that reason
        # is gone and an unconditional rollback would be actively wrong: it would
        # discard whatever the caller had already written -- for the trigger path,
        # the fence row that claims the cycle.
        #
        # It survives only as `end_lookup_transaction`, opt-in, for a caller that
        # owns a clean session and wants the pinned row lock released before the
        # run is inserted rather than at commit.
        selected_pipeline_id = pipeline.id
        selected_owner = pipeline.user_id
        selected_file_path = pipeline.file_path
        selected_digest = version_row.content_digest
        stored_task_json = copy.deepcopy(version_row.root_pipeline_task)
        stored_annotations: dict[str, str] = copy.deepcopy(
            version_row.pipeline_run_annotations or {}
        )
        # Dropped rather than merged: a project saved onto the pipeline by an older
        # deploy would otherwise be inherited by every run from it, with no way to
        # submit outside that project. By prefix, so a stored map carrying several
        # loses all of them.
        #
        # Near misses included, unlike everywhere else. A row written before the
        # write path refused them still carries a key MySQL files runs under, and
        # the validation below would now reject it -- stranding a saved pipeline
        # its owner cannot submit. Dropping it here clears both at once.
        stored_project_keys = pipeline_annotation_validation.project_run_keys(
            stored_annotations, include_inexact=True
        )
        for stored_project_key in stored_project_keys:
            stored_annotations.pop(stored_project_key, None)

        if end_lookup_transaction:
            # Nothing below may touch `pipeline` or `version_row`: this expires
            # both, and re-reading an attribute would issue a fresh query with no
            # lock behind it. That is why every value needed later is copied above.
            session.rollback()

        # Normal API writes validate before persistence. Keep this shared guard for
        # legacy or directly inserted rows that bypassed the CRUD boundary.
        pipeline_annotation_validation.validate_pipeline_run_annotations(
            stored_annotations,
            allow_server_owned_provenance=True,
        )

        hook_context = (
            self._hooks.prepare_saved_run(stored_task_json)
            if self._hooks is not None
            else None
        )

        root_task = component_structures.TaskSpec.from_json_dict(stored_task_json)
        effective_arguments = {
            **dict(root_task.arguments or {}),
            **copy.deepcopy(run_arguments or {}),
        }
        root_task = dataclasses.replace(root_task, arguments=effective_arguments)
        # Normal API writes validate structure before persistence. Keep this shared
        # guard for legacy or directly inserted rows that bypassed the CRUD
        # boundary: without it a stale task-output reference reaches the backend's
        # unguarded `task_output_artifact_nodes[...][...]` lookups and surfaces as a
        # bare KeyError / catch-all 500 instead of a 422.
        pipeline_structure_validation.validate_task_output_references(root_task)
        effective_annotations = {
            **stored_annotations,
            **copy.deepcopy(request_annotations),
            pipeline_annotation_validation.SOURCE_ANNOTATION: "true",
            pipeline_annotation_validation.PIPELINE_ID_ANNOTATION: selected_pipeline_id,
            pipeline_annotation_validation.VERSION_ANNOTATION: selected_digest,
            pipeline_annotation_validation.OWNER_ANNOTATION: selected_owner,
            pipeline_annotation_validation.FILE_PATH_ANNOTATION: selected_file_path,
        }

        # No project stamping here: the key arrives in `request_annotations` and is
        # already in the merge above, so a caller that names no project produces a
        # run with no key at all.
        pipeline_run = (
            api_server_sql.PipelineRunsApiService_Sql()._create_in_transaction(
                session=session,
                root_task=root_task,
                annotations=effective_annotations,
                created_by=created_by,
            )
        )
        if self._hooks is not None:
            self._hooks.run_created(pipeline_run, hook_context)
        return pipeline_run

    def create_from_pipeline(
        self,
        *,
        session: orm.Session,
        pipeline_id: str | None,
        user_id: str | None,
        file_path: str | None,
        version: str | None,
        run_arguments: dict[str, component_structures.ArgumentType] | None,
        pipeline_run_annotations: dict[str, str] | None,
        created_by: str,
        require_pinnable_versioning: bool = False,
        end_lookup_transaction: bool = False,
    ) -> api_server_sql.PipelineRunResponse:
        """Builds a pipeline run from a saved pipeline and commits it.

        The self-contained entry point: the run is durable when this returns. Callers
        that need the run written atomically with rows of their own should use
        `create_from_pipeline_no_commit` and own the commit themselves.

        `require_pinnable_versioning` and `end_lookup_transaction` are forwarded
        unchanged; see the no-commit entry point for what they do and, for the
        second, when it is safe.
        """
        pipeline_run = self.create_from_pipeline_no_commit(
            session=session,
            pipeline_id=pipeline_id,
            user_id=user_id,
            file_path=file_path,
            version=version,
            run_arguments=run_arguments,
            pipeline_run_annotations=pipeline_run_annotations,
            created_by=created_by,
            require_pinnable_versioning=require_pinnable_versioning,
            end_lookup_transaction=end_lookup_transaction,
        )
        session.commit()
        session.refresh(pipeline_run)
        return api_server_sql.PipelineRunResponse.from_db(pipeline_run)
