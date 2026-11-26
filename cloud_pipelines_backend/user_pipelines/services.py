import copy
import dataclasses
import datetime
import hashlib
import json
import uuid
from collections import abc
from typing import Any, Final

import sqlalchemy as sql
from sqlalchemy import orm

from .. import api_server_sql, component_structures
from .. import backend_types_sql as bts
from . import db_models, errors
from . import pipeline_run_annotations as run_annotations

MAX_PIPELINE_NAME_LENGTH: Final[int] = 1024
_LAYOUT_ANNOTATION: Final[str] = "editor.position"
_VALIDATION_PIPELINE_LOAD: Final = orm.load_only(
    db_models.UserPipeline.user_id,
    db_models.UserPipeline.current_version_key,
    db_models.UserPipeline.versioning_mode,
    raiseload=True,
)
_VALIDATION_VERSION_LOAD: Final = orm.load_only(
    db_models.UserPipelineVersion.content_digest, raiseload=True
)


@dataclasses.dataclass(kw_only=True)
class PipelineContent:
    root_pipeline_task: dict[str, Any]
    pipeline_run_annotations: dict[str, str]
    pipeline_name: str | None
    digest: str


@dataclasses.dataclass(kw_only=True)
class PipelineWriteResult:
    pipeline: db_models.UserPipeline
    version: db_models.UserPipelineVersion
    message: str
    updated: bool
    reused_version: bool


def normalize_file_path(file_path: str) -> str:
    """Strips a file path and checks its length."""
    normalized = file_path.strip()
    if not (0 < len(normalized) <= db_models.MAX_FILE_PATH_LENGTH):
        raise errors.PipelineValidationError(
            f"Pipeline file path must be between 1 and {db_models.MAX_FILE_PATH_LENGTH} characters."
        )
    return normalized


def normalize_pipeline_id(pipeline_id: str) -> str:
    """Returns the canonical form of a pipeline UUID."""
    try:
        return str(uuid.UUID(pipeline_id))
    except ValueError as exc:
        raise errors.PipelineValidationError(
            f"pipeline_id must be a valid UUID; received {pipeline_id!r}."
        ) from exc


def live_pipeline_ids(
    *, session: orm.Session, pipeline_ids: abc.Iterable[str]
) -> set[str]:
    """Which of these pipeline ids exist and are not deleted; invalid ids are skipped."""
    normalized: dict[str, str] = {}
    for pipeline_id in pipeline_ids:
        try:
            normalized[normalize_pipeline_id(pipeline_id)] = pipeline_id
        except errors.PipelineValidationError:
            continue
    if not normalized:
        return set()
    found = session.scalars(
        sql.select(db_models.UserPipeline.id).where(
            db_models.UserPipeline.id.in_(normalized),
            db_models.UserPipeline.deleted_at.is_(None),
        )
    )
    return {normalized[found_id] for found_id in found}


def get_live_owned_pipeline(
    *,
    session: orm.Session,
    pipeline_id: str,
    user_id: str | None,
    for_share: bool = False,
    validation_only: bool = False,
) -> db_models.UserPipeline:
    """The live pipeline with this id, owned by `user_id` unless it is None."""
    normalized_id = normalize_pipeline_id(pipeline_id)
    statement = sql.select(db_models.UserPipeline).where(
        db_models.UserPipeline.id == normalized_id,
        db_models.UserPipeline.deleted_at.is_(None),
    )
    if user_id is not None:
        statement = statement.where(db_models.UserPipeline.user_id == user_id)
    pipeline = session.scalar(
        _lookup(statement, for_share=for_share, validation_only=validation_only)
    )
    if pipeline is None:
        raise errors.PipelineNotFoundError(
            f"Pipeline with id {normalized_id!r} was not found."
        )
    return pipeline


def require_pinnable_versioning(pipeline: db_models.UserPipeline) -> None:
    """Refuses a version pin unless the pipeline keeps full version history."""
    if pipeline.versioning_mode is not db_models.PipelineVersioningMode.FULL:
        raise errors.PipelineValidationError(
            f"Pipeline {pipeline.id!r} does not keep version history, so a version cannot be pinned."
        )


def resolve_pinnable_version_key(
    *,
    session: orm.Session,
    pipeline: db_models.UserPipeline,
    content_digest: str,
) -> str:
    """The stored version key for a digest that this pipeline can pin."""
    require_pinnable_versioning(pipeline)
    version = session.get(db_models.UserPipelineVersion, (pipeline.id, content_digest))
    if version is None:
        raise errors.PipelineValidationError(
            f"No version {content_digest!r} for pipeline {pipeline.id!r}."
        )
    return version.version_key


def calculate_pipeline_digest(
    *,
    root_pipeline_task: dict[str, Any],
    pipeline_run_annotations: dict[str, str],
) -> str:
    """SHA-256 of the canonical JSON of a pipeline's content."""
    canonical_json = json.dumps(
        {
            "pipeline_run_annotations": pipeline_run_annotations,
            "root_pipeline_task": root_pipeline_task,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def prepare_pipeline_content(
    *,
    root_pipeline_task: dict[str, Any],
    pipeline_run_annotations: dict[str, str] | None,
) -> PipelineContent:
    """Validates a pipeline and returns its canonical content and digest."""
    try:
        task_spec = component_structures.TaskSpec.from_json_dict(root_pipeline_task)
    except Exception as exc:
        raise errors.PipelineValidationError(
            f"root_pipeline_task is not a valid TaskSpec: {exc}"
        ) from exc
    component_spec = task_spec.component_ref.spec
    pipeline_name = component_spec.name if component_spec else None
    if pipeline_name is not None and len(pipeline_name) > MAX_PIPELINE_NAME_LENGTH:
        raise errors.PipelineValidationError(
            f"Pipeline name cannot exceed {MAX_PIPELINE_NAME_LENGTH} characters."
        )
    annotations = pipeline_run_annotations or {}
    run_annotations.validate_pipeline_run_annotations(annotations)
    canonical_task = task_spec.to_json_dict()
    return PipelineContent(
        root_pipeline_task=canonical_task,
        pipeline_run_annotations=annotations,
        pipeline_name=pipeline_name,
        digest=calculate_pipeline_digest(
            root_pipeline_task=canonical_task,
            pipeline_run_annotations=annotations,
        ),
    )


def materialize_root_input_defaults(
    root_task: component_structures.TaskSpec,
) -> component_structures.TaskSpec:
    """Fills each root-input default that the task's arguments omit."""
    spec = root_task.component_ref.spec
    defaults = {
        input_spec.name: input_spec.default
        for input_spec in (spec.inputs if spec else None) or []
        if input_spec.default is not None
    }
    if not defaults:
        return root_task
    return dataclasses.replace(
        root_task, arguments={**defaults, **(root_task.arguments or {})}
    )


class UserPipelineService:
    def set_pipeline(
        self,
        *,
        session: orm.Session,
        user_id: str,
        file_path: str,
        root_pipeline_task: dict[str, Any],
        pipeline_run_annotations: dict[str, str] | None,
        versioning_mode: db_models.PipelineVersioningMode | None = None,
        may_change_secrets: bool = True,
    ) -> PipelineWriteResult:
        """Creates, updates or revives the pipeline at `user_id`/`file_path`."""
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
                pipeline = db_models.UserPipeline(user_id=user_id, file_path=file_path)
                session.add(pipeline)
                session.flush()
            previous = (
                session.get(
                    db_models.UserPipelineVersion,
                    (pipeline.id, pipeline.current_version_key),
                )
                if pipeline.deleted_at is None and pipeline.current_version_key
                else None
            )
            if not may_change_secrets and _secret_tasks(
                previous.root_pipeline_task if previous else None
            ) != _secret_tasks(content.root_pipeline_task):
                raise errors.SecretChangeNotAllowedError(
                    f"Only {user_id} can change secrets or the tasks that use them."
                )
            mode = versioning_mode or pipeline.versioning_mode
            updated = (
                previous is None
                or previous.content_digest != content.digest
                or pipeline.versioning_mode is not mode
            )
            version, reused_version = _point_at(
                session=session, pipeline=pipeline, content=content, mode=mode
            )
            pipeline.deleted_at = None
            if updated:
                pipeline.updated_at = db_models.utc_now()
            return PipelineWriteResult(
                pipeline=pipeline,
                version=version,
                message="Pipeline saved." if updated else "Pipeline is unchanged.",
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
        """Switches an owned pipeline's versioning mode, keeping its current content."""
        with session.begin():
            pipeline = get_live_owned_pipeline(
                session=session, pipeline_id=pipeline_id, user_id=user_id
            )
            current = _current_version(session=session, pipeline=pipeline)
            updated = pipeline.versioning_mode is not versioning_mode
            version, reused_version = current, True
            if updated:
                version, reused_version = _point_at(
                    session=session,
                    pipeline=pipeline,
                    content=PipelineContent(
                        root_pipeline_task=copy.deepcopy(current.root_pipeline_task),
                        pipeline_run_annotations=copy.deepcopy(
                            current.pipeline_run_annotations
                        ),
                        pipeline_name=(current.extra_data or {}).get("pipeline_name"),
                        digest=current.content_digest,
                    ),
                    mode=versioning_mode,
                )
                pipeline.updated_at = db_models.utc_now()
            return PipelineWriteResult(
                pipeline=pipeline,
                version=version,
                message=(
                    f"Pipeline versioning mode set to {versioning_mode.value}."
                    if updated
                    else "Pipeline is unchanged."
                ),
                updated=updated,
                reused_version=reused_version,
            )

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
        """Returns the pipeline, the requested version (default current), and the current version.

        A pinnable pin takes a shared row lock; `validation_only` loads only the columns validation reads.
        """
        for_share = require_pinnable and version is not None
        version_options = [_VALIDATION_VERSION_LOAD] if validation_only else None
        if pipeline_id is not None:
            pipeline = get_live_owned_pipeline(
                session=session,
                pipeline_id=pipeline_id,
                user_id=user_id,
                for_share=for_share,
                validation_only=validation_only,
            )
        elif user_id is not None and file_path is not None:
            pipeline = session.scalar(
                _lookup(
                    sql.select(db_models.UserPipeline).where(
                        db_models.UserPipeline.user_id == user_id,
                        db_models.UserPipeline.file_path
                        == normalize_file_path(file_path),
                        db_models.UserPipeline.deleted_at.is_(None),
                    ),
                    for_share=for_share,
                    validation_only=validation_only,
                )
            )
            if pipeline is None:
                raise errors.PipelineNotFoundError(
                    f"Pipeline {file_path!r} was not found for user {user_id!r}."
                )
        else:
            raise errors.PipelineValidationError(
                "A pipeline lookup requires pipeline_id or user_id with file_path."
            )
        if for_share:
            require_pinnable_versioning(pipeline)
        current = _current_version(
            session=session, pipeline=pipeline, options=version_options
        )
        if version is None or version == current.content_digest:
            return pipeline, current, current
        selected = (
            None
            if version == db_models.CURRENT_VERSION_KEY
            else session.get(
                db_models.UserPipelineVersion,
                (pipeline.id, version),
                options=version_options,
            )
        )
        if selected is None:
            raise errors.PipelineNotFoundError(
                f"Version {version!r} was not found for pipeline {pipeline.id!r}."
            )
        return pipeline, selected, current

    def list_pipelines(
        self,
        *,
        session: orm.Session,
        user_id: str,
        page_size: int,
        cursor: tuple[datetime.datetime, str] | None,
        file_path_prefix: str | None,
    ) -> tuple[
        list[tuple[db_models.UserPipeline, str, dict[str, Any] | None]], int, bool
    ]:
        """Lists a user's live pipelines, newest update first, with the total count."""
        pipeline = db_models.UserPipeline
        version = db_models.UserPipelineVersion
        filters = [pipeline.user_id == user_id, pipeline.deleted_at.is_(None)]
        if file_path_prefix is not None:
            filters.append(
                pipeline.file_path.startswith(file_path_prefix, autoescape=True)
            )
        total_count = session.scalar(
            sql.select(sql.func.count(pipeline.id)).where(*filters)
        )
        query = (
            sql.select(pipeline, version.content_digest, version.extra_data)
            .join(
                version,
                sql.and_(
                    version.pipeline_id == pipeline.id,
                    version.version_key == pipeline.current_version_key,
                ),
            )
            .where(*filters)
        )
        if cursor is not None:
            query = query.where(
                sql.tuple_(pipeline.updated_at, pipeline.id)
                < sql.tuple_(sql.literal(cursor[0]), sql.literal(cursor[1]))
            )
        rows = list(
            session.execute(
                query.order_by(pipeline.updated_at.desc(), pipeline.id.desc()).limit(
                    page_size + 1
                )
            ).tuples()
        )
        return rows[:page_size], total_count or 0, len(rows) > page_size

    def delete_pipeline(
        self,
        *,
        session: orm.Session,
        user_id: str,
        file_path: str,
    ) -> None:
        """Soft-deletes the pipeline at `user_id`/`file_path`."""
        file_path = normalize_file_path(file_path)
        with session.begin():
            pipeline = session.scalar(
                sql.select(db_models.UserPipeline)
                .where(
                    db_models.UserPipeline.user_id == user_id,
                    db_models.UserPipeline.file_path == file_path,
                    db_models.UserPipeline.deleted_at.is_(None),
                )
                .with_for_update()
            )
            if pipeline is None:
                raise errors.PipelineNotFoundError(
                    f"Pipeline {file_path!r} was not found for user {user_id!r}."
                )
            pipeline.deleted_at = pipeline.updated_at = db_models.utc_now()

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
        """Builds and flushes a run of a saved pipeline, leaving the commit to the caller.

        `end_lookup_transaction` rolls the session back after the lookup to release its lock early.
        """
        request_annotations = pipeline_run_annotations or {}
        run_annotations.validate_pipeline_run_annotations(request_annotations)
        pipeline, selected, _ = self.get_pipeline_and_version(
            session=session,
            pipeline_id=pipeline_id,
            user_id=user_id,
            file_path=file_path,
            version=version,
            require_pinnable=require_pinnable_versioning,
        )
        root_task = component_structures.TaskSpec.from_json_dict(
            copy.deepcopy(selected.root_pipeline_task)
        )
        root_task = dataclasses.replace(
            root_task,
            arguments={**(root_task.arguments or {}), **(run_arguments or {})},
        )
        annotations = {
            **copy.deepcopy(selected.pipeline_run_annotations or {}),
            **request_annotations,
            run_annotations.SOURCE_ANNOTATION: "true",
            run_annotations.PIPELINE_ID_ANNOTATION: pipeline.id,
            run_annotations.VERSION_ANNOTATION: selected.content_digest,
            run_annotations.OWNER_ANNOTATION: pipeline.user_id,
            run_annotations.FILE_PATH_ANNOTATION: pipeline.file_path,
        }
        if end_lookup_transaction:
            session.rollback()
        return api_server_sql.PipelineRunsApiService_Sql()._create_in_transaction(
            session=session,
            root_task=materialize_root_input_defaults(root_task),
            annotations=annotations,
            created_by=created_by,
        )

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
        """Creates and commits a run of a saved pipeline."""
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


def _current_version(
    *,
    session: orm.Session,
    pipeline: db_models.UserPipeline,
    options: list[Any] | None = None,
) -> db_models.UserPipelineVersion:
    """The version a pipeline currently points at."""
    current = (
        session.get(
            db_models.UserPipelineVersion,
            (pipeline.id, pipeline.current_version_key),
            options=options,
        )
        if pipeline.current_version_key
        else None
    )
    if current is None:
        raise errors.PipelineNotFoundError(
            f"Pipeline {pipeline.id!r} has no current version."
        )
    return current


def _point_at(
    *,
    session: orm.Session,
    pipeline: db_models.UserPipeline,
    content: PipelineContent,
    mode: db_models.PipelineVersioningMode,
) -> tuple[db_models.UserPipelineVersion, bool]:
    """Stores `content` as the pipeline's current version under `mode`; returns it and whether it already existed."""
    full = mode is db_models.PipelineVersioningMode.FULL
    key = content.digest if full else db_models.CURRENT_VERSION_KEY
    version = session.get(db_models.UserPipelineVersion, (pipeline.id, key))
    reused = version is not None and version.content_digest == content.digest
    if version is None:
        version = db_models.UserPipelineVersion(
            pipeline_id=pipeline.id,
            version_key=key,
            content_digest=content.digest,
            root_pipeline_task=content.root_pipeline_task,
        )
        session.add(version)
    if not reused:
        version.content_digest = content.digest
        version.root_pipeline_task = content.root_pipeline_task
        version.pipeline_run_annotations = content.pipeline_run_annotations
        version.extra_data = (
            {"pipeline_name": content.pipeline_name} if content.pipeline_name else None
        )
        version.created_at = db_models.utc_now()
    session.flush()
    pipeline.current_version_key = key
    pipeline.versioning_mode = mode
    session.flush()
    if full:
        session.execute(
            sql.delete(db_models.UserPipelineVersion).where(
                db_models.UserPipelineVersion.pipeline_id == pipeline.id,
                db_models.UserPipelineVersion.version_key
                == db_models.CURRENT_VERSION_KEY,
            )
        )
    return version, reused


def _is_secret(value: Any) -> bool:
    """Whether a value is a secret reference argument."""
    return (
        isinstance(value, dict)
        and isinstance(value.get("dynamicData"), dict)
        and "secret" in value["dynamicData"]
    )


def _nested(value: Any) -> abc.Iterator[Any]:
    """Yields a value and everything nested in it."""
    yield value
    children = value.values() if isinstance(value, dict) else value
    if isinstance(value, (dict, list)):
        for child in children:
            yield from _nested(child)


def _without_layout(value: Any) -> Any:
    """A copy of a task with canvas positions removed."""
    if isinstance(value, list):
        return [_without_layout(item) for item in value]
    if not isinstance(value, dict):
        return value
    return {
        key: (
            {k: v for k, v in item.items() if k != _LAYOUT_ANNOTATION}
            if key == "annotations" and isinstance(item, dict)
            else _without_layout(item)
        )
        for key, item in value.items()
    }


def _secret_tasks(root_pipeline_task: dict[str, Any] | None) -> dict[str, Any]:
    """Root secret arguments and every task that reads a secret, keyed by task path."""
    if not root_pipeline_task:
        return {}
    root_arguments = root_pipeline_task.get("arguments") or {}
    secret_inputs = {
        name for name, value in root_arguments.items() if _is_secret(value)
    }
    found: dict[str, Any] = {}
    if secret_inputs:
        found[""] = {name: root_arguments[name] for name in secret_inputs}
    pending = [("", root_pipeline_task, secret_inputs)]
    while pending:
        prefix, parent, parent_secret_inputs = pending.pop()
        spec = (parent.get("componentRef") or {}).get("spec") or {}
        tasks = ((spec.get("implementation") or {}).get("graph") or {}).get("tasks")
        for task_id, task in (tasks or {}).items():
            path = f"{prefix}/{task_id}"
            reads = list(_nested([task.get("arguments"), task.get("isEnabled")]))
            if any(
                _is_secret(value)
                or (
                    isinstance(value, dict)
                    and isinstance(value.get("graphInput"), dict)
                    and value["graphInput"].get("inputName") in parent_secret_inputs
                )
                for value in reads
            ):
                found[path] = _without_layout(task)
            else:
                pending.append((path, task, set()))
    return found


def _lookup(
    statement: "sql.Select[Any]", *, for_share: bool, validation_only: bool
) -> "sql.Select[Any]":
    """Applies the optional shared row lock and validation column projection."""
    if for_share:
        statement = statement.with_for_update(read=True)
    if validation_only:
        statement = statement.options(_VALIDATION_PIPELINE_LOAD)
    return statement
