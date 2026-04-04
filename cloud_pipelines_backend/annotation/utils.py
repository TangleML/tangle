import logging

from sqlalchemy import orm

from .. import backend_types_sql as bts
from .. import errors
from ..search import filter_query_sql

_logger = logging.getLogger(__name__)

_SYSTEM_KEY_RESERVED_MSG = (
    "Annotation keys starting with "
    f"{filter_query_sql.SYSTEM_KEY_PREFIX!r} are reserved for system use."
)


def fail_if_changing_system_annotation(*, key: str) -> None:
    if key.startswith(filter_query_sql.SYSTEM_KEY_PREFIX):
        raise errors.ApiValidationError(_SYSTEM_KEY_RESERVED_MSG)


def _truncate_for_annotation(
    *,
    value: str,
    field_name: str,
    pipeline_run_id: bts.IdType,
) -> str:
    """Truncate a string to fit the annotation VARCHAR column.

    Returns the value unchanged if it fits within _STR_MAX_LENGTH,
    otherwise truncates and logs a warning with the run ID and
    original length.
    """
    max_len = bts._STR_MAX_LENGTH
    if len(value) <= max_len:
        return value

    _logger.warning(
        f"Truncating {field_name} annotation for run {pipeline_run_id}: "
        f"{len(value)} chars -> {max_len} chars"
    )
    return value[:max_len]


def mirror_system_annotations(
    *,
    session: orm.Session,
    pipeline_run_id: bts.IdType,
    created_by: str | None,
    pipeline_name: str | None,
) -> None:
    """Mirror pipeline run fields as system annotations for filter_query search.

    Always creates an annotation for every run, even when the source value is
    None or empty (stored as ""). This ensures data parity so every run has a
    row for each system key.
    """

    # TODO: The original pipeline_run.created_by and the pipeline name stored in
    # extra_data / task_spec are saved untruncated, while the annotation mirror
    # is truncated to VARCHAR(255). This creates a data parity mismatch between
    # the source columns and their annotation copies. Revisit this to either
    # widen the annotation column or enforce the same limit at the source.

    created_by_value = created_by
    if created_by_value is None:
        created_by_value = ""
        _logger.warning(
            f"Pipeline run id {pipeline_run_id} `created_by` is None, "
            'setting it to empty string "" for data parity'
        )

    created_by_value = _truncate_for_annotation(
        value=created_by_value,
        field_name=filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
        pipeline_run_id=pipeline_run_id,
    )

    session.add(
        bts.PipelineRunAnnotation(
            pipeline_run_id=pipeline_run_id,
            key=filter_query_sql.PipelineRunAnnotationSystemKey.CREATED_BY,
            value=created_by_value,
        )
    )

    pipeline_name_value = pipeline_name
    if pipeline_name_value is None:
        pipeline_name_value = ""
        _logger.warning(
            f"Pipeline run id {pipeline_run_id} `pipeline_name` is None, "
            'setting it to empty string "" for data parity'
        )

    pipeline_name_value = _truncate_for_annotation(
        value=pipeline_name_value,
        field_name=filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME,
        pipeline_run_id=pipeline_run_id,
    )

    session.add(
        bts.PipelineRunAnnotation(
            pipeline_run_id=pipeline_run_id,
            key=filter_query_sql.PipelineRunAnnotationSystemKey.PIPELINE_NAME,
            value=pipeline_name_value,
        )
    )
