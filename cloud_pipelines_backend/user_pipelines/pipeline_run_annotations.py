from collections.abc import Mapping
from typing import Final

from .. import filter_query_sql
from . import errors

SOURCE_ANNOTATION: Final[str] = "tangleml.com/source/user-pipeline"
PIPELINE_ID_ANNOTATION: Final[str] = "tangleml.com/user-pipeline/pipeline-id"
VERSION_ANNOTATION: Final[str] = "tangleml.com/user-pipeline/version"
OWNER_ANNOTATION: Final[str] = "tangleml.com/user-pipeline/owner"
FILE_PATH_ANNOTATION: Final[str] = "tangleml.com/user-pipeline/file-path"

PROVENANCE_ANNOTATIONS: Final[frozenset[str]] = frozenset(
    {
        SOURCE_ANNOTATION,
        PIPELINE_ID_ANNOTATION,
        VERSION_ANNOTATION,
        OWNER_ANNOTATION,
        FILE_PATH_ANNOTATION,
    }
)


def validate_pipeline_run_annotations(annotations: Mapping[str, str]) -> None:
    """Rejects system keys and the provenance keys the server sets on saved-pipeline runs."""
    for key in annotations:
        if key.startswith(filter_query_sql.SYSTEM_KEY_PREFIX):
            raise errors.PipelineValidationError(
                f"Annotation keys starting with {filter_query_sql.SYSTEM_KEY_PREFIX!r} are reserved."
            )
        if key in PROVENANCE_ANNOTATIONS:
            raise errors.PipelineValidationError(
                f"Annotation key {key!r} is set by the server."
            )
