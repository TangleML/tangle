"""Validation and server-owned keys for saved pipeline-run annotations."""

from collections.abc import Mapping
from typing import Final

from cloud_pipelines_backend import filter_query_sql
from cloud_pipelines_backend.user_pipelines.errors import (
    PipelineValidationError,
)

SOURCE_ANNOTATION: Final[str] = "tangleml.com/source/user-pipeline"
PIPELINE_ID_ANNOTATION: Final[str] = "tangleml.com/user-pipeline/pipeline-id"
VERSION_ANNOTATION: Final[str] = "tangleml.com/user-pipeline/version"
OWNER_ANNOTATION: Final[str] = "tangleml.com/user-pipeline/owner"
FILE_PATH_ANNOTATION: Final[str] = "tangleml.com/user-pipeline/file-path"

# Which project a run was submitted for: `.../project/id/<id>`, one key per membership, with the
# value a presence marker. The id is in the key because `pipeline_run_annotation`'s primary key
# is `(pipeline_run_id, key)` -- a constant key holding the id as its value could never carry a
# second project. One project per run is held in behaviour instead, by
# `_reject_more_than_one_project` below.
#
# Not a `system/` key: `api_server_sql._mirror_single_pipeline_run_annotation` silently skips that
# prefix, which would leave the table the run filter reads empty with no error to explain it. Not
# in `PROVENANCE_ANNOTATIONS` either -- reserving the key would block the only mechanism that
# supplies it.
PROJECT_ANNOTATION_PREFIX: Final[str] = "tangleml.com/project/id/"

# The value under a project key. Carries nothing -- the row's presence is the membership -- but a
# marker rather than `""`, which the mirror cannot tell from a write that lost its value.
# `canonicalized_project_run_annotations` rewrites every project key's value to this, because the
# feed predicate is `key_exists`: passed through, `"false"` filed a run into its project exactly
# as `"true"` did. It is also what a later tightening to `value_equals` would need.
PROJECT_MEMBERSHIP_VALUE: Final[str] = "true"


def project_run_key(project_id: str) -> str:
    """The annotation key that files a run under `project_id`.

    The one place the format is spelled, so the feed predicate and every writer agree by
    construction.
    """
    return f"{PROJECT_ANNOTATION_PREFIX}{project_id}"


def _project_key_suffix(key: str) -> str | None:
    """What follows `PROJECT_ANNOTATION_PREFIX` in `key`, or `None` if `key` is not one of ours.

    Matched exactly, byte for byte. Key equality is the database's and the dialects disagree
    about case -- `pipeline_run_annotation.key` takes MySQL's case-insensitive default while
    SQLite is byte-exact -- so folding here would agree with one dialect and contradict the
    other.

    Which makes exact matching only half a rule. On its own it turns a differently spelled key
    into an ordinary annotation, and MySQL then files the run into the project's feed anyway
    while every guard in this module counts that key as nothing.
    `_is_inexact_project_key` is the other half, and `validate_pipeline_run_annotations`
    refuses on it.

    Recognised, not accepted: what each caller does with one is still its own rule.
    """
    if not key.startswith(PROJECT_ANNOTATION_PREFIX):
        return None
    return key[len(PROJECT_ANNOTATION_PREFIX) :]


def _is_inexact_project_key(key: str) -> bool:
    """Whether `key` differs from `PROJECT_ANNOTATION_PREFIX` only in ASCII case.

    The prefix is spelled in lowercase, so lowercasing the candidate is the whole comparison.

    ASCII case only, deliberately. It is what a client actually produces, it needs neither
    `unicodedata` nor knowledge of the dialect underneath, and it is the folding MySQL does that
    is reachable by typing. Accent and zero-weight variants still disagree with
    `utf8mb4_0900_ai_ci` and stay inside the documented collation gap: this narrows that gap
    rather than closing it.
    """
    return key.lower().startswith(PROJECT_ANNOTATION_PREFIX) and not key.startswith(
        PROJECT_ANNOTATION_PREFIX
    )


def project_run_keys(
    annotations: Mapping[str, str], *, include_inexact: bool = False
) -> list[str]:
    """Every project key in an annotations map, in iteration order.

    A list rather than a count because two callers want the keys themselves: the guard below,
    to name them in its error, and the submit path, to drop all of them off a stored pipeline.

    `include_inexact` widens the sweep to the near misses `_is_inexact_project_key` catches.
    Off for the guard, which runs after those are refused and should count only what it can
    name. On for the drop, whose job is to leave no key behind that MySQL would still file a
    run under -- including one stored before the refusal existed, which is also what keeps such
    a row submittable instead of a 422 its owner cannot clear.
    """
    return [
        key
        for key in annotations
        if _project_key_suffix(key) is not None
        or (include_inexact and _is_inexact_project_key(key))
    ]


def canonicalized_project_run_annotations(
    annotations: Mapping[str, str],
) -> dict[str, str]:
    """The same map with every project key's value normalized to `PROJECT_MEMBERSHIP_VALUE`.

    A key with no id after the prefix is refused: it can never match a feed and would file the
    run under nothing, silently. See `PROJECT_MEMBERSHIP_VALUE` for why the value is normalized.
    """
    canonicalized: dict[str, str] = {}
    for key, value in annotations.items():
        suffix = _project_key_suffix(key)
        if suffix is not None:
            if not suffix:
                raise PipelineValidationError(
                    f"Pipeline run annotation key {key!r} must end in a project id."
                )
            value = PROJECT_MEMBERSHIP_VALUE
        canonicalized[key] = value
    return canonicalized


PROVENANCE_ANNOTATIONS: Final[frozenset[str]] = frozenset(
    {
        SOURCE_ANNOTATION,
        PIPELINE_ID_ANNOTATION,
        VERSION_ANNOTATION,
        OWNER_ANNOTATION,
        FILE_PATH_ANNOTATION,
    }
)


def _reject_more_than_one_project(annotations: Mapping[str, str]) -> None:
    """Hold the API at one project per run.

    A policy, not a limit: the storage under it takes any number of memberships, and this is the
    whole of what stops a client using them. Deleting this function is what turns many-to-many
    on. See `PROJECT_ANNOTATION_PREFIX`.

    Kept here rather than in `projects`, inside the function the submit path already calls, so a
    second submit route cannot acquire the mechanism without the rule. It is still only as
    reachable as this validation is, and two writers do not reach it:

    - `POST /api/pipeline_runs/` runs none of it -- nor `canonicalized_project_run_annotations`
      -- the same gap the reserved-key rules above already have. Closing it for both routes
      means the `pipeline_run_creation_hook` that `_setup_routes_internal` accepts, which the
      public `setup_routes` does not forward, so the wiring starts in the core API router.
    - `POST /api/pipeline_runs/{id}/annotations/{key}` (`api_server_sql.set_annotation`) blocks
      only `system/` keys, so a run's own creator can add a second project key *after*
      submission. Same upstream seam.

    Until both are closed this is a policy the guarded route holds, not an invariant the data
    carries -- anything reading the feed should tolerate a run with more than one project key.
    """
    keys = project_run_keys(annotations)
    if len(keys) > 1:
        raise PipelineValidationError(
            f"A pipeline run can name at most one project; received {len(keys)}: {sorted(keys)}."
        )


def validate_pipeline_run_annotations(
    annotations: Mapping[str, str],
    *,
    allow_server_owned_provenance: bool = False,
    allow_submission_scoped: bool = True,
) -> None:
    """Reject reserved keys, near misses on the project prefix, and more than one project.

    `allow_server_owned_provenance` permits legacy provenance already on a row to be read back
    and overwritten. `allow_submission_scoped` is false on the pipeline write path, the one
    caller storing annotations rather than attaching them to a single run.
    """
    for key in annotations:
        if key.startswith(filter_query_sql.SYSTEM_KEY_PREFIX):
            raise PipelineValidationError(
                "Pipeline run annotation keys starting with "
                f"{filter_query_sql.SYSTEM_KEY_PREFIX!r} are reserved for system use."
            )
        if not allow_server_owned_provenance and key in PROVENANCE_ANNOTATIONS:
            raise PipelineValidationError(
                f"Pipeline run annotation key {key!r} is reserved for saved-pipeline provenance."
            )
        # MySQL's collation matches such a key against the canonical one, so the feed files the
        # run into that project while both rules below read it as not a project key at all.
        # Unconditional, because those two rules sit on different paths and the spelling walks
        # past each. `triggers._reject_inexact_key` refuses the same shape, at a match; this is
        # the edge, so it refuses on the way in.
        if _is_inexact_project_key(key):
            raise PipelineValidationError(
                f"Pipeline run annotation key {key!r} differs from "
                f"{PROJECT_ANNOTATION_PREFIX!r} only in case; spell it exactly."
            )
        # Legal on a run, illegal on a saved pipeline: the same pipeline submitted from two
        # projects is one pipeline. `calculate_pipeline_digest` also folds stored annotations
        # into the content digest, so a project id parked here would make "the same pipeline in
        # a different project" a different version of it.
        if not allow_submission_scoped and _project_key_suffix(key) is not None:
            raise PipelineValidationError(
                f"Pipeline run annotation key {key!r} belongs to a run submission, not to a "
                "saved pipeline. Send it when submitting the run instead."
            )
    # After the loop, so a caller that may not send a project key at all gets the more specific
    # refusal above rather than a count.
    _reject_more_than_one_project(annotations)
