"""The pipeline-run annotation that files a run under a project.

A run is never a `project_resource` row. Its submitter sets one annotation on it, and the
project's run feed is a filter over the run list for that annotation. So a run reaches a project
from any client that can submit one, without that client calling the project routes at all.
"""

from typing import Final

# Which project a run was submitted for: `.../project/id/<id>`, one key per membership, with the
# value a presence marker. The id is in the key because `pipeline_run_annotation`'s primary key
# is `(pipeline_run_id, key)` -- a constant key holding the id as its value could never carry a
# second project.
#
# Not a `system/` key: `api_server_sql._mirror_single_pipeline_run_annotation` silently skips that
# prefix, which would leave the table the run filter reads empty with no error to explain it.
PROJECT_ANNOTATION_PREFIX: Final[str] = "tangleml.com/project/id/"

# The value under a project key. Carries nothing -- the row's presence is the membership -- but a
# marker rather than `""`, which the mirror cannot tell from a write that lost its value.
PROJECT_MEMBERSHIP_VALUE: Final[str] = "true"


def project_run_key(project_id: str) -> str:
    """The annotation key that files a run under `project_id`.

    The one place the format is spelled, so the feed predicate and every writer agree by
    construction.
    """
    return f"{PROJECT_ANNOTATION_PREFIX}{project_id}"
