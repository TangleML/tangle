"""Argument templates, as they sit inside a row's JSON blob.

The schedule keeps its blob in `ScheduledPipelineRun.settings`, the subscription inside
`TriggerSubscription.definition`; the key is the same on both, so it lives here once.
"""

import enum
from collections.abc import Mapping
from typing import Any


# `(str, enum.Enum)` so members are plain strings for dict lookups and JSON writes.
class _Key(str, enum.Enum):
    PIPELINE_TEMPLATES = "pipeline_templates"


def get_pipeline_templates(*, original: Mapping[str, Any] | None) -> dict[str, Any]:
    """NULL, an empty blob, a missing key and an empty envelope all read as `{}`."""
    return (original or {}).get(_Key.PIPELINE_TEMPLATES) or {}


def set_pipeline_templates(
    *, original: Mapping[str, Any] | None, updates: Mapping[str, Any]
) -> dict[str, Any]:
    """`original` with the templates applied -- a new dict, since assigning it is what
    tells SQLAlchemy the JSON changed. Empty updates remove the key rather than storing
    `{}`."""
    # `.value`, not the member: a blob decoded from MySQL has a plain `str` key, and one
    # built here should be indistinguishable from it.
    updated = dict(original or {})
    if updates:
        updated[_Key.PIPELINE_TEMPLATES.value] = dict(updates)
    else:
        updated.pop(_Key.PIPELINE_TEMPLATES, None)
    return updated
