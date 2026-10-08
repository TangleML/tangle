"""The one place the annotation vendor domain is written down.

Every annotation key Tangle owns is path-based and starts here: emissions, orchestration,
user pipelines, scheduling, the launchers. Each subsystem appends its own namespace segment
and keeps ownership of the keys under it — this module holds the shared root and nothing else.

The root carries its trailing "/" so callers concatenate rather than re-spell the separator.
"""

from typing import Final

# The vendor domain every Tangle annotation key hangs off. "/" is the only separator; no dots.
ANNOTATION_ROOT: Final[str] = "tangleml.com/"
