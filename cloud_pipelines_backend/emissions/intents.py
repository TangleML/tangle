"""The intent contract every emission kind implements.

An intent is what a node's annotations parse into: a typed, validated description of an
emission the node opted into. It travels from the producer (which decides whether to write a
row) through to the handler and its sinks (which act on it).

The producer asks an intent exactly one question -- *does this status change fire you?* --
and this module is where that question lives, so the producer never learns any individual
kind's field names. Two shapes answer it:

- `SingleStatusIntent` for the kinds a user picks a status for. Readiness and metadata both
  let a node declare `on-status: FAILED`, so the field is theirs and the comparison is trivial.
- A direct `EmissionIntent` subclass for a kind whose firing rule is not a user's choice.
  Quota fires on any terminal status because a freed slot is a node that ended, however it
  ended, so it overrides `matches` and declares no status at all.

The split is what lets quota drop `on_status` without readiness growing a tuple it would
always hold exactly one element in.
"""

import abc
import dataclasses

from cloud_pipelines_backend import backend_types_sql as bts
from cloud_pipelines_backend.emissions import annotations as emission_annotations


@dataclasses.dataclass(frozen=True, kw_only=True)
class EmissionIntent(abc.ABC):
    """What every emission intent owes the producer: whether a status change fires it.

    Deliberately narrow. The intent is a *predicate*, not a source of the status -- the value
    written to `emission_event.container_execution_status` comes from the ORM object the
    producer already holds, never from here. So an intent that names no status still produces
    a well-formed row: a node that failed records "failed".
    """

    @abc.abstractmethod
    def matches(
        self,
        *,
        node_status: bts.ContainerExecutionStatus,
    ) -> bool:
        """Report whether this intent fires on the status the node just changed to.

        Args:
            node_status: The status the node just changed to. Any status, not only terminal
                ones -- the producer runs on every transition.

        Returns:
            True when this status change should write an emission row for this intent.
        """


@dataclasses.dataclass(frozen=True, kw_only=True)
class SingleStatusIntent(EmissionIntent):
    """The common case: one declared status, defaulting to SUCCEEDED.

    `kw_only=True` is what makes this base practical. The usual blocker on a dataclass
    hierarchy -- "non-default argument follows default argument" -- does not apply to
    keyword-only fields, so this class can carry a defaulted `on_status` while a subclass
    still adds a required field of its own.
    """

    # The container status that fires this emission. Defaults to SUCCEEDED and may be any
    # status; a subclass inherits both the field and its annotation round trip.
    on_status: bts.ContainerExecutionStatus = emission_annotations.DEFAULT_ON_STATUS

    def matches(
        self,
        *,
        node_status: bts.ContainerExecutionStatus,
    ) -> bool:
        """Fire when the node reached exactly the status this intent declared.

        Args:
            node_status: The status the node just changed to.

        Returns:
            True when it equals the declared `on_status`.
        """
        # Both sides are ContainerExecutionStatus members, so this is an enum-to-enum
        # comparison and needs no `.value` on either side.
        return self.on_status == node_status
