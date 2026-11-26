import dataclasses
import enum
from typing import Protocol

from . import backend_types_sql as bts


class TroubleshootingSeverity(enum.StrEnum):
    """How strongly the UI should present a troubleshooting guide."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclasses.dataclass(frozen=True, kw_only=True)
class TroubleshootingReference:
    """A stable pointer to a guide, stored on a failed execution in place of the guide's text.

    `params` holds only safe values parsed from the error, such as ids, never secrets or workload specs.
    """

    code: str
    params: dict[str, str] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass(frozen=True, kw_only=True)
class TroubleshootingAction:
    """A link the UI shows as a button under a troubleshooting guide."""

    label: str
    url: str


@dataclasses.dataclass(frozen=True, kw_only=True)
class TroubleshootingGuide:
    """The steps a user can take to fix a known failure, rendered when the execution is read."""

    title: str
    severity: TroubleshootingSeverity
    body_markdown: str
    actions: list[TroubleshootingAction] = dataclasses.field(default_factory=list)


class TroubleshootingClassifier(Protocol):
    """Recognizes known launch failures so a guide can be shown for them later. Implemented downstream."""

    def classify(
        self, *, execution: bts.ExecutionNode, exception: Exception
    ) -> TroubleshootingReference | None:
        """The guide reference for this failure, or None when it is not a known failure."""


class TroubleshootingProvider(Protocol):
    """Turns a stored guide reference into a guide when the execution is read. Implemented downstream."""

    def render(
        self, *, reference: TroubleshootingReference
    ) -> TroubleshootingGuide | None:
        """The guide for this reference, or None when there is no guide for its code."""
