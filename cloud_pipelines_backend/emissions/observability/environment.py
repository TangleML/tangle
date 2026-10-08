"""The deployment environment, as a metric attribute.

Kept apart from the service name because the two are different questions: which process wrote
the point, and which deployment it was running in. Splitting them is what lets one alert filter
one deployment across every emission metric instead of matching a service suffix.

It has to be a regular metric attribute and never a resource attribute -- measured: resource
attributes are filtered out of the Prometheus export, so a `deployment.environment` resource
attribute would be honoured by the SDK and then silently vanish before any query could see it.
"""

import os
from typing import Final

#: A deployment label configured independently of the service name.
_ENV_VAR: Final[str] = "TANGLE_ENV"
_UNKNOWN: Final[str] = "unknown"

#: A dedicated attribute avoids collisions with exporter-provided labels.
LABEL: Final[str] = "tangle_environment"


def current_environment() -> str:
    """The deployment this process is running in, for use as a metric attribute.

    Read at call time rather than at import so a test can set it, and defaulted rather than
    raising: an unlabelled metric is worth more than a consumer that will not start.

    Returns:
        The value of TANGLE_ENV, or "unknown" when it is unset or empty.
    """
    return os.environ.get(_ENV_VAR) or _UNKNOWN


def with_environment(*, attributes: dict[str, str]) -> dict[str, str]:
    """Copy `attributes` with the environment added.

    Applied inside the shared recording helpers rather than at each call site, so a new metric
    cannot be added without it.

    Args:
        attributes: The labels the caller wants recorded. Not mutated.

    Returns:
        A new dict carrying the caller's labels plus the environment.
    """
    return {**attributes, LABEL: current_environment()}
