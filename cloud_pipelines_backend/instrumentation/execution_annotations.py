"""Routing facts read off an ExecutionNode's annotations, without touching the database.

One home for each annotation key, so the span emitter, the log enricher and the metric
emitter cannot drift apart. Every reader is pure -- ``task_spec`` is a loaded column,
never a relationship -- so these are safe to call inside ``before_commit``, where a lazy
load would put a SELECT in the commit path.
"""

import json
import re
from typing import NamedTuple

from .. import backend_types_sql as bts
from ..launchers import common_annotations
from ..launchers import kubernetes_launchers

# Aliased, not respelled: a launcher renaming a key cannot silently empty these readers.
RESOURCES_CPU_KEY = kubernetes_launchers.RESOURCES_CPU_ANNOTATION_KEY
RESOURCES_MEMORY_KEY = kubernetes_launchers.RESOURCES_MEMORY_ANNOTATION_KEY
RESOURCES_ACCELERATORS_KEY = kubernetes_launchers.RESOURCES_ACCELERATORS_ANNOTATION_KEY

# Set at routing time rather than by a launcher, so no launcher module owns it.
CLOUD_PROVIDER_KEY = "cloud-pipelines.net/orchestration/cloud_provider"

# Read by the deployment's pod builder, which puts such a task in a namespace of its
# own rather than the cluster's.
GOOGLE_SERVICE_ACCOUNT_KEY = "cloud-pipelines.net/launchers/google/service_account"


class Target(NamedTuple):
    """Where a task runs, once its annotation has been resolved.

        Target("google", "us-ce1-lt3",            a cluster the config names,
               "kueue-jobs-staging")              with the namespace it runs in
        Target("nebius", "b300-cluster",
               "tangle-jobs-production")
        Target("davies", "davies", "none",        a provider with no clusters of
               cluster_from_annotation=<key>)     its own; each task names one

    ``cluster_from_annotation`` lets the deployment register which annotation key its
    tasks use for that, so a deployment-specific key never appears in this repository.
    ``namespace`` is the cluster's own; a task carrying a service account overrides it.
    """

    provider: str
    cluster: str
    namespace: str = "none"
    cluster_from_annotation: str | None = None


# Filled once at startup from the launcher config, which names either spelling of every
# target. Reading that config validates a file, so it must not happen per status change.
_TARGETS: dict[str, Target] = {}

# The field a deployment's launcher stamps its own target into on the persisted
# handle. Registered rather than spelled here, like cluster_from_annotation: the
# handle's shape belongs to the deployment. None means it records no such field, and
# the annotation is all there is to go on.
_LAUNCHER_KEY_FIELD: str | None = None

# A label value a client wrote is reshaped before it becomes a series. Forty is well
# past every name in use and short enough that a pasted stack trace cannot become a
# label; anything not a lowercase ASCII alphanumeric collapses to a single dash.
_LABEL_MAX_LENGTH = 40
_NOT_LABEL_SAFE = re.compile(r"[^a-z0-9]+")


def as_label(*, value: str) -> str:
    """Reshape a value written by a client into one safe to be a metric label.

        "B300 cluster"          → "b300-cluster"
        "NVIDIA-B300-SXM6-PC"   → "nvidia-b300-sxm6-pc"
        "b300"                  → "b300"              already fine
        "  "  or  "---"         → "none"              nothing left to name  (a)
        a 200-character paste   → its first 40        (b)

        (a) "none" rather than "": an empty label value is dropped by some
            backends, and a dropped label reads as a different series
        (b) a cap bounds the width of one series, not how many there are

    Two spellings of one name becoming one series is the point: "B300 Cluster" and
    "b300-cluster" are the same machine.
    """
    slug = _NOT_LABEL_SAFE.sub("-", value.strip().lower()).strip("-")
    if not slug:
        return "none"
    return slug[:_LABEL_MAX_LENGTH].rstrip("-") or "none"


def register_targets(
    *, targets: dict[str, Target], launcher_key_field: str | None = None
) -> None:
    """Install the alias-and-key → Target map. Called once, at startup.

        {"lt3":     Target("google", "lt3"),
         "gke-lt3": Target("google", "lt3"),   ← an alias for the same cluster
         "h200":    Target("nebius", "h200"),
         "davies":  Target("davies", "davies", cluster_from_annotation=<key>)}

    Replaces the map rather than adding to it, so a reload cannot leave behind a
    target the config no longer names.

    ``launcher_key_field`` names the field the deployment's handles record their own
    target in, keyed the same way as this map. Giving it is what lets a label say
    where a task ran rather than where it was aimed.
    """
    global _LAUNCHER_KEY_FIELD
    _TARGETS.clear()
    _TARGETS.update(targets)
    _LAUNCHER_KEY_FIELD = launcher_key_field


def _annotations(*, execution: "bts.ExecutionNode") -> dict[str, str]:
    """Every annotation on the task, or an empty dict.

        reads: task_spec["annotations"]

    A loaded column, never a relationship, which is what makes every reader here
    safe inside ``before_commit``.
    """
    return ((execution.task_spec or {}).get("annotations")) or {}


def cloud_provider(*, execution: "bts.ExecutionNode") -> str | None:
    """The provider annotation exactly as written -- "gke-lt3", never "google".

        reads: cloud-pipelines.net/orchestration/cloud_provider

        cloud_provider="lt3"       → "lt3"
        cloud_provider="gke-lt3"   → "gke-lt3"    an alias is NOT resolved here
        no annotation              → None
        cloud_provider=["lt3"]     → None         not a string (a)

        (a) annotations are Dict[str, Any], so a client can send any JSON value.
            Returning None here is what keeps ``target``'s dict lookup from
            raising on an unhashable key.

    Kept for readers that want what the task asked for rather than where it landed.
    Use ``target`` to resolve an alias to a provider and a cluster.
    """
    selected = _annotations(execution=execution).get(CLOUD_PROVIDER_KEY)
    return selected if isinstance(selected, str) else None


def launched_launcher_key(*, launcher_data: object) -> str | None:
    """The target a persisted handle says it ran on, or None if it does not say.

        {"tangle_launcher_key": "eo9", ...}  → "eo9"
        a handle predating the field         → None
        a handle from another deployment     → None   (a)
        not a dict                           → None

        (a) the field name is registered by the deployment, so a handle written by
            one that records nothing reads as None rather than wrongly

    This is the only thing that knows where a task actually ran. The annotation says
    where it was aimed, and routing can move it afterwards without rewriting it.
    """
    if _LAUNCHER_KEY_FIELD is None or not isinstance(launcher_data, dict):
        return None
    key = launcher_data.get(_LAUNCHER_KEY_FIELD)
    return key if isinstance(key, str) and key else None


def target(
    *, execution: "bts.ExecutionNode", launcher_data: object = None
) -> Target | None:
    """The provider and cluster a task ran on, or was aimed at before it launched.

        reads: cloud-pipelines.net/orchestration/cloud_provider
               + the cluster key registered by a provider that declares none (a)

        cloud_provider="lt3"                      → Target("google", "lt3")
        cloud_provider="gke-lt3"                  → Target("google", "lt3")
        cloud_provider="h200"                     → Target("nebius", "h200")
        cloud_provider="davies" + cluster="b300"  → Target("davies", "b300")
        cloud_provider="davies", no cluster named → Target("davies", "none")
        cloud_provider="retired-tpu"              → None    not in the map
        no annotation at all                      → None

        (a) that key is deployment-specific, so it is registered rather than
            spelled here; davies is the only provider using one today

    None means "this deployment does not route that", not "unlabelled": callers turn
    it into "none" so a filter cannot drop the executions that lack it.
    """
    # Where it ran beats where it was aimed. Routing can move a task to a successor
    # cluster after the annotation is written -- a percentage redirect does exactly
    # that -- and only the handle records the outcome. Before the launch there is no
    # handle, so the annotation is still the answer.
    selected = launched_launcher_key(launcher_data=launcher_data) or cloud_provider(
        execution=execution
    )
    if selected is None:
        return None
    resolved = _TARGETS.get(selected)
    if resolved is None or resolved.cluster_from_annotation is None:
        return resolved
    named = _annotations(execution=execution).get(resolved.cluster_from_annotation)
    if not isinstance(named, str) or not named.strip():
        return resolved._replace(cluster="none")
    return resolved._replace(cluster=as_label(value=named))


def namespace(*, execution: "bts.ExecutionNode") -> str | None:
    """The namespace a task's own service account puts it in, or None for its cluster's.

        reads: cloud-pipelines.net/launchers/google/service_account
               + cloud-pipelines.net/pipeline_runs/created_by

        service_account="sidekick-oasis-runner@s5y-sidekick-52ee
                         .iam.gserviceaccount.com", created_by set
                                   → "sidekick-oasis-runner-at-s5y-sidekick-52ee"
        service_account set, created_by absent  → None   (a)
        neither                                 → None   (a)

        (a) the pod lands in the cluster's namespace instead, which this module
            cannot see; the caller falls back to Target.namespace

    Both annotations are required because the deployment's pod builder only creates a
    per-account namespace when it has both. Deriving it here duplicates that rule
    rather than querying Kubernetes, which no reader in this module may do.
    """
    annotations = _annotations(execution=execution)
    account = annotations.get(GOOGLE_SERVICE_ACCOUNT_KEY)
    created_by = annotations.get(
        common_annotations.PIPELINE_RUN_CREATED_BY_ANNOTATION_KEY
    )
    if not account or not created_by:
        return None
    if not isinstance(account, str):
        # Dict[str, Any]: a client can send any JSON value, and the caller's
        # fallback to the cluster namespace is the right answer for one we
        # cannot slugify.
        return None
    return re.sub(
        r"[^-a-z0-9]",
        "-",
        account.lower().removesuffix(".iam.gserviceaccount.com").replace("@", "-at-"),
    ).strip("-")


def accelerators(*, execution: "bts.ExecutionNode") -> tuple[str, int] | None:
    """The accelerator model and count a task asked for.

        reads: cloud-pipelines.net/launchers/generic/resources.accelerators

        '{"NVIDIA-B300-SXM6-PC": "8"}'  → ("nvidia-b300-sxm6-pc", 8)
        '{"NVIDIA-B300-SXM6-PC":"8"}'   → ("nvidia-b300-sxm6-pc", 8)   (a)
        absent  or  '{}'                → None
        '{"A": "1", "B": "1"}'          → ValueError, two models
        'not json'                      → ValueError
        {"nvidia-l4": "8"}              → ValueError, a dict not a string (b)
        '{"nvidia-l4": null}'           → ValueError, no count (b)

        (a) one space from the row above -- one request, two spellings
        (b) ValueError, not TypeError: every exit this function can take is one
            the caller already handles, so a client's JSON cannot reach the
            orchestrator's commit

    Parsing is what makes the two spacings one series instead of two.

    The model is reshaped, not checked against a list. Hardware arrives faster than
    this file changes, so an unknown accelerator has to stay visible under its own
    name -- bounding the shape keeps one series narrow, and a model nobody recognises
    is exactly the thing a reader needs to see.
    """
    raw = _annotations(execution=execution).get(RESOURCES_ACCELERATORS_KEY)
    if not raw:
        return None
    if not isinstance(raw, str):
        raise ValueError(f"The accelerators annotation is not a string: {raw=}")
    requested = json.loads(raw)  # ValueError on malformed input
    if not isinstance(requested, dict) or not requested:
        return None
    if len(requested) > 1:
        raise ValueError(f"Multiple accelerator types were specified: {requested=}")
    model, quantity = next(iter(requested.items()))
    try:
        count = int(quantity)
    except (TypeError, ValueError):
        raise ValueError(
            f"The accelerator count is not a number: {quantity=}"
        ) from None
    return as_label(value=model), count
