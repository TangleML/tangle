"""Routing facts read off an ExecutionNode's annotations, without touching the database.

One home for each annotation key, so the span emitter, the log enricher and the metric
emitter cannot drift apart. Every reader is pure -- ``task_spec`` is a loaded column,
never a relationship -- so these are safe to call inside ``before_commit``, where a lazy
load would put a SELECT in the commit path.
"""

import enum
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


class PendingReason(str, enum.Enum):
    """Why a task is not running yet, in our words rather than Kubernetes'.

    A closed set. A Kubernetes release that invents a new waiting reason widens OTHER
    rather than adding a label value, so the metric cannot grow a series behind us.
    """

    # What:    the Job exists but has produced no pod at all.
    # Fault:   admission refused it -- a namespace quota, an RBAC rule or a pod
    #          security policy. Which one is NOT knowable here: Kubernetes records
    #          the refusal as a FailedCreate Event, and launcher_data serialises the
    #          Job and its pods, never Events. Read the Events to find out why.
    # K8s:     an empty debug_pods. Every task starts this way for the first second
    #          or two, so this value only means something once the wait is long --
    #          see the overwrite note on pending_reason below.
    # Example: a team whose namespace quota is already exhausted.
    NO_POD_CREATED = "no_pod_created"

    # What:    nothing has placed this pod on a machine yet.
    # Fault:   the cluster's capacity, not the task's configuration.
    # K8s:     a Pending pod with no containerStatuses at all -- there is nothing to
    #          report on until something schedules it, so this one has no entry in
    #          the mapping below.
    # Example: a job asking for an accelerator (e.g. a B300 GPU) while every one
    #          of them is already taken.
    WAITING_FOR_NODE = "waiting_for_node"

    # What:    the pod has a machine and the kubelet is bringing it up.
    # Fault:   nobody's, until it lasts.
    # Example: a very large image, still pulling ten minutes in.
    STARTING_CONTAINER = "starting_container"

    # What:    the image cannot be fetched.
    # Fault:   the task's -- a tag that does not exist, or credentials the registry
    #          rejected. Kubernetes does not separate a typo from a permissions
    #          failure here, so neither can we.
    # Example: a task pinned to an image tag that was never pushed.
    IMAGE_UNAVAILABLE = "image_unavailable"

    # What:    the pod spec names something missing or unreadable.
    # Fault:   the task's own configuration or permissions, not the cluster's.
    # Example: a renamed Secret that one pipeline still references.
    BAD_CONFIG = "bad_config"

    # What:    a waiting reason we have not mapped.
    # Fault:   unknown, which is the point -- it stays out of the label set until
    #          someone decides which bucket it belongs in.
    # Example: a string a future Kubernetes release invents.
    OTHER = "other"


# Kubernetes' open vocabulary on the left, ours on the right. What each of ours means
# is on the enum above; these are the strings the kubelet actually reports.
#
# The pod lifecycle they come from, top to bottom:
#
#   pod created
#     |
#     +-- nothing schedules it ................... (no containerStatuses)
#     v
#   scheduled to a node
#     |
#     +-- image reference malformed .............. InvalidImageName
#     v
#   init containers running .................... PodInitializing
#     |
#     v
#   main container being created ............... ContainerCreating
#     |
#     +-- pull failed ............................ ErrImagePull
#     |     +-- and kept failing ................. ImagePullBackOff
#     +-- manifest unreadable .................... ImageInspectError
#     +-- registry silent ........................ RegistryUnavailable
#     +-- Secret or ConfigMap missing ............ CreateContainerConfigError
#     +-- runtime refused the spec ............... CreateContainerError
#     |
#     v
#   RUNNING
#
# The common path, not a state machine: ContainerCreating covers the image pull too,
# so the kubelet may report either it or ErrImagePull for the same moment.
_PENDING_REASONS = {
    # The kubelet is bringing the pod up, in this order:
    #   PodInitializing ──then──► ContainerCreating
    #   init containers           the main container
    #
    # Init containers are still running, so the main container has not started.
    "PodInitializing": PendingReason.STARTING_CONTAINER,
    # The main container is being created: pulling layers, mounting volumes.
    "ContainerCreating": PendingReason.STARTING_CONTAINER,
    # The image did not arrive.
    #   ErrImagePull ──retries──► ImagePullBackOff     one incident, two names
    #
    # The first pull attempt failed.
    "ErrImagePull": PendingReason.IMAGE_UNAVAILABLE,
    # The back-off loop the above decays into after repeated failures.
    "ImagePullBackOff": PendingReason.IMAGE_UNAVAILABLE,
    # The image was fetched but its manifest could not be read.
    "ImageInspectError": PendingReason.IMAGE_UNAVAILABLE,
    # The registry did not answer at all.
    "RegistryUnavailable": PendingReason.IMAGE_UNAVAILABLE,
    # The spec is wrong or not permitted, caught at two different moments:
    #   InvalidImageName                                before any pull
    #   CreateContainerConfigError, CreateContainerError at container creation
    #
    # The image reference is malformed, so nothing is ever pulled.
    "InvalidImageName": PendingReason.BAD_CONFIG,
    # A Secret, ConfigMap or token the spec names is missing or unreadable.
    "CreateContainerConfigError": PendingReason.BAD_CONFIG,
    # The runtime refused to create the container from this spec.
    "CreateContainerError": PendingReason.BAD_CONFIG,
}


# Written onto ContainerExecution.extra_data while a task waits, read back when it stops
# waiting -- by then the pod is running and the reason is gone from it.
PENDING_REASON_KEY = "last_pending_reason"


def pending_reason(*, launcher_data: dict) -> PendingReason | None:
    """Why a task is not running yet, in our words rather than Kubernetes'.

        a Job with no pods at all            → "no_pod_created"       (a)
        a pod with no containerStatuses yet  → "waiting_for_node"      (b)
        waiting.reason "ContainerCreating"   → "starting_container"
        waiting.reason "ImagePullBackOff"    → "image_unavailable"
        waiting.reason "CreateContainerConfigError"  → "bad_config"
        waiting.reason "SomeNewK8sString"    → "other"                 (c)
        a pod whose phase is not Pending     → None                    (d)
        a launcher that is not Kubernetes    → None

        (a) admission refused every pod, or the Job is one second old
        (b) scheduling has not placed it, so there is no container to report on
        (c) a Kubernetes release adding a reason must not add a label value
        (d) nothing is waiting, so there is nothing to attribute

    Reads the serialised pod already persisted on ContainerExecution.launcher_data, so
    no Kubernetes call and no database read happen here.

    "no_pod_created" is last-stamp-wins, not an observation. A Job is launched with an
    empty pod map, so every task is briefly in this state; a healthy one is overwritten
    within seconds by the first poll that sees a pod. It survives to be read only when
    no pod ever appears, which is the case it is for.
    """
    root = launcher_data.get("kubernetes_job") or launcher_data.get("kubernetes")
    if root is None:
        return None
    pods = root.get("debug_pods")
    if pods is None:
        single = root.get("debug_pod")
        pods = {"0": single} if single else {}
    for pod in pods.values():
        status = (pod or {}).get("status") or {}
        if status.get("phase") != "Pending":
            continue
        container_statuses = status.get("containerStatuses")
        if not container_statuses:
            return PendingReason.WAITING_FOR_NODE
        waiting = (container_statuses[0].get("state") or {}).get("waiting") or {}
        reason = waiting.get("reason")
        if reason:
            return _PENDING_REASONS.get(reason, PendingReason.OTHER)
        return PendingReason.WAITING_FOR_NODE
    if not pods:
        return PendingReason.NO_POD_CREATED
    return None
