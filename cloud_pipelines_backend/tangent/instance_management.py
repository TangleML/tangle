"""Build and create the Kubernetes resources that make up a Tangent agent instance."""

from __future__ import annotations

import dataclasses
import pathlib
import re
import textwrap
from collections.abc import Callable

import yaml
from kubernetes import client as k8s_client_lib

DEFAULT_NAMESPACE = "default"
DEFAULT_SERVICE_ACCOUNT_NAME = None
DEFAULT_MITMPROXY_IMAGE = "mitmproxy/mitmproxy:12.2.2"
DEFAULT_AGENT_PORT = 8000
DEFAULT_PROXY_PORT = 8080
DEFAULT_PVC_SIZE = "5Gi"

# Secret keys
PROXY_CONFIG_SECRET_KEY = "auth_proxy_config.yaml"

INSTANCE_ID_LABEL_NAME = "tangent.tangleml.com/instance.id"
INSTANCE_CREATED_BY_ANNOTATION_NAME = "tangent.tangleml.com/instance.created_by"
# Labels cannot hold arbitrary strings (such as e-mail addresses), so the label carries the
# sanitized name and is only used for listing. The annotation carries the exact name.
INSTANCE_CREATED_BY_SANITIZED_LABEL_NAME = (
    INSTANCE_CREATED_BY_ANNOTATION_NAME + ".sanitized"
)

DEFAULT_AGENT_KIND = "opencode"


def make_resource_name(instance_id: str) -> str:
    return f"tangent-{instance_id}"


def _sanitize_kubernetes_label_value(value: str) -> str:
    """Coerce an arbitrary string into a valid Kubernetes label value.

    Label values must be <=63 chars, begin and end with [a-z0-9A-Z], and only
    contain alphanumerics, dashes, underscores, and dots in between.
    """
    sanitized = re.sub(r"[^a-zA-Z0-9._-]", "_", value)[:63]
    return re.sub(r"^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+$", "", sanitized)


def _build_secret(
    instance_id: str,
    proxy_config: dict,
    namespace: str = DEFAULT_NAMESPACE,
) -> k8s_client_lib.V1Secret:
    """Secret holding the proxy rule config and the Tangle CLI basic auth."""
    name = make_resource_name(instance_id)
    return k8s_client_lib.V1Secret(
        api_version="v1",
        kind="Secret",
        metadata=k8s_client_lib.V1ObjectMeta(
            name=name,
            namespace=namespace,
            labels={INSTANCE_ID_LABEL_NAME: instance_id},
        ),
        type="Opaque",
        string_data={
            PROXY_CONFIG_SECRET_KEY: yaml.safe_dump(proxy_config, sort_keys=False),
        },
    )


# The mitmproxy addon script that reads the rule file and rewrites headers.
_AUTH_PROXY_MITMPROXY_ADDON_PY = (
    pathlib.Path(__file__).parent / "auth_proxy_mitmproxy_addon.py"
).read_text()


def _build_proxy_container(image: str, port: int) -> k8s_client_lib.V1Container:
    bootstrap = textwrap.dedent(f"""\
        python3 -m pip install PyYaml
        program_path=$(mktemp)
        printf "%s" "$0" > "$program_path"
        PROXY_CONFIG_PATH=/tangent/proxy-config/{PROXY_CONFIG_SECRET_KEY} \\
            mitmdump -p {port} --script "$program_path"
        """)
    return k8s_client_lib.V1Container(
        name="tangle-proxy",
        image=image,
        command=["sh", "-ec", bootstrap, _AUTH_PROXY_MITMPROXY_ADDON_PY],
        volume_mounts=[
            k8s_client_lib.V1VolumeMount(
                name="proxy-config",
                mount_path="/tangent/proxy-config",
            ),
            k8s_client_lib.V1VolumeMount(
                name="proxy-ca-cert",
                mount_path="/root/.mitmproxy",
            ),
        ],
    )


def _build_mitmproxy_ca_cert_init_container(
    image: str,
) -> k8s_client_lib.V1Container:
    # `mitmdump --no-server --rfile /dev/null` exits immediately after writing
    # the CA cert at ~/.mitmproxy/mitmproxy-ca-cert.pem, which we then share
    # with the agent container via the proxy-ca-cert emptyDir.
    return k8s_client_lib.V1Container(
        name="proxy-ca-cert-generator",
        image=image,
        command=["sh", "-ec", "mitmdump --no-server --rfile /dev/null"],
        volume_mounts=[
            k8s_client_lib.V1VolumeMount(
                name="proxy-ca-cert",
                mount_path="/root/.mitmproxy",
            ),
        ],
    )


def _build_volumes(
    secret_name: str, extra_volumes: list[k8s_client_lib.V1Volume]
) -> list[k8s_client_lib.V1Volume]:
    return [
        *extra_volumes,
        k8s_client_lib.V1Volume(
            name="proxy-config",
            secret=k8s_client_lib.V1SecretVolumeSource(secret_name=secret_name),
        ),
        k8s_client_lib.V1Volume(
            name="proxy-ca-cert",
            empty_dir=k8s_client_lib.V1EmptyDirVolumeSource(),
        ),
    ]


def _build_stateful_set(
    instance_id: str,
    created_by: str,
    agent_kind: str,
    agent_container_factory: Callable[[str], k8s_client_lib.V1Container],
    extra_volumes: list[k8s_client_lib.V1Volume],
    pod_annotations: dict[str, str],
    namespace: str = DEFAULT_NAMESPACE,
    service_account_name: str | None = DEFAULT_SERVICE_ACCOUNT_NAME,
    mitmproxy_image: str = DEFAULT_MITMPROXY_IMAGE,
    pvc_size: str = DEFAULT_PVC_SIZE,
) -> k8s_client_lib.V1StatefulSet:
    name = make_resource_name(instance_id)

    selector_labels = {"app": name}
    created_by_label = _sanitize_kubernetes_label_value(created_by.replace("@", "-at-"))

    resource_labels = {
        **selector_labels,
        "tangent.tangleml.com": "true",
        INSTANCE_ID_LABEL_NAME: instance_id,
        INSTANCE_CREATED_BY_SANITIZED_LABEL_NAME: created_by_label,
    }
    resource_annotations = {
        "tangent.tangleml.com": "true",
        INSTANCE_ID_LABEL_NAME: instance_id,
        INSTANCE_CREATED_BY_ANNOTATION_NAME: created_by,
    }
    pod_annotations = {
        **pod_annotations,
        **resource_annotations,
        "tangent.tangleml.com/instance.agent": agent_kind,
    }

    agent_container = agent_container_factory(instance_id)

    pod_spec = k8s_client_lib.V1PodSpec(
        service_account_name=service_account_name,
        init_containers=[_build_mitmproxy_ca_cert_init_container(mitmproxy_image)],
        containers=[
            agent_container,
            _build_proxy_container(image=mitmproxy_image, port=DEFAULT_PROXY_PORT),
        ],
        volumes=_build_volumes(secret_name=name, extra_volumes=extra_volumes),
    )

    pod_template = k8s_client_lib.V1PodTemplateSpec(
        metadata=k8s_client_lib.V1ObjectMeta(
            labels=resource_labels, annotations=pod_annotations
        ),
        spec=pod_spec,
    )

    pvc = k8s_client_lib.V1PersistentVolumeClaim(
        metadata=k8s_client_lib.V1ObjectMeta(
            name=name,
            namespace=namespace,
            labels=resource_labels,
            annotations=resource_annotations,
        ),
        spec=k8s_client_lib.V1PersistentVolumeClaimSpec(
            access_modes=["ReadWriteOnce"],
            resources=k8s_client_lib.V1VolumeResourceRequirements(
                requests={"storage": pvc_size},
            ),
        ),
    )

    stateful_set_spec = k8s_client_lib.V1StatefulSetSpec(
        service_name=name,
        replicas=1,
        selector=k8s_client_lib.V1LabelSelector(match_labels=selector_labels),
        template=pod_template,
        volume_claim_templates=[pvc],
    )

    return k8s_client_lib.V1StatefulSet(
        api_version="apps/v1",
        kind="StatefulSet",
        metadata=k8s_client_lib.V1ObjectMeta(name=name, namespace=namespace),
        spec=stateful_set_spec,
    )


def _generate_random_id() -> str:
    import os
    import time

    random_bytes = os.urandom(4)
    nanoseconds = time.time_ns()
    milliseconds = nanoseconds // 1_000_000

    return ("%012x" % milliseconds) + random_bytes.hex()


@dataclasses.dataclass(kw_only=True)
class TangentInstance:
    instance_id: str
    agent_kinds: list[str] | None = None
    # kubernetes_namespace: str
    # kubernetes_resource_name: str


def create_instance(
    *,
    api_client: k8s_client_lib.ApiClient,
    created_by: str,
    agent_kind: str,
    agent_container_factory: Callable[[str], k8s_client_lib.V1Container],
    extra_volumes: list[k8s_client_lib.V1Volume],
    pod_annotations: dict[str, str],
    proxy_config: dict,
    namespace: str = DEFAULT_NAMESPACE,
    service_account_name: str | None = DEFAULT_SERVICE_ACCOUNT_NAME,
    mitmproxy_image: str = DEFAULT_MITMPROXY_IMAGE,
    pvc_size: str = DEFAULT_PVC_SIZE,
) -> TangentInstance:
    """Create the Secret and StatefulSet for a new agent instance.

    The Secret is created before the StatefulSet so the proxy-config volume
    mount succeeds on first pod start.
    """
    instance_id = _generate_random_id()

    secret = _build_secret(
        instance_id=instance_id,
        namespace=namespace,
        proxy_config=proxy_config,
    )
    stateful_set = _build_stateful_set(
        instance_id=instance_id,
        created_by=created_by,
        namespace=namespace,
        service_account_name=service_account_name,
        mitmproxy_image=mitmproxy_image,
        agent_container_factory=agent_container_factory,
        extra_volumes=extra_volumes,
        pod_annotations=pod_annotations,
        agent_kind=agent_kind,
        pvc_size=pvc_size,
    )

    core_v1 = k8s_client_lib.CoreV1Api(api_client=api_client)
    apps_v1 = k8s_client_lib.AppsV1Api(api_client=api_client)

    core_v1.create_namespaced_secret(namespace=namespace, body=secret)
    apps_v1.create_namespaced_stateful_set(namespace=namespace, body=stateful_set)

    result = TangentInstance(
        instance_id=instance_id,
        agent_kinds=[agent_kind],
        # kubernetes_namespace=namespace,
        # kubernetes_resource_name=stateful_set.metadata.name,
    )
    return result


def list_instances(
    *,
    api_client: k8s_client_lib.ApiClient,
    created_by: str | None = None,
    namespace: str | None = None,
):
    core_v1 = k8s_client_lib.CoreV1Api(api_client=api_client)
    label_selector = "tangent.tangleml.com=true"
    if created_by:
        created_by_label = _sanitize_kubernetes_label_value(
            created_by.replace("@", "-at-")
        )
        label_selector = (
            label_selector
            + f",{INSTANCE_CREATED_BY_SANITIZED_LABEL_NAME}={created_by_label}"
        )
    if namespace:
        pod_list = core_v1.list_namespaced_pod(
            namespace=namespace, label_selector=label_selector
        )
    else:
        pod_list = core_v1.list_pod_for_all_namespaces(label_selector=label_selector)

    instances = []
    for pod in pod_list.items:
        instance_id = pod.metadata.labels[INSTANCE_ID_LABEL_NAME]
        instance = TangentInstance(
            instance_id=instance_id,
            # kubernetes_namespace=pod.metadata.namespace,
            # # TODO: Improve
            # kubernetes_resource_name=pod.metadata.name.removesuffix("-0"),
        )
        instances.append(instance)
    return instances


def get_instance_created_by(
    *,
    api_client: k8s_client_lib.ApiClient,
    instance_id: str,
    namespace: str = DEFAULT_NAMESPACE,
) -> str | None:
    """Returns who created the instance, or `None` if there is no such instance.

    The answer comes from the StatefulSet rather than from the Pod, because the StatefulSet
    exists as soon as the instance is created, while its Pod may still be getting scheduled.
    """
    apps_v1 = k8s_client_lib.AppsV1Api(api_client=api_client)
    try:
        stateful_set = apps_v1.read_namespaced_stateful_set(
            name=make_resource_name(instance_id),
            namespace=namespace,
        )
    except k8s_client_lib.ApiException as error:
        if error.status == 404:
            return None
        raise
    pod_metadata = stateful_set.spec.template.metadata
    return (pod_metadata.annotations or {}).get(INSTANCE_CREATED_BY_ANNOTATION_NAME)
