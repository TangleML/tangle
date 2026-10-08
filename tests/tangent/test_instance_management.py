"""The host supplies agent deployment details to the reusable lifecycle."""

from kubernetes import client as k8s

from cloud_pipelines_backend.tangent import instance_management


def test_stateful_set_uses_injected_container_volumes_and_annotations():
    container = k8s.V1Container(name="agent", image="provider.example/agent:v1")
    volume = k8s.V1Volume(name="agent-data", empty_dir=k8s.V1EmptyDirVolumeSource())
    requested_instances = []

    def build_agent(instance_id):
        requested_instances.append(instance_id)
        return container

    stateful_set = instance_management._build_stateful_set(
        instance_id="instance1",
        created_by="owner@example.com",
        agent_kind="agent",
        agent_container_factory=build_agent,
        extra_volumes=[volume],
        pod_annotations={"provider.example/storage": "enabled"},
        namespace="agents",
        service_account_name="agent-account",
    )

    pod = stateful_set.spec.template
    assert requested_instances == ["instance1"]
    assert pod.spec.containers[0] is container
    assert pod.spec.volumes[0] is volume
    assert [item.name for item in pod.spec.volumes] == [
        "agent-data",
        "proxy-config",
        "proxy-ca-cert",
    ]
    assert pod.spec.service_account_name == "agent-account"
    assert pod.metadata.annotations["provider.example/storage"] == "enabled"
    assert (
        pod.metadata.annotations[
            instance_management.INSTANCE_CREATED_BY_ANNOTATION_NAME
        ]
        == "owner@example.com"
    )
    assert stateful_set.metadata.namespace == "agents"
