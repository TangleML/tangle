"""Tests for opt-in retries of infrastructure-disrupted Kubernetes Job pods."""

import pytest
from kubernetes import client as k8s_client_lib

from cloud_pipelines_backend.launchers import interfaces
from cloud_pipelines_backend.launchers import kubernetes_launchers


def _job_spec() -> k8s_client_lib.V1JobSpec:
    return k8s_client_lib.V1JobSpec(
        completion_mode="Indexed",
        completions=1,
        max_failed_indexes=0,
        parallelism=1,
        template=k8s_client_lib.V1PodTemplateSpec(
            spec=k8s_client_lib.V1PodSpec(
                containers=[k8s_client_lib.V1Container(name="main", image="python")],
                restart_policy="Never",
            )
        ),
    )


def test_default_keeps_zero_retry_job_contract() -> None:
    spec = _job_spec()

    kubernetes_launchers._configure_job_disruption_retries(spec, annotations=None)

    assert spec.backoff_limit_per_index == 0
    assert spec.pod_failure_policy is None
    assert spec.pod_replacement_policy is None


def test_opt_in_counts_disruptions_but_fails_user_code_immediately() -> None:
    spec = _job_spec()

    kubernetes_launchers._configure_job_disruption_retries(
        spec,
        annotations={kubernetes_launchers.JOB_DISRUPTION_RETRIES_ANNOTATION_KEY: "1"},
    )

    assert spec.backoff_limit_per_index == 1
    assert spec.pod_replacement_policy == "Failed"
    assert spec.pod_failure_policy is not None
    disruption_rule, user_code_rule = spec.pod_failure_policy.rules

    assert disruption_rule.action == "Count"
    assert disruption_rule.on_exit_codes is None
    assert len(disruption_rule.on_pod_conditions) == 1
    disruption_condition = disruption_rule.on_pod_conditions[0]
    assert disruption_condition.type == "DisruptionTarget"
    assert disruption_condition.status == "True"

    assert user_code_rule.action == "FailIndex"
    assert user_code_rule.on_pod_conditions is None
    assert user_code_rule.on_exit_codes.container_name == "main"
    assert user_code_rule.on_exit_codes.operator == "NotIn"
    assert user_code_rule.on_exit_codes.values == [0]

    serialized = kubernetes_launchers._kubernetes_serialize(spec)
    assert serialized["backoffLimitPerIndex"] == 1
    assert serialized["maxFailedIndexes"] == 0
    assert serialized["podReplacementPolicy"] == "Failed"
    assert serialized["podFailurePolicy"]["rules"] == [
        {
            "action": "Count",
            "onPodConditions": [{"status": "True", "type": "DisruptionTarget"}],
        },
        {
            "action": "FailIndex",
            "onExitCodes": {
                "containerName": "main",
                "operator": "NotIn",
                "values": [0],
            },
        },
    ]


@pytest.mark.parametrize("value", ["-1", "11", "1.0", "true", ""])
def test_invalid_disruption_retry_count_fails_closed(value: str) -> None:
    with pytest.raises(
        interfaces.LauncherError, match="expected an integer between 0 and 10"
    ):
        kubernetes_launchers._configure_job_disruption_retries(
            _job_spec(),
            annotations={
                kubernetes_launchers.JOB_DISRUPTION_RETRIES_ANNOTATION_KEY: value
            },
        )
