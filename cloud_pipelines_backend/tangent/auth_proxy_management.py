"""Read and update the auth proxy config of a Tangent instance.

The config (see `auth_proxy_mitmproxy_addon.py` for its schema) lives in the per-instance
Kubernetes Secret under the `auth_proxy_config.yaml` key. Changes reach the running proxy
without restarting the pod: kubelet refreshes the mounted Secret (within about a minute)
and the proxy addon reloads the file once it changes.

The management API exposes user-managed rules and secrets. Platform-managed rules and
credentials live in separate `system_rules` and `system_secrets` sections. This module
preserves but never exposes them, and user rules cannot reference them.

Secret values can be set and deleted, but never read back. Rules can carry inline constants,
but anything sensitive belongs in a secret. Every write validates the resulting config, so a
missing secret reference is rejected instead of breaking the running proxy.
"""

import base64
import copy
import typing

import yaml
from kubernetes import client as k8s_client_lib

from . import auth_proxy_mitmproxy_addon, instance_management

# ! When exposing via routes, verify that the user owns the instance


# region Config document helpers (pure functions)
def validate_config(config: dict) -> None:
    """Raises `ValueError` if the proxy would fail to load the config."""
    auth_proxy_mitmproxy_addon.parse_config(config)


def _manageable_rules(config: dict) -> list[dict]:
    """Returns rules that cannot expose credentials from a legacy config."""
    rules = config.get("rules") or []
    if any(isinstance(rule, dict) and "add_headers" in rule for rule in rules):
        raise ValueError(
            "Legacy rules with inline add_headers cannot be managed; replace all rules with handler-based rules first"
        )
    return rules


def with_rule(config: dict, rule: dict) -> dict:
    """Adds a rule or replaces the rule with the same `id`. New rules are appended last."""
    rule_id = rule.get("id")
    if not rule_id:
        raise ValueError("The rule needs an 'id' to be addressable")
    updated = copy.deepcopy(config)
    rules = updated.setdefault("rules", [])
    for index, existing_rule in enumerate(rules):
        if isinstance(existing_rule, dict) and existing_rule.get("id") == rule_id:
            rules[index] = copy.deepcopy(rule)
            break
    else:
        rules.append(copy.deepcopy(rule))
    return updated


def without_rule(config: dict, rule_id: str) -> dict:
    updated = copy.deepcopy(config)
    rules = updated.get("rules") or []
    kept_rules = [
        rule
        for rule in rules
        if not (isinstance(rule, dict) and rule.get("id") == rule_id)
    ]
    if len(kept_rules) == len(rules):
        raise KeyError(f"There is no rule with id {rule_id!r}")
    updated["rules"] = kept_rules
    return updated


def with_secret(config: dict, name: str, value: str) -> dict:
    updated = copy.deepcopy(config)
    updated.setdefault("secrets", {})[name] = {"value": value}
    return updated


def without_secret(config: dict, name: str) -> dict:
    updated = copy.deepcopy(config)
    if name not in (updated.get("secrets") or {}):
        raise KeyError(f"There is no secret named {name!r}")
    del updated["secrets"][name]
    return updated


# endregion


# region Rules API
# GET /api/tangent/instances/<id>/auth_proxy/rules
def get_rules(
    *,
    api_client: k8s_client_lib.ApiClient,
    instance_id: str,
    kubernetes_namespace: str = instance_management.DEFAULT_NAMESPACE,
) -> list[dict]:
    return _manageable_rules(
        _read_config(
            api_client=api_client,
            instance_id=instance_id,
            kubernetes_namespace=kubernetes_namespace,
        )
    )


# PUT /api/tangent/instances/<id>/auth_proxy/rules
def set_rules(
    *,
    api_client: k8s_client_lib.ApiClient,
    instance_id: str,
    rules: list[dict],
    kubernetes_namespace: str = instance_management.DEFAULT_NAMESPACE,
) -> list[dict]:
    """Replaces all rules. The secrets they reference are left untouched."""
    _manageable_rules({"rules": rules})
    return _update_config(
        api_client=api_client,
        instance_id=instance_id,
        kubernetes_namespace=kubernetes_namespace,
        update=lambda config: {**config, "rules": copy.deepcopy(rules)},
    )["rules"]


# PUT /api/tangent/instances/<id>/auth_proxy/rules/<rule_id>
def set_rule(
    *,
    api_client: k8s_client_lib.ApiClient,
    instance_id: str,
    rule: dict,
    kubernetes_namespace: str = instance_management.DEFAULT_NAMESPACE,
) -> list[dict]:
    """Adds or replaces a single rule, leaving the other rules alone."""
    _manageable_rules({"rules": [rule]})

    def update(config: dict) -> dict:
        _manageable_rules(config)
        return with_rule(config, rule)

    return _update_config(
        api_client=api_client,
        instance_id=instance_id,
        kubernetes_namespace=kubernetes_namespace,
        update=update,
    )["rules"]


# DELETE /api/tangent/instances/<id>/auth_proxy/rules/<rule_id>
def delete_rule(
    *,
    api_client: k8s_client_lib.ApiClient,
    instance_id: str,
    rule_id: str,
    kubernetes_namespace: str = instance_management.DEFAULT_NAMESPACE,
) -> list[dict]:
    def update(config: dict) -> dict:
        _manageable_rules(config)
        return without_rule(config, rule_id)

    return _update_config(
        api_client=api_client,
        instance_id=instance_id,
        kubernetes_namespace=kubernetes_namespace,
        update=update,
    )["rules"]


# endregion


# region Secrets API (write-only: values can be set and deleted, but never read)
# GET /api/tangent/instances/<id>/auth_proxy/secrets
def get_secret_names(
    *,
    api_client: k8s_client_lib.ApiClient,
    instance_id: str,
    kubernetes_namespace: str = instance_management.DEFAULT_NAMESPACE,
) -> list[str]:
    config = _read_config(
        api_client=api_client,
        instance_id=instance_id,
        kubernetes_namespace=kubernetes_namespace,
    )
    return sorted(config.get("secrets") or {})


# PUT /api/tangent/instances/<id>/auth_proxy/secrets/<name>
def set_secret(
    *,
    api_client: k8s_client_lib.ApiClient,
    instance_id: str,
    name: str,
    value: str,
    kubernetes_namespace: str = instance_management.DEFAULT_NAMESPACE,
) -> list[str]:
    config = _update_config(
        api_client=api_client,
        instance_id=instance_id,
        kubernetes_namespace=kubernetes_namespace,
        update=lambda config: with_secret(config, name, value),
    )
    return sorted(config["secrets"])


# DELETE /api/tangent/instances/<id>/auth_proxy/secrets/<name>
def delete_secret(
    *,
    api_client: k8s_client_lib.ApiClient,
    instance_id: str,
    name: str,
    kubernetes_namespace: str = instance_management.DEFAULT_NAMESPACE,
) -> list[str]:
    """Fails if a rule still references the secret, since the proxy could not load that config."""
    config = _update_config(
        api_client=api_client,
        instance_id=instance_id,
        kubernetes_namespace=kubernetes_namespace,
        update=lambda config: without_secret(config, name),
    )
    return sorted(config.get("secrets") or {})


# endregion


def _read_config(
    *,
    api_client: k8s_client_lib.ApiClient,
    instance_id: str,
    kubernetes_namespace: str,
) -> dict:
    """Returns the whole stored config, secret values included. Never expose it as it is."""
    _, config = _read_secret_and_config(
        api_client=api_client,
        instance_id=instance_id,
        kubernetes_namespace=kubernetes_namespace,
    )
    return config


def _read_secret_and_config(
    *,
    api_client: k8s_client_lib.ApiClient,
    instance_id: str,
    kubernetes_namespace: str,
) -> tuple[k8s_client_lib.V1Secret, dict]:
    core_client = k8s_client_lib.CoreV1Api(api_client=api_client)
    secret = core_client.read_namespaced_secret(
        name=instance_management.make_resource_name(instance_id),
        namespace=kubernetes_namespace,
    )
    # Kubernetes returns Secret data base64-encoded.
    encoded_config = (secret.data or {}).get(
        instance_management.PROXY_CONFIG_SECRET_KEY
    )
    if not encoded_config:
        return secret, {}
    return secret, yaml.safe_load(base64.b64decode(encoded_config)) or {}


def _update_config(
    *,
    api_client: k8s_client_lib.ApiClient,
    instance_id: str,
    kubernetes_namespace: str,
    update: typing.Callable[[dict], dict],
) -> dict:
    """Conditionally read-modify-writes the config and returns the stored config."""
    secret, stored_config = _read_secret_and_config(
        api_client=api_client,
        instance_id=instance_id,
        kubernetes_namespace=kubernetes_namespace,
    )
    config = update(stored_config)
    validate_config(config)

    resource_version = secret.metadata and secret.metadata.resource_version
    if not resource_version:
        raise RuntimeError("The instance Secret has no resourceVersion")

    core_client = k8s_client_lib.CoreV1Api(api_client=api_client)
    core_client.patch_namespaced_secret(
        name=instance_management.make_resource_name(instance_id),
        namespace=kubernetes_namespace,
        # Including the read resourceVersion makes this patch fail with 409 if another writer
        # changed the Secret. `string_data` leaves every other key of the Secret untouched.
        body=k8s_client_lib.V1Secret(
            metadata=k8s_client_lib.V1ObjectMeta(resource_version=resource_version),
            string_data={
                instance_management.PROXY_CONFIG_SECRET_KEY: yaml.safe_dump(
                    config, sort_keys=False
                )
            },
        ),
    )
    return config
