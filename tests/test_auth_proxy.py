"""Tests for the auth proxy config format, its handlers, the management helpers and the routes."""

import base64
import copy
import pathlib
import types

import fastapi
import pytest
import yaml
from kubernetes import client as k8s_client_lib
from starlette import testclient

from cloud_pipelines_backend.tangent import (
    auth_proxy_management,
    auth_proxy_management_routes,
    auth_proxy_mitmproxy_addon,
    instance_management,
)

_EXAMPLE_CONFIG_PATH = (
    pathlib.Path(__file__).parents[1]
    / "cloud_pipelines_backend"
    / "tangent"
    / "auth_proxy_config.example.yaml"
)


def _make_flow(url: str, headers: dict[str, str] | None = None):
    """A real mitmproxy flow. The addon only duck-types it, but the real API keeps us honest."""
    tflow = pytest.importorskip("mitmproxy.test.tflow")
    flow = tflow.tflow(resp=True)
    flow.request.url = url
    for name, value in (headers or {}).items():
        flow.request.headers[name] = value
    return flow


def _apply(config: dict, url: str, headers: dict[str, str] | None = None):
    """Applies a config to a request and returns the resulting request."""
    rules = auth_proxy_mitmproxy_addon.parse_config(config)
    flow = _make_flow(url, headers)
    for rule in rules:
        if rule.matches(flow.request.pretty_url):
            for handler in rule.handlers:
                handler.apply(flow.request)
    return flow.request


def _rule(*handlers: dict, url_pattern: str | None = None) -> dict:
    return {
        "id": "rule1",
        "url_pattern": url_pattern,
        "handlers": list(handlers),
    }


# region Config parsing
def test_example_config_is_valid():
    config = yaml.safe_load(_EXAMPLE_CONFIG_PATH.read_text())
    (rule,) = auth_proxy_mitmproxy_addon.parse_config(config)
    assert rule.id == "rule1"
    assert len(rule.handlers) == 6


def test_empty_config():
    assert auth_proxy_mitmproxy_addon.parse_config(None) == ()
    assert auth_proxy_mitmproxy_addon.parse_config({}) == ()
    assert (
        auth_proxy_mitmproxy_addon.parse_config({"secrets": None, "rules": None}) == ()
    )


def test_rule_matching_ignores_the_scheme_and_matches_prefixes():
    rule = auth_proxy_mitmproxy_addon.Rule(
        id=None, url_pattern="api.openai.com/v1", handlers=()
    )
    assert rule.matches("https://api.openai.com/v1/chat/completions")
    assert rule.matches("http://api.openai.com/v1")
    assert rule.matches("api.openai.com/v1")
    assert not rule.matches("https://api.openai.com/v2")
    assert not rule.matches("https://api.openai.com/v10")
    assert not rule.matches("https://api.openai.com.attacker.example/v1")
    assert not rule.matches("https://api.openai.com@attacker.example/v1")
    assert not rule.matches("https://api.openai.com:8443/v1")
    assert not rule.matches("https://evil.com/?q=api.openai.com/v1")


def test_a_rule_without_a_url_pattern_matches_everything():
    rule = auth_proxy_mitmproxy_addon.Rule(id=None, url_pattern=None, handlers=())
    assert rule.matches("https://example.com/")


@pytest.mark.parametrize(
    "config, expected_error",
    [
        ({"rulez": []}, "unsupported key"),
        ({"rules": [{"url_patternn": "x"}]}, "unsupported key"),
        ({"rules": [{"handlers": [{"nope": {}}]}]}, "unsupported handler"),
        (
            {"rules": [{"handlers": [{"set_header": {}, "set_cookie": {}}]}]},
            "single key",
        ),
        (
            {"rules": [{"handlers": [{"url_replace": {"old_substring": "a"}}]}]},
            "missing required key",
        ),
        (
            {
                "rules": [
                    {
                        "handlers": [
                            {
                                "url_regexp_replace": {
                                    "pattern": "([",
                                    "replacement": "",
                                }
                            }
                        ]
                    }
                ]
            },
            "invalid regular",
        ),
        (
            {"rules": [{"handlers": [{"set_bearer_auth_header": {}}]}]},
            "missing required key",
        ),
        # A value is a string or a secret reference, so a bare YAML number has to be quoted.
        (
            {"rules": [{"handlers": [{"set_bearer_auth_header": {"value": 1}}]}]},
            "expected a string or",
        ),
        (
            {"rules": [{"handlers": [{"set_bearer_auth_header": {"value": {}}}]}]},
            "missing required key",
        ),
        (
            {
                "rules": [
                    {
                        "handlers": [
                            {"set_bearer_auth_header": {"value": {"secret": {}}}}
                        ]
                    }
                ]
            },
            "missing required key",
        ),
        (
            {
                "rules": [
                    {
                        "handlers": [
                            {
                                "set_bearer_auth_header": {
                                    "value": {"secret": {"name": "nope"}}
                                }
                            }
                        ]
                    }
                ]
            },
            "undefined secret",
        ),
        (
            {"rules": [{"handlers": [{"set_cookie": {"value": "a"}}]}]},
            "missing required key",
        ),
        # The combined `value` and the separate halves are two spellings, not to be mixed.
        (
            {
                "rules": [
                    {
                        "handlers": [
                            {
                                "set_basic_auth_header": {
                                    "username": "a",
                                    "value": "b",
                                }
                            }
                        ]
                    }
                ]
            },
            "unsupported key",
        ),
        (
            {
                "rules": [
                    {
                        "handlers": [
                            {"set_basic_auth_header": {"password": {"valu": "a"}}}
                        ]
                    }
                ]
            },
            "unsupported key",
        ),
        (
            {"rules": [{"handlers": [{"set_basic_auth_header": {"usernamee": "a"}}]}]},
            "unsupported key",
        ),
        ({"rules": [{"replacement_pattern": "b"}]}, "requires 'url_pattern'"),
        ({"secrets": {"secret1": {"valu": "x"}}}, "unsupported key"),
        # A secret holds its value inline, so it cannot reference another secret.
        (
            {"secrets": {"secret1": {"value": {"secret": {"name": "secret1"}}}}},
            "expected a string",
        ),
        ({"rules": {}}, "expected a list"),
        ({"rules": [{"handlers": {}}]}, "expected a list"),
        ({"rules": [{"url_pattern": 1}]}, "expected a string"),
    ],
)
def test_invalid_configs_are_rejected(config: dict, expected_error: str):
    with pytest.raises(ValueError, match=expected_error):
        auth_proxy_mitmproxy_addon.parse_config(config)


def test_secret_values_are_never_included_in_error_messages():
    config = {
        "secrets": {"secret1": {"value": "hunter2"}},
        "rules": [
            _rule({"set_bearer_auth_header": {"value": {"secret": {"name": "typo"}}}})
        ],
    }
    with pytest.raises(ValueError) as error_info:
        auth_proxy_mitmproxy_addon.parse_config(config)
    assert "secret1" in str(error_info.value)
    assert "hunter2" not in str(error_info.value)


def test_user_rules_cannot_reference_system_secrets():
    config = {
        "system_secrets": {"platform_token": {"value": "platform-token-value"}},
        "rules": [
            _rule(
                {
                    "set_bearer_auth_header": {
                        "value": {"secret": {"name": "platform_token"}}
                    }
                }
            )
        ],
    }
    with pytest.raises(ValueError, match="undefined secret 'platform_token'"):
        auth_proxy_mitmproxy_addon.parse_config(config)


def test_user_rules_run_before_system_rules():
    config = {
        "system_secrets": {"platform_token": {"value": "platform-token-value"}},
        "system_rules": [
            {
                "id": "platform",
                "url_pattern": "trusted.example",
                "handlers": [
                    {
                        "set_bearer_auth_header": {
                            "value": {"secret": {"name": "platform_token"}}
                        }
                    }
                ],
            }
        ],
        "rules": [
            _rule(
                {
                    "url_replace": {
                        "old_substring": "trusted.example",
                        "new_substring": "attacker.example",
                    }
                },
                url_pattern="trusted.example",
            )
        ],
    }
    request = types.SimpleNamespace(
        url="https://trusted.example/path",
        headers={},
    )
    for rule in auth_proxy_mitmproxy_addon.parse_config(config):
        if rule.matches(request.url):
            for handler in rule.handlers:
                handler.apply(request)

    assert request.url == "https://attacker.example/path"
    assert "Authorization" not in request.headers


# endregion


# region Handlers
def test_url_replace():
    config = {
        "rules": [
            _rule(
                {
                    "url_replace": {
                        "old_substring": "api.openai.com",
                        "new_substring": "ai-proxy",
                    }
                }
            )
        ]
    }
    assert (
        _apply(config, "https://api.openai.com/v1/models").url
        == "https://ai-proxy/v1/models"
    )


def test_url_regexp_replace():
    config = {
        "rules": [
            _rule(
                {
                    "url_regexp_replace": {
                        "pattern": r"/v(\d+)/",
                        "replacement": r"/api/v\1/",
                    }
                }
            )
        ],
    }
    assert (
        _apply(config, "https://host1/v2/models").url == "https://host1/api/v2/models"
    )


def test_handlers_are_applied_in_order():
    config = {
        "rules": [
            _rule(
                {
                    "url_replace": {
                        "old_substring": "host1",
                        "new_substring": "host2",
                    }
                },
                {
                    "url_replace": {
                        "old_substring": "host2",
                        "new_substring": "host3",
                    }
                },
            )
        ]
    }
    assert _apply(config, "https://host1/path1").url == "https://host3/path1"


def test_set_header():
    config = {
        "rules": [_rule({"set_header": {"name": "X-Header1", "value": "value1"}})]
    }
    assert _apply(config, "https://host1/").headers["X-Header1"] == "value1"


def test_set_bearer_auth_header():
    config = {"rules": [_rule({"set_bearer_auth_header": {"value": "token1"}})]}
    assert _apply(config, "https://host1/").headers["Authorization"] == "Bearer token1"


def test_set_basic_auth_header_encodes_combined_credentials():
    config = {"rules": [_rule({"set_basic_auth_header": {"value": "user1:password1"}})]}
    # base64("user1:password1")
    assert (
        _apply(config, "https://host1/").headers["Authorization"]
        == "Basic dXNlcjE6cGFzc3dvcmQx"
    )


def test_set_basic_auth_header_passes_encoded_credentials_through():
    config = {
        "rules": [_rule({"set_basic_auth_header": {"value": "dXNlcjE6cGFzc3dvcmQx"}})]
    }
    assert (
        _apply(config, "https://host1/").headers["Authorization"]
        == "Basic dXNlcjE6cGFzc3dvcmQx"
    )


def test_set_basic_auth_header_takes_each_half_separately():
    """The common case: a constant username, and only the password worth keeping secret."""
    config = {
        "secrets": {"password1": {"value": "password1"}},
        "rules": [
            _rule(
                {
                    "set_basic_auth_header": {
                        "username": "user1",
                        "password": {"secret": {"name": "password1"}},
                    }
                }
            )
        ],
    }
    # base64("user1:password1")
    assert (
        _apply(config, "https://host1/").headers["Authorization"]
        == "Basic dXNlcjE6cGFzc3dvcmQx"
    )


def test_set_basic_auth_header_halves_can_come_from_different_secrets():
    config = {
        "secrets": {
            "user1": {"value": "user1"},
            "password1": {"value": "password1"},
        },
        "rules": [
            _rule(
                {
                    "set_basic_auth_header": {
                        "username": {"secret": {"name": "user1"}},
                        "password": {"secret": {"name": "password1"}},
                    }
                }
            )
        ],
    }
    assert (
        _apply(config, "https://host1/").headers["Authorization"]
        == "Basic dXNlcjE6cGFzc3dvcmQx"
    )


def test_set_basic_auth_header_takes_constant_halves():
    """Neither half has to be a secret, so a config can carry both inline."""
    config = {
        "rules": [
            _rule(
                {
                    "set_basic_auth_header": {
                        "username": "user1",
                        "password": "password1",
                    }
                }
            )
        ]
    }
    assert (
        _apply(config, "https://host1/").headers["Authorization"]
        == "Basic dXNlcjE6cGFzc3dvcmQx"
    )


@pytest.mark.parametrize(
    ("spec", "expected_credentials"),
    [
        ({"username": "key1"}, "key1:"),
        ({"username": "key1", "password": None}, "key1:"),
        ({"password": "password1"}, ":password1"),
    ],
    ids=["no password key", "empty password", "no username key"],
)
def test_set_basic_auth_header_leaves_a_missing_half_empty(
    spec: dict, expected_credentials: str
):
    """An API key as the username with an empty password is a common scheme."""
    config = {"rules": [_rule({"set_basic_auth_header": spec})]}
    expected = base64.b64encode(expected_credentials.encode()).decode()
    assert (
        _apply(config, "https://host1/").headers["Authorization"] == f"Basic {expected}"
    )


def test_a_username_containing_a_colon_is_rejected():
    """A ":" in the username would move the split point and change who we authenticate as."""
    config = {
        "secrets": {"user1": {"value": "user1:sneaky1"}},
        "rules": [
            _rule(
                {"set_basic_auth_header": {"username": {"secret": {"name": "user1"}}}}
            )
        ],
    }
    with pytest.raises(ValueError, match="username cannot contain") as error_info:
        auth_proxy_mitmproxy_addon.parse_config(config)
    assert "sneaky1" not in str(error_info.value)


def test_set_cookie_keeps_the_other_cookies():
    config = {"rules": [_rule({"set_cookie": {"name": "cookie2", "value": "new2"}})]}
    request = _apply(
        config,
        "https://host1/",
        {"Cookie": "cookie1=value1; cookie2=old2; cookie3=value3"},
    )
    assert request.headers["Cookie"] == "cookie1=value1; cookie3=value3; cookie2=new2"
    assert dict(request.cookies) == {
        "cookie1": "value1",
        "cookie3": "value3",
        "cookie2": "new2",
    }


def test_set_cookie_without_existing_cookies():
    config = {"rules": [_rule({"set_cookie": {"name": "cookie1", "value": "value1"}})]}
    assert _apply(config, "https://host1/").headers["Cookie"] == "cookie1=value1"


def test_only_matching_rules_are_applied():
    config = {
        "rules": [
            {
                "id": "rule1",
                "url_pattern": "host1",
                "handlers": [{"set_header": {"name": "X-1", "value": "1"}}],
            },
            {
                "id": "rule2",
                "url_pattern": "host2",
                "handlers": [{"set_header": {"name": "X-2", "value": "2"}}],
            },
        ]
    }
    headers = _apply(config, "https://host1/").headers
    assert headers["X-1"] == "1"
    assert "X-2" not in headers


def test_secret_references_are_resolved():
    config = {
        "secrets": {"token1": {"value": "secret_value1"}},
        "rules": [
            _rule(
                {"set_bearer_auth_header": {"value": {"secret": {"name": "token1"}}}},
                {
                    "set_cookie": {
                        "name": "cookie1",
                        "value": {"secret": {"name": "token1"}},
                    }
                },
            )
        ],
    }
    request = _apply(config, "https://host1/")
    assert request.headers["Authorization"] == "Bearer secret_value1"
    assert request.headers["Cookie"] == "cookie1=secret_value1"


def test_deprecated_v1_rule_keys_still_work():
    config = {
        "rules": [
            {
                "url_pattern": "api.openai.com/v1",
                "replacement_pattern": "proxy.example.com/v1",
                "add_headers": {"Authorization": "Bearer token1"},
            }
        ]
    }
    request = _apply(config, "https://api.openai.com/v1/models")
    assert request.url == "https://proxy.example.com/v1/models"
    assert request.headers["Authorization"] == "Bearer token1"


# endregion


# region Addon
def _write_config(path: pathlib.Path, header_value: str) -> None:
    config = {
        "rules": [
            {
                "id": "rule1",
                "handlers": [{"set_header": {"name": "X-1", "value": header_value}}],
            }
        ]
    }
    path.write_text(yaml.safe_dump(config))


def test_addon_marks_requests_and_responses(tmp_path: pathlib.Path):
    config_path = tmp_path / "auth_proxy_config.yaml"
    _write_config(config_path, "value1")
    addon = auth_proxy_mitmproxy_addon.AuthProxyAddon(config_path=str(config_path))

    flow = _make_flow("https://host1/")
    addon.request(flow)
    assert flow.request.headers["X-1"] == "value1"
    assert (
        flow.request.headers[auth_proxy_mitmproxy_addon.PROXY_MARKER_HEADER_NAME]
        == "true"
    )

    addon.response(flow)
    assert (
        flow.response.headers[auth_proxy_mitmproxy_addon.PROXY_MARKER_HEADER_NAME]
        == "true"
    )


def test_addon_fails_to_start_on_an_invalid_config(tmp_path: pathlib.Path):
    config_path = tmp_path / "auth_proxy_config.yaml"
    config_path.write_text(yaml.safe_dump({"rules": [{"handlers": [{"nope": {}}]}]}))
    with pytest.raises(ValueError, match="unsupported handler"):
        auth_proxy_mitmproxy_addon.AuthProxyAddon(config_path=str(config_path))


def test_addon_reloads_a_changed_config(tmp_path: pathlib.Path):
    config_path = tmp_path / "auth_proxy_config.yaml"
    _write_config(config_path, "value1")
    addon = auth_proxy_mitmproxy_addon.AuthProxyAddon(
        config_path=str(config_path), reload_interval_seconds=0
    )

    _write_config(config_path, "value2")
    flow = _make_flow("https://host1/")
    addon.request(flow)
    assert flow.request.headers["X-1"] == "value2"


def test_addon_keeps_the_previous_rules_when_a_reload_fails(
    tmp_path: pathlib.Path,
):
    config_path = tmp_path / "auth_proxy_config.yaml"
    _write_config(config_path, "value1")
    addon = auth_proxy_mitmproxy_addon.AuthProxyAddon(
        config_path=str(config_path), reload_interval_seconds=0
    )

    config_path.write_text("rules: [{handlers: [{nope: {}}]}]")
    flow = _make_flow("https://host1/")
    addon.request(flow)
    assert flow.request.headers["X-1"] == "value1"


# endregion


# region Management helpers
_STORED_CONFIG = {
    "secrets": {"secret1": {"value": "secret_value1"}},
    "rules": [
        {
            "id": "rule1",
            "url_pattern": "host1",
            "handlers": [
                {"set_bearer_auth_header": {"value": {"secret": {"name": "secret1"}}}}
            ],
        },
        {
            "id": "rule2",
            "url_pattern": "host2",
            "handlers": [{"set_header": {"name": "X-1", "value": "constant1"}}],
        },
    ],
}


def test_rules_never_contain_secret_values():
    """What the rules API hands out and takes back: secret references, not credentials."""
    assert "secret_value1" not in yaml.safe_dump(_STORED_CONFIG["rules"])


def test_with_rule_replaces_a_rule_by_id_and_keeps_its_position():
    updated = auth_proxy_management.with_rule(
        _STORED_CONFIG, {"id": "rule1", "url_pattern": "host3"}
    )
    assert [rule["id"] for rule in updated["rules"]] == ["rule1", "rule2"]
    assert updated["rules"][0] == {"id": "rule1", "url_pattern": "host3"}
    # The secrets and the other rules are left alone, so editing a rule needs no credentials.
    assert updated["secrets"] == _STORED_CONFIG["secrets"]
    assert updated["rules"][1] == _STORED_CONFIG["rules"][1]
    assert _STORED_CONFIG["rules"][0]["url_pattern"] == "host1"


def test_with_rule_appends_a_new_rule():
    updated = auth_proxy_management.with_rule(_STORED_CONFIG, {"id": "rule3"})
    assert [rule["id"] for rule in updated["rules"]] == [
        "rule1",
        "rule2",
        "rule3",
    ]


def test_with_rule_requires_an_id():
    with pytest.raises(ValueError, match="needs an 'id'"):
        auth_proxy_management.with_rule(_STORED_CONFIG, {"url_pattern": "host3"})


def test_without_rule():
    updated = auth_proxy_management.without_rule(_STORED_CONFIG, "rule1")
    assert [rule["id"] for rule in updated["rules"]] == ["rule2"]
    with pytest.raises(KeyError, match="rule3"):
        auth_proxy_management.without_rule(_STORED_CONFIG, "rule3")


def test_with_and_without_secret():
    updated = auth_proxy_management.with_secret(
        _STORED_CONFIG, "secret2", "secret_value2"
    )
    assert updated["secrets"]["secret2"] == {"value": "secret_value2"}
    assert updated["rules"] == _STORED_CONFIG["rules"]
    assert auth_proxy_management.without_secret(updated, "secret2") == _STORED_CONFIG
    with pytest.raises(KeyError, match="secret3"):
        auth_proxy_management.without_secret(_STORED_CONFIG, "secret3")


def test_a_secret_that_is_still_referenced_cannot_be_removed():
    with pytest.raises(ValueError, match="undefined secret"):
        auth_proxy_management.validate_config(
            auth_proxy_management.without_secret(_STORED_CONFIG, "secret1")
        )


def test_a_rule_referencing_an_undefined_secret_is_rejected():
    rule = {
        "id": "rule3",
        "handlers": [
            {"set_bearer_auth_header": {"value": {"secret": {"name": "nope"}}}}
        ],
    }
    with pytest.raises(ValueError, match="undefined secret"):
        auth_proxy_management.validate_config(
            auth_proxy_management.with_rule(_STORED_CONFIG, rule)
        )


# endregion


# region Routes
_INSTANCE_ID = "instance1"
_OWNER = "owner1@example.com"
_API = f"/api/tangent/instances/{_INSTANCE_ID}/auth_proxy"


class _FakeCluster:
    """The Secret and the StatefulSet of one instance, as far as the routes touch them."""

    def __init__(self):
        self.instance_id = _INSTANCE_ID
        self.created_by = _OWNER
        self.config = copy.deepcopy(_STORED_CONFIG)
        self.resource_version = 1
        self.concurrent_config_before_next_patch: dict | None = None

    def resource_name_of(self, name: str) -> bool:
        return name == instance_management.make_resource_name(self.instance_id)


class _FakeCoreV1Api:
    def __init__(self, *, api_client: _FakeCluster):
        self._cluster = api_client

    def read_namespaced_secret(self, *, name: str, namespace: str):
        assert self._cluster.resource_name_of(
            name
        ), f"read the Secret of another instance: {name}"
        encoded = base64.b64encode(yaml.safe_dump(self._cluster.config).encode())
        return types.SimpleNamespace(
            data={instance_management.PROXY_CONFIG_SECRET_KEY: encoded},
            metadata=types.SimpleNamespace(
                resource_version=str(self._cluster.resource_version)
            ),
        )

    def patch_namespaced_secret(self, *, name: str, namespace: str, body):
        assert self._cluster.resource_name_of(
            name
        ), f"wrote the Secret of another instance: {name}"
        # Only the config key is ever written, so the other keys of the Secret survive.
        assert list(body.string_data) == [instance_management.PROXY_CONFIG_SECRET_KEY]

        concurrent_config = self._cluster.concurrent_config_before_next_patch
        if concurrent_config is not None:
            self._cluster.config = copy.deepcopy(concurrent_config)
            self._cluster.resource_version += 1
            self._cluster.concurrent_config_before_next_patch = None

        if body.metadata.resource_version != str(self._cluster.resource_version):
            raise k8s_client_lib.ApiException(status=409)

        self._cluster.config = yaml.safe_load(
            body.string_data[instance_management.PROXY_CONFIG_SECRET_KEY]
        )
        self._cluster.resource_version += 1


class _FakeAppsV1Api:
    def __init__(self, *, api_client: _FakeCluster):
        self._cluster = api_client

    def read_namespaced_stateful_set(self, *, name: str, namespace: str):
        if not self._cluster.resource_name_of(name):
            raise k8s_client_lib.ApiException(status=404)
        annotations = {
            instance_management.INSTANCE_CREATED_BY_ANNOTATION_NAME: self._cluster.created_by
        }
        metadata = types.SimpleNamespace(annotations=annotations)
        return types.SimpleNamespace(
            spec=types.SimpleNamespace(
                template=types.SimpleNamespace(metadata=metadata)
            )
        )


@pytest.fixture
def cluster(monkeypatch: pytest.MonkeyPatch) -> _FakeCluster:
    monkeypatch.setattr(k8s_client_lib, "CoreV1Api", _FakeCoreV1Api)
    monkeypatch.setattr(k8s_client_lib, "AppsV1Api", _FakeAppsV1Api)
    return _FakeCluster()


def _client(
    cluster: _FakeCluster, *, user_name: str | None = _OWNER
) -> testclient.TestClient:
    app = fastapi.FastAPI()
    app.include_router(
        auth_proxy_management_routes.build_api_router(
            get_user_name=lambda: user_name,
            kubernetes_client=cluster,
            kubernetes_namespace="namespace1",
        )
    )
    return testclient.TestClient(app)


def test_rules_can_be_read_and_edited(cluster: _FakeCluster):
    client = _client(cluster)

    assert [rule["id"] for rule in client.get(f"{_API}/rules").json()["rules"]] == [
        "rule1",
        "rule2",
    ]

    # The id comes from the URL, so the body does not have to repeat it.
    response = client.put(f"{_API}/rules/rule3", json={"url_pattern": "host3"})
    assert response.status_code == 200
    assert response.json()["rules"][-1] == {
        "url_pattern": "host3",
        "id": "rule3",
    }

    delete_response = client.delete(f"{_API}/rules/rule1")
    assert delete_response.json()["rules"] == [rule for rule in cluster.config["rules"]]
    assert [rule["id"] for rule in cluster.config["rules"]] == [
        "rule2",
        "rule3",
    ]


def test_replacing_all_rules_leaves_the_secrets_alone(cluster: _FakeCluster):
    rules = [
        {
            "id": "rule1",
            "handlers": [
                {"set_bearer_auth_header": {"value": {"secret": {"name": "secret1"}}}}
            ],
        }
    ]
    response = _client(cluster).put(f"{_API}/rules", json={"rules": rules})
    assert response.status_code == 200
    assert response.json()["rules"] == rules
    assert cluster.config["secrets"] == _STORED_CONFIG["secrets"]


def test_system_config_is_hidden_and_preserved(cluster: _FakeCluster):
    cluster.config["system_secrets"] = {
        "platform_token": {"value": "platform-token-value"}
    }
    cluster.config["system_rules"] = [
        {"id": "platform", "url_pattern": "platform.example"}
    ]
    client = _client(cluster)

    rules_response = client.get(f"{_API}/rules")
    secrets_response = client.get(f"{_API}/secrets")
    assert "platform" not in rules_response.text
    assert "platform_token" not in secrets_response.text

    client.put(f"{_API}/rules", json={"rules": [{"id": "replacement"}]})
    assert cluster.config["system_rules"][0]["id"] == "platform"
    assert cluster.config["system_secrets"]["platform_token"]["value"] == (
        "platform-token-value"
    )


def test_legacy_inline_headers_are_not_returned_and_can_be_replaced(
    cluster: _FakeCluster,
):
    cluster.config = {
        "rules": [
            {
                "id": "legacy",
                "add_headers": {"Authorization": "Bearer legacy-secret-value"},
            }
        ]
    }
    client = _client(cluster)

    get_response = client.get(f"{_API}/rules")
    assert get_response.status_code == 400
    assert "legacy-secret-value" not in get_response.text

    replacement_rules = [{"id": "replacement"}]
    put_response = client.put(f"{_API}/rules", json={"rules": replacement_rules})
    assert put_response.status_code == 200
    assert cluster.config["rules"] == replacement_rules


def test_a_concurrent_write_returns_conflict_without_overwriting_it(
    cluster: _FakeCluster,
):
    concurrent_config = auth_proxy_management.with_secret(
        cluster.config, "concurrent-secret", "concurrent-value"
    )
    cluster.concurrent_config_before_next_patch = concurrent_config

    response = _client(cluster).put(
        f"{_API}/secrets/request-secret", json={"value": "request-value"}
    )

    assert response.status_code == 409
    assert response.json()["detail"] == (
        "Auth proxy config changed concurrently; retry the request"
    )
    assert cluster.config == concurrent_config


def test_no_route_ever_returns_a_secret_value(cluster: _FakeCluster):
    client = _client(cluster)
    responses = [
        client.get(f"{_API}/rules"),
        client.put(f"{_API}/rules/rule1", json=_STORED_CONFIG["rules"][0]),
        client.get(f"{_API}/secrets"),
        client.put(f"{_API}/secrets/secret2", json={"value": "secret_value2"}),
        client.delete(f"{_API}/secrets/secret2"),
    ]
    assert [response.status_code for response in responses] == [200] * len(responses)
    for response in responses:
        assert "secret_value" not in response.text, response.text


def test_secret_values_can_be_set_and_deleted_but_never_read(
    cluster: _FakeCluster,
):
    client = _client(cluster)

    assert client.get(f"{_API}/secrets").json() == {"secret_names": ["secret1"]}

    put_response = client.put(
        f"{_API}/secrets/secret2", json={"value": "secret_value2"}
    )
    assert put_response.json() == {"secret_names": ["secret1", "secret2"]}
    # The value did reach the config, it is only never handed back out.
    assert cluster.config["secrets"]["secret2"] == {"value": "secret_value2"}

    delete_response = client.delete(f"{_API}/secrets/secret2")
    assert delete_response.json() == {"secret_names": ["secret1"]}
    assert "secret2" not in cluster.config["secrets"]


def test_a_secret_that_a_rule_still_references_cannot_be_deleted(
    cluster: _FakeCluster,
):
    response = _client(cluster).delete(f"{_API}/secrets/secret1")
    assert response.status_code == 400
    assert "undefined secret" in response.json()["detail"]
    assert cluster.config == _STORED_CONFIG


def test_an_invalid_rule_is_rejected(cluster: _FakeCluster):
    response = _client(cluster).put(
        f"{_API}/rules/rule3", json={"handlers": [{"nonexistent_handler": {}}]}
    )
    assert response.status_code == 400
    assert cluster.config == _STORED_CONFIG


def test_a_rule_id_that_disagrees_with_the_url_is_rejected(
    cluster: _FakeCluster,
):
    response = _client(cluster).put(f"{_API}/rules/rule3", json={"id": "rule4"})
    assert response.status_code == 400
    assert cluster.config == _STORED_CONFIG


def test_a_missing_rule_or_secret_is_not_found(cluster: _FakeCluster):
    client = _client(cluster)

    delete_response1 = client.delete(f"{_API}/rules/nonexistent")
    assert delete_response1.status_code == 404

    delete_response2 = client.delete(f"{_API}/secrets/nonexistent")
    assert delete_response2.status_code == 404
    assert cluster.config == _STORED_CONFIG


@pytest.mark.parametrize(
    "user_name", ["other1@example.com", None], ids=["another user", "no user"]
)
def test_only_the_creator_of_the_instance_can_reach_its_config(
    cluster: _FakeCluster, user_name: str | None
):
    client = _client(cluster, user_name=user_name)
    responses = [
        client.get(f"{_API}/rules"),
        client.put(f"{_API}/rules", json={"rules": []}),
        client.put(f"{_API}/rules/rule1", json={}),
        client.delete(f"{_API}/rules/rule1"),
        client.get(f"{_API}/secrets"),
        client.put(f"{_API}/secrets/secret2", json={"value": "secret_value2"}),
        client.delete(f"{_API}/secrets/secret1"),
    ]
    # A 404 rather than a 403, so that the API does not confirm that the instance exists.
    assert [response.status_code for response in responses] == [404] * len(responses)
    assert cluster.config == _STORED_CONFIG


def test_an_instance_that_does_not_exist_is_not_found(cluster: _FakeCluster):
    other = "/api/tangent/instances/nonexistent/auth_proxy"
    assert _client(cluster).get(f"{other}/rules").status_code == 404


# endregion
