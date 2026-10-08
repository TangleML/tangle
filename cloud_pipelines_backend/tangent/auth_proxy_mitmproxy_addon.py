"""mitmproxy addon that rewrites and authenticates the agent's egress requests.

This file is self-contained on purpose: `instance_management` reads it as text and ships
it to the proxy sidecar as an inline script, so it cannot import anything from this repo.
mitmproxy is only imported for type checking, which keeps the module importable by the
backend, where `parse_config` validates user-supplied configs before they are written.

Config schema (see `auth_proxy_config.example.yaml` for a full example):

    system_secrets:             # platform-managed; unavailable to user rules
      <secret_name>:
        value: <string>
    system_rules: [...]         # platform-managed; applied after user rules

    secrets:                    # values that the user rules reference by name
      <secret_name>:
        value: <string>

    rules:
      - id: <string>            # optional; addresses the rule in the management API
        url_pattern: <string>   # URL prefix, scheme optional; omit to match every request
        handlers:               # applied in order to every matching request
          - url_replace: {old_substring: <string>, new_substring: <string>}
          - url_regexp_replace: {pattern: <regexp>, replacement: <string>}
          - set_header: {name: <string>, value: <value>}
          - set_cookie: {name: <string>, value: <value>}
          - set_basic_auth_header: {username: <value>, password: <value>}
          - set_bearer_auth_header: {value: <value>}

where a `<value>` is either the string itself or a reference to a secret:

    value: constant1
    value: {secret: {name: <secret_name>}}

Quote a string that YAML would otherwise read as a number, a boolean or a date.

Each half of `set_basic_auth_header` is a `<value>` of its own, so the username can be a
constant while the password comes from a secret. A half that is left out is empty (an API
key as the username with no password is a common scheme). The handler also accepts a single
`value` holding `username:password` or the already-encoded credentials, which is how a whole
credential pair can live in one secret.

The deprecated v1 rule keys `replacement_pattern` and `add_headers` are still honored and
mean the same as a `url_replace` / `set_header` handler.
"""

import base64
import collections.abc
import dataclasses
import logging
import os
import re
import time
import typing
import urllib.parse

import yaml

if typing.TYPE_CHECKING:
    from mitmproxy import http

# ! Do not add `from __future__ import annotations` here: mitmproxy runs this file as a module
# that it does not register in `sys.modules`, and `dataclasses` cannot resolve the resulting
# string annotations of the fields ("AttributeError: 'NoneType' object has no attribute
# '__dict__'"). The mitmproxy types are therefore quoted one by one.

_logger = logging.getLogger(__name__)

# Marks requests/responses that went through this proxy. Handy when debugging.
PROXY_MARKER_HEADER_NAME = "x-tangent-proxy"

# Kubernetes refreshes mounted Secrets about once a minute, so checking this often is
# enough to pick up rule changes without stat()ing the config on every request.
DEFAULT_CONFIG_RELOAD_INTERVAL_SECONDS = 5.0


# region Handlers
class Handler(typing.Protocol):
    def apply(self, request: "http.Request") -> None:
        pass


@dataclasses.dataclass(frozen=True)
class UrlReplaceHandler:
    old_substring: str
    new_substring: str

    def apply(self, request: "http.Request") -> None:
        request.url = request.url.replace(self.old_substring, self.new_substring)


@dataclasses.dataclass(frozen=True)
class UrlRegexpReplaceHandler:
    pattern: re.Pattern
    replacement: str

    def apply(self, request: "http.Request") -> None:
        request.url = self.pattern.sub(self.replacement, request.url)


@dataclasses.dataclass(frozen=True)
class SetHeaderHandler:
    name: str
    value: str

    def apply(self, request: "http.Request") -> None:
        request.headers[self.name] = self.value


@dataclasses.dataclass(frozen=True)
class SetCookieHandler:
    """Sets one cookie, leaving the other cookies of the request alone."""

    name: str
    value: str

    def apply(self, request: "http.Request") -> None:
        cookies = [
            cookie
            for cookie in (request.headers.get("Cookie") or "").split(";")
            if cookie.strip() and cookie.split("=", 1)[0].strip() != self.name
        ]
        cookies.append(f"{self.name}={self.value}")
        request.headers["Cookie"] = "; ".join(cookie.strip() for cookie in cookies)


# endregion


# region Config parsing
class SecretReference(typing.TypedDict):
    name: str


class ValueReference(typing.TypedDict):
    """A value that names one of the config's secrets instead of holding it inline."""

    secret: SecretReference


@dataclasses.dataclass(frozen=True)
class Rule:
    id: str | None
    url_pattern: str | None
    handlers: tuple[Handler, ...]

    def matches(self, url: str) -> bool:
        if not self.url_pattern:
            return True
        target = urllib.parse.urlsplit(url if "://" in url else "//" + url)
        pattern = urllib.parse.urlsplit(
            self.url_pattern if "://" in self.url_pattern else "//" + self.url_pattern
        )
        if target.hostname != pattern.hostname:
            return False
        if pattern.scheme and target.scheme != pattern.scheme:
            return False
        default_port = {"http": 80, "https": 443}.get(target.scheme or pattern.scheme)
        if (target.port or default_port) != (pattern.port or default_port):
            return False
        pattern_path = pattern.path.rstrip("/")
        target_path = target.path.rstrip("/")
        return (
            not pattern_path
            or target_path == pattern_path
            or target_path.startswith(pattern_path + "/")
        )


def parse_config(config: dict | None) -> tuple[Rule, ...]:
    """Validates a proxy config and compiles it into rules. Raises `ValueError` if invalid.

    Secret references are resolved here, so the returned rules carry the plaintext values.
    """
    config = config or {}
    _check_keys(
        config,
        path="config",
        optional=("secrets", "rules", "system_secrets", "system_rules"),
    )
    secrets = _parse_secrets(config.get("secrets") or {}, path="secrets")
    system_secrets = _parse_secrets(
        config.get("system_secrets") or {}, path="system_secrets"
    )
    rules = tuple(
        _parse_rule(rule, secrets=secrets, path=f"rules[{index}]")
        for index, rule in enumerate(_as_list(config.get("rules"), path="rules"))
    )
    system_rules = tuple(
        _parse_rule(rule, secrets=system_secrets, path=f"system_rules[{index}]")
        for index, rule in enumerate(
            _as_list(config.get("system_rules"), path="system_rules")
        )
    )
    return rules + system_rules


def _parse_secrets(secrets_config: dict, *, path: str) -> dict[str, str]:
    if not isinstance(secrets_config, dict):
        raise ValueError(f"{path}: expected a mapping of secret name to {{value: ...}}")
    secrets = {}
    for name, spec in secrets_config.items():
        _check_keys(spec or {}, path=f"{path}.{name}", required=("value",))
        if not isinstance(spec["value"], str):
            # A secret holds the value itself, so it cannot reference another secret.
            raise ValueError(
                f"{path}.{name}.value: expected a string, got {type(spec['value']).__name__}"
            )
        secrets[name] = spec["value"]
    return secrets


_V1_RULE_KEYS = ("replacement_pattern", "add_headers")


def _parse_rule(rule_config: dict, *, secrets: dict[str, str], path: str) -> Rule:
    _check_keys(
        rule_config,
        path=path,
        optional=("id", "url_pattern", "handlers", *_V1_RULE_KEYS),
    )
    url_pattern = rule_config.get("url_pattern")
    if url_pattern is not None and not isinstance(url_pattern, str):
        raise ValueError(f"{path}.url_pattern: expected a string")

    handlers: list[Handler] = []
    # Deprecated v1 keys, kept so that configs written before `handlers` existed keep working.
    replacement_pattern = rule_config.get("replacement_pattern")
    if replacement_pattern:
        if not url_pattern:
            raise ValueError(f"{path}.replacement_pattern: requires 'url_pattern'")
        handlers.append(
            UrlReplaceHandler(
                old_substring=url_pattern, new_substring=replacement_pattern
            )
        )
    handlers += [
        SetHeaderHandler(name=name, value=value)
        for name, value in (rule_config.get("add_headers") or {}).items()
    ]

    handler_configs = _as_list(rule_config.get("handlers"), path=f"{path}.handlers")
    handlers += [
        _parse_handler(
            handler_config, secrets=secrets, path=f"{path}.handlers[{index}]"
        )
        for index, handler_config in enumerate(handler_configs)
    ]
    return Rule(
        id=rule_config.get("id"),
        url_pattern=url_pattern,
        handlers=tuple(handlers),
    )


def _parse_handler(
    handler_config: dict, *, secrets: dict[str, str], path: str
) -> Handler:
    if not isinstance(handler_config, dict) or len(handler_config) != 1:
        raise ValueError(
            f"{path}: expected a mapping with a single key naming the handler"
        )
    ((kind, spec),) = handler_config.items()
    parser = _HANDLER_PARSERS.get(kind)
    if parser is None:
        raise ValueError(
            f"{path}: unsupported handler {kind!r}. Supported: {sorted(_HANDLER_PARSERS)}"
        )
    return parser(spec or {}, secrets=secrets, path=f"{path}.{kind}")


def _parse_url_replace(spec: dict, *, secrets: dict[str, str], path: str) -> Handler:
    _check_keys(spec, path=path, required=("old_substring", "new_substring"))
    return UrlReplaceHandler(
        old_substring=spec["old_substring"], new_substring=spec["new_substring"]
    )


def _parse_url_regexp_replace(
    spec: dict, *, secrets: dict[str, str], path: str
) -> Handler:
    _check_keys(spec, path=path, required=("pattern", "replacement"))
    try:
        pattern = re.compile(spec["pattern"])
    except re.error as error:
        raise ValueError(
            f"{path}.pattern: invalid regular expression: {error}"
        ) from error
    return UrlRegexpReplaceHandler(pattern=pattern, replacement=spec["replacement"])


def _parse_set_header(spec: dict, *, secrets: dict[str, str], path: str) -> Handler:
    _check_keys(spec, path=path, required=("name", "value"))
    return SetHeaderHandler(
        name=spec["name"],
        value=_resolve_value(spec["value"], secrets=secrets, path=f"{path}.value"),
    )


def _parse_set_cookie(spec: dict, *, secrets: dict[str, str], path: str) -> Handler:
    _check_keys(spec, path=path, required=("name", "value"))
    return SetCookieHandler(
        name=spec["name"],
        value=_resolve_value(spec["value"], secrets=secrets, path=f"{path}.value"),
    )


def _parse_set_basic_auth_header(
    spec: dict, *, secrets: dict[str, str], path: str
) -> Handler:
    """Takes a `username`/`password` pair, or one `value` holding both halves at once."""
    if "username" in spec or "password" in spec:
        _check_keys(spec, path=path, optional=("username", "password"))
        # Either half may be left out: an API key as the username with an empty password is a
        # common scheme, and so is the reverse.
        username = _resolve_half(
            spec.get("username"), secrets=secrets, path=f"{path}.username"
        )
        password = _resolve_half(
            spec.get("password"), secrets=secrets, path=f"{path}.password"
        )
        if ":" in username:
            # The first ":" is the separator, so this would silently authenticate as someone
            # else. The message names no value, since the username may come from a secret.
            raise ValueError(f"{path}.username: a username cannot contain ':'")
        credentials = f"{username}:{password}"
    else:
        # The halves are listed here too, so that a misspelled one is answered with all three
        # keys rather than with `value` alone.
        _check_keys(
            spec,
            path=path,
            required=("value",),
            optional=("username", "password"),
        )
        credentials = _resolve_value(
            spec["value"], secrets=secrets, path=f"{path}.value"
        )
    # `username:password` needs to be encoded, an already-encoded value must not be
    # encoded twice. The two cases cannot be confused: base64 has no ":" in its alphabet.
    if ":" in credentials:
        credentials = base64.b64encode(credentials.encode("utf-8")).decode("ascii")
    return SetHeaderHandler(name="Authorization", value=f"Basic {credentials}")


def _parse_set_bearer_auth_header(
    spec: dict, *, secrets: dict[str, str], path: str
) -> Handler:
    _check_keys(spec, path=path, required=("value",))
    token = _resolve_value(spec["value"], secrets=secrets, path=f"{path}.value")
    return SetHeaderHandler(name="Authorization", value=f"Bearer {token}")


_HANDLER_PARSERS: dict[str, typing.Callable[..., Handler]] = {
    "url_replace": _parse_url_replace,
    "url_regexp_replace": _parse_url_regexp_replace,
    "set_header": _parse_set_header,
    "set_cookie": _parse_set_cookie,
    "set_basic_auth_header": _parse_set_basic_auth_header,
    "set_bearer_auth_header": _parse_set_bearer_auth_header,
}


def _resolve_half(
    value: str | ValueReference | None, *, secrets: dict[str, str], path: str
) -> str:
    """One half of a credential pair. A half that is left out is empty."""
    return "" if value is None else _resolve_value(value, secrets=secrets, path=path)


def _resolve_value(
    value: str | ValueReference, *, secrets: dict[str, str], path: str
) -> str:
    """Reads a value: either the string itself, or a reference to a secret."""
    if isinstance(value, str):
        return value
    if not isinstance(value, dict):
        # Anything else is a YAML scalar that has to be quoted to reach us as a string.
        raise ValueError(
            f"{path}: expected a string or {{secret: {{name: ...}}}}, got {type(value).__name__}"
        )

    _check_keys(value, path=path, required=("secret",))
    _check_keys(value["secret"] or {}, path=f"{path}.secret", required=("name",))
    secret_name = value["secret"]["name"]
    if secret_name not in secrets:
        # Names only. Never log or raise secret values.
        raise ValueError(
            f"{path}: undefined secret {secret_name!r}. Defined secrets: {sorted(secrets)}"
        )
    return secrets[secret_name]


def _as_list(value: typing.Any, *, path: str) -> list:
    """An empty YAML section (`None`) is an empty list, anything but a list is an error."""
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{path}: expected a list, got {type(value).__name__}")
    return value


def _check_keys(
    mapping: collections.abc.Mapping[str, typing.Any],
    *,
    path: str,
    required: tuple[str, ...] = (),
    optional: tuple[str, ...] = (),
) -> None:
    if not isinstance(mapping, dict):
        raise ValueError(f"{path}: expected a mapping, got {type(mapping).__name__}")
    # Unsupported keys are reported first: a misspelled key is also a missing one, and naming
    # the typo is more useful than reporting the key it was meant to be.
    if unsupported := sorted(set(mapping) - set(required) - set(optional)):
        raise ValueError(
            f"{path}: unsupported key(s) {unsupported}. Supported: {sorted(set(required) | set(optional))}"
        )
    if missing := [key for key in required if key not in mapping]:
        raise ValueError(f"{path}: missing required key(s) {missing}")


# endregion


class AuthProxyAddon:
    """Applies the configured rules to every proxied request."""

    def __init__(
        self,
        *,
        config_path: str,
        reload_interval_seconds: float = DEFAULT_CONFIG_RELOAD_INTERVAL_SECONDS,
    ):
        self._config_path = config_path
        self._reload_interval_seconds = reload_interval_seconds
        self._rules: tuple[Rule, ...] = ()
        self._loaded_file_signature: tuple[int, int] | None = None
        self._next_reload_check_time = 0.0
        # An invalid config fails the proxy container at startup, which is loud and obvious.
        self.reload_config()

    def reload_config(self) -> None:
        signature = self._config_file_signature()
        with open(self._config_path, "r") as reader:
            config = yaml.safe_load(reader)
        # `parse_config` raises before the assignment, so an invalid config leaves the rules as they are.
        self._rules = parse_config(config)
        self._loaded_file_signature = signature
        _logger.info(
            "Loaded %d auth proxy rule(s) from %s",
            len(self._rules),
            self._config_path,
        )

    def _config_file_signature(self) -> tuple[int, int] | None:
        try:
            stat = os.stat(self._config_path)
        except OSError:
            return None
        return (stat.st_mtime_ns, stat.st_size)

    def _reload_config_if_changed(self) -> None:
        """Picks up rule changes made through the management API without restarting the pod."""
        if time.monotonic() < self._next_reload_check_time:
            return
        self._next_reload_check_time = time.monotonic() + self._reload_interval_seconds
        signature = self._config_file_signature()
        if signature is None or signature == self._loaded_file_signature:
            return
        try:
            self.reload_config()
        except Exception:
            # A broken config must not take down the agent's only route to the network.
            # Remember the signature so that the failure is not retried on every request.
            self._loaded_file_signature = signature
            _logger.exception(
                "Failed to reload %s. Keeping the previous rules.",
                self._config_path,
            )

    def request(self, flow: "http.HTTPFlow") -> None:
        self._reload_config_if_changed()
        for rule in self._rules:
            # The URL is re-read for every rule since a previous rule may have rewritten it.
            if rule.matches(flow.request.pretty_url):
                for handler in rule.handlers:
                    handler.apply(flow.request)
        flow.request.headers[PROXY_MARKER_HEADER_NAME] = "true"

    def response(self, flow: "http.HTTPFlow") -> None:
        if flow.response:
            flow.response.headers[PROXY_MARKER_HEADER_NAME] = "true"


# mitmproxy discovers addons through this module-level list. The backend imports this
# module as well (for `parse_config`), where PROXY_CONFIG_PATH is unset and no addon runs.
_CONFIG_PATH = os.environ.get("PROXY_CONFIG_PATH")
addons = [AuthProxyAddon(config_path=_CONFIG_PATH)] if _CONFIG_PATH else []
