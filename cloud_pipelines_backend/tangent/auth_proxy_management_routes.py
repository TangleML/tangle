"""FastAPI routes for the auth proxy config of a Tangent instance.

The API exposes user-managed rules and secrets (see `auth_proxy_management`): rules can be
read and written in full, while secret values can only be set and deleted, never read back.
Platform-managed rules and secrets remain hidden. Every route verifies that the caller is the
one who created the instance.
"""

import collections.abc
import contextlib
import dataclasses
import typing

import fastapi
from kubernetes import client as k8s_client_lib

from . import auth_proxy_management, instance_management


@dataclasses.dataclass(kw_only=True)
class RulesResponse:
    rules: list[dict]


@dataclasses.dataclass(kw_only=True)
class ListSecretNamesResponse:
    secret_names: list[str]


def build_api_router(
    *,
    api_prefix: str = "/api/tangent",
    get_user_name: typing.Callable[..., str | None],
    kubernetes_client: "k8s_client_lib.ApiClient",
    kubernetes_namespace: str = instance_management.DEFAULT_NAMESPACE,
) -> fastapi.APIRouter:

    def verify_instance_ownership(
        instance_id: str,
        user_name: typing.Annotated[str | None, fastapi.Depends(get_user_name)],
    ) -> None:
        """Refuses an instance that the caller did not create.

        Editing the proxy config of an instance injects credentials into its egress traffic and
        redirects it, so this check is the only thing standing between users.
        """
        created_by = instance_management.get_instance_created_by(
            api_client=kubernetes_client,
            instance_id=instance_id,
            namespace=kubernetes_namespace,
        )
        # The comparison also covers a missing instance and one without a recorded creator, and
        # a caller without a name is refused outright rather than by comparison.
        if not user_name or created_by != user_name:
            # 404 rather than 403: the API does not confirm that other users' instances exist.
            raise fastapi.HTTPException(
                status_code=404, detail=f"There is no instance {instance_id!r}"
            )

    router = fastapi.APIRouter(
        prefix=api_prefix,
        tags=["tangent"],
        # The check belongs to the whole router so that no route can forget it. Every route
        # below must therefore keep `{instance_id}` in its path.
        dependencies=[fastapi.Depends(verify_instance_ownership)],
    )

    def _instance(instance_id: str) -> dict:
        return dict(
            api_client=kubernetes_client,
            instance_id=instance_id,
            kubernetes_namespace=kubernetes_namespace,
        )

    # region Rules
    @router.get("/instances/{instance_id}/auth_proxy/rules")
    def get_rules(instance_id: str) -> RulesResponse:
        """Returns the rules. They reference secrets by name, so no secret value is exposed."""
        with _http_errors():
            return RulesResponse(
                rules=auth_proxy_management.get_rules(**_instance(instance_id))
            )

    @router.put("/instances/{instance_id}/auth_proxy/rules")
    def set_rules(
        instance_id: str,
        rules: typing.Annotated[list[dict], fastapi.Body(embed=True)],
    ) -> RulesResponse:
        with _http_errors():
            return RulesResponse(
                rules=auth_proxy_management.set_rules(
                    **_instance(instance_id), rules=rules
                )
            )

    @router.put("/instances/{instance_id}/auth_proxy/rules/{rule_id}")
    def set_rule(
        instance_id: str,
        rule_id: str,
        rule: typing.Annotated[dict, fastapi.Body()],
    ) -> RulesResponse:
        if rule.get("id", rule_id) != rule_id:
            raise fastapi.HTTPException(
                status_code=400,
                detail=f"The rule id {rule['id']!r} does not match the {rule_id!r} of the URL",
            )
        with _http_errors():
            rules = auth_proxy_management.set_rule(
                **_instance(instance_id), rule={**rule, "id": rule_id}
            )
        return RulesResponse(rules=rules)

    @router.delete("/instances/{instance_id}/auth_proxy/rules/{rule_id}")
    def delete_rule(instance_id: str, rule_id: str) -> RulesResponse:
        with _http_errors():
            return RulesResponse(
                rules=auth_proxy_management.delete_rule(
                    **_instance(instance_id), rule_id=rule_id
                )
            )

    # endregion

    # region Secrets
    @router.get("/instances/{instance_id}/auth_proxy/secrets")
    def get_secret_names(instance_id: str) -> ListSecretNamesResponse:
        """Lists the secret names. The values are write-only and are never returned."""
        with _http_errors():
            return ListSecretNamesResponse(
                secret_names=auth_proxy_management.get_secret_names(
                    **_instance(instance_id)
                )
            )

    @router.put("/instances/{instance_id}/auth_proxy/secrets/{name}")
    def set_secret(
        instance_id: str,
        name: str,
        value: typing.Annotated[str, fastapi.Body(embed=True)],
    ) -> ListSecretNamesResponse:
        with _http_errors():
            names = auth_proxy_management.set_secret(
                **_instance(instance_id), name=name, value=value
            )
        return ListSecretNamesResponse(secret_names=names)

    @router.delete("/instances/{instance_id}/auth_proxy/secrets/{name}")
    def delete_secret(instance_id: str, name: str) -> ListSecretNamesResponse:
        """Fails with a 400 while a rule still references the secret."""
        with _http_errors():
            names = auth_proxy_management.delete_secret(
                **_instance(instance_id), name=name
            )
        return ListSecretNamesResponse(secret_names=names)

    # endregion

    return router


@contextlib.contextmanager
def _http_errors() -> collections.abc.Generator[None]:
    """Turns the errors of `auth_proxy_management` into HTTP responses."""
    try:
        yield
    except ValueError as error:
        # Config validation. These messages name config paths and secret names, never values.
        raise fastapi.HTTPException(status_code=400, detail=str(error)) from error
    except KeyError as error:
        # A rule or secret that is not there. `str(KeyError(...))` would add quotes.
        raise fastapi.HTTPException(
            status_code=404, detail=str(error.args[0] if error.args else error)
        ) from error
    except k8s_client_lib.ApiException as error:
        if error.status == 404:
            raise fastapi.HTTPException(
                status_code=404, detail="The instance has no auth proxy config"
            ) from error
        if error.status == 409:
            raise fastapi.HTTPException(
                status_code=409,
                detail="Auth proxy config changed concurrently; retry the request",
            ) from error
        raise
