import dataclasses
import typing

import fastapi

from . import instance_management

if typing.TYPE_CHECKING:
    from kubernetes import client as k8s_client_lib


def build_api_router(
    *,
    api_prefix: str = "/api/tangent",
    agent_container_factory: typing.Callable[[str], "k8s_client_lib.V1Container"],
    extra_volumes: list["k8s_client_lib.V1Volume"],
    pod_annotations: dict[str, str],
    get_user_name: typing.Callable[..., str],
    generate_proxy_config: typing.Callable[..., dict],
    kubernetes_client: "k8s_client_lib.ApiClient",
    kubernetes_namespace: str = "default",
    kubernetes_service_account_name: str | None = None,
) -> fastapi.APIRouter:

    router = fastapi.APIRouter(prefix=api_prefix, tags=["tangent"])

    @dataclasses.dataclass(kw_only=True)
    class ListInstancesResponse:
        instances: list[instance_management.TangentInstance]

    @router.get("/instances")
    def list_instances(
        user_name: typing.Annotated[str, fastapi.Depends(get_user_name)],
    ) -> ListInstancesResponse:
        instances = instance_management.list_instances(
            api_client=kubernetes_client,
            created_by=user_name,
            namespace=kubernetes_namespace,
        )
        return ListInstancesResponse(instances=instances)

    @router.post("/instances")
    def create_instance(
        user_name: typing.Annotated[str, fastapi.Depends(get_user_name)],
        proxy_config: typing.Annotated[dict, fastapi.Depends(generate_proxy_config)],
    ) -> instance_management.TangentInstance:
        created_by = user_name

        return instance_management.create_instance(
            api_client=kubernetes_client,
            created_by=created_by,
            agent_kind=instance_management.DEFAULT_AGENT_KIND,
            agent_container_factory=agent_container_factory,
            extra_volumes=extra_volumes,
            pod_annotations=pod_annotations,
            proxy_config=proxy_config,
            namespace=kubernetes_namespace,
            service_account_name=kubernetes_service_account_name,
        )

    @router.get("/go")
    async def redirect_to_default_instance(
        user_name: typing.Annotated[str, fastapi.Depends(get_user_name)],
        proxy_config: typing.Annotated[dict, fastapi.Depends(generate_proxy_config)],
    ):
        instances = instance_management.list_instances(
            api_client=kubernetes_client,
            created_by=user_name,
        )
        instance_ids = [instance.instance_id for instance in instances]
        if instance_ids:
            # Take smallest (earliest) instance
            instance_ids = sorted(instance_ids)
            instance_id = instance_ids[0]
        else:
            # Create new instance
            instance_response = create_instance(
                user_name=user_name, proxy_config=proxy_config
            )
            instance_id = instance_response.instance_id
            # TODO: Maybe wait for 5 seconds so that the Pod can get created

        # TODO: Change the URL when the app URLs changes
        opencode_ui_url = (
            api_prefix.rstrip("/") + f"/instances/{instance_id}/opencode/app/default"
        )
        # opencode_ui_url = api_prefix.rstrip("/") + f"/instances/{instance_id}/agents/opencode/apps/default"
        return fastapi.responses.RedirectResponse(url=opencode_ui_url)

    return router
