import functools
import typing

import fastapi
import httpx
from kubernetes.client import api_client as k8s_client_lib

from . import kubernetes_proxy_utils


def build_api_router(
    kubernetes_client: k8s_client_lib.ApiClient,
    get_user_id: typing.Callable[..., str],
    kubernetes_namespace: str = "default",
    api_prefix: str = "/api/tangent",
) -> fastapi.APIRouter:

    kubernetes_configuration = kubernetes_client.configuration
    # Workaround for non-thread-safe kubernetes.stream.portforward method that hacks ApiClient.request
    # See https://github.com/kubernetes-client/python/blob/f8c3ba2a7be3c90f2e94ea1f655f8a589c642333/kubernetes/base/stream/stream.py#L21
    kubernetes_client_for_ws_proxying = k8s_client_lib.ApiClient(
        configuration=kubernetes_client.configuration
    )

    kubernetes_server_info = kubernetes_proxy_utils.KubernetesApiServerInfo(
        kubernetes_configuration
    )

    http_client = httpx.AsyncClient(
        verify=kubernetes_server_info.ssl_context or True, timeout=None
    )

    router = fastapi.APIRouter(prefix="", tags=["proxy"])

    _ALL_HTTP_METHODS = [
        "GET",
        "POST",
        "PUT",
        "PATCH",
        "DELETE",
        "HEAD",
        "OPTIONS",
    ]

    # There are two options for the API routes for HTTP and WebSockets:
    # Single URL:
    # HTTP/WS   .../http_proxy/ports/8000/path
    # Separate URLs:
    # HTTP      .../http_proxy/ports/8000/http/path
    # WS        .../http_proxy/ports/8000/ws/path
    # I'm choosing the "single URL" option.
    # Most apps just use the same URL for both HTTP and WS without being able to set the endpoints separately. So using separate endpoints will be problematic.

    # FastAPI's OpenAPI docs page has issues when passing multiple HTTP methods at once.
    # See https://github.com/fastapi/fastapi/issues/13175
    # So, we add individually add route for each method.
    # @router.api_route(
    #     path=api_prefix + "/instances/{instance_id}/http_proxy/ports/{port}/http/{path:path}",
    #     methods=_ALL_HTTP_METHODS,
    # )
    async def proxy_http(
        instance_id: str,
        port: int,
        path: str,
        request: fastapi.Request,
        user_id: str = fastapi.Depends(get_user_id),
    ) -> fastapi.Response:
        # TODO: ! Check whether the user is the owner!
        pod_name = _get_k8s_pod_name_for_instance(
            instance_id=instance_id,
            kubernetes_namespace=kubernetes_namespace,
            kubernetes_client=kubernetes_client,
        )
        if not pod_name:
            raise fastapi.HTTPException(
                status_code=fastapi.status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Instance pod is not yet running",
            )
        query_string = request.url.query
        if query_string:
            path = path + "?" + query_string
        return await kubernetes_proxy_utils.proxy_http(
            request=request,
            http_client=http_client,
            kubernetes_server_info=kubernetes_server_info,
            namespace=kubernetes_namespace,
            pod=pod_name,
            port=port,
            path=path,
        )

    for method in _ALL_HTTP_METHODS:
        router.add_api_route(
            path=api_prefix
            + "/instances/{instance_id}/http_proxy/ports/{port}/{path:path}",
            endpoint=proxy_http,
            methods=[method],
        )

    @router.websocket(
        path=api_prefix + "/instances/{instance_id}/http_proxy/ports/{port}/{path:path}"
    )
    async def proxy_ws(
        instance_id: str,
        port: int,
        path: str,
        websocket: fastapi.WebSocket,
        user_id: str = fastapi.Depends(get_user_id),
    ):
        # TODO: ! Check whether the user is the owner!
        pod_name = _get_k8s_pod_name_for_instance(
            instance_id=instance_id,
            kubernetes_namespace=kubernetes_namespace,
            kubernetes_client=kubernetes_client,
        )
        if not pod_name:
            raise fastapi.HTTPException(
                status_code=fastapi.status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Instance pod is not yet running",
            )
        query_string = websocket.url.query
        if query_string:
            path = path + "?" + query_string

        # There are several implementation of WebSockets proxying.
        # return await kubernetes_proxy_utils.proxy_websocket(
        #     websocket=websocket,
        #     kubernetes_server_info=kubernetes_server_info,
        #     namespace=kubernetes_namespace,
        #     pod=pod_name,
        #     port=port,
        #     path=path,
        # )
        return await kubernetes_proxy_utils.proxy_websocket_via_port_forward_using_kubernetes_lib_and_threads(
            websocket=websocket,
            api_client=kubernetes_client_for_ws_proxying,
            namespace=kubernetes_namespace,
            pod=pod_name,
            port=port,
            path_and_query_string=path,
        )
        # return await kubernetes_proxy_utils.proxy_websocket_via_port_forward()
        #     websocket=websocket,
        #     kubernetes_server_info=kubernetes_server_info,
        #     namespace=kubernetes_namespace,
        #     pod=pod_name,
        #     port=port,
        #     path_and_query_string=path,
        # )

    return router


@functools.cache
def _get_k8s_pod_name_for_instance(
    instance_id: str,
    kubernetes_namespace: str,
    kubernetes_client: k8s_client_lib.ApiClient | None = None,
) -> str | None:
    return f"tangent-{instance_id}-0"
    """Return the first running pod name for a session deployment."""
    core_v1 = k8s_client_lib.CoreV1Api(api_client=kubernetes_client)
    pods = core_v1.list_namespaced_pod(
        namespace=kubernetes_namespace,
        label_selector=f"tangent.tangleml.com/instance.id={instance_id}",
    )
    for pod in pods.items:
        if pod.status and pod.status.phase in ("Running", "Pending"):
            return pod.metadata.name
    return None
