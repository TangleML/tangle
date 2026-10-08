import fastapi
import httpx
from fastapi import responses
from kubernetes import client as k8s_client_lib
from kubernetes import config as k8s_config_lib

# Headers that should not be forwarded upstream
_HOP_BY_HOP = {
    "content-encoding",  # we decompress via httpx
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
    # "connection",
    # "keep-alive",
}


_instance_id_to_pod_name: dict[str, str] = {}


async def proxy_request_for_instance(
    request: fastapi.Request,
    instance_id: str,
    path: str,
    user_id: str,
    kubernetes_client: k8s_client_lib.ApiClient,
    kubernetes_namespace: str,
) -> fastapi.Response:
    # TODO: ! Check whether the user is the owner!

    # TODO: Maybe, handle pod name changes. But in StatefulSet, Pod names are stable.
    pod_name = _instance_id_to_pod_name.get(instance_id)
    if not pod_name:
        pod_name = _get_k8s_pod_name_for_instance(
            instance_id=instance_id,
            kubernetes_namespace=kubernetes_namespace,
            kubernetes_client=kubernetes_client,
        )
        if pod_name:
            _instance_id_to_pod_name[instance_id] = pod_name

    if not pod_name:
        raise fastapi.HTTPException(
            status_code=fastapi.status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Instance pod is not yet running",
        )

    query_string = request.url.query
    if query_string:
        path = path + "?" + query_string
    return await _proxy_request_impl(
        request=request,
        pod_namespace=kubernetes_namespace,
        pod_name=pod_name,
        path=path,
        port=8000,
        kubernetes_configuration=kubernetes_client.configuration,
    )


async def _proxy_request_impl(
    request: fastapi.Request,
    pod_namespace: str,
    pod_name: str,
    path: str,
    port: int = 8000,
    kubernetes_configuration: k8s_client_lib.Configuration | None = None,
) -> fastapi.Response:
    upstream_url = _get_k8s_pod_proxy_url(
        pod_name,
        pod_namespace,
        port=port,
        path=path,
        kubernetes_configuration=kubernetes_configuration,
    )
    if request.url.query:
        upstream_url = f"{upstream_url}?{request.url.query}"

    token = _get_k8s_client_bearer_token(
        kubernetes_configuration=kubernetes_configuration
    )
    ssl_verify = _get_k8s_client_ssl_verify_config(
        kubernetes_configuration=kubernetes_configuration
    )
    # Transforming headers
    headers = {k: v for k, v in request.headers.items() if k.lower() not in _HOP_BY_HOP}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    body = await request.body()

    try:
        http = httpx.AsyncClient(verify=ssl_verify)
        stream_ctx = http.stream(
            method=request.method,
            url=upstream_url,
            headers=headers,
            content=body,
            timeout=httpx.Timeout(30.0, read=None),
        )
        streamed = await stream_ctx.__aenter__()
    except httpx.HTTPError as exc:
        raise fastapi.HTTPException(
            status_code=fastapi.status.HTTP_502_BAD_GATEWAY,
            detail=f"Agent unreachable: {exc}",
        ) from exc

    content_type = streamed.headers.get("content-type", "")
    response_headers = {
        k: v for k, v in streamed.headers.items() if k.lower() not in _HOP_BY_HOP
    }

    if "text/event-stream" in content_type:

        async def _stream_sse():
            try:
                async for chunk in streamed.aiter_bytes():
                    yield chunk
            finally:
                await stream_ctx.__aexit__(None, None, None)
                await http.aclose()

        return responses.StreamingResponse(
            _stream_sse(),
            status_code=streamed.status_code,
            headers=response_headers,
            media_type="text/event-stream",
        )

    # Regular response: read the body then close
    content = await streamed.aread()
    await stream_ctx.__aexit__(None, None, None)
    await http.aclose()

    return fastapi.Response(
        content=content,
        status_code=streamed.status_code,
        headers=response_headers,
        media_type=content_type or None,
    )


# region Kubernetes utils


def _get_k8s_pod_name_for_instance(
    instance_id: str,
    kubernetes_namespace: str,
    kubernetes_client: k8s_client_lib.ApiClient | None = None,
) -> str | None:
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


def _get_k8s_pod_proxy_url(
    pod_name: str,
    namespace: str,
    port: int,
    path: str = "",
    kubernetes_configuration: k8s_client_lib.Configuration | None = None,
) -> str:
    """
    Build a URL that proxies to a pod via the K8s API server's pod proxy subresource.

    Shape: {api_server}/api/v1/namespaces/{ns}/pods/{pod}:{port}/proxy/{path}
    """
    if not kubernetes_configuration:
        k8s_config_lib.load_kube_config()
        kubernetes_configuration = k8s_client_lib.Configuration.get_default_copy()
    api_server = kubernetes_configuration.host.rstrip("/")
    path = path.lstrip("/")
    return f"{api_server}/api/v1/namespaces/{namespace}/pods/{pod_name}:{port}/proxy/{path}"


def _get_k8s_client_bearer_token(
    kubernetes_configuration: k8s_client_lib.Configuration | None = None,
) -> str | None:
    """Return the bearer token from the current kubeconfig, refreshing if expired."""
    if not kubernetes_configuration:
        k8s_config_lib.load_kube_config()
        kubernetes_configuration = k8s_client_lib.Configuration.get_default_copy()
    # get_api_key_with_prefix triggers refresh_api_key_hook, which handles token
    # expiry for whoever built this configuration.
    value = kubernetes_configuration.get_api_key_with_prefix("authorization")
    return value.removeprefix("Bearer ") if value else None


def _get_k8s_client_ssl_verify_config(
    kubernetes_configuration: k8s_client_lib.Configuration | None = None,
) -> bool | str:
    """Return SSL verification setting (False or path to CA bundle)."""
    if not kubernetes_configuration:
        k8s_config_lib.load_kube_config()
        kubernetes_configuration = k8s_client_lib.Configuration.get_default_copy()

    if not kubernetes_configuration.verify_ssl:
        return False
    return kubernetes_configuration.ssl_ca_cert or True


# endregion
