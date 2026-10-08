"""
Reverse proxy: forwards requests from /api/agents/sessions/{id}/ui/{path}
to the OpenCode web UI running inside the session's K8s pod (port 3000)
via the K8s API server pod proxy subresource.
"""

import mimetypes
import pathlib
import typing

import fastapi

if typing.TYPE_CHECKING:
    from kubernetes import client as k8s_client_lib

from . import proxy_utils


def build_api_router(
    *,
    kubernetes_client: "k8s_client_lib.ApiClient | None" = None,
    kubernetes_namespace: str = "default",
    opencode_app_build_dir: str | None = None,
    get_user_name: typing.Callable[..., str],
) -> fastapi.APIRouter:

    router = fastapi.APIRouter(prefix="", tags=["opencode"])

    _opencode_api_route = "/instances/{instance_id}/opencode/api/{path:path}"

    @router.get(_opencode_api_route)
    @router.post(_opencode_api_route)
    @router.put(_opencode_api_route)
    @router.delete(_opencode_api_route)
    @router.patch(_opencode_api_route)
    async def proxy(
        instance_id: str,
        path: str,
        request: fastapi.Request,
        user_id: typing.Annotated[str, fastapi.Depends(get_user_name)],
    ) -> fastapi.Response:
        return await proxy_utils.proxy_request_for_instance(
            request=request,
            instance_id=instance_id,
            path=path,
            user_id=user_id,
            kubernetes_client=kubernetes_client,
            kubernetes_namespace=kubernetes_namespace,
        )

    _OPENCODE_APP_STATIC_DIR = (
        pathlib.Path(opencode_app_build_dir) if opencode_app_build_dir else None
    )

    if _OPENCODE_APP_STATIC_DIR and _OPENCODE_APP_STATIC_DIR.exists():
        # Replacement of starlette.staticfiles.Mount
        @router.get("/instances/{instance_id}/opencode/app/default/{path:path}")
        async def proxy_static(
            instance_id: str,
            path: str,
            request: fastapi.Request,
            user_id: str = fastapi.Depends(get_user_name),
        ) -> fastapi.Response:
            file_path = (_OPENCODE_APP_STATIC_DIR / path).resolve()
            # Preventing path traversals
            if not str(file_path).startswith(str(_OPENCODE_APP_STATIC_DIR.resolve())):
                raise fastapi.HTTPException(status_code=404)

            if not file_path.is_file():
                file_path = _OPENCODE_APP_STATIC_DIR / "index.html"

            content = file_path.read_bytes()
            content_type = (
                mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
            )

            _REWRITE_TYPES = {
                "text/html",
                "text/javascript",
                "application/javascript",
                "text/css",
            }

            if any(t in content_type for t in _REWRITE_TYPES):
                req_path = request.url.path
                app_marker = "/opencode/app/default"
                idx = req_path.find(app_marker)
                if idx < 0:
                    raise ValueError(f"Bad URL path: {req_path}")
                prefix = req_path[:idx]
                web_ui_base = prefix + app_marker
                api_base = prefix + "/opencode/api"
                content = _opencode_app_rewrite_content(
                    content=content,
                    k8s_path_prefix="",
                    web_ui_base=web_ui_base,
                    api_base=api_base,
                )

            return fastapi.Response(content=content, media_type=content_type)

    else:
        print("Warning: Could not find OpenCode app static files.")

    return router


def _opencode_app_rewrite_content(
    content: bytes, k8s_path_prefix: str, web_ui_base: str, api_base: str
) -> bytes:
    ### VITE_BASE_URL="/XXX_OPENCODE_APP_XXX" VITE_OPENCODE_SERVER_BASE_URL="/XXX_OPENCODE_SERVER_XXX"
    # Rewrite K8s proxy paths (HTML src/href already rewritten by K8s API server; map to our prefix)
    # Example:
    # <script src="/api/v1/namespaces/kueue-jobs-staging/pods/session-1801a1b67430-5847b4d47d-d7qs8:3000/proxy/assets/index-Dwzj_leU.js"/>
    # k8s_path_prefix='/api/v1/namespaces/kueue-jobs-staging/pods/session-1801a1b67430-5847b4d47d-d7qs8:3000/proxy/',
    # web_ui_base='/api/tangent/api/agents/sessions/1801a1b6-7430-4213-9b07-da92fff7bfde/opencode/app/default'
    # api_base='/api/tangent/api/agents/sessions/1801a1b6-7430-4213-9b07-da92fff7bfde/opencode/api'

    web_ui_base = web_ui_base.rstrip("/")
    api_base = api_base.rstrip("/")

    ### content = content.replace(k8s_path_prefix.encode(), web_ui_base.encode())
    content = content.replace("/XXX_OPENCODE_APP_XXX".encode(), web_ui_base.encode())
    content = content.replace("/XXX_OPENCODE_SERVER_XXX".encode(), api_base.encode())
    return content
