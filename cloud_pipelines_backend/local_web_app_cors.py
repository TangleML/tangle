import fastapi
from fastapi.middleware import cors

LOCAL_WEB_APP_ORIGIN_REGEX = r"http://(localhost|127\.0\.0\.1)(:\d+)?"


def allow_local_web_app_origins(app: fastapi.FastAPI) -> None:
    """Lets a Web app served from another local port, such as the tangle-ui dev server, call the API."""
    app.add_middleware(
        cors.CORSMiddleware,
        allow_origin_regex=LOCAL_WEB_APP_ORIGIN_REGEX,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
