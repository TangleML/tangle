"""
Global middleware configuration for Tangle API servers.

This module provides reusable middleware setup functions that should be used
by all entry points to ensure consistent behavior across deployments.
"""

import logging
import os
from urllib.parse import urlparse

import fastapi
from fastapi.middleware.cors import CORSMiddleware


logger = logging.getLogger(__name__)


def _is_valid_origin(origin: str) -> bool:
    """
    Validate that an origin string is a valid URL format.

    Args:
        origin: The origin URL to validate

    Returns:
        True if valid, False otherwise
    """
    try:
        parsed = urlparse(origin)
        # Must have a scheme (http/https) and a netloc (domain/host)
        if not parsed.scheme or not parsed.netloc:
            return False
        # Scheme must be http or https
        if parsed.scheme not in ("http", "https"):
            return False
        # Should not have a path beyond '/'
        if parsed.path and parsed.path != "/":
            return False
        return True
    except Exception:
        return False


def setup_cors_middleware(app: fastapi.FastAPI) -> None:
    """
    Configure CORS middleware for the FastAPI application.

    Environment Variables:
        TANGLE_CORS_ALLOWED_ORIGINS: Comma-separated list of allowed origins.
            Default: "http://localhost:3000,http://127.0.0.1:3000" if not set.
    """
    cors_origins_str = os.environ.get(
        "TANGLE_CORS_ALLOWED_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000"
    )

    # Parse the comma-separated list and strip whitespace from each origin
    raw_origins = [
        origin.strip() for origin in cors_origins_str.split(",") if origin.strip()
    ]

    # Validate each origin
    allowed_origins = []
    invalid_origins = []

    for origin in raw_origins:
        if _is_valid_origin(origin):
            allowed_origins.append(origin)
        else:
            invalid_origins.append(origin)

    # Log warnings for invalid origins
    if invalid_origins:
        logger.warning(
            f"Invalid CORS origins found and ignored: {', '.join(invalid_origins)}. "
            f"Origins must be valid URLs with http:// or https:// scheme."
        )

    if not allowed_origins:
        logger.warning("No valid CORS origins found. CORS middleware not configured.")
        return

    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    logger.info(
        f"CORS middleware configured for {len(allowed_origins)} origin(s): "
        f"{', '.join(allowed_origins)}"
    )
