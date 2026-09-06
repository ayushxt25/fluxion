from collections.abc import Awaitable, Callable
from http import HTTPStatus

from fastapi import Request
from fastapi.responses import JSONResponse, Response

from app.core.config import get_settings


async def security_headers_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    if request.url.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store"
    return response


async def body_size_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    content_length = request.headers.get("content-length")
    limit = get_settings().max_request_body_bytes
    if content_length is not None and int(content_length) > limit:
        return JSONResponse(
            status_code=HTTPStatus.REQUEST_ENTITY_TOO_LARGE.value,
            content={
                "error": {
                    "code": "payload_too_large",
                    "message": "Request body is too large.",
                }
            },
        )
    return await call_next(request)
