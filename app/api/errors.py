import logging
import re
import time
from collections.abc import Awaitable, Callable
from http import HTTPStatus
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from app.engine.exceptions import (
    DispatchError,
    ExecutionStateError,
    PersistenceError,
    RecoveryStateError,
    TaskImplementationError,
    UnknownTaskRunError,
    WorkerLeaseError,
    WorkflowAlreadyExistsError,
    WorkflowNotFoundError,
    WorkflowRunAlreadyExistsError,
    WorkflowRunNotFoundError,
    WorkflowRunNotResumableError,
    WorkflowValidationError,
)
from app.observability.context import clear_log_context, set_request_id
from app.observability.metrics import (
    record_auth_denied,
    record_http_request,
    record_rate_limit_denied,
)
from app.security.auth import (
    AuthenticationError,
    AuthorizationError,
    RateLimitExceededError,
    RateLimitUnavailableError,
)

logger = logging.getLogger(__name__)


def install_api_handlers(app: FastAPI) -> None:
    app.middleware("http")(request_id_middleware)
    app.add_exception_handler(WorkflowNotFoundError, not_found_handler)
    app.add_exception_handler(WorkflowRunNotFoundError, not_found_handler)
    app.add_exception_handler(UnknownTaskRunError, not_found_handler)
    app.add_exception_handler(WorkflowAlreadyExistsError, conflict_handler)
    app.add_exception_handler(WorkflowRunAlreadyExistsError, conflict_handler)
    app.add_exception_handler(WorkflowRunNotResumableError, conflict_handler)
    app.add_exception_handler(ExecutionStateError, conflict_handler)
    app.add_exception_handler(RecoveryStateError, conflict_handler)
    app.add_exception_handler(WorkflowValidationError, validation_handler)
    app.add_exception_handler(TaskImplementationError, validation_handler)
    app.add_exception_handler(DispatchError, unavailable_handler)
    app.add_exception_handler(WorkerLeaseError, unavailable_handler)
    app.add_exception_handler(PersistenceError, unavailable_handler)
    app.add_exception_handler(AuthenticationError, authentication_handler)
    app.add_exception_handler(AuthorizationError, authorization_handler)
    app.add_exception_handler(RateLimitExceededError, rate_limit_handler)
    app.add_exception_handler(RateLimitUnavailableError, unavailable_handler)


async def request_id_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    request_id = request.headers.get("X-Request-ID") or str(uuid4())
    request.state.request_id = request_id
    set_request_id(request_id)
    started = time.perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        response.headers["X-Request-ID"] = request_id
        return response
    finally:
        duration = time.perf_counter() - started
        path = _route_template(request)
        record_http_request(
            method=request.method,
            path=path,
            status_code=status_code,
            duration_seconds=duration,
        )
        principal = getattr(request.state, "principal", None)
        logger.info(
            "HTTP request completed.",
            extra={
                "event": "http.request",
                "method": request.method,
                "path": path,
                "status_code": status_code,
                "duration_ms": round(duration * 1000, 3),
                "principal_subject": (
                    principal.subject if principal is not None else None
                ),
                "principal_role": (
                    principal.role.value if principal is not None else None
                ),
            },
        )
        clear_log_context()


async def not_found_handler(request: Request, exc: Exception) -> JSONResponse:
    return _error_response(HTTPStatus.NOT_FOUND, exc)


async def conflict_handler(request: Request, exc: Exception) -> JSONResponse:
    return _error_response(HTTPStatus.CONFLICT, exc)


async def validation_handler(request: Request, exc: Exception) -> JSONResponse:
    return _error_response(HTTPStatus.UNPROCESSABLE_ENTITY, exc)


async def unavailable_handler(request: Request, exc: Exception) -> JSONResponse:
    return _error_response(HTTPStatus.SERVICE_UNAVAILABLE, exc)


async def authentication_handler(request: Request, exc: Exception) -> JSONResponse:
    record_auth_denied()
    response = _error_response(HTTPStatus.UNAUTHORIZED, exc)
    response.headers["WWW-Authenticate"] = "Bearer"
    return response


async def authorization_handler(request: Request, exc: Exception) -> JSONResponse:
    record_auth_denied()
    return _error_response(HTTPStatus.FORBIDDEN, exc)


async def rate_limit_handler(
    request: Request,
    exc: RateLimitExceededError,
) -> JSONResponse:
    record_rate_limit_denied()
    response = _error_response(HTTPStatus.TOO_MANY_REQUESTS, exc)
    response.headers["Retry-After"] = str(exc.retry_after)
    response.headers["X-RateLimit-Limit"] = str(exc.limit)
    response.headers["X-RateLimit-Remaining"] = str(exc.remaining)
    return response


def _error_response(status: HTTPStatus, exc: Exception) -> JSONResponse:
    return JSONResponse(
        status_code=status.value,
        content={
            "error": {
                "code": _error_code(exc),
                "message": str(exc),
            }
        },
    )


def _error_code(exc: Exception) -> str:
    name = type(exc).__name__.removesuffix("Error")
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def _route_template(request: Request) -> str:
    route = request.scope.get("route")
    template = getattr(route, "path_format", None) or getattr(route, "path", None)
    if isinstance(template, str):
        root_path = request.scope.get("root_path") or ""
        if root_path:
            return _join_paths(root_path, template)
        return _with_request_prefix(request.scope.get("path") or "", template)
    return request.url.path


def _with_request_prefix(raw_path: str, template: str) -> str:
    raw_parts = _path_parts(raw_path)
    template_parts = _path_parts(template)
    if not template_parts:
        return template
    if len(raw_parts) < len(template_parts):
        return template

    raw_suffix = raw_parts[-len(template_parts) :]
    if not all(
        template_part.startswith("{")
        and template_part.endswith("}")
        or template_part == raw_part
        for raw_part, template_part in zip(raw_suffix, template_parts, strict=True)
    ):
        return template

    prefixed_parts = (*raw_parts[: -len(template_parts)], *template_parts)
    return "/" + "/".join(prefixed_parts)


def _path_parts(path: str) -> tuple[str, ...]:
    return tuple(part for part in path.split("/") if part)


def _join_paths(prefix: str, path: str) -> str:
    return f"/{prefix.strip('/')}/{path.strip('/')}"
