import re
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
from app.security.auth import (
    AuthenticationError,
    AuthorizationError,
    RateLimitExceededError,
    RateLimitUnavailableError,
)


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
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response


async def not_found_handler(request: Request, exc: Exception) -> JSONResponse:
    return _error_response(HTTPStatus.NOT_FOUND, exc)


async def conflict_handler(request: Request, exc: Exception) -> JSONResponse:
    return _error_response(HTTPStatus.CONFLICT, exc)


async def validation_handler(request: Request, exc: Exception) -> JSONResponse:
    return _error_response(HTTPStatus.UNPROCESSABLE_ENTITY, exc)


async def unavailable_handler(request: Request, exc: Exception) -> JSONResponse:
    return _error_response(HTTPStatus.SERVICE_UNAVAILABLE, exc)


async def authentication_handler(request: Request, exc: Exception) -> JSONResponse:
    response = _error_response(HTTPStatus.UNAUTHORIZED, exc)
    response.headers["WWW-Authenticate"] = "Bearer"
    return response


async def authorization_handler(request: Request, exc: Exception) -> JSONResponse:
    return _error_response(HTTPStatus.FORBIDDEN, exc)


async def rate_limit_handler(
    request: Request,
    exc: RateLimitExceededError,
) -> JSONResponse:
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
