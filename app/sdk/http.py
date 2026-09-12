from contextlib import suppress
from typing import Any

import httpx

from app.sdk.errors import (
    AuthenticationError,
    ConflictError,
    FluxionAPIError,
    FluxionConnectionError,
    FluxionTimeoutError,
    NotFoundError,
    PermissionDeniedError,
    RateLimitError,
    ServiceUnavailableError,
    ValidationError,
)
from app.version import __version__


def normalize_base_url(base_url: str) -> str:
    value = base_url.strip().rstrip("/")
    if not value:
        raise ValueError("base_url must not be empty.")
    return value


def build_headers(token: str | None, user_agent: str | None) -> dict[str, str]:
    headers = {
        "Accept": "application/json",
        "User-Agent": user_agent or f"Fluxion-Python/{__version__}",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def parse_json(response: httpx.Response) -> Any:
    try:
        return response.json()
    except ValueError as exc:
        raise FluxionAPIError(
            "Fluxion API returned malformed JSON.",
            status_code=response.status_code,
            request_id=response.headers.get("X-Request-ID"),
        ) from exc


def raise_for_response(response: httpx.Response) -> None:
    if response.is_success:
        return
    payload: Any = None
    with suppress(ValueError):
        payload = response.json()
    error = payload.get("error", {}) if isinstance(payload, dict) else {}
    message = error.get("message") if isinstance(error, dict) else None
    code = error.get("code") if isinstance(error, dict) else None
    kwargs = {
        "status_code": response.status_code,
        "request_id": response.headers.get("X-Request-ID"),
        "code": code if isinstance(code, str) else None,
    }
    message = message if isinstance(message, str) else "Fluxion API request failed."
    error_type: type[FluxionAPIError] = {
        401: AuthenticationError,
        403: PermissionDeniedError,
        404: NotFoundError,
        409: ConflictError,
        422: ValidationError,
        503: ServiceUnavailableError,
    }.get(response.status_code, FluxionAPIError)
    if response.status_code == 429:
        retry_after = response.headers.get("Retry-After")
        try:
            retry_seconds = int(retry_after) if retry_after is not None else None
        except ValueError:
            retry_seconds = None
        raise RateLimitError(message, retry_after=retry_seconds, **kwargs)
    raise error_type(message, **kwargs)


def map_http_error(
    exc: httpx.HTTPError,
) -> FluxionConnectionError | FluxionTimeoutError:
    if isinstance(exc, httpx.TimeoutException):
        return FluxionTimeoutError("Fluxion API request timed out.")
    return FluxionConnectionError("Could not connect to the Fluxion API.")
