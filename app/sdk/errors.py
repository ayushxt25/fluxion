from typing import Any


class FluxionError(Exception):
    """Base error raised by the public Fluxion SDK."""


class FluxionConnectionError(FluxionError):
    pass


class FluxionTimeoutError(FluxionError):
    pass


class FluxionAPIError(FluxionError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        request_id: str | None = None,
        code: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.request_id = request_id
        self.code = code


class AuthenticationError(FluxionAPIError):
    pass


class PermissionDeniedError(FluxionAPIError):
    pass


class NotFoundError(FluxionAPIError):
    pass


class ConflictError(FluxionAPIError):
    pass


class ValidationError(FluxionAPIError):
    pass


class RateLimitError(FluxionAPIError):
    def __init__(
        self,
        *args: Any,
        retry_after: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.retry_after = retry_after


class ServiceUnavailableError(FluxionAPIError):
    pass
