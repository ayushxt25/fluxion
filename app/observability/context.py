from contextvars import ContextVar
from dataclasses import dataclass

_request_id: ContextVar[str | None] = ContextVar("request_id", default=None)
_principal_subject: ContextVar[str | None] = ContextVar(
    "principal_subject",
    default=None,
)
_principal_role: ContextVar[str | None] = ContextVar("principal_role", default=None)


@dataclass(frozen=True)
class LogContext:
    request_id: str | None
    principal_subject: str | None
    principal_role: str | None


def set_request_id(request_id: str) -> None:
    _request_id.set(request_id)


def set_principal(subject: str, role: str) -> None:
    _principal_subject.set(subject)
    _principal_role.set(role)


def get_log_context() -> LogContext:
    return LogContext(
        request_id=_request_id.get(),
        principal_subject=_principal_subject.get(),
        principal_role=_principal_role.get(),
    )


def clear_log_context() -> None:
    _request_id.set(None)
    _principal_subject.set(None)
    _principal_role.set(None)
