import json
import logging
from datetime import UTC, datetime
from typing import Any

from app.observability.context import get_log_context

_RESERVED = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
}
_SENSITIVE_FIELDS = {
    "authorization",
    "authorization_header",
    "jwt",
    "jwt_secret",
    "lease_token",
    "password",
    "secret",
    "token",
}


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        context = get_log_context()
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if context.request_id:
            payload["request_id"] = context.request_id
        if context.principal_subject:
            payload["principal_subject"] = context.principal_subject
        if context.principal_role:
            payload["principal_role"] = context.principal_role
        for key, value in record.__dict__.items():
            if key in _RESERVED or key.lower() in _SENSITIVE_FIELDS:
                continue
            payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, separators=(",", ":"))


class TextLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        context = get_log_context()
        pieces = [
            datetime.now(UTC).isoformat(),
            record.levelname,
            record.name,
            record.getMessage(),
        ]
        if context.request_id:
            pieces.append(f"request_id={context.request_id}")
        if context.principal_subject:
            pieces.append(f"principal_subject={context.principal_subject}")
        if context.principal_role:
            pieces.append(f"principal_role={context.principal_role}")
        return " ".join(pieces)


def configure_logging(level: str, log_format: str) -> None:
    formatter: logging.Formatter = (
        JsonLogFormatter() if log_format == "json" else TextLogFormatter()
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level.upper())
