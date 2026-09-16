import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


class TaskLogValidationError(ValueError):
    pass


class TaskLogLimitError(TaskLogValidationError):
    pass


@dataclass(frozen=True)
class PendingTaskLog:
    sequence_number: int
    level: str
    message: str
    fields: dict | None


class FluxionTaskLogger:
    """Attempt-local, bounded diagnostic buffer exposed to task code."""

    def __init__(
        self,
        *,
        max_message_bytes: int,
        max_fields_bytes: int,
        max_entries: int,
        buffer_size: int = 50,
        on_threshold: Callable[[], None] | None = None,
    ) -> None:
        self._max_message_bytes = max_message_bytes
        self._max_fields_bytes = max_fields_bytes
        self._max_entries = max_entries
        self._buffer_size = buffer_size
        self._on_threshold = on_threshold
        self._entries: list[PendingTaskLog] = []
        self._next_sequence = 1
        self._entry_count = 0

    def debug(self, message: str, **fields: Any) -> None:
        self._append("DEBUG", message, fields)

    def info(self, message: str, **fields: Any) -> None:
        self._append("INFO", message, fields)

    def warning(self, message: str, **fields: Any) -> None:
        self._append("WARNING", message, fields)

    def error(self, message: str, **fields: Any) -> None:
        self._append("ERROR", message, fields)

    def drain(self) -> tuple[PendingTaskLog, ...]:
        entries = tuple(self._entries)
        self._entries.clear()
        return entries

    def set_threshold_callback(self, callback: Callable[[], None]) -> None:
        self._on_threshold = callback

    def _append(self, level: str, message: str, fields: dict[str, Any]) -> None:
        if not isinstance(message, str):
            raise TaskLogValidationError("Task log message must be a string.")
        if len(message.encode("utf-8")) > self._max_message_bytes:
            raise TaskLogValidationError("Task log message exceeds the size limit.")
        if self._entry_count >= self._max_entries:
            raise TaskLogLimitError("Task log entry limit was reached.")
        safe_fields = _sanitize_fields(fields)
        encoded = json.dumps(safe_fields, ensure_ascii=False, allow_nan=False)
        if len(encoded.encode("utf-8")) > self._max_fields_bytes:
            raise TaskLogValidationError("Task log fields exceed the size limit.")
        self._entries.append(
            PendingTaskLog(self._next_sequence, level, message, safe_fields or None)
        )
        self._next_sequence += 1
        self._entry_count += 1
        if len(self._entries) >= self._buffer_size and self._on_threshold is not None:
            self._on_threshold()


def _sanitize_fields(fields: dict[str, Any]) -> dict:
    sanitized: dict[str, Any] = {}
    for key, value in fields.items():
        if not isinstance(key, str):
            raise TaskLogValidationError("Task log field names must be strings.")
        sanitized[key] = "[REDACTED]" if _sensitive(key) else value
    try:
        json.dumps(sanitized, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise TaskLogValidationError(
            "Task log fields must be JSON-serializable."
        ) from exc
    return sanitized


def _sensitive(key: str) -> bool:
    normalized = key.lower().replace("-", "_")
    sensitive_terms = (
        "token",
        "authorization",
        "password",
        "secret",
        "api_key",
        "lease_token",
    )
    return any(term in normalized for term in sensitive_terms)
