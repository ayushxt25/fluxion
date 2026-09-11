from __future__ import annotations

import json
from typing import TypeAlias

from app.engine.exceptions import (
    TaskResultValidationError,
    WorkflowInputValidationError,
)

JSONValue: TypeAlias = (
    dict[str, "JSONValue"] | list["JSONValue"] | str | int | float | bool | None
)


def normalize_task_result(value: object, max_bytes: int) -> JSONValue:
    return _normalize_json_value(
        value,
        max_bytes,
        error_type=TaskResultValidationError,
        value_name="task result",
    )


def normalize_workflow_input(value: object, max_bytes: int) -> JSONValue:
    return _normalize_json_value(
        value,
        max_bytes,
        error_type=WorkflowInputValidationError,
        value_name="workflow input",
    )


def _normalize_json_value(value, max_bytes: int, *, error_type, value_name: str):
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise error_type(f"{value_name} must be JSON serializable.") from exc

    if len(encoded) > max_bytes:
        raise error_type(f"{value_name} exceeds {max_bytes} byte limit.")

    return json.loads(encoded.decode("utf-8"))


def clone_json_value(value: JSONValue) -> JSONValue:
    return json.loads(
        json.dumps(
            value,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    )
