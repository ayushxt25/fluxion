from __future__ import annotations

import json
from typing import TypeAlias

from app.engine.exceptions import TaskResultValidationError

JSONValue: TypeAlias = (
    dict[str, "JSONValue"] | list["JSONValue"] | str | int | float | bool | None
)


def normalize_task_result(value: object, max_bytes: int) -> JSONValue:
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise TaskResultValidationError(
            "task result must be JSON serializable."
        ) from exc

    if len(encoded) > max_bytes:
        raise TaskResultValidationError(
            f"task result exceeds {max_bytes} byte limit."
        )

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
