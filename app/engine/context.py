from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from app.engine.results import JSONValue, clone_json_value
from app.engine.task_logging import FluxionTaskLogger


@dataclass(frozen=True)
class TaskExecutionContext:
    workflow_id: str
    run_id: str
    task_id: str
    attempt_number: int
    attempt_key: str
    idempotency_key: str
    dependency_results: Mapping[str, JSONValue] = field(default_factory=dict)
    workflow_input: JSONValue = None
    workflow_input_present: bool = False
    logger: FluxionTaskLogger = field(
        compare=False,
        default_factory=lambda: FluxionTaskLogger(
            max_message_bytes=8192,
            max_fields_bytes=16384,
            max_entries=10000,
        )
    )

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "dependency_results",
            MappingProxyType(
                {
                    task_id: _freeze_json_value(result)
                    for task_id, result in self.dependency_results.items()
                }
            ),
        )
        object.__setattr__(
            self,
            "workflow_input",
            _freeze_json_value(self.workflow_input),
        )


def _freeze_json_value(value: JSONValue) -> Any:
    cloned = clone_json_value(value)
    if isinstance(cloned, dict):
        return MappingProxyType(
            {key: _freeze_json_value(item) for key, item in cloned.items()}
        )
    if isinstance(cloned, list):
        return tuple(_freeze_json_value(item) for item in cloned)
    return cloned
