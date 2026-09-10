from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from app.engine.results import JSONValue, clone_json_value


@dataclass(frozen=True)
class TaskExecutionContext:
    workflow_id: str
    run_id: str
    task_id: str
    attempt_number: int
    attempt_key: str
    idempotency_key: str
    dependency_results: Mapping[str, JSONValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "dependency_results",
            MappingProxyType(
                {
                    task_id: clone_json_value(result)
                    for task_id, result in self.dependency_results.items()
                }
            ),
        )
