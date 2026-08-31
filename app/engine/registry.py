from collections.abc import Callable
from types import MappingProxyType
from typing import Any

from app.engine.exceptions import (
    DuplicateTaskImplementationError,
    MissingTaskImplementationError,
)
from app.schemas.workflow import WorkflowDefinition

TaskCallable = Callable[[], Any]


class TaskRegistry:
    def __init__(self, implementations: dict[str, TaskCallable] | None = None) -> None:
        self._implementations: dict[str, TaskCallable] = {}
        for task_id, implementation in (implementations or {}).items():
            self.register(task_id, implementation)

    @property
    def implementations(self) -> MappingProxyType[str, TaskCallable]:
        return MappingProxyType(dict(self._implementations))

    def register(self, task_id: str, implementation: TaskCallable) -> None:
        if task_id in self._implementations:
            raise DuplicateTaskImplementationError(task_id)
        self._implementations[task_id] = implementation

    def get(self, task_id: str) -> TaskCallable:
        try:
            return self._implementations[task_id]
        except KeyError as exc:
            raise MissingTaskImplementationError(task_id) from exc

    def validate_workflow(self, workflow: WorkflowDefinition) -> None:
        for task in workflow.tasks:
            if task.id not in self._implementations:
                raise MissingTaskImplementationError(task.id)
