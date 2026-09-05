import inspect
from collections.abc import Callable
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from app.engine.exceptions import (
    DuplicateTaskImplementationError,
    InvalidTaskCallableError,
    MissingTaskImplementationError,
)
from app.schemas.workflow import WorkflowDefinition

TaskCallable = Callable[..., Any]


@dataclass(frozen=True)
class TaskCallableBinding:
    implementation: TaskCallable
    accepts_context: bool
    is_async: bool


class TaskRegistry:
    def __init__(self, implementations: dict[str, TaskCallable] | None = None) -> None:
        self._implementations: dict[str, TaskCallable] = {}
        self._bindings: dict[str, TaskCallableBinding] = {}
        for task_id, implementation in (implementations or {}).items():
            self.register(task_id, implementation)

    @property
    def implementations(self) -> MappingProxyType[str, TaskCallable]:
        return MappingProxyType(dict(self._implementations))

    def register(self, task_id: str, implementation: TaskCallable) -> None:
        if task_id in self._implementations:
            raise DuplicateTaskImplementationError(task_id)
        self._bindings[task_id] = self._bind(task_id, implementation)
        self._implementations[task_id] = implementation

    def get(self, task_id: str) -> TaskCallable:
        try:
            return self._implementations[task_id]
        except KeyError as exc:
            raise MissingTaskImplementationError(task_id) from exc

    def binding(self, task_id: str) -> TaskCallableBinding:
        self.get(task_id)
        return self._bindings[task_id]

    def validate_workflow(self, workflow: WorkflowDefinition) -> None:
        for task in workflow.tasks:
            if task.id not in self._implementations:
                raise MissingTaskImplementationError(task.id)
            self.binding(task.id)

    def _bind(
        self,
        task_id: str,
        implementation: TaskCallable,
    ) -> TaskCallableBinding:
        try:
            signature = inspect.signature(implementation)
        except (TypeError, ValueError) as exc:
            raise InvalidTaskCallableError(
                task_id,
                "signature could not be inspected.",
            ) from exc

        parameters = tuple(signature.parameters.values())
        if any(
            parameter.kind
            in {
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            }
            for parameter in parameters
        ):
            raise InvalidTaskCallableError(
                task_id,
                "expected either no parameters or exactly one context parameter.",
            )

        try:
            signature.bind()
        except TypeError:
            can_bind_zero_args = False
        else:
            can_bind_zero_args = True

        if can_bind_zero_args:
            accepts_context = False
        elif (
            len(parameters) == 1
            and parameters[0].default is inspect.Parameter.empty
            and parameters[0].kind
            in {
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            }
        ):
            accepts_context = True
        else:
            raise InvalidTaskCallableError(
                task_id,
                "expected either no parameters or exactly one context parameter.",
            )

        return TaskCallableBinding(
            implementation=implementation,
            accepts_context=accepts_context,
            is_async=inspect.iscoroutinefunction(implementation),
        )
