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
from app.schemas.workflow import TaskDefinition, WorkflowDefinition

TaskCallable = Callable[..., Any]


@dataclass(frozen=True)
class TaskCallableBinding:
    implementation: TaskCallable
    accepts_context: bool
    is_async: bool
    keyword_parameters: frozenset[str]


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
            self.validate_task(task)

    def validate_task(self, task: TaskDefinition) -> None:
        if task.id not in self._implementations:
            raise MissingTaskImplementationError(task.id)
        binding = self.binding(task.id)
        configured = set(task.parameters)
        expected = set(binding.keyword_parameters)
        if configured != expected:
            raise InvalidTaskCallableError(
                task.id,
                "configured parameters must match callable keyword-only "
                "parameters.",
            )

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
            in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}
            for parameter in parameters
        ):
            raise InvalidTaskCallableError(
                task_id,
                "varargs and varkwargs are not supported.",
            )

        positional = tuple(
            parameter
            for parameter in parameters
            if parameter.kind
            in {
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            }
        )
        keyword_only = tuple(
            parameter
            for parameter in parameters
            if parameter.kind == inspect.Parameter.KEYWORD_ONLY
        )

        if len(positional) == 0:
            accepts_context = False
        elif (
            len(positional) == 1
            and positional[0].default is inspect.Parameter.empty
        ):
            accepts_context = True
        else:
            try:
                signature.bind()
            except TypeError as exc:
                raise InvalidTaskCallableError(
                    task_id,
                    "expected zero-argument execution or one context parameter.",
                ) from exc
            accepts_context = False

        return TaskCallableBinding(
            implementation=implementation,
            accepts_context=accepts_context,
            is_async=inspect.iscoroutinefunction(implementation),
            keyword_parameters=frozenset(parameter.name for parameter in keyword_only),
        )
