import json
import keyword
from collections import deque
from typing import Any

from pydantic import TypeAdapter

from app.sdk.models import (
    DependencyResultParameter,
    JSONValue,
    LiteralParameter,
    ParameterMapping,
    Retry,
    TaskDefinition,
    Workflow,
    WorkflowInputParameter,
)

_PARAMETER = TypeAdapter(ParameterMapping)


def _path(parts: tuple[str | int, ...]) -> tuple[str | int, ...]:
    if not all(isinstance(part, str | int) for part in parts):
        raise ValueError("Parameter paths may contain only strings or integers.")
    return parts


def workflow_input(*path: str | int) -> WorkflowInputParameter:
    return WorkflowInputParameter(path=_path(path))


def dependency_result(
    task_id: str,
    *path: str | int,
) -> DependencyResultParameter:
    if not task_id:
        raise ValueError("Dependency task ID must not be empty.")
    return DependencyResultParameter(task_id=task_id, path=_path(path))


def literal(value: JSONValue) -> LiteralParameter:
    try:
        json.dumps(value, allow_nan=False)
        return LiteralParameter(value=value)
    except (TypeError, ValueError) as exc:
        raise ValueError("Literal parameters must be JSON-serializable.") from exc


class WorkflowBuilder:
    def __init__(self, workflow_id: str, *, name: str | None = None) -> None:
        if not workflow_id:
            raise ValueError("Workflow ID must not be empty.")
        self._workflow_id = workflow_id
        self._name = name or workflow_id
        self._tasks: list[TaskDefinition] = []

    def task(
        self,
        task_id: str,
        *,
        name: str | None = None,
        depends_on: list[str] | tuple[str, ...] = (),
        parameters: dict[str, ParameterMapping | dict[str, Any]] | None = None,
        retry: Retry | None = None,
    ) -> "WorkflowBuilder":
        if not task_id:
            raise ValueError("Task ID must not be empty.")
        if any(task.id == task_id for task in self._tasks):
            raise ValueError(f"Duplicate task ID '{task_id}'.")
        dependencies = tuple(depends_on)
        if task_id in dependencies:
            raise ValueError(f"Task '{task_id}' cannot depend on itself.")
        normalized = {
            key: _PARAMETER.validate_python(value)
            for key, value in (parameters or {}).items()
        }
        for parameter_name, parameter in normalized.items():
            if not parameter_name.isidentifier() or keyword.iskeyword(parameter_name):
                raise ValueError(
                    f"Task parameter '{parameter_name}' must be a Python identifier."
                )
            if (
                isinstance(parameter, DependencyResultParameter)
                and parameter.task_id not in dependencies
            ):
                raise ValueError(
                    f"Task '{task_id}' parameter references non-direct dependency "
                    f"'{parameter.task_id}'."
                )
            if isinstance(parameter, LiteralParameter):
                literal(parameter.value)
        self._tasks.append(
            TaskDefinition(
                id=task_id,
                name=name,
                depends_on=dependencies,
                retry_policy=retry or Retry(),
                parameters=normalized,
            )
        )
        return self

    def build(self) -> Workflow:
        if not self._tasks:
            raise ValueError("Workflow must contain at least one task.")
        task_ids = {task.id for task in self._tasks}
        unknown = {
            dependency
            for task in self._tasks
            for dependency in task.depends_on
            if dependency not in task_ids
        }
        if unknown:
            names = ", ".join(sorted(unknown))
            raise ValueError(f"Unknown task dependencies: {names}.")
        remaining = {task.id: set(task.depends_on) for task in self._tasks}
        ready = deque(
            sorted(
                task_id
                for task_id, dependencies in remaining.items()
                if not dependencies
            )
        )
        visited = 0
        while ready:
            task_id = ready.popleft()
            visited += 1
            for candidate, dependencies in remaining.items():
                if task_id in dependencies:
                    dependencies.remove(task_id)
                    if not dependencies:
                        ready.append(candidate)
        if visited != len(self._tasks):
            raise ValueError("Workflow dependencies contain a cycle.")
        return Workflow(id=self._workflow_id, name=self._name, tasks=tuple(self._tasks))
