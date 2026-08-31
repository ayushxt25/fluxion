from collections import defaultdict
from heapq import heappop, heappush
from types import MappingProxyType
from typing import Self

from app.engine.exceptions import (
    CycleDetectedError,
    DuplicateDependencyError,
    DuplicateTaskError,
    EmptyWorkflowError,
    SelfDependencyError,
    UnknownDependencyError,
    UnknownTaskError,
)
from app.schemas.workflow import WorkflowDefinition


class WorkflowDAG:
    """Validated workflow DAG with deterministic lexical ordering."""

    def __init__(self, workflow: WorkflowDefinition) -> None:
        if not workflow.tasks:
            raise EmptyWorkflowError(workflow.id)

        task_ids: set[str] = set()
        dependencies: dict[str, tuple[str, ...]] = {}
        dependents: dict[str, list[str]] = defaultdict(list)

        for task in workflow.tasks:
            if task.id in task_ids:
                raise DuplicateTaskError(task.id)
            task_ids.add(task.id)

        for task in workflow.tasks:
            seen_dependencies: set[str] = set()

            for dependency_id in task.depends_on:
                if dependency_id in seen_dependencies:
                    raise DuplicateDependencyError(task.id, dependency_id)
                if dependency_id == task.id:
                    raise SelfDependencyError(task.id)
                if dependency_id not in task_ids:
                    raise UnknownDependencyError(task.id, dependency_id)

                seen_dependencies.add(dependency_id)
                dependents[dependency_id].append(task.id)

            dependencies[task.id] = tuple(sorted(task.depends_on))
            dependents.setdefault(task.id, [])

        self._tasks = MappingProxyType({task.id: task for task in workflow.tasks})
        self._dependencies = MappingProxyType(dependencies)
        self._dependents = MappingProxyType(
            {task_id: tuple(sorted(ids)) for task_id, ids in dependents.items()}
        )
        self._topological_order = tuple(self._compute_topological_order())

    @classmethod
    def from_workflow(cls, workflow: WorkflowDefinition) -> Self:
        return cls(workflow)

    @property
    def roots(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                task_id
                for task_id, dependencies in self._dependencies.items()
                if not dependencies
            )
        )

    @property
    def leaves(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                task_id
                for task_id, dependents in self._dependents.items()
                if not dependents
            )
        )

    @property
    def task_count(self) -> int:
        return len(self._tasks)

    def __contains__(self, task_id: object) -> bool:
        return task_id in self._tasks

    def __len__(self) -> int:
        return self.task_count

    def dependencies_of(self, task_id: str) -> tuple[str, ...]:
        self._ensure_task_exists(task_id)
        return self._dependencies[task_id]

    def dependents_of(self, task_id: str) -> tuple[str, ...]:
        self._ensure_task_exists(task_id)
        return self._dependents[task_id]

    def topological_order(self) -> tuple[str, ...]:
        return self._topological_order

    def _compute_topological_order(self) -> list[str]:
        in_degree = {
            task_id: len(dependencies)
            for task_id, dependencies in self._dependencies.items()
        }
        ready: list[str] = []

        for task_id, degree in in_degree.items():
            if degree == 0:
                heappush(ready, task_id)

        ordered: list[str] = []
        while ready:
            task_id = heappop(ready)
            ordered.append(task_id)

            for dependent_id in self._dependents[task_id]:
                in_degree[dependent_id] -= 1
                if in_degree[dependent_id] == 0:
                    heappush(ready, dependent_id)

        if len(ordered) != len(self._tasks):
            cyclic_task_ids = [
                task_id for task_id, degree in in_degree.items() if degree > 0
            ]
            raise CycleDetectedError(cyclic_task_ids)

        return ordered

    def _ensure_task_exists(self, task_id: str) -> None:
        if task_id not in self._tasks:
            raise UnknownTaskError(task_id)
