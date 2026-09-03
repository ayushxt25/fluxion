import asyncio

import pytest

from app.engine.exceptions import ExecutionPersistenceError, WorkflowNotFoundError
from app.engine.status import TaskStatus, WorkflowStatus
from app.schemas.workflow import TaskDefinition, WorkflowDefinition
from app.services.execution import PersistentWorkflowExecutor


def task(task_id: str, depends_on: tuple[str, ...] = ()) -> TaskDefinition:
    return TaskDefinition(id=task_id, depends_on=depends_on)


def workflow(*tasks: TaskDefinition) -> WorkflowDefinition:
    return WorkflowDefinition(id="workflow", name="Workflow", tasks=tasks)


class FakeWorkflowRepository:
    def __init__(self, exists: bool = True) -> None:
        self._exists = exists

    async def exists(self, workflow_id: str) -> bool:
        return self._exists


class RecordingRunRepository:
    def __init__(self, fail_on_save_number: int | None = None) -> None:
        self.fail_on_save_number = fail_on_save_number
        self.created_snapshots: list[dict[str, TaskStatus]] = []
        self.saved_snapshots: list[dict[str, TaskStatus]] = []

    async def create(self, workflow_run) -> None:
        self.created_snapshots.append(self._task_statuses(workflow_run))

    async def save_state(self, workflow_run) -> None:
        if len(self.saved_snapshots) + 1 == self.fail_on_save_number:
            raise RuntimeError("db down")
        self.saved_snapshots.append(self._task_statuses(workflow_run))

    def _task_statuses(self, workflow_run) -> dict[str, TaskStatus]:
        return {
            task_id: task_run.status
            for task_id, task_run in workflow_run.task_runs.items()
        }


def test_initial_run_snapshot_is_persisted_before_task_start() -> None:
    async def scenario():
        repository = RecordingRunRepository()
        calls = []
        await PersistentWorkflowExecutor(
            workflow(task("a"), task("b", ("a",))),
            {"a": lambda: calls.append("a"), "b": lambda: calls.append("b")},
            FakeWorkflowRepository(),
            repository,
            max_concurrency=1,
        ).run()
        return repository, calls

    repository, calls = asyncio.run(scenario())

    assert repository.created_snapshots[0] == {
        "a": TaskStatus.READY,
        "b": TaskStatus.BLOCKED,
    }
    assert calls == ["a", "b"]


def test_running_state_is_persisted_before_callable_body_executes() -> None:
    async def scenario():
        repository = RecordingRunRepository()
        observed = []

        async def task_a() -> None:
            observed.append(repository.saved_snapshots[-1]["a"])

        await PersistentWorkflowExecutor(
            workflow(task("a")),
            {"a": task_a},
            FakeWorkflowRepository(),
            repository,
        ).run()
        return observed

    assert asyncio.run(scenario()) == [TaskStatus.RUNNING]


def test_persistence_failure_before_start_prevents_callable_invocation() -> None:
    async def scenario():
        calls = []
        with pytest.raises(ExecutionPersistenceError):
            await PersistentWorkflowExecutor(
                workflow(task("a")),
                {"a": lambda: calls.append("a")},
                FakeWorkflowRepository(),
                RecordingRunRepository(fail_on_save_number=1),
            ).run()
        return calls

    assert asyncio.run(scenario()) == []


def test_persistence_failure_after_success_is_not_task_failure() -> None:
    async def scenario():
        calls = []
        with pytest.raises(ExecutionPersistenceError):
            await PersistentWorkflowExecutor(
                workflow(task("a")),
                {"a": lambda: calls.append("a")},
                FakeWorkflowRepository(),
                RecordingRunRepository(fail_on_save_number=2),
            ).run()
        return calls

    assert asyncio.run(scenario()) == ["a"]


def test_missing_persisted_workflow_prevents_callable_invocation() -> None:
    async def scenario():
        calls = []
        with pytest.raises(WorkflowNotFoundError):
            await PersistentWorkflowExecutor(
                workflow(task("a")),
                {"a": lambda: calls.append("a")},
                FakeWorkflowRepository(exists=False),
                RecordingRunRepository(),
            ).run()
        return calls

    assert asyncio.run(scenario()) == []


def test_concurrent_completions_persist_canonical_snapshots() -> None:
    async def scenario():
        repository = RecordingRunRepository()
        both_started = asyncio.Event()
        active = 0

        async def task_body() -> None:
            nonlocal active
            active += 1
            if active == 2:
                both_started.set()
            await both_started.wait()

        result = await PersistentWorkflowExecutor(
            workflow(task("b"), task("c")),
            {"b": task_body, "c": task_body},
            FakeWorkflowRepository(),
            repository,
        ).run()
        return result, repository.saved_snapshots

    result, snapshots = asyncio.run(scenario())

    assert result.status == WorkflowStatus.SUCCEEDED
    assert snapshots[-1] == {
        "b": TaskStatus.SUCCEEDED,
        "c": TaskStatus.SUCCEEDED,
    }
