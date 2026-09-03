import asyncio

import pytest

from app.engine.exceptions import (
    ExecutionPersistenceError,
    MissingTaskImplementationError,
    WorkflowRunNotResumableError,
)
from app.engine.execution import WorkflowRun
from app.engine.status import TaskStatus, WorkflowStatus
from app.schemas.workflow import TaskDefinition, WorkflowDefinition
from app.services.repositories import IncompleteWorkflowRunRef
from app.services.resume import WorkflowResumeService


def task(task_id: str, depends_on: tuple[str, ...] = ()) -> TaskDefinition:
    return TaskDefinition(id=task_id, depends_on=depends_on)


def workflow(*tasks: TaskDefinition) -> WorkflowDefinition:
    return WorkflowDefinition(id="workflow", name="Workflow", tasks=tasks)


def restored_run(
    definition: WorkflowDefinition,
    status: WorkflowStatus,
    task_statuses: dict[str, TaskStatus],
) -> WorkflowRun:
    return WorkflowRun.restore(
        run_id="run-1",
        workflow=definition,
        status=status,
        task_statuses=task_statuses,
    )


class FakeWorkflowRepository:
    def __init__(self, definition: WorkflowDefinition) -> None:
        self.definition = definition

    async def get(self, workflow_id: str) -> WorkflowDefinition:
        return self.definition


class FakeRunRepository:
    def __init__(
        self,
        workflow_run: WorkflowRun,
        *,
        fail_on_save_number: int | None = None,
    ) -> None:
        self.workflow_run = workflow_run
        self.fail_on_save_number = fail_on_save_number
        self.save_count = 0
        self.saved_snapshots: list[dict[str, TaskStatus]] = []

    async def get_workflow_id(self, run_id: str) -> str:
        return self.workflow_run.workflow_id

    async def get(self, run_id: str, workflow: WorkflowDefinition) -> WorkflowRun:
        return self.workflow_run

    async def save_state(self, workflow_run: WorkflowRun) -> None:
        self.save_count += 1
        if self.save_count == self.fail_on_save_number:
            raise RuntimeError("db down")
        self.workflow_run = workflow_run
        self.saved_snapshots.append(
            {
                task_id: task_run.status
                for task_id, task_run in workflow_run.task_runs.items()
            }
        )

    async def list_incomplete(self) -> tuple[IncompleteWorkflowRunRef, ...]:
        return (
            IncompleteWorkflowRunRef(
                run_id=self.workflow_run.run_id,
                workflow_id=self.workflow_run.workflow_id,
            ),
        )


def test_resume_pending_single_task_run() -> None:
    async def scenario():
        definition = workflow(task("a"))
        repository = FakeRunRepository(WorkflowRun.create("run-1", definition))
        calls = []
        result = await WorkflowResumeService(
            FakeWorkflowRepository(definition),
            repository,
        ).resume_run("run-1", {"a": lambda: calls.append("a")})
        return result, calls

    result, calls = asyncio.run(scenario())

    assert result.run_id == "run-1"
    assert result.status == WorkflowStatus.SUCCEEDED
    assert result.task_statuses["a"] == TaskStatus.SUCCEEDED
    assert calls == ["a"]


def test_resume_running_run_does_not_rerun_succeeded_task() -> None:
    async def scenario():
        definition = workflow(task("a"), task("b", ("a",)))
        run = restored_run(
            definition,
            WorkflowStatus.RUNNING,
            {"a": TaskStatus.SUCCEEDED, "b": TaskStatus.READY},
        )
        repository = FakeRunRepository(run)
        calls = []
        result = await WorkflowResumeService(
            FakeWorkflowRepository(definition),
            repository,
        ).resume_run("run-1", {"b": lambda: calls.append("b")})
        return result, calls

    result, calls = asyncio.run(scenario())

    assert result.status == WorkflowStatus.SUCCEEDED
    assert calls == ["b"]


def test_blocked_task_waits_for_resumed_dependency_completion() -> None:
    async def scenario():
        definition = workflow(task("a"), task("b", ("a",)))
        repository = FakeRunRepository(WorkflowRun.create("run-1", definition))
        calls = []
        result = await WorkflowResumeService(
            FakeWorkflowRepository(definition),
            repository,
        ).resume_run(
            "run-1",
            {"a": lambda: calls.append("a"), "b": lambda: calls.append("b")},
            max_concurrency=1,
        )
        return result, calls

    result, calls = asyncio.run(scenario())

    assert result.status == WorkflowStatus.SUCCEEDED
    assert calls == ["a", "b"]


def test_resume_rejects_interrupted_task_without_invocation() -> None:
    async def scenario():
        definition = workflow(task("a"))
        run = restored_run(
            definition,
            WorkflowStatus.FAILED,
            {"a": TaskStatus.INTERRUPTED},
        )
        calls = []
        with pytest.raises(WorkflowRunNotResumableError):
            await WorkflowResumeService(
                FakeWorkflowRepository(definition),
                FakeRunRepository(run),
            ).resume_run("run-1", {"a": lambda: calls.append("a")})
        return calls

    assert asyncio.run(scenario()) == []


def test_stale_running_task_is_recovered_then_resume_rejected() -> None:
    async def scenario():
        definition = workflow(task("a"))
        run = restored_run(
            definition,
            WorkflowStatus.RUNNING,
            {"a": TaskStatus.RUNNING},
        )
        calls = []
        repository = FakeRunRepository(run)
        with pytest.raises(WorkflowRunNotResumableError):
            await WorkflowResumeService(
                FakeWorkflowRepository(definition),
                repository,
            ).resume_run("run-1", {"a": lambda: calls.append("a")})
        return calls, repository.workflow_run.get_task_status("a")

    calls, status = asyncio.run(scenario())

    assert calls == []
    assert status == TaskStatus.INTERRUPTED


def test_missing_implementation_rejected_before_resumed_callable_runs() -> None:
    async def scenario():
        definition = workflow(task("a"), task("b"))
        repository = FakeRunRepository(WorkflowRun.create("run-1", definition))
        calls = []
        with pytest.raises(MissingTaskImplementationError):
            await WorkflowResumeService(
                FakeWorkflowRepository(definition),
                repository,
            ).resume_run("run-1", {"a": lambda: calls.append("a")})
        return calls

    assert asyncio.run(scenario()) == []


def test_persistence_failure_before_resumed_callable_prevents_invocation() -> None:
    async def scenario():
        definition = workflow(task("a"))
        repository = FakeRunRepository(
            WorkflowRun.create("run-1", definition),
            fail_on_save_number=2,
        )
        calls = []
        with pytest.raises(ExecutionPersistenceError):
            await WorkflowResumeService(
                FakeWorkflowRepository(definition),
                repository,
            ).resume_run("run-1", {"a": lambda: calls.append("a")})
        return calls

    assert asyncio.run(scenario()) == []


def test_callable_failure_during_resume_returns_failed_result() -> None:
    async def scenario():
        definition = workflow(task("a"))
        repository = FakeRunRepository(WorkflowRun.create("run-1", definition))

        def fail() -> None:
            raise RuntimeError("boom")

        return await WorkflowResumeService(
            FakeWorkflowRepository(definition),
            repository,
        ).resume_run("run-1", {"a": fail})

    result = asyncio.run(scenario())

    assert result.status == WorkflowStatus.FAILED
    assert result.task_statuses["a"] == TaskStatus.FAILED
    assert result.errors == {"a": "RuntimeError: boom"}


def test_repeated_resume_after_success_is_rejected() -> None:
    async def scenario():
        definition = workflow(task("a"))
        repository = FakeRunRepository(WorkflowRun.create("run-1", definition))
        service = WorkflowResumeService(FakeWorkflowRepository(definition), repository)

        await service.resume_run("run-1", {"a": lambda: None})
        with pytest.raises(WorkflowRunNotResumableError):
            await service.resume_run("run-1", {})

    asyncio.run(scenario())


def test_multiple_ready_tasks_resume_concurrently() -> None:
    async def scenario():
        definition = workflow(task("a"), task("b"))
        repository = FakeRunRepository(WorkflowRun.create("run-1", definition))
        both_started = asyncio.Event()
        active = 0
        max_active = 0

        async def body() -> None:
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            if active == 2:
                both_started.set()
            await both_started.wait()
            active -= 1

        await WorkflowResumeService(
            FakeWorkflowRepository(definition),
            repository,
        ).resume_run("run-1", {"a": body, "b": body})
        return max_active

    assert asyncio.run(scenario()) == 2


def test_resume_max_concurrency_is_preserved() -> None:
    async def scenario():
        definition = workflow(task("a"), task("b"))
        repository = FakeRunRepository(WorkflowRun.create("run-1", definition))
        active = 0
        max_active = 0

        async def body() -> None:
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0)
            active -= 1

        await WorkflowResumeService(
            FakeWorkflowRepository(definition),
            repository,
        ).resume_run("run-1", {"a": body, "b": body}, max_concurrency=1)
        return max_active

    assert asyncio.run(scenario()) == 1
