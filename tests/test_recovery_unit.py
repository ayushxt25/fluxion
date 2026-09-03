import asyncio

import pytest

from app.engine.exceptions import RecoveryStateError
from app.engine.execution import WorkflowRun
from app.engine.status import TaskStatus, WorkflowStatus
from app.schemas.workflow import TaskDefinition, WorkflowDefinition
from app.services.recovery import WorkflowRecoveryService
from app.services.repositories import IncompleteWorkflowRunRef


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
    def __init__(self, workflow_run: WorkflowRun) -> None:
        self.workflow_run = workflow_run
        self.saved: list[WorkflowRun] = []

    async def get(self, run_id: str, workflow: WorkflowDefinition) -> WorkflowRun:
        return self.workflow_run

    async def save_state(self, workflow_run: WorkflowRun) -> None:
        self.saved.append(workflow_run)

    async def list_incomplete(self) -> tuple[IncompleteWorkflowRunRef, ...]:
        return (
            IncompleteWorkflowRunRef(
                run_id=self.workflow_run.run_id,
                workflow_id=self.workflow_run.workflow_id,
            ),
        )


def test_pending_run_remains_resumable() -> None:
    async def scenario():
        definition = workflow(task("a"), task("b", ("a",)))
        run = WorkflowRun.create("run-1", definition)
        repository = FakeRunRepository(run)
        return await WorkflowRecoveryService(
            FakeWorkflowRepository(definition),
            repository,
        ).recover_run("run-1", "workflow")

    result = asyncio.run(scenario())

    assert result.previous_status == WorkflowStatus.PENDING
    assert result.recovered_status == WorkflowStatus.PENDING
    assert result.interrupted_task_ids == ()
    assert result.resumable is True


def test_stale_running_task_becomes_interrupted_and_failed() -> None:
    async def scenario():
        definition = workflow(task("a"), task("b", ("a",)))
        run = restored_run(
            definition,
            WorkflowStatus.RUNNING,
            {"a": TaskStatus.SUCCEEDED, "b": TaskStatus.RUNNING},
        )
        return await WorkflowRecoveryService(
            FakeWorkflowRepository(definition),
            FakeRunRepository(run),
        ).recover_run("run-1", "workflow")

    result = asyncio.run(scenario())

    assert result.recovered_status == WorkflowStatus.FAILED
    assert result.interrupted_task_ids == ("b",)
    assert result.task_statuses["a"] == TaskStatus.SUCCEEDED
    assert result.task_statuses["b"] == TaskStatus.INTERRUPTED
    assert result.resumable is False


def test_multiple_interrupted_tasks_are_ordered() -> None:
    async def scenario():
        definition = workflow(task("b"), task("a"))
        run = restored_run(
            definition,
            WorkflowStatus.RUNNING,
            {"a": TaskStatus.RUNNING, "b": TaskStatus.RUNNING},
        )
        return await WorkflowRecoveryService(
            FakeWorkflowRepository(definition),
            FakeRunRepository(run),
        ).recover_run("run-1", "workflow")

    assert asyncio.run(scenario()).interrupted_task_ids == ("a", "b")


def test_blocked_descendant_remains_blocked_after_interruption() -> None:
    async def scenario():
        definition = workflow(task("a"), task("b", ("a",)))
        run = restored_run(
            definition,
            WorkflowStatus.RUNNING,
            {"a": TaskStatus.RUNNING, "b": TaskStatus.BLOCKED},
        )
        return await WorkflowRecoveryService(
            FakeWorkflowRepository(definition),
            FakeRunRepository(run),
        ).recover_run("run-1", "workflow")

    result = asyncio.run(scenario())

    assert result.task_statuses["a"] == TaskStatus.INTERRUPTED
    assert result.task_statuses["b"] == TaskStatus.BLOCKED


def test_running_workflow_with_no_running_tasks_remains_resumable() -> None:
    async def scenario():
        definition = workflow(task("a"), task("b", ("a",)))
        run = restored_run(
            definition,
            WorkflowStatus.RUNNING,
            {"a": TaskStatus.SUCCEEDED, "b": TaskStatus.READY},
        )
        return await WorkflowRecoveryService(
            FakeWorkflowRepository(definition),
            FakeRunRepository(run),
        ).recover_run("run-1", "workflow")

    result = asyncio.run(scenario())

    assert result.recovered_status == WorkflowStatus.RUNNING
    assert result.interrupted_task_ids == ()
    assert result.resumable is True


def test_readiness_is_recomputed_safely() -> None:
    async def scenario():
        definition = workflow(task("a"), task("b", ("a",)), task("c", ("b",)))
        run = restored_run(
            definition,
            WorkflowStatus.RUNNING,
            {
                "a": TaskStatus.SUCCEEDED,
                "b": TaskStatus.BLOCKED,
                "c": TaskStatus.READY,
            },
        )
        return await WorkflowRecoveryService(
            FakeWorkflowRepository(definition),
            FakeRunRepository(run),
        ).recover_run("run-1", "workflow")

    result = asyncio.run(scenario())

    assert result.task_statuses["b"] == TaskStatus.READY
    assert result.task_statuses["c"] == TaskStatus.BLOCKED


def test_downstream_of_failed_or_interrupted_dependency_is_not_ready() -> None:
    async def scenario():
        definition = workflow(task("a"), task("b"), task("c", ("a", "b")))
        run = restored_run(
            definition,
            WorkflowStatus.RUNNING,
            {
                "a": TaskStatus.FAILED,
                "b": TaskStatus.INTERRUPTED,
                "c": TaskStatus.READY,
            },
        )
        return await WorkflowRecoveryService(
            FakeWorkflowRepository(definition),
            FakeRunRepository(run),
        ).recover_run("run-1", "workflow")

    assert asyncio.run(scenario()).task_statuses["c"] == TaskStatus.BLOCKED


def test_recovery_result_is_immutable() -> None:
    async def scenario():
        definition = workflow(task("a"))
        run = WorkflowRun.create("run-1", definition)
        return await WorkflowRecoveryService(
            FakeWorkflowRepository(definition),
            FakeRunRepository(run),
        ).recover_run("run-1", "workflow")

    result = asyncio.run(scenario())

    with pytest.raises(TypeError):
        result.task_statuses["a"] = TaskStatus.FAILED


def test_pending_run_with_running_task_is_rejected() -> None:
    async def scenario():
        definition = workflow(task("a"))
        run = restored_run(
            definition,
            WorkflowStatus.PENDING,
            {"a": TaskStatus.RUNNING},
        )
        await WorkflowRecoveryService(
            FakeWorkflowRepository(definition),
            FakeRunRepository(run),
        ).recover_run("run-1", "workflow")

    with pytest.raises(RecoveryStateError):
        asyncio.run(scenario())


def test_succeeded_task_with_failed_dependency_is_rejected() -> None:
    async def scenario():
        definition = workflow(task("a"), task("b", ("a",)))
        run = restored_run(
            definition,
            WorkflowStatus.RUNNING,
            {"a": TaskStatus.FAILED, "b": TaskStatus.SUCCEEDED},
        )
        await WorkflowRecoveryService(
            FakeWorkflowRepository(definition),
            FakeRunRepository(run),
        ).recover_run("run-1", "workflow")

    with pytest.raises(RecoveryStateError):
        asyncio.run(scenario())


def test_recover_incomplete_runs_uses_repository_order() -> None:
    async def scenario():
        definition = workflow(task("a"))
        run = WorkflowRun.create("run-1", definition)
        return await WorkflowRecoveryService(
            FakeWorkflowRepository(definition),
            FakeRunRepository(run),
        ).recover_incomplete_runs()

    results = asyncio.run(scenario())

    assert [result.run_id for result in results] == ["run-1"]
