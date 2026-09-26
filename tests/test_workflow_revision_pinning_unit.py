import asyncio

import pytest

from app.dispatch.messages import TaskDispatchMessage
from app.engine.exceptions import DispatchStateError
from app.engine.execution import WorkflowRun
from app.schemas.workflow import TaskDefinition, WorkflowDefinition
from app.services.recovery import WorkflowRecoveryService
from app.services.resume import WorkflowResumeService
from app.services.worker import TaskWorker


class _RunRepository:
    async def get_workflow_reference(self, run_id: str) -> tuple[str, int]:
        assert run_id == "run-1"
        return "workflow", 1


class _WorkflowRepository:
    async def get_revision(
        self, workflow_id: str, revision: int
    ) -> WorkflowDefinition:
        raise AssertionError("a mismatched dispatch must not load a definition")


def _message(*, revision: int) -> TaskDispatchMessage:
    return TaskDispatchMessage(
        workflow_id="workflow",
        workflow_revision=revision,
        run_id="run-1",
        task_id="task",
        attempt_number=1,
        attempt_key="run-1:task:1",
        idempotency_key="run-1:task",
    )


def test_workflow_run_captures_definition_revision() -> None:
    workflow = WorkflowDefinition(
        id="workflow",
        name="workflow",
        revision=7,
        tasks=(TaskDefinition(id="task"),),
    )

    run = WorkflowRun.create("run-1", workflow)

    assert run.workflow_revision == 7


def test_worker_rejects_mismatched_dispatch_before_task_execution() -> None:
    worker = TaskWorker(
        _WorkflowRepository(),  # type: ignore[arg-type]
        _RunRepository(),  # type: ignore[arg-type]
        object(),  # type: ignore[arg-type]
        object(),  # type: ignore[arg-type]
        {},
        lease_seconds=10,
        heartbeat_seconds=1,
    )

    with pytest.raises(DispatchStateError, match="workflow revision"):
        asyncio.run(worker.process_message(_message(revision=2)))


class _PinnedWorkflowRepository:
    def __init__(self, first: WorkflowDefinition, second: WorkflowDefinition) -> None:
        self._revisions = {first.revision: first, second.revision: second}
        self.requests: list[int] = []

    async def get_revision(
        self, workflow_id: str, revision: int
    ) -> WorkflowDefinition:
        assert workflow_id == "workflow"
        self.requests.append(revision)
        return self._revisions[revision]


class _PinnedRunRepository:
    def __init__(self, workflow_run: WorkflowRun) -> None:
        self.workflow_run = workflow_run

    async def get_workflow_reference(self, run_id: str) -> tuple[str, int]:
        assert run_id == self.workflow_run.run_id
        return self.workflow_run.workflow_id, self.workflow_run.workflow_revision

    async def get(self, run_id: str, workflow: WorkflowDefinition) -> WorkflowRun:
        assert workflow.revision == self.workflow_run.workflow_revision
        return self.workflow_run

    async def save_state(self, workflow_run: WorkflowRun) -> None:
        self.workflow_run = workflow_run


def _revision(revision: int, task_id: str) -> WorkflowDefinition:
    return WorkflowDefinition(
        id="workflow",
        name=f"revision {revision}",
        revision=revision,
        tasks=(TaskDefinition(id=task_id),),
    )


def test_recovery_and_resume_use_the_persisted_revision() -> None:
    async def scenario() -> tuple[list[int], list[str], int]:
        first = _revision(1, "first")
        second = _revision(2, "second")
        repository = _PinnedWorkflowRepository(first, second)
        run_repository = _PinnedRunRepository(WorkflowRun.create("run-1", first))

        await WorkflowRecoveryService(repository, run_repository).recover_run(
            "run-1", "workflow", 1
        )
        await WorkflowResumeService(repository, run_repository).resume_run(
            "run-1", {"first": lambda: "revision-one"}
        )
        return repository.requests, [run_repository.workflow_run.workflow_id], (
            run_repository.workflow_run.workflow_revision
        )

    requested, workflow_ids, revision = asyncio.run(scenario())

    assert requested == [1, 1, 1]
    assert workflow_ids == ["workflow"]
    assert revision == 1
