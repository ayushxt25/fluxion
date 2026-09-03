from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from app.engine.status import TaskStatus, WorkflowStatus
from app.services.repositories import WorkflowRepository, WorkflowRunRepository


@dataclass(frozen=True)
class WorkflowRecoveryResult:
    run_id: str
    workflow_id: str
    previous_status: WorkflowStatus
    recovered_status: WorkflowStatus
    interrupted_task_ids: tuple[str, ...]
    task_statuses: Mapping[str, TaskStatus]
    resumable: bool

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "interrupted_task_ids",
            tuple(sorted(self.interrupted_task_ids)),
        )
        object.__setattr__(
            self,
            "task_statuses",
            MappingProxyType(dict(self.task_statuses)),
        )


class WorkflowRecoveryService:
    def __init__(
        self,
        workflow_repository: WorkflowRepository,
        run_repository: WorkflowRunRepository,
    ) -> None:
        self._workflow_repository = workflow_repository
        self._run_repository = run_repository

    async def recover_incomplete_runs(self) -> tuple[WorkflowRecoveryResult, ...]:
        incomplete_runs = await self._run_repository.list_incomplete()
        results = []
        for run_ref in incomplete_runs:
            results.append(
                await self.recover_run(
                    run_id=run_ref.run_id,
                    workflow_id=run_ref.workflow_id,
                )
            )
        return tuple(results)

    async def recover_run(
        self,
        run_id: str,
        workflow_id: str,
    ) -> WorkflowRecoveryResult:
        workflow = await self._workflow_repository.get(workflow_id)
        workflow_run = await self._run_repository.get(run_id, workflow)
        previous_status = workflow_run.status

        workflow_run.reconcile_readiness_for_recovery()
        interrupted_task_ids = workflow_run.interrupt_running_tasks_for_recovery()
        workflow_run.reconcile_readiness_for_recovery()

        await self._run_repository.save_state(workflow_run)

        return WorkflowRecoveryResult(
            run_id=workflow_run.run_id,
            workflow_id=workflow_run.workflow_id,
            previous_status=previous_status,
            recovered_status=workflow_run.status,
            interrupted_task_ids=interrupted_task_ids,
            task_statuses={
                task_id: task_run.status
                for task_id, task_run in workflow_run.task_runs.items()
            },
            resumable=(
                not interrupted_task_ids
                and workflow_run.status
                in {WorkflowStatus.PENDING, WorkflowStatus.RUNNING}
            ),
        )
