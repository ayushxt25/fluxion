from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType

from app.engine.exceptions import RecoveryStateError
from app.engine.status import AttemptStatus, TaskStatus, WorkflowStatus
from app.services.repositories import (
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)

_NON_RESUMABLE_TASK_STATUSES = {
    TaskStatus.DISPATCHED,
    TaskStatus.RUNNING,
    TaskStatus.INTERRUPTED,
    TaskStatus.FAILED,
    TaskStatus.CANCELLED,
}


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
        attempt_repository: TaskAttemptRepository | None = None,
    ) -> None:
        self._workflow_repository = workflow_repository
        self._run_repository = run_repository
        self._attempt_repository = attempt_repository or (
            TaskAttemptRepository(run_repository._session)
            if hasattr(run_repository, "_session")
            else None
        )

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
        now = datetime.now(UTC)

        leased_running_task_ids = set()
        if self._attempt_repository is not None:
            leased_running_task_ids = await self._validate_attempts(workflow_run)
        workflow_run.reconcile_readiness_for_recovery(now)
        interrupted_task_ids = workflow_run.interrupt_running_tasks_for_recovery(
            leased_running_task_ids
        )
        workflow_run.reconcile_readiness_for_recovery(now)

        if interrupted_task_ids and self._attempt_repository is not None:
            await self._attempt_repository.interrupt_running_attempts(
                workflow_run,
                now,
                interrupted_task_ids,
            )
        else:
            await self._run_repository.save_state(workflow_run)

        task_statuses = {
            task_id: task_run.status
            for task_id, task_run in workflow_run.task_runs.items()
        }

        return WorkflowRecoveryResult(
            run_id=workflow_run.run_id,
            workflow_id=workflow_run.workflow_id,
            previous_status=previous_status,
            recovered_status=workflow_run.status,
            interrupted_task_ids=interrupted_task_ids,
            task_statuses=task_statuses,
            resumable=(
                workflow_run.status in {WorkflowStatus.PENDING, WorkflowStatus.RUNNING}
                and not any(
                    status in _NON_RESUMABLE_TASK_STATUSES
                    for status in task_statuses.values()
                )
            ),
        )

    async def _validate_attempts(self, workflow_run) -> set[str]:
        attempts = await self._attempt_repository.list_run_attempts(workflow_run.run_id)
        by_task: dict[str, list] = {task_id: [] for task_id in workflow_run.task_runs}
        for attempt in attempts:
            if attempt.task_id not in by_task:
                raise RecoveryStateError(
                    workflow_run.run_id,
                    f"attempt references unknown task '{attempt.task_id}'.",
                )
            by_task[attempt.task_id].append(attempt)

        leased_running_task_ids = set()
        for task_id, task_attempts in by_task.items():
            numbers = [attempt.attempt_number for attempt in task_attempts]
            if numbers != list(range(1, len(numbers) + 1)):
                raise RecoveryStateError(
                    workflow_run.run_id,
                    f"attempt numbers for task '{task_id}' are not contiguous.",
                )

            dispatched_attempts = [
                attempt
                for attempt in task_attempts
                if attempt.status == AttemptStatus.DISPATCHED
            ]
            running_attempts = [
                attempt
                for attempt in task_attempts
                if attempt.status == AttemptStatus.RUNNING
            ]
            if len(dispatched_attempts) > 1:
                raise RecoveryStateError(
                    workflow_run.run_id,
                    f"task '{task_id}' has multiple dispatched attempts.",
                )
            if len(running_attempts) > 1:
                raise RecoveryStateError(
                    workflow_run.run_id,
                    f"task '{task_id}' has multiple running attempts.",
                )
            task_status = workflow_run.get_task_status(task_id)
            if task_status == TaskStatus.DISPATCHED and not dispatched_attempts:
                raise RecoveryStateError(
                    workflow_run.run_id,
                    f"task '{task_id}' is DISPATCHED without a DISPATCHED attempt.",
                )
            if task_status == TaskStatus.RUNNING and not running_attempts:
                raise RecoveryStateError(
                    workflow_run.run_id,
                    f"task '{task_id}' is RUNNING without a RUNNING attempt.",
                )
            if dispatched_attempts and task_status != TaskStatus.DISPATCHED:
                raise RecoveryStateError(
                    workflow_run.run_id,
                    f"task '{task_id}' has a DISPATCHED attempt while {task_status}.",
                )
            if running_attempts and task_status != TaskStatus.RUNNING:
                raise RecoveryStateError(
                    workflow_run.run_id,
                    f"task '{task_id}' has a RUNNING attempt while {task_status}.",
                )
            for attempt in task_attempts:
                has_any_lease_metadata = any(
                    (
                        attempt.worker_id,
                        attempt.lease_token,
                        attempt.last_heartbeat_at,
                        attempt.lease_expires_at,
                    )
                )
                if (
                    attempt.status != AttemptStatus.RUNNING
                    and has_any_lease_metadata
                ):
                    raise RecoveryStateError(
                        workflow_run.run_id,
                        f"task '{task_id}' has lease metadata on a "
                        f"{attempt.status} attempt.",
                    )

            for attempt in running_attempts:
                has_lease = all(
                    (
                        attempt.worker_id,
                        attempt.lease_token,
                        attempt.last_heartbeat_at,
                        attempt.lease_expires_at,
                    )
                )
                has_any_lease_metadata = any(
                    (
                        attempt.worker_id,
                        attempt.lease_token,
                        attempt.last_heartbeat_at,
                        attempt.lease_expires_at,
                    )
                )
                if has_any_lease_metadata and not has_lease:
                    raise RecoveryStateError(
                        workflow_run.run_id,
                        f"task '{task_id}' has incomplete lease metadata.",
                    )
                if has_lease:
                    leased_running_task_ids.add(task_id)
        return leased_running_task_ids
