import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from app.engine.status import AttemptStatus, TaskStatus, WorkflowStatus
from app.observability.metrics import record_lease_reclaims
from app.services.repositories import (
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LeaseReclaimResult:
    run_id: str
    workflow_id: str
    task_id: str
    attempt_number: int
    attempt_status: AttemptStatus
    task_status: TaskStatus
    workflow_status: WorkflowStatus
    reclaimed: bool


class LeaseReaper:
    def __init__(
        self,
        workflow_repository: WorkflowRepository,
        run_repository: WorkflowRunRepository,
        attempt_repository: TaskAttemptRepository,
    ) -> None:
        self._workflow_repository = workflow_repository
        self._run_repository = run_repository
        self._attempt_repository = attempt_repository

    async def reclaim_expired(self) -> tuple[LeaseReclaimResult, ...]:
        now = datetime.now(UTC)
        expired = await self._attempt_repository.list_expired_running_attempts(now)
        results = []
        for attempt_ref in expired:
            workflow = await self._workflow_repository.get(attempt_ref.workflow_id)
            workflow_run = await self._run_repository.get(attempt_ref.run_id, workflow)
            workflow_run.interrupt_tasks_for_recovery((attempt_ref.task_id,))
            reclaimed = await self._attempt_repository.reclaim_expired_attempt(
                workflow_run,
                attempt_ref,
                now,
            )
            if reclaimed:
                results.append(
                    LeaseReclaimResult(
                        run_id=attempt_ref.run_id,
                        workflow_id=attempt_ref.workflow_id,
                        task_id=attempt_ref.task_id,
                        attempt_number=attempt_ref.attempt_number,
                        attempt_status=AttemptStatus.INTERRUPTED,
                        task_status=TaskStatus.INTERRUPTED,
                        workflow_status=WorkflowStatus.FAILED,
                        reclaimed=True,
                    )
                )
                logger.warning(
                    "Expired worker lease reclaimed.",
                    extra={
                        "event": "lease.reclaim",
                        "workflow_id": attempt_ref.workflow_id,
                        "run_id": attempt_ref.run_id,
                        "task_id": attempt_ref.task_id,
                        "attempt_number": attempt_ref.attempt_number,
                        "worker_id": attempt_ref.worker_id,
                    },
                )
        record_lease_reclaims(len(results))
        return tuple(results)
