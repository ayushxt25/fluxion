import logging
from uuid import uuid4

from app.core.config import get_settings
from app.engine.exceptions import UnknownTaskRunError, WorkflowRunNotResumableError
from app.engine.execution import TaskAttempt, WorkflowRun
from app.engine.results import JSONValue, normalize_workflow_input
from app.engine.status import WorkflowStatus
from app.observability.metrics import record_workflow_run_created
from app.schemas.api import (
    RecoveryResponse,
    TaskAttemptResponse,
    TaskRunResponse,
    WorkflowRunListItem,
    WorkflowRunResponse,
)
from app.schemas.workflow import WorkflowDefinition
from app.services.recovery import WorkflowRecoveryResult, WorkflowRecoveryService
from app.services.repositories import (
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)

logger = logging.getLogger(__name__)


class WorkflowManagementService:
    def __init__(self, repository: WorkflowRepository) -> None:
        self._repository = repository

    async def create(self, workflow: WorkflowDefinition) -> WorkflowDefinition:
        await self._repository.save(workflow)
        return workflow

    async def get(self, workflow_id: str) -> WorkflowDefinition:
        return await self._repository.get(workflow_id)

    async def list(self, limit: int, offset: int) -> tuple[WorkflowDefinition, ...]:
        return await self._repository.list(limit=limit, offset=offset)


class WorkflowRunManagementService:
    def __init__(
        self,
        workflow_repository: WorkflowRepository,
        run_repository: WorkflowRunRepository,
        attempt_repository: TaskAttemptRepository,
    ) -> None:
        self._workflow_repository = workflow_repository
        self._run_repository = run_repository
        self._attempt_repository = attempt_repository

    async def create_run(
        self,
        workflow_id: str,
        run_id: str | None = None,
        workflow_input: object = None,
        workflow_input_present: bool = False,
    ) -> WorkflowRunResponse:
        workflow = await self._workflow_repository.get(workflow_id)
        normalized_input: JSONValue = None
        if workflow_input_present:
            normalized_input = normalize_workflow_input(
                workflow_input,
                get_settings().max_workflow_input_bytes,
            )
        workflow_run = WorkflowRun.create(
            run_id or str(uuid4()),
            workflow,
            workflow_input=normalized_input,
            workflow_input_present=workflow_input_present,
        )
        await self._run_repository.create(workflow_run)
        record_workflow_run_created()
        return await self.get_run(workflow_run.run_id)

    async def get_run(self, run_id: str) -> WorkflowRunResponse:
        workflow_id = await self._run_repository.get_workflow_id(run_id)
        workflow = await self._workflow_repository.get(workflow_id)
        workflow_run = await self._run_repository.get(run_id, workflow)
        summary = await self._run_repository.get_summary(run_id)
        attempts = await self._attempt_repository.list_run_attempts(run_id)
        return _run_response(workflow, workflow_run, summary.created_at, attempts)

    async def list_runs(
        self,
        *,
        workflow_id: str | None,
        status: WorkflowStatus | None,
        limit: int,
        offset: int,
    ) -> tuple[WorkflowRunListItem, ...]:
        summaries = await self._run_repository.list(
            workflow_id=workflow_id,
            status=status,
            limit=limit,
            offset=offset,
        )
        return tuple(
            WorkflowRunListItem(
                run_id=item.run_id,
                workflow_id=item.workflow_id,
                status=item.status.value,
                created_at=item.created_at,
            )
            for item in summaries
        )

    async def list_attempts(
        self,
        run_id: str,
        task_id: str,
    ) -> tuple[TaskAttemptResponse, ...]:
        run = await self.get_run(run_id)
        if task_id not in {task.task_id for task in run.tasks}:
            raise UnknownTaskRunError(task_id)
        return tuple(
            _attempt_response(attempt)
            for attempt in await self._attempt_repository.list_attempts(
                run_id,
                task_id,
            )
        )

    async def cancel_run(self, run_id: str) -> WorkflowRunResponse:
        workflow_id = await self._run_repository.get_workflow_id(run_id)
        workflow = await self._workflow_repository.get(workflow_id)
        workflow_run = await self._run_repository.get(run_id, workflow)
        if workflow_run.status == WorkflowStatus.CANCELLED:
            return await self.get_run(run_id)
        if workflow_run.status in {WorkflowStatus.SUCCEEDED, WorkflowStatus.FAILED}:
            raise WorkflowRunNotResumableError(
                run_id,
                f"terminal workflow status is {workflow_run.status}.",
            )
        workflow_run.cancel_workflow()
        await self._run_repository.save_state(workflow_run)
        logger.info(
            "Workflow run cancelled.",
            extra={
                "event": "workflow.cancel",
                "workflow_id": workflow_id,
                "run_id": run_id,
            },
        )
        return await self.get_run(run_id)

    async def recover_run(self, run_id: str) -> RecoveryResponse:
        workflow_id = await self._run_repository.get_workflow_id(run_id)
        result = await WorkflowRecoveryService(
            self._workflow_repository,
            self._run_repository,
            self._attempt_repository,
        ).recover_run(run_id, workflow_id)
        return _recovery_response(result)

    async def continue_run(self, run_id: str) -> WorkflowRunResponse:
        recovery = await self.recover_run(run_id)
        if not recovery.resumable:
            raise WorkflowRunNotResumableError(
                run_id,
                f"status={recovery.recovered_status}, "
                f"interrupted={recovery.interrupted_task_ids}",
            )
        return await self.get_run(run_id)


def _run_response(
    workflow: WorkflowDefinition,
    workflow_run: WorkflowRun,
    created_at,
    attempts: tuple[TaskAttempt, ...],
) -> WorkflowRunResponse:
    attempts_by_task: dict[str, list[TaskAttempt]] = {
        task.id: [] for task in workflow.tasks
    }
    for attempt in attempts:
        attempts_by_task.setdefault(attempt.task_id, []).append(attempt)
    dependencies = {task.id: task.depends_on for task in workflow.tasks}
    tasks = tuple(
        TaskRunResponse(
            task_id=task_id,
            status=task_run.status.value,
            next_retry_at=task_run.next_retry_at,
            idempotency_key=(
                task_run.idempotency_key or f"{workflow_run.run_id}:{task_id}"
            ),
            result=task_run.result if task_run.result_present else None,
            has_result=task_run.result_present,
            attempt_count=len(attempts_by_task[task_id]),
            latest_attempt_status=(
                attempts_by_task[task_id][-1].status.value
                if attempts_by_task[task_id]
                else None
            ),
            dependencies=dependencies.get(task_id, ()),
        )
        for task_id, task_run in workflow_run.task_runs.items()
    )
    return WorkflowRunResponse(
        run_id=workflow_run.run_id,
        workflow_id=workflow_run.workflow_id,
        status=workflow_run.status.value,
        created_at=created_at,
        input=(
            workflow_run.workflow_input
            if workflow_run.workflow_input_present
            else None
        ),
        has_input=workflow_run.workflow_input_present,
        tasks=tasks,
    )


def _attempt_response(attempt: TaskAttempt) -> TaskAttemptResponse:
    return TaskAttemptResponse(
        attempt_number=attempt.attempt_number,
        status=attempt.status.value,
        created_at=attempt.created_at,
        started_at=attempt.started_at,
        finished_at=attempt.finished_at,
        worker_id=attempt.worker_id,
        last_heartbeat_at=attempt.last_heartbeat_at,
        lease_expires_at=attempt.lease_expires_at,
        error_type=attempt.error_type,
        error_message=attempt.error_message,
        attempt_key=attempt.attempt_key,
    )


def _recovery_response(result: WorkflowRecoveryResult) -> RecoveryResponse:
    return RecoveryResponse(
        run_id=result.run_id,
        workflow_id=result.workflow_id,
        previous_status=result.previous_status.value,
        recovered_status=result.recovered_status.value,
        interrupted_task_ids=result.interrupted_task_ids,
        task_statuses={
            task_id: status.value for task_id, status in result.task_statuses.items()
        },
        resumable=result.resumable,
    )
