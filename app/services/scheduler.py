from dataclasses import dataclass
from datetime import UTC, datetime

from app.dispatch.messages import TaskDispatchMessage
from app.dispatch.transport import TaskDispatcher
from app.engine.exceptions import DispatchError, InvalidConcurrencyLimitError
from app.engine.execution import WorkflowRun
from app.engine.status import TaskStatus
from app.services.repositories import (
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)


@dataclass(frozen=True)
class DispatchSummary:
    run_id: str
    workflow_id: str
    dispatched_task_ids: tuple[str, ...]
    messages: tuple[TaskDispatchMessage, ...]


class WorkflowScheduler:
    def __init__(
        self,
        workflow_repository: WorkflowRepository,
        run_repository: WorkflowRunRepository,
        attempt_repository: TaskAttemptRepository,
        dispatcher: TaskDispatcher,
    ) -> None:
        self._workflow_repository = workflow_repository
        self._run_repository = run_repository
        self._attempt_repository = attempt_repository
        self._dispatcher = dispatcher

    async def dispatch_ready(
        self,
        run_id: str,
        *,
        max_concurrency: int | None = None,
    ) -> DispatchSummary:
        if max_concurrency is not None and max_concurrency <= 0:
            raise InvalidConcurrencyLimitError(max_concurrency)

        workflow_id = await self._run_repository.get_workflow_id(run_id)
        workflow = await self._workflow_repository.get(workflow_id)
        workflow_run = await self._run_repository.get(run_id, workflow)
        await self._promote_due_retries(workflow_run)

        open_slots = self._open_slots(workflow_run, max_concurrency)
        messages = []
        for task_id in workflow_run.ready_tasks()[:open_slots]:
            workflow_run.dispatch_task(task_id)
            attempt_number = await self._attempt_repository.next_attempt_number(
                run_id,
                task_id,
            )
            attempt = await self._attempt_repository.create_dispatched_attempt(
                workflow_run,
                task_id,
                attempt_number,
            )
            message = TaskDispatchMessage(
                workflow_id=workflow_id,
                run_id=run_id,
                task_id=task_id,
                attempt_number=attempt.attempt_number,
                attempt_key=attempt.attempt_key,
                idempotency_key=workflow_run.task_runs[task_id].idempotency_key
                or f"{run_id}:{task_id}",
            )
            try:
                await self._dispatcher.dispatch(message)
            except Exception as exc:
                raise DispatchError("Failed to publish dispatch message.") from exc
            messages.append(message)

        return DispatchSummary(
            run_id=run_id,
            workflow_id=workflow_id,
            dispatched_task_ids=tuple(message.task_id for message in messages),
            messages=tuple(messages),
        )

    async def _promote_due_retries(self, workflow_run: WorkflowRun) -> None:
        now = datetime.now(UTC)
        promoted = False
        for task_id, task_run in workflow_run.task_runs.items():
            if (
                task_run.status == TaskStatus.RETRY_WAITING
                and task_run.next_retry_at is not None
                and task_run.next_retry_at <= now
            ):
                workflow_run.make_retry_ready(task_id)
                promoted = True
        if promoted:
            await self._run_repository.save_state(workflow_run)

    def _open_slots(
        self,
        workflow_run: WorkflowRun,
        max_concurrency: int | None,
    ) -> int:
        if max_concurrency is None:
            return len(workflow_run.ready_tasks())
        outstanding = sum(
            task_run.status in {TaskStatus.DISPATCHED, TaskStatus.RUNNING}
            for task_run in workflow_run.task_runs.values()
        )
        return max(max_concurrency - outstanding, 0)
