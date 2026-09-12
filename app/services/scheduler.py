import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from app.core.config import get_settings
from app.dispatch.messages import TaskDispatchMessage
from app.dispatch.transport import TaskDispatcher
from app.engine.exceptions import InvalidConcurrencyLimitError
from app.engine.execution import WorkflowRun
from app.engine.status import TaskStatus
from app.observability.metrics import (
    record_scheduler_backpressure,
    record_task_dispatches,
)
from app.services.repositories import (
    DispatchOutboxRepository,
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DispatchSummary:
    run_id: str
    workflow_id: str
    dispatched_task_ids: tuple[str, ...]
    messages: tuple[TaskDispatchMessage, ...]
    outbox_event_ids: tuple[str, ...]


class WorkflowScheduler:
    def __init__(
        self,
        workflow_repository: WorkflowRepository,
        run_repository: WorkflowRunRepository,
        attempt_repository: TaskAttemptRepository,
        dispatcher: TaskDispatcher | None = None,
        outbox_repository: DispatchOutboxRepository | None = None,
        *,
        max_dispatch_per_run: int | None = None,
        queue_high_watermark: int | None = None,
    ) -> None:
        settings = get_settings()
        self._workflow_repository = workflow_repository
        self._run_repository = run_repository
        self._attempt_repository = attempt_repository
        self._dispatcher = dispatcher
        self._max_dispatch_per_run = (
            max_dispatch_per_run
            if max_dispatch_per_run is not None
            else settings.scheduler_max_dispatch_per_run
        )
        self._queue_high_watermark = (
            queue_high_watermark
            if queue_high_watermark is not None
            else settings.dispatch_queue_high_watermark
        )
        if self._max_dispatch_per_run < 1:
            raise InvalidConcurrencyLimitError(self._max_dispatch_per_run)
        if self._queue_high_watermark < 1:
            raise InvalidConcurrencyLimitError(self._queue_high_watermark)
        self._outbox_repository = outbox_repository or (
            DispatchOutboxRepository(run_repository._session)
            if hasattr(run_repository, "_session")
            else None
        )

    async def dispatch_ready(
        self,
        run_id: str,
        *,
        max_concurrency: int | None = None,
        max_dispatch: int | None = None,
    ) -> DispatchSummary:
        if max_concurrency is not None and max_concurrency <= 0:
            raise InvalidConcurrencyLimitError(max_concurrency)
        if max_dispatch is not None and max_dispatch <= 0:
            raise InvalidConcurrencyLimitError(max_dispatch)

        workflow_id = await self._run_repository.get_workflow_id(run_id)
        workflow = await self._workflow_repository.get(workflow_id)
        workflow_run = await self._run_repository.get(run_id, workflow)
        await self._promote_due_retries(workflow_run)

        if self._dispatcher is not None:
            queue_depth = await self._dispatcher.queue_depth()
            if queue_depth >= self._queue_high_watermark:
                record_scheduler_backpressure()
                logger.info(
                    "Scheduler paused dispatch due to queue backpressure.",
                    extra={
                        "event": "scheduler.backpressure",
                        "run_id": run_id,
                        "workflow_id": workflow_id,
                        "queue_depth": queue_depth,
                    },
                )
                return DispatchSummary(
                    run_id=run_id,
                    workflow_id=workflow_id,
                    dispatched_task_ids=(),
                    messages=(),
                    outbox_event_ids=(),
                )

        open_slots = self._open_slots(workflow_run, max_concurrency)
        messages = []
        outbox_event_ids = []
        dispatch_limit = min(
            open_slots,
            self._max_dispatch_per_run,
            max_dispatch if max_dispatch is not None else self._max_dispatch_per_run,
        )
        for task_id in workflow_run.ready_tasks()[:dispatch_limit]:
            workflow_run.dispatch_task(task_id)
            attempt_number = await self._attempt_repository.next_attempt_number(
                run_id,
                task_id,
            )
            message = TaskDispatchMessage(
                workflow_id=workflow_id,
                run_id=run_id,
                task_id=task_id,
                attempt_number=attempt_number,
                attempt_key=f"{run_id}:{task_id}:{attempt_number}",
                idempotency_key=workflow_run.task_runs[task_id].idempotency_key
                or f"{run_id}:{task_id}",
            )
            if self._outbox_repository is None:
                raise RuntimeError("WorkflowScheduler requires an outbox repository.")
            _, event = await self._outbox_repository.create_dispatch_intent(
                workflow_run,
                task_id,
                attempt_number,
                message,
            )
            logger.info(
                "Task dispatch intent created.",
                extra={
                    "event": "task.dispatch",
                    "workflow_id": workflow_id,
                    "run_id": run_id,
                    "task_id": task_id,
                    "attempt_number": attempt_number,
                },
            )
            messages.append(message)
            outbox_event_ids.append(event.id)

        record_task_dispatches(len(messages))
        return DispatchSummary(
            run_id=run_id,
            workflow_id=workflow_id,
            dispatched_task_ids=tuple(message.task_id for message in messages),
            messages=tuple(messages),
            outbox_event_ids=tuple(outbox_event_ids),
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
