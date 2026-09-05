import asyncio
import inspect
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from app.dispatch.messages import TaskDispatchMessage
from app.dispatch.transport import TaskDispatcher
from app.engine.context import TaskExecutionContext
from app.engine.exceptions import DispatchStateError
from app.engine.execution import TaskAttempt, WorkflowRun
from app.engine.registry import TaskCallable, TaskRegistry
from app.engine.status import AttemptStatus, TaskStatus, WorkflowStatus
from app.schemas.workflow import WorkflowDefinition
from app.services.repositories import (
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)


@dataclass(frozen=True)
class TaskWorkerResult:
    run_id: str
    workflow_id: str
    task_id: str
    attempt_number: int
    attempt_key: str
    attempt_status: AttemptStatus
    task_status: TaskStatus
    workflow_status: WorkflowStatus


class TaskWorker:
    def __init__(
        self,
        workflow_repository: WorkflowRepository,
        run_repository: WorkflowRunRepository,
        attempt_repository: TaskAttemptRepository,
        dispatcher: TaskDispatcher,
        task_registry: TaskRegistry | dict[str, TaskCallable],
    ) -> None:
        self._workflow_repository = workflow_repository
        self._run_repository = run_repository
        self._attempt_repository = attempt_repository
        self._dispatcher = dispatcher
        self._registry = (
            task_registry
            if isinstance(task_registry, TaskRegistry)
            else TaskRegistry(task_registry)
        )

    async def run_once(self, timeout: float | None = None) -> TaskWorkerResult | None:
        message = await self._dispatcher.receive(timeout)
        if message is None:
            return None
        return await self.process_message(message)

    async def process_message(
        self,
        message: TaskDispatchMessage,
    ) -> TaskWorkerResult:
        workflow = await self._workflow_repository.get(message.workflow_id)
        workflow_run = await self._run_repository.get(message.run_id, workflow)
        attempt = await self._load_and_validate(message, workflow, workflow_run)
        self._registry.binding(message.task_id)

        workflow_run.start_dispatched_task(message.task_id)
        running_attempt = await self._attempt_repository.start_dispatched_attempt(
            workflow_run,
            attempt,
            datetime.now(UTC),
        )

        error: str | None = None
        try:
            await self._call_task(message, running_attempt, workflow_run)
        except Exception as exc:  # noqa: BLE001
            error = f"{type(exc).__name__}: {exc}"

        if error is None:
            workflow_run.complete_task(message.task_id)
            await self._attempt_repository.finish_attempt(
                workflow_run,
                running_attempt,
                AttemptStatus.SUCCEEDED,
                datetime.now(UTC),
            )
            attempt_status = AttemptStatus.SUCCEEDED
        else:
            retry_at = self._retry_time(
                workflow,
                message.task_id,
                message.attempt_number,
            )
            error_type, _, error_message = error.partition(": ")
            if retry_at is None:
                workflow_run.fail_task(message.task_id)
            else:
                workflow_run.schedule_retry(message.task_id, retry_at)
            await self._attempt_repository.finish_attempt(
                workflow_run,
                running_attempt,
                AttemptStatus.FAILED,
                datetime.now(UTC),
                error_type=error_type,
                error_message=error_message,
            )
            attempt_status = AttemptStatus.FAILED

        return TaskWorkerResult(
            run_id=message.run_id,
            workflow_id=message.workflow_id,
            task_id=message.task_id,
            attempt_number=message.attempt_number,
            attempt_key=message.attempt_key,
            attempt_status=attempt_status,
            task_status=workflow_run.get_task_status(message.task_id),
            workflow_status=workflow_run.status,
        )

    async def _load_and_validate(
        self,
        message: TaskDispatchMessage,
        workflow: WorkflowDefinition,
        workflow_run: WorkflowRun,
    ) -> TaskAttempt:
        if workflow.id != message.workflow_id:
            raise DispatchStateError(
                message.run_id,
                message.task_id,
                "workflow_id does not match persisted workflow.",
            )
        if message.task_id not in workflow_run.task_runs:
            raise DispatchStateError(message.run_id, message.task_id, "unknown task.")
        task_run = workflow_run.task_runs[message.task_id]
        if task_run.idempotency_key != message.idempotency_key:
            raise DispatchStateError(
                message.run_id,
                message.task_id,
                "idempotency_key does not match persisted task run.",
            )
        attempts = await self._attempt_repository.list_attempts(
            message.run_id,
            message.task_id,
        )
        attempt = next(
            (
                item
                for item in attempts
                if item.attempt_number == message.attempt_number
            ),
            None,
        )
        if attempt is None:
            raise DispatchStateError(
                message.run_id,
                message.task_id,
                "unknown attempt.",
            )
        if attempt.attempt_key != message.attempt_key:
            raise DispatchStateError(
                message.run_id,
                message.task_id,
                "attempt_key does not match persisted attempt.",
            )
        if task_run.status != TaskStatus.DISPATCHED:
            raise DispatchStateError(
                message.run_id,
                message.task_id,
                f"task is {task_run.status}, not DISPATCHED.",
            )
        if attempt.status != AttemptStatus.DISPATCHED:
            raise DispatchStateError(
                message.run_id,
                message.task_id,
                f"attempt is {attempt.status}, not DISPATCHED.",
            )
        return attempt

    async def _call_task(
        self,
        message: TaskDispatchMessage,
        attempt: TaskAttempt,
        workflow_run: WorkflowRun,
    ) -> None:
        binding = self._registry.binding(message.task_id)
        arguments = ()
        if binding.accepts_context:
            arguments = (
                TaskExecutionContext(
                    workflow_id=message.workflow_id,
                    run_id=message.run_id,
                    task_id=message.task_id,
                    attempt_number=message.attempt_number,
                    attempt_key=message.attempt_key,
                    idempotency_key=workflow_run.task_runs[
                        message.task_id
                    ].idempotency_key,
                ),
            )

        if binding.is_async:
            await binding.implementation(*arguments)
        else:
            result = await asyncio.to_thread(binding.implementation, *arguments)
            if inspect.isawaitable(result):
                await result

    def _retry_time(
        self,
        workflow: WorkflowDefinition,
        task_id: str,
        attempt_number: int,
    ) -> datetime | None:
        task = next(task for task in workflow.tasks if task.id == task_id)
        if attempt_number >= task.retry_policy.max_attempts:
            return None
        delay = task.retry_policy.delay_after_failure(attempt_number)
        return datetime.now(UTC) + timedelta(seconds=delay)
