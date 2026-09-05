import asyncio
import inspect
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from app.core.config import get_settings
from app.dispatch.messages import TaskDispatchMessage
from app.dispatch.transport import TaskDispatcher
from app.engine.context import TaskExecutionContext
from app.engine.exceptions import (
    DispatchStateError,
    InvalidWorkerLeaseConfigurationError,
    LeaseLostError,
)
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
        *,
        worker_id: str | None = None,
        lease_seconds: float | None = None,
        heartbeat_seconds: float | None = None,
    ) -> None:
        settings = get_settings()
        self.worker_id = worker_id or str(uuid4())
        self._lease_seconds = (
            lease_seconds
            if lease_seconds is not None
            else settings.worker_lease_seconds
        )
        self._heartbeat_seconds = (
            heartbeat_seconds
            if heartbeat_seconds is not None
            else settings.worker_heartbeat_seconds
        )
        if self._lease_seconds <= 0:
            raise InvalidWorkerLeaseConfigurationError(
                "lease duration must be positive."
            )
        if (
            self._heartbeat_seconds <= 0
            or self._heartbeat_seconds >= self._lease_seconds
        ):
            raise InvalidWorkerLeaseConfigurationError(
                "heartbeat interval must be positive and less than lease duration."
            )
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

        lease_token = str(uuid4())
        now = datetime.now(UTC)
        workflow_run.start_dispatched_task(message.task_id)
        running_attempt = await self._attempt_repository.claim_dispatched_attempt(
            workflow_run,
            attempt,
            self.worker_id,
            lease_token,
            now,
            self._lease_seconds,
        )

        error: str | None = None
        try:
            await self._call_task_with_heartbeat(message, running_attempt, workflow_run)
        except Exception as exc:  # noqa: BLE001
            if isinstance(exc, LeaseLostError):
                raise
            error = f"{type(exc).__name__}: {exc}"

        if error is None:
            workflow_run.complete_task(message.task_id)
            await self._attempt_repository.finish_leased_attempt(
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
            await self._attempt_repository.finish_leased_attempt(
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

    async def _call_task_with_heartbeat(
        self,
        message: TaskDispatchMessage,
        attempt: TaskAttempt,
        workflow_run: WorkflowRun,
    ) -> None:
        callable_task = asyncio.create_task(
            self._call_task(message, attempt, workflow_run)
        )
        heartbeat_task = asyncio.create_task(
            self._heartbeat_until_done(message, attempt, callable_task)
        )
        done, _ = await asyncio.wait(
            {callable_task, heartbeat_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if heartbeat_task in done:
            heartbeat_error = heartbeat_task.exception()
            if heartbeat_error is not None:
                callable_task.cancel()
                with suppress(asyncio.CancelledError):
                    await callable_task
                raise heartbeat_error
        else:
            heartbeat_task.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat_task
        await callable_task

    async def _heartbeat_until_done(
        self,
        message: TaskDispatchMessage,
        attempt: TaskAttempt,
        callable_task: asyncio.Task[None],
    ) -> None:
        while not callable_task.done():
            await asyncio.sleep(self._heartbeat_seconds)
            if callable_task.done():
                return
            await self._attempt_repository.heartbeat(
                message.run_id,
                message.task_id,
                message.attempt_number,
                self.worker_id,
                attempt.lease_token,
                datetime.now(UTC),
                self._lease_seconds,
            )

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
