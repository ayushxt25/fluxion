import asyncio
import inspect
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from app.engine.context import TaskExecutionContext
from app.engine.dag import WorkflowDAG
from app.engine.exceptions import (
    ExecutionPersistenceError,
    InvalidConcurrencyLimitError,
    WorkflowNotFoundError,
    WorkflowRunAlreadyExistsError,
)
from app.engine.execution import TaskAttempt, WorkflowRun
from app.engine.executor import WorkflowExecutionResult
from app.engine.registry import TaskCallable, TaskRegistry
from app.engine.status import AttemptStatus, TaskStatus, WorkflowStatus
from app.schemas.workflow import WorkflowDefinition
from app.services.repositories import (
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)


class _SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


class _AsyncSleeper:
    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


class _InMemoryTaskAttemptRepository:
    def __init__(self, run_repository=None) -> None:
        self._run_repository = run_repository
        self._attempts: dict[tuple[str, str], list[TaskAttempt]] = {}

    async def next_attempt_number(self, run_id: str, task_id: str) -> int:
        return len(self._attempts.get((run_id, task_id), [])) + 1

    async def create_running_attempt(
        self,
        workflow_run: WorkflowRun,
        task_id: str,
        attempt_number: int,
        started_at: datetime,
    ) -> TaskAttempt:
        if self._run_repository is not None:
            await self._run_repository.save_state(workflow_run)
        attempt = TaskAttempt(
            run_id=workflow_run.run_id,
            workflow_id=workflow_run.workflow_id,
            task_id=task_id,
            attempt_number=attempt_number,
            status=AttemptStatus.RUNNING,
            started_at=started_at,
        )
        self._attempts.setdefault((workflow_run.run_id, task_id), []).append(attempt)
        return attempt

    async def create_dispatched_attempt(
        self,
        workflow_run: WorkflowRun,
        task_id: str,
        attempt_number: int,
    ) -> TaskAttempt:
        if self._run_repository is not None:
            await self._run_repository.save_state(workflow_run)
        attempt = TaskAttempt(
            run_id=workflow_run.run_id,
            workflow_id=workflow_run.workflow_id,
            task_id=task_id,
            attempt_number=attempt_number,
            status=AttemptStatus.DISPATCHED,
        )
        self._attempts.setdefault((workflow_run.run_id, task_id), []).append(attempt)
        return attempt

    async def start_dispatched_attempt(
        self,
        workflow_run: WorkflowRun,
        attempt: TaskAttempt,
        started_at: datetime,
    ) -> TaskAttempt:
        if self._run_repository is not None:
            await self._run_repository.save_state(workflow_run)
        replacement = TaskAttempt(
            run_id=attempt.run_id,
            workflow_id=attempt.workflow_id,
            task_id=attempt.task_id,
            attempt_number=attempt.attempt_number,
            status=AttemptStatus.RUNNING,
            started_at=started_at,
        )
        attempts = self._attempts[(attempt.run_id, attempt.task_id)]
        attempts[attempt.attempt_number - 1] = replacement
        return replacement

    async def finish_attempt(
        self,
        workflow_run: WorkflowRun,
        attempt: TaskAttempt,
        status: AttemptStatus,
        finished_at: datetime,
        *,
        error_type: str | None = None,
        error_message: str | None = None,
    ) -> None:
        if self._run_repository is not None:
            await self._run_repository.save_state(workflow_run)
        replacement = TaskAttempt(
            run_id=attempt.run_id,
            workflow_id=attempt.workflow_id,
            task_id=attempt.task_id,
            attempt_number=attempt.attempt_number,
            status=status,
            started_at=attempt.started_at,
            finished_at=finished_at,
            error_type=error_type,
            error_message=error_message,
        )
        attempts = self._attempts[(attempt.run_id, attempt.task_id)]
        attempts[attempt.attempt_number - 1] = replacement

    async def list_attempts(
        self,
        run_id: str,
        task_id: str,
    ) -> tuple[TaskAttempt, ...]:
        return tuple(self._attempts.get((run_id, task_id), ()))

    async def list_run_attempts(self, run_id: str) -> tuple[TaskAttempt, ...]:
        return tuple(
            attempt
            for (attempt_run_id, _), attempts in sorted(self._attempts.items())
            if attempt_run_id == run_id
            for attempt in attempts
        )

    async def interrupt_running_attempts(
        self,
        workflow_run: WorkflowRun,
        finished_at: datetime,
    ) -> None:
        if self._run_repository is not None:
            await self._run_repository.save_state(workflow_run)

        for key, attempts in self._attempts.items():
            attempt_run_id, _ = key
            if attempt_run_id != workflow_run.run_id:
                continue

            self._attempts[key] = [
                TaskAttempt(
                    run_id=attempt.run_id,
                    workflow_id=attempt.workflow_id,
                    task_id=attempt.task_id,
                    attempt_number=attempt.attempt_number,
                    status=AttemptStatus.INTERRUPTED
                    if attempt.status == AttemptStatus.RUNNING
                    else attempt.status,
                    started_at=attempt.started_at,
                    finished_at=finished_at
                    if attempt.status == AttemptStatus.RUNNING
                    else attempt.finished_at,
                    error_type=attempt.error_type,
                    error_message=attempt.error_message,
                )
                for attempt in attempts
            ]


class PersistentWorkflowExecutor:
    def __init__(
        self,
        workflow: WorkflowDefinition,
        task_registry: TaskRegistry | dict[str, TaskCallable],
        workflow_repository: WorkflowRepository,
        run_repository: WorkflowRunRepository,
        attempt_repository: TaskAttemptRepository | None = None,
        *,
        dag: WorkflowDAG | None = None,
        run_id: str | None = None,
        max_concurrency: int | None = None,
    ) -> None:
        if max_concurrency is not None and max_concurrency <= 0:
            raise InvalidConcurrencyLimitError(max_concurrency)

        self._workflow = workflow
        self._dag = dag or WorkflowDAG(workflow)
        self._registry = (
            task_registry
            if isinstance(task_registry, TaskRegistry)
            else TaskRegistry(task_registry)
        )
        self._workflow_repository = workflow_repository
        self._run_repository = run_repository
        self._attempt_repository = attempt_repository or (
            TaskAttemptRepository(run_repository._session)
            if hasattr(run_repository, "_session")
            else _InMemoryTaskAttemptRepository(run_repository)
        )
        self._run = WorkflowRun.create(run_id or str(uuid4()), workflow, self._dag)
        self._max_concurrency = max_concurrency

    async def run(self) -> WorkflowExecutionResult:
        self._registry.validate_workflow(self._workflow)
        if not await self._workflow_repository.exists(self._workflow.id):
            raise WorkflowNotFoundError(self._workflow.id)
        await self._persist_initial_run()

        return await _DurableWorkflowRunner(
            workflow=self._workflow,
            workflow_run=self._run,
            task_registry=self._registry,
            run_repository=self._run_repository,
            attempt_repository=self._attempt_repository,
            max_concurrency=self._max_concurrency,
        ).run()

    async def _persist_initial_run(self) -> None:
        try:
            await self._run_repository.create(self._run)
        except WorkflowRunAlreadyExistsError:
            raise
        except Exception as exc:
            raise ExecutionPersistenceError(self._run.run_id, "run creation") from exc


class _DurableWorkflowRunner:
    def __init__(
        self,
        workflow: WorkflowDefinition,
        workflow_run: WorkflowRun,
        task_registry: TaskRegistry,
        run_repository: WorkflowRunRepository,
        attempt_repository: TaskAttemptRepository,
        max_concurrency: int | None,
        clock: _SystemClock | None = None,
        sleeper: _AsyncSleeper | None = None,
    ) -> None:
        self._workflow = workflow
        self._tasks = {task.id: task for task in workflow.tasks}
        self._run = workflow_run
        self._registry = task_registry
        self._run_repository = run_repository
        self._attempt_repository = attempt_repository
        self._max_concurrency = max_concurrency
        self._clock = clock or _SystemClock()
        self._sleeper = sleeper or _AsyncSleeper()
        self._errors: dict[str, str] = {}

    async def run(self) -> WorkflowExecutionResult:
        running: dict[
            asyncio.Task[tuple[str, str | None]],
            tuple[str, TaskAttempt],
        ] = {}

        while True:
            if self._run.status != WorkflowStatus.FAILED:
                await self._promote_due_retries()
                await self._schedule_ready_tasks(running)

            if not running:
                if await self._sleep_until_next_retry():
                    continue
                break

            done, _ = await asyncio.wait(
                running,
                return_when=asyncio.FIRST_COMPLETED,
            )
            await self._process_completed_tasks(done, running)

        return self._snapshot()

    async def _persist_state(self, operation: str) -> None:
        try:
            await self._run_repository.save_state(self._run)
        except Exception as exc:
            raise ExecutionPersistenceError(self._run.run_id, operation) from exc

    async def _schedule_ready_tasks(
        self,
        running: dict[asyncio.Task[tuple[str, str | None]], tuple[str, TaskAttempt]],
    ) -> None:
        open_slots = self._open_slots(len(running))
        for task_id in self._run.ready_tasks()[:open_slots]:
            self._run.start_task(task_id)
            try:
                attempt_number = await self._attempt_repository.next_attempt_number(
                    self._run.run_id,
                    task_id,
                )
                attempt = await self._attempt_repository.create_running_attempt(
                    self._run,
                    task_id,
                    attempt_number,
                    self._clock.now(),
                )
            except Exception as exc:
                raise ExecutionPersistenceError(
                    self._run.run_id,
                    f"starting attempt for task '{task_id}'",
                ) from exc
            task = asyncio.create_task(self._execute_task(task_id, attempt))
            running[task] = (task_id, attempt)

    def _open_slots(self, running_count: int) -> int:
        if self._max_concurrency is None:
            return len(self._run.ready_tasks())
        return max(self._max_concurrency - running_count, 0)

    async def _process_completed_tasks(
        self,
        done: set[asyncio.Task[tuple[str, str | None]]],
        running: dict[asyncio.Task[tuple[str, str | None]], tuple[str, TaskAttempt]],
    ) -> None:
        completed = sorted(
            (task_id, attempt, task)
            for task in done
            for task_id, attempt in (running.pop(task),)
        )
        failed_task_ids = set()

        for task_id, attempt, task in completed:
            _, error = task.result()
            if error is not None:
                failed_task_ids.add(task_id)
                retry_at = self._retry_time(task_id, attempt.attempt_number)
                if retry_at is None:
                    self._errors[task_id] = error
                    self._run.fail_task(task_id)
                else:
                    self._run.schedule_retry(task_id, retry_at)
                error_type, _, error_message = error.partition(": ")
                await self._finish_attempt(
                    task_id,
                    attempt,
                    AttemptStatus.FAILED,
                    error_type=error_type,
                    error_message=error_message,
                )

        for task_id, attempt, task in completed:
            if task_id in failed_task_ids:
                continue
            task.result()
            self._run.complete_task(task_id)
            await self._finish_attempt(task_id, attempt, AttemptStatus.SUCCEEDED)

    async def _finish_attempt(
        self,
        task_id: str,
        attempt: TaskAttempt,
        status: AttemptStatus,
        *,
        error_type: str | None = None,
        error_message: str | None = None,
    ) -> None:
        try:
            await self._attempt_repository.finish_attempt(
                self._run,
                attempt,
                status,
                self._clock.now(),
                error_type=error_type,
                error_message=error_message,
            )
        except Exception as exc:
            raise ExecutionPersistenceError(
                self._run.run_id,
                f"finishing attempt for task '{task_id}'",
            ) from exc

    async def _execute_task(
        self,
        task_id: str,
        attempt: TaskAttempt,
    ) -> tuple[str, str | None]:
        try:
            await self._call_task(task_id, attempt)
        except Exception as exc:  # noqa: BLE001
            return task_id, f"{type(exc).__name__}: {exc}"
        return task_id, None

    def _retry_time(self, task_id: str, attempt_number: int) -> datetime | None:
        policy = self._tasks[task_id].retry_policy
        if attempt_number >= policy.max_attempts:
            return None
        delay = policy.delay_after_failure(attempt_number)
        return self._clock.now() + timedelta(seconds=delay)

    async def _promote_due_retries(self) -> None:
        now = self._clock.now()
        promoted = False
        for task_id, task_run in self._run.task_runs.items():
            if (
                task_run.status == TaskStatus.RETRY_WAITING
                and task_run.next_retry_at is not None
                and task_run.next_retry_at <= now
            ):
                self._run.make_retry_ready(task_id)
                promoted = True
        if promoted:
            await self._persist_state("promoting due retries")

    async def _sleep_until_next_retry(self) -> bool:
        if self._run.status == WorkflowStatus.FAILED:
            return False

        retry_times = [
            task_run.next_retry_at
            for task_run in self._run.task_runs.values()
            if (
                task_run.status == TaskStatus.RETRY_WAITING
                and task_run.next_retry_at is not None
            )
        ]
        if not retry_times:
            return False

        delay = max((min(retry_times) - self._clock.now()).total_seconds(), 0)
        await self._sleeper.sleep(delay)
        return True

    async def _call_task(self, task_id: str, attempt: TaskAttempt) -> None:
        binding = self._registry.binding(task_id)
        arguments = ()
        if binding.accepts_context:
            task_run = self._run.task_runs[task_id]
            arguments = (
                TaskExecutionContext(
                    workflow_id=self._run.workflow_id,
                    run_id=self._run.run_id,
                    task_id=task_id,
                    attempt_number=attempt.attempt_number,
                    attempt_key=attempt.attempt_key,
                    idempotency_key=task_run.idempotency_key
                    or f"{self._run.run_id}:{task_id}",
                ),
            )

        if binding.is_async:
            await binding.implementation(*arguments)
        else:
            result = await asyncio.to_thread(binding.implementation, *arguments)
            if inspect.isawaitable(result):
                await result

    def _snapshot(self) -> WorkflowExecutionResult:
        return WorkflowExecutionResult(
            run_id=self._run.run_id,
            workflow_id=self._run.workflow_id,
            status=self._run.status,
            task_statuses={
                task_id: task_run.status
                for task_id, task_run in self._run.task_runs.items()
            },
            errors=self._errors,
        )
