import asyncio
import inspect
from uuid import uuid4

from app.engine.dag import WorkflowDAG
from app.engine.exceptions import (
    ExecutionPersistenceError,
    InvalidConcurrencyLimitError,
    WorkflowNotFoundError,
    WorkflowRunAlreadyExistsError,
)
from app.engine.execution import WorkflowRun
from app.engine.executor import WorkflowExecutionResult
from app.engine.registry import TaskCallable, TaskRegistry
from app.engine.status import WorkflowStatus
from app.schemas.workflow import WorkflowDefinition
from app.services.repositories import WorkflowRepository, WorkflowRunRepository


class PersistentWorkflowExecutor:
    def __init__(
        self,
        workflow: WorkflowDefinition,
        task_registry: TaskRegistry | dict[str, TaskCallable],
        workflow_repository: WorkflowRepository,
        run_repository: WorkflowRunRepository,
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
        self._run = WorkflowRun.create(run_id or str(uuid4()), workflow, self._dag)
        self._max_concurrency = max_concurrency
        self._errors: dict[str, str] = {}

    async def run(self) -> WorkflowExecutionResult:
        self._registry.validate_workflow(self._workflow)
        if not await self._workflow_repository.exists(self._workflow.id):
            raise WorkflowNotFoundError(self._workflow.id)
        await self._persist_initial_run()
        running: dict[asyncio.Task[str | None], str] = {}

        while True:
            if self._run.status != WorkflowStatus.FAILED:
                await self._schedule_ready_tasks(running)

            if not running:
                break

            done, _ = await asyncio.wait(
                running,
                return_when=asyncio.FIRST_COMPLETED,
            )
            await self._process_completed_tasks(done, running)

        return self._snapshot()

    async def _persist_initial_run(self) -> None:
        try:
            await self._run_repository.create(self._run)
        except WorkflowRunAlreadyExistsError:
            raise
        except Exception as exc:
            raise ExecutionPersistenceError(self._run.run_id, "run creation") from exc

    async def _persist_state(self, operation: str) -> None:
        try:
            await self._run_repository.save_state(self._run)
        except Exception as exc:
            raise ExecutionPersistenceError(self._run.run_id, operation) from exc

    async def _schedule_ready_tasks(
        self,
        running: dict[asyncio.Task[str | None], str],
    ) -> None:
        open_slots = self._open_slots(len(running))
        for task_id in self._run.ready_tasks()[:open_slots]:
            self._run.start_task(task_id)
            await self._persist_state(f"starting task '{task_id}'")
            task = asyncio.create_task(self._execute_task(task_id))
            running[task] = task_id

    def _open_slots(self, running_count: int) -> int:
        if self._max_concurrency is None:
            return len(self._run.ready_tasks())
        return max(self._max_concurrency - running_count, 0)

    async def _process_completed_tasks(
        self,
        done: set[asyncio.Task[str | None]],
        running: dict[asyncio.Task[str | None], str],
    ) -> None:
        completed = sorted((running.pop(task), task) for task in done)

        for task_id, task in completed:
            error = task.result()
            if error is not None:
                self._errors[task_id] = error
                self._run.fail_task(task_id)
                await self._persist_state(f"failing task '{task_id}'")

        for task_id, task in completed:
            if task_id in self._errors:
                continue
            task.result()
            self._run.complete_task(task_id)
            await self._persist_state(f"completing task '{task_id}'")

    async def _execute_task(self, task_id: str) -> str | None:
        try:
            await self._call_task(task_id)
        except Exception as exc:  # noqa: BLE001
            return f"{type(exc).__name__}: {exc}"
        return None

    async def _call_task(self, task_id: str) -> None:
        implementation = self._registry.get(task_id)
        if inspect.iscoroutinefunction(implementation):
            await implementation()
        else:
            result = await asyncio.to_thread(implementation)
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
