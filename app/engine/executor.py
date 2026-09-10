import asyncio
import inspect
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from uuid import uuid4

from app.engine.context import TaskExecutionContext
from app.engine.dag import WorkflowDAG
from app.engine.exceptions import InvalidConcurrencyLimitError
from app.engine.execution import WorkflowRun
from app.engine.registry import TaskCallable, TaskRegistry
from app.engine.results import JSONValue, normalize_task_result
from app.engine.status import TaskStatus, WorkflowStatus
from app.schemas.workflow import WorkflowDefinition


@dataclass(frozen=True)
class WorkflowExecutionResult:
    run_id: str
    workflow_id: str
    status: WorkflowStatus
    task_statuses: Mapping[str, TaskStatus]
    errors: Mapping[str, str]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "task_statuses",
            MappingProxyType(dict(self.task_statuses)),
        )
        object.__setattr__(self, "errors", MappingProxyType(dict(self.errors)))


class WorkflowExecutor:
    def __init__(
        self,
        workflow: WorkflowDefinition,
        task_registry: TaskRegistry | dict[str, TaskCallable],
        *,
        dag: WorkflowDAG | None = None,
        run_id: str | None = None,
        max_concurrency: int | None = None,
        max_task_result_bytes: int = 262144,
    ) -> None:
        if max_concurrency is not None and max_concurrency <= 0:
            raise InvalidConcurrencyLimitError(max_concurrency)
        if max_task_result_bytes <= 0:
            raise ValueError("max_task_result_bytes must be positive.")

        self._workflow = workflow
        self._dag = dag or WorkflowDAG(workflow)
        self._registry = (
            task_registry
            if isinstance(task_registry, TaskRegistry)
            else TaskRegistry(task_registry)
        )
        self._run = WorkflowRun.create(run_id or str(uuid4()), workflow, self._dag)
        self._max_concurrency = max_concurrency
        self._max_task_result_bytes = max_task_result_bytes
        self._errors: dict[str, str] = {}
        self._results: dict[str, JSONValue] = {}

    async def run(self) -> WorkflowExecutionResult:
        self._registry.validate_workflow(self._workflow)
        running: dict[asyncio.Task[tuple[str, JSONValue, str | None]], str] = {}

        while True:
            if self._run.status != WorkflowStatus.FAILED:
                self._schedule_ready_tasks(running)

            if not running:
                break

            done, _ = await asyncio.wait(
                running,
                return_when=asyncio.FIRST_COMPLETED,
            )
            self._process_completed_tasks(done, running)

        return self._snapshot()

    def _schedule_ready_tasks(
        self,
        running: dict[asyncio.Task[tuple[str, JSONValue, str | None]], str],
    ) -> None:
        open_slots = self._open_slots(len(running))
        for task_id in self._run.ready_tasks()[:open_slots]:
            self._run.start_task(task_id)
            task = asyncio.create_task(self._execute_task(task_id))
            running[task] = task_id

    def _open_slots(self, running_count: int) -> int:
        if self._max_concurrency is None:
            return len(self._run.ready_tasks())
        return max(self._max_concurrency - running_count, 0)

    def _process_completed_tasks(
        self,
        done: set[asyncio.Task[tuple[str, JSONValue, str | None]]],
        running: dict[asyncio.Task[tuple[str, JSONValue, str | None]], str],
    ) -> None:
        completed = sorted((running.pop(task), task) for task in done)

        for task_id, task in completed:
            _, _, error = task.result()
            if error is not None:
                self._errors[task_id] = error
                self._run.fail_task(task_id)

        for task_id, task in completed:
            if task_id in self._errors:
                continue
            _, result, _ = task.result()
            self._results[task_id] = result
            self._run.complete_task(task_id, result=result)

    async def _execute_task(self, task_id: str) -> tuple[str, JSONValue, str | None]:
        try:
            result = await self._call_task(task_id)
            result = normalize_task_result(result, self._max_task_result_bytes)
        except Exception as exc:  # noqa: BLE001
            return task_id, None, f"{type(exc).__name__}: {exc}"
        return task_id, result, None

    async def _call_task(self, task_id: str) -> object:
        binding = self._registry.binding(task_id)
        arguments = ()
        if binding.accepts_context:
            arguments = (
                TaskExecutionContext(
                    workflow_id=self._run.workflow_id,
                    run_id=self._run.run_id,
                    task_id=task_id,
                    attempt_number=1,
                    attempt_key=f"{self._run.run_id}:{task_id}:1",
                    idempotency_key=f"{self._run.run_id}:{task_id}",
                    dependency_results=self._run.direct_dependency_results(task_id),
                ),
            )

        if binding.is_async:
            return await binding.implementation(*arguments)
        result = await asyncio.to_thread(binding.implementation, *arguments)
        if inspect.isawaitable(result):
            return await result
        return result

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
