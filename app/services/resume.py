from app.engine.exceptions import (
    InvalidConcurrencyLimitError,
    WorkflowRunNotResumableError,
)
from app.engine.executor import WorkflowExecutionResult
from app.engine.registry import TaskCallable, TaskRegistry
from app.engine.status import TaskStatus
from app.services.execution import (
    _DurableWorkflowRunner,
    _InMemoryTaskAttemptRepository,
)
from app.services.recovery import WorkflowRecoveryService
from app.services.repositories import (
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)


class WorkflowResumeService:
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
            else _InMemoryTaskAttemptRepository(run_repository)
        )
        self._recovery = WorkflowRecoveryService(
            workflow_repository,
            run_repository,
            self._attempt_repository,
        )

    async def resume_run(
        self,
        run_id: str,
        task_registry: TaskRegistry | dict[str, TaskCallable],
        *,
        max_concurrency: int | None = None,
    ) -> WorkflowExecutionResult:
        if max_concurrency is not None and max_concurrency <= 0:
            raise InvalidConcurrencyLimitError(max_concurrency)

        registry = (
            task_registry
            if isinstance(task_registry, TaskRegistry)
            else TaskRegistry(task_registry)
        )
        workflow_id = await self._run_repository.get_workflow_id(run_id)
        recovery_result = await self._recovery.recover_run(run_id, workflow_id)
        if not recovery_result.resumable:
            raise WorkflowRunNotResumableError(
                run_id,
                f"status={recovery_result.recovered_status}, "
                f"interrupted={recovery_result.interrupted_task_ids}",
            )

        workflow = await self._workflow_repository.get(workflow_id)
        workflow_run = await self._run_repository.get(run_id, workflow)
        for task_id, task_run in workflow_run.task_runs.items():
            if task_run.status != TaskStatus.SUCCEEDED:
                registry.get(task_id)

        return await _DurableWorkflowRunner(
            workflow=workflow,
            workflow_run=workflow_run,
            task_registry=registry,
            run_repository=self._run_repository,
            attempt_repository=self._attempt_repository,
            max_concurrency=max_concurrency,
        ).run()
