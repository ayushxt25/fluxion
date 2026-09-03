from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.db.models.execution import TaskRunRecord, WorkflowRunRecord
from app.db.models.workflow import (
    TaskDefinitionRecord,
    TaskDependencyRecord,
    WorkflowDefinitionRecord,
)
from app.engine.dag import WorkflowDAG
from app.engine.exceptions import (
    PersistenceError,
    WorkflowAlreadyExistsError,
    WorkflowNotFoundError,
    WorkflowRunAlreadyExistsError,
    WorkflowRunNotFoundError,
)
from app.engine.execution import WorkflowRun
from app.engine.status import TaskStatus, WorkflowStatus
from app.schemas.workflow import TaskDefinition, WorkflowDefinition


class WorkflowRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, workflow: WorkflowDefinition) -> None:
        WorkflowDAG(workflow)

        try:
            async with self._session.begin():
                if await self._session.get(WorkflowDefinitionRecord, workflow.id):
                    raise WorkflowAlreadyExistsError(workflow.id)

                record = WorkflowDefinitionRecord(id=workflow.id, name=workflow.name)
                record.tasks = [
                    TaskDefinitionRecord(
                        workflow_id=workflow.id,
                        task_id=task.id,
                        name=task.name,
                    )
                    for task in workflow.tasks
                ]
                self._session.add(record)
                await self._session.flush()

                dependencies = [
                    TaskDependencyRecord(
                        workflow_id=workflow.id,
                        task_id=task.id,
                        depends_on_task_id=dependency_id,
                    )
                    for task in workflow.tasks
                    for dependency_id in task.depends_on
                ]
                self._session.add_all(dependencies)
        except IntegrityError as exc:
            raise PersistenceError(
                f"Failed to persist workflow '{workflow.id}'."
            ) from exc

    async def get(self, workflow_id: str) -> WorkflowDefinition:
        async with self._session.begin():
            result = await self._session.execute(
                select(WorkflowDefinitionRecord)
                .where(WorkflowDefinitionRecord.id == workflow_id)
                .options(selectinload(WorkflowDefinitionRecord.tasks))
            )
            record = result.scalar_one_or_none()
            if record is None:
                raise WorkflowNotFoundError(workflow_id)

            dependency_result = await self._session.execute(
                select(TaskDependencyRecord).where(
                    TaskDependencyRecord.workflow_id == workflow_id
                )
            )
            dependencies: dict[str, list[str]] = {
                task.task_id: [] for task in record.tasks
            }
            for dependency in dependency_result.scalars():
                dependencies[dependency.task_id].append(dependency.depends_on_task_id)

            workflow = WorkflowDefinition(
                id=record.id,
                name=record.name,
                tasks=tuple(
                    TaskDefinition(
                        id=task.task_id,
                        name=task.name,
                        depends_on=tuple(sorted(dependencies[task.task_id])),
                    )
                    for task in sorted(record.tasks, key=lambda item: item.task_id)
                ),
            )

        WorkflowDAG(workflow)
        return workflow

    async def exists(self, workflow_id: str) -> bool:
        async with self._session.begin():
            return (
                await self._session.get(WorkflowDefinitionRecord, workflow_id)
                is not None
            )


class WorkflowRunRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(self, workflow_run: WorkflowRun) -> None:
        try:
            async with self._session.begin():
                if await self._session.get(WorkflowRunRecord, workflow_run.run_id):
                    raise WorkflowRunAlreadyExistsError(workflow_run.run_id)

                record = WorkflowRunRecord(
                    run_id=workflow_run.run_id,
                    workflow_id=workflow_run.workflow_id,
                    status=workflow_run.status.value,
                )
                self._session.add(record)
                await self._session.flush()

                self._session.add_all(
                    [
                        TaskRunRecord(
                            run_id=workflow_run.run_id,
                            workflow_id=workflow_run.workflow_id,
                            task_id=task_id,
                            status=task_run.status.value,
                        )
                        for task_id, task_run in workflow_run.task_runs.items()
                    ]
                )
        except IntegrityError as exc:
            raise PersistenceError(
                f"Failed to persist workflow run '{workflow_run.run_id}'."
            ) from exc

    async def get(self, run_id: str, workflow: WorkflowDefinition) -> WorkflowRun:
        async with self._session.begin():
            result = await self._session.execute(
                select(WorkflowRunRecord)
                .where(WorkflowRunRecord.run_id == run_id)
                .where(WorkflowRunRecord.workflow_id == workflow.id)
                .options(selectinload(WorkflowRunRecord.task_runs))
            )
            record = result.scalar_one_or_none()
            if record is None:
                raise WorkflowRunNotFoundError(run_id)

            return WorkflowRun.restore(
                run_id=record.run_id,
                workflow=workflow,
                status=WorkflowStatus(record.status),
                task_statuses={
                    task_run.task_id: TaskStatus(task_run.status)
                    for task_run in record.task_runs
                },
            )

    async def save_state(self, workflow_run: WorkflowRun) -> None:
        async with self._session.begin():
            result = await self._session.execute(
                select(WorkflowRunRecord)
                .where(WorkflowRunRecord.run_id == workflow_run.run_id)
                .where(WorkflowRunRecord.workflow_id == workflow_run.workflow_id)
                .options(selectinload(WorkflowRunRecord.task_runs))
            )
            record = result.scalar_one_or_none()
            if record is None:
                raise WorkflowRunNotFoundError(workflow_run.run_id)

            record.status = workflow_run.status.value
            task_records = {task.task_id: task for task in record.task_runs}
            if set(task_records) != set(workflow_run.task_runs):
                raise PersistenceError(
                    f"Persisted task runs for '{workflow_run.run_id}' do not match "
                    "the domain workflow run."
                )

            for task_id, task_run in workflow_run.task_runs.items():
                task_records[task_id].status = task_run.status.value
