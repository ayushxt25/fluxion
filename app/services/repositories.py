from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.db.models.execution import TaskAttemptRecord, TaskRunRecord, WorkflowRunRecord
from app.db.models.workflow import (
    TaskDefinitionRecord,
    TaskDependencyRecord,
    WorkflowDefinitionRecord,
)
from app.engine.dag import WorkflowDAG
from app.engine.exceptions import (
    LeaseClaimError,
    LeaseLostError,
    PersistenceError,
    RecoveryStateError,
    UnknownTaskRunError,
    WorkflowAlreadyExistsError,
    WorkflowNotFoundError,
    WorkflowRunAlreadyExistsError,
    WorkflowRunNotFoundError,
)
from app.engine.execution import TaskAttempt, WorkflowRun
from app.engine.status import AttemptStatus, TaskStatus, WorkflowStatus
from app.schemas.workflow import RetryPolicy, TaskDefinition, WorkflowDefinition


@dataclass(frozen=True)
class IncompleteWorkflowRunRef:
    run_id: str
    workflow_id: str


@dataclass(frozen=True)
class ExpiredTaskAttemptRef:
    run_id: str
    workflow_id: str
    task_id: str
    attempt_number: int
    worker_id: str | None
    lease_token: str | None


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
                        retry_max_attempts=task.retry_policy.max_attempts,
                        retry_initial_backoff_seconds=(
                            task.retry_policy.initial_backoff_seconds
                        ),
                        retry_backoff_multiplier=(
                            task.retry_policy.backoff_multiplier
                        ),
                        retry_max_backoff_seconds=(
                            task.retry_policy.max_backoff_seconds
                        ),
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
                        retry_policy=RetryPolicy(
                            max_attempts=task.retry_max_attempts,
                            initial_backoff_seconds=(
                                task.retry_initial_backoff_seconds
                            ),
                            backoff_multiplier=task.retry_backoff_multiplier,
                            max_backoff_seconds=task.retry_max_backoff_seconds,
                        ),
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
                            next_retry_at=task_run.next_retry_at,
                            idempotency_key=task_run.idempotency_key
                            or f"{workflow_run.run_id}:{task_id}",
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

            try:
                workflow_status = WorkflowStatus(record.status)
                task_statuses = {
                    task_run.task_id: TaskStatus(task_run.status)
                    for task_run in record.task_runs
                }
                next_retry_at = {
                    task_run.task_id: task_run.next_retry_at
                    for task_run in record.task_runs
                }
                idempotency_keys = {
                    task_run.task_id: task_run.idempotency_key
                    for task_run in record.task_runs
                }
            except ValueError as exc:
                raise RecoveryStateError(
                    run_id,
                    "persisted run contains an unknown workflow or task status.",
                ) from exc

            try:
                return WorkflowRun.restore(
                    run_id=record.run_id,
                    workflow=workflow,
                    status=workflow_status,
                    task_statuses=task_statuses,
                    next_retry_at=next_retry_at,
                    idempotency_keys=idempotency_keys,
                )
            except UnknownTaskRunError as exc:
                raise RecoveryStateError(
                    run_id,
                    "persisted task run rows do not match workflow tasks.",
                ) from exc

    async def list_incomplete(self) -> tuple[IncompleteWorkflowRunRef, ...]:
        async with self._session.begin():
            result = await self._session.execute(
                select(WorkflowRunRecord)
                .where(
                    WorkflowRunRecord.status.in_(
                        (WorkflowStatus.PENDING.value, WorkflowStatus.RUNNING.value)
                    )
                )
                .order_by(WorkflowRunRecord.run_id)
            )
            return tuple(
                IncompleteWorkflowRunRef(
                    run_id=record.run_id,
                    workflow_id=record.workflow_id,
                )
                for record in result.scalars()
            )

    async def get_workflow_id(self, run_id: str) -> str:
        async with self._session.begin():
            result = await self._session.execute(
                select(WorkflowRunRecord.workflow_id).where(
                    WorkflowRunRecord.run_id == run_id
                )
            )
            workflow_id = result.scalar_one_or_none()
            if workflow_id is None:
                raise WorkflowRunNotFoundError(run_id)
            return workflow_id

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
                task_records[task_id].next_retry_at = task_run.next_retry_at
                task_records[task_id].idempotency_key = (
                    task_run.idempotency_key or f"{workflow_run.run_id}:{task_id}"
                )


class TaskAttemptRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def next_attempt_number(self, run_id: str, task_id: str) -> int:
        attempts = await self.list_attempts(run_id, task_id)
        if not attempts:
            return 1
        return attempts[-1].attempt_number + 1

    async def create_running_attempt(
        self,
        workflow_run: WorkflowRun,
        task_id: str,
        attempt_number: int,
        started_at: datetime,
    ) -> TaskAttempt:
        attempt = TaskAttempt(
            run_id=workflow_run.run_id,
            workflow_id=workflow_run.workflow_id,
            task_id=task_id,
            attempt_number=attempt_number,
            status=AttemptStatus.RUNNING,
            started_at=started_at,
        )
        async with self._session.begin():
            await self._save_workflow_state(workflow_run)
            self._session.add(
                TaskAttemptRecord(
                    run_id=attempt.run_id,
                    workflow_id=attempt.workflow_id,
                    task_id=attempt.task_id,
                    attempt_number=attempt.attempt_number,
                    status=attempt.status.value,
                    started_at=attempt.started_at,
                )
            )
        return attempt

    async def create_dispatched_attempt(
        self,
        workflow_run: WorkflowRun,
        task_id: str,
        attempt_number: int,
    ) -> TaskAttempt:
        attempt = TaskAttempt(
            run_id=workflow_run.run_id,
            workflow_id=workflow_run.workflow_id,
            task_id=task_id,
            attempt_number=attempt_number,
            status=AttemptStatus.DISPATCHED,
        )
        async with self._session.begin():
            await self._save_workflow_state(workflow_run)
            self._session.add(
                TaskAttemptRecord(
                    run_id=attempt.run_id,
                    workflow_id=attempt.workflow_id,
                    task_id=attempt.task_id,
                    attempt_number=attempt.attempt_number,
                    status=attempt.status.value,
                )
            )
        return attempt

    async def start_dispatched_attempt(
        self,
        workflow_run: WorkflowRun,
        attempt: TaskAttempt,
        started_at: datetime,
    ) -> TaskAttempt:
        async with self._session.begin():
            await self._save_workflow_state(workflow_run)
            record = await self._session.get(
                TaskAttemptRecord,
                (attempt.run_id, attempt.task_id, attempt.attempt_number),
            )
            if record is None:
                raise PersistenceError(
                    f"Task attempt '{attempt.attempt_key}' was not found."
                )
            record.status = AttemptStatus.RUNNING.value
            record.started_at = started_at
        return TaskAttempt(
            run_id=attempt.run_id,
            workflow_id=attempt.workflow_id,
            task_id=attempt.task_id,
            attempt_number=attempt.attempt_number,
            status=AttemptStatus.RUNNING,
            started_at=started_at,
        )

    async def claim_dispatched_attempt(
        self,
        workflow_run: WorkflowRun,
        attempt: TaskAttempt,
        worker_id: str,
        lease_token: str,
        now: datetime,
        lease_seconds: float,
    ) -> TaskAttempt:
        async with self._session.begin():
            record = await self._session.get(
                TaskAttemptRecord,
                (attempt.run_id, attempt.task_id, attempt.attempt_number),
                with_for_update=True,
            )
            if record is None or record.status != AttemptStatus.DISPATCHED.value:
                raise LeaseClaimError(
                    attempt.run_id,
                    attempt.task_id,
                    "attempt is not DISPATCHED.",
                )
            await self._save_workflow_state(workflow_run)
            record.status = AttemptStatus.RUNNING.value
            record.started_at = now
            record.worker_id = worker_id
            record.lease_token = lease_token
            record.last_heartbeat_at = now
            record.lease_expires_at = _add_seconds(now, lease_seconds)
        return TaskAttempt(
            run_id=attempt.run_id,
            workflow_id=attempt.workflow_id,
            task_id=attempt.task_id,
            attempt_number=attempt.attempt_number,
            status=AttemptStatus.RUNNING,
            started_at=now,
            worker_id=worker_id,
            lease_token=lease_token,
            last_heartbeat_at=now,
            lease_expires_at=_add_seconds(now, lease_seconds),
        )

    async def heartbeat(
        self,
        run_id: str,
        task_id: str,
        attempt_number: int,
        worker_id: str,
        lease_token: str,
        now: datetime,
        lease_seconds: float,
    ) -> None:
        async with self._session.begin():
            record = await self._session.get(
                TaskAttemptRecord,
                (run_id, task_id, attempt_number),
                with_for_update=True,
            )
            if (
                record is None
                or record.status != AttemptStatus.RUNNING.value
                or record.worker_id != worker_id
                or record.lease_token != lease_token
            ):
                raise LeaseLostError(run_id, task_id, "lease token does not match.")
            record.last_heartbeat_at = now
            record.lease_expires_at = _add_seconds(now, lease_seconds)

    async def finish_leased_attempt(
        self,
        workflow_run: WorkflowRun,
        attempt: TaskAttempt,
        status: AttemptStatus,
        finished_at: datetime,
        *,
        error_type: str | None = None,
        error_message: str | None = None,
    ) -> None:
        async with self._session.begin():
            record = await self._session.get(
                TaskAttemptRecord,
                (attempt.run_id, attempt.task_id, attempt.attempt_number),
                with_for_update=True,
            )
            if (
                record is None
                or record.status != AttemptStatus.RUNNING.value
                or record.worker_id != attempt.worker_id
                or record.lease_token != attempt.lease_token
            ):
                raise LeaseLostError(
                    attempt.run_id,
                    attempt.task_id,
                    "lease token does not authorize completion.",
                )
            await self._save_workflow_state(workflow_run)
            record.status = status.value
            record.finished_at = finished_at
            record.error_type = error_type
            record.error_message = error_message
            record.worker_id = None
            record.lease_token = None
            record.lease_expires_at = None
            record.last_heartbeat_at = None

    async def list_expired_running_attempts(
        self,
        now: datetime,
    ) -> tuple[ExpiredTaskAttemptRef, ...]:
        async with self._session.begin():
            result = await self._session.execute(
                select(TaskAttemptRecord)
                .where(TaskAttemptRecord.status == AttemptStatus.RUNNING.value)
                .where(TaskAttemptRecord.lease_expires_at.is_not(None))
                .where(TaskAttemptRecord.lease_expires_at < now)
                .order_by(
                    TaskAttemptRecord.run_id,
                    TaskAttemptRecord.task_id,
                    TaskAttemptRecord.attempt_number,
                )
            )
            return tuple(
                ExpiredTaskAttemptRef(
                    run_id=record.run_id,
                    workflow_id=record.workflow_id,
                    task_id=record.task_id,
                    attempt_number=record.attempt_number,
                    worker_id=record.worker_id,
                    lease_token=record.lease_token,
                )
                for record in result.scalars()
            )

    async def reclaim_expired_attempt(
        self,
        workflow_run: WorkflowRun,
        attempt_ref: ExpiredTaskAttemptRef,
        now: datetime,
    ) -> bool:
        async with self._session.begin():
            record = await self._session.get(
                TaskAttemptRecord,
                (
                    attempt_ref.run_id,
                    attempt_ref.task_id,
                    attempt_ref.attempt_number,
                ),
                with_for_update=True,
            )
            if (
                record is None
                or record.status != AttemptStatus.RUNNING.value
                or record.lease_token != attempt_ref.lease_token
                or record.lease_expires_at is None
                or record.lease_expires_at >= now
            ):
                return False
            await self._save_workflow_state(workflow_run)
            record.status = AttemptStatus.INTERRUPTED.value
            record.finished_at = now
            record.worker_id = None
            record.lease_token = None
            record.lease_expires_at = None
            record.last_heartbeat_at = None
        return True

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
        async with self._session.begin():
            await self._save_workflow_state(workflow_run)
            record = await self._session.get(
                TaskAttemptRecord,
                (attempt.run_id, attempt.task_id, attempt.attempt_number),
            )
            if record is None:
                raise PersistenceError(
                    f"Task attempt '{attempt.attempt_key}' was not found."
                )
            record.status = status.value
            record.finished_at = finished_at
            record.error_type = error_type
            record.error_message = error_message

    async def list_attempts(
        self,
        run_id: str,
        task_id: str,
    ) -> tuple[TaskAttempt, ...]:
        async with self._session.begin():
            result = await self._session.execute(
                select(TaskAttemptRecord)
                .where(TaskAttemptRecord.run_id == run_id)
                .where(TaskAttemptRecord.task_id == task_id)
                .order_by(TaskAttemptRecord.attempt_number)
            )
            return tuple(
                TaskAttempt(
                    run_id=record.run_id,
                    workflow_id=record.workflow_id,
                    task_id=record.task_id,
                    attempt_number=record.attempt_number,
                    status=AttemptStatus(record.status),
                    started_at=record.started_at,
                    finished_at=record.finished_at,
                    error_type=record.error_type,
                    error_message=record.error_message,
                    worker_id=record.worker_id,
                    lease_token=record.lease_token,
                    lease_expires_at=record.lease_expires_at,
                    last_heartbeat_at=record.last_heartbeat_at,
                )
                for record in result.scalars()
            )

    async def list_run_attempts(self, run_id: str) -> tuple[TaskAttempt, ...]:
        async with self._session.begin():
            result = await self._session.execute(
                select(TaskAttemptRecord)
                .where(TaskAttemptRecord.run_id == run_id)
                .order_by(TaskAttemptRecord.task_id, TaskAttemptRecord.attempt_number)
            )
            return tuple(
                TaskAttempt(
                    run_id=record.run_id,
                    workflow_id=record.workflow_id,
                    task_id=record.task_id,
                    attempt_number=record.attempt_number,
                    status=AttemptStatus(record.status),
                    started_at=record.started_at,
                    finished_at=record.finished_at,
                    error_type=record.error_type,
                    error_message=record.error_message,
                    worker_id=record.worker_id,
                    lease_token=record.lease_token,
                    lease_expires_at=record.lease_expires_at,
                    last_heartbeat_at=record.last_heartbeat_at,
                )
                for record in result.scalars()
            )

    async def interrupt_running_attempts(
        self,
        workflow_run: WorkflowRun,
        finished_at: datetime,
        task_ids: tuple[str, ...] | None = None,
    ) -> None:
        async with self._session.begin():
            await self._save_workflow_state(workflow_run)
            query = (
                select(TaskAttemptRecord)
                .where(TaskAttemptRecord.run_id == workflow_run.run_id)
                .where(TaskAttemptRecord.status == AttemptStatus.RUNNING.value)
            )
            if task_ids is not None:
                query = query.where(TaskAttemptRecord.task_id.in_(task_ids))
            result = await self._session.execute(query)
            for record in result.scalars():
                record.status = AttemptStatus.INTERRUPTED.value
                record.finished_at = finished_at
    async def _save_workflow_state(self, workflow_run: WorkflowRun) -> None:
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
            task_records[task_id].next_retry_at = task_run.next_retry_at
            task_records[task_id].idempotency_key = (
                task_run.idempotency_key or f"{workflow_run.run_id}:{task_id}"
            )


def _add_seconds(value: datetime, seconds: float) -> datetime:
    return value + timedelta(seconds=seconds)
