import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.db.models.execution import (
    DispatchOutboxRecord,
    TaskAttemptRecord,
    TaskRunRecord,
    WorkflowRunRecord,
)
from app.db.models.interventions import TaskInterventionRecord
from app.db.models.workflow import (
    TaskDefinitionRecord,
    TaskDependencyRecord,
    WorkflowDefinitionRecord,
    WorkflowRevisionDependencyRecord,
    WorkflowRevisionRecord,
    WorkflowRevisionTaskRecord,
)
from app.dispatch.messages import TaskDispatchMessage
from app.engine.dag import WorkflowDAG
from app.engine.exceptions import (
    CoordinatorLeaseLostError,
    InvalidOutboxPayloadError,
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
from app.observability.metrics import record_task_intervention_created
from app.schemas.workflow import RetryPolicy, TaskDefinition, WorkflowDefinition
from app.services.events import RunEventRepository

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IncompleteWorkflowRunRef:
    run_id: str
    workflow_id: str
    workflow_revision: int


@dataclass(frozen=True)
class ExpiredTaskAttemptRef:
    run_id: str
    workflow_id: str
    task_id: str
    attempt_number: int
    worker_id: str | None
    lease_token: str | None


@dataclass(frozen=True)
class DispatchOutboxEvent:
    id: str
    event_type: str
    message: TaskDispatchMessage
    run_id: str
    workflow_id: str
    task_id: str
    attempt_number: int
    created_at: datetime
    published_at: datetime | None
    claimed_by: str | None
    claim_token: str | None
    claimed_at: datetime | None
    claim_expires_at: datetime | None
    publish_attempts: int
    last_error: str | None
    discarded_at: datetime | None = None
    discard_reason: str | None = None
    last_reconciled_at: datetime | None = None
    reconcile_count: int = 0


@dataclass(frozen=True)
class WorkflowRunSummary:
    run_id: str
    workflow_id: str
    workflow_revision: int
    status: WorkflowStatus
    created_at: datetime


class WorkflowRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(
        self,
        workflow: WorkflowDefinition,
        *,
        created_by_subject: str | None = None,
        created_by_role: str | None = None,
    ) -> None:
        WorkflowDAG(workflow)

        try:
            async with self._session.begin():
                if await self._session.get(WorkflowDefinitionRecord, workflow.id):
                    raise WorkflowAlreadyExistsError(workflow.id)

                record = WorkflowDefinitionRecord(
                    id=workflow.id,
                    revision=1,
                    name=workflow.name,
                )
                record.tasks = [
                    TaskDefinitionRecord(
                        workflow_id=workflow.id,
                        workflow_revision=1,
                        task_id=task.id,
                        name=task.name,
                        retry_max_attempts=task.retry_policy.max_attempts,
                        retry_initial_backoff_seconds=(
                            task.retry_policy.initial_backoff_seconds
                        ),
                        retry_backoff_multiplier=(task.retry_policy.backoff_multiplier),
                        retry_max_backoff_seconds=(
                            task.retry_policy.max_backoff_seconds
                        ),
                        parameters={
                            name: parameter.model_dump(mode="json")
                            for name, parameter in task.parameters.items()
                        },
                    )
                    for task in workflow.tasks
                ]
                self._session.add(record)
                await self._session.flush()

                dependencies = [
                    TaskDependencyRecord(
                        workflow_id=workflow.id,
                        workflow_revision=1,
                        task_id=task.id,
                        depends_on_task_id=dependency_id,
                    )
                    for task in workflow.tasks
                    for dependency_id in task.depends_on
                ]
                self._session.add_all(dependencies)
                self._session.add(
                    WorkflowRevisionRecord(
                        workflow_id=workflow.id,
                        revision=1,
                        name=workflow.name,
                        created_by_subject=created_by_subject,
                        created_by_role=created_by_role,
                    )
                )
                await self._session.flush()
                self._session.add_all(self._revision_task_rows(workflow, 1))
                await self._session.flush()
                self._session.add_all(self._revision_dependency_rows(workflow, 1))
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
                revision=record.revision,
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
                        parameters=task.parameters or {},
                    )
                    for task in sorted(record.tasks, key=lambda item: item.task_id)
                ),
            )

        WorkflowDAG(workflow)
        return workflow

    async def get_revision(self, workflow_id: str, revision: int) -> WorkflowDefinition:
        async with self._session.begin():
            return await self._load_revision(workflow_id, revision)

    async def get_latest(self, workflow_id: str) -> WorkflowDefinition:
        async with self._session.begin():
            revision = await self._session.scalar(
                select(WorkflowRevisionRecord.revision)
                .where(WorkflowRevisionRecord.workflow_id == workflow_id)
                .order_by(WorkflowRevisionRecord.revision.desc())
                .limit(1)
            )
            if revision is None:
                raise WorkflowNotFoundError(workflow_id)
            return await self._load_revision(workflow_id, revision)

    async def list_revisions(self, workflow_id: str) -> tuple[WorkflowDefinition, ...]:
        async with self._session.begin():
            revisions = tuple(
                (
                    await self._session.execute(
                        select(WorkflowRevisionRecord.revision)
                        .where(WorkflowRevisionRecord.workflow_id == workflow_id)
                        .order_by(WorkflowRevisionRecord.revision)
                    )
                ).scalars()
            )
            if not revisions:
                raise WorkflowNotFoundError(workflow_id)
            return tuple(
                [
                    await self._load_revision(workflow_id, revision)
                    for revision in revisions
                ]
            )

    async def _load_revision(
        self, workflow_id: str, revision: int
    ) -> WorkflowDefinition:
        record = await self._session.get(
            WorkflowRevisionRecord, (workflow_id, revision)
        )
        if record is None:
            raise WorkflowNotFoundError(f"{workflow_id}@{revision}")
        tasks = tuple(
            (
                await self._session.execute(
                    select(WorkflowRevisionTaskRecord)
                    .where(
                        WorkflowRevisionTaskRecord.workflow_id == workflow_id,
                        WorkflowRevisionTaskRecord.revision == revision,
                    )
                    .order_by(WorkflowRevisionTaskRecord.task_id)
                )
            ).scalars()
        )
        edges = (
            await self._session.execute(
                select(WorkflowRevisionDependencyRecord).where(
                    WorkflowRevisionDependencyRecord.workflow_id == workflow_id,
                    WorkflowRevisionDependencyRecord.revision == revision,
                )
            )
        ).scalars()
        dependencies = {task.task_id: [] for task in tasks}
        for edge in edges:
            dependencies[edge.task_id].append(edge.depends_on_task_id)
        workflow = WorkflowDefinition(
            id=record.workflow_id,
            name=record.name,
            revision=record.revision,
            created_at=record.created_at,
            created_by_subject=record.created_by_subject,
            created_by_role=record.created_by_role,
            tasks=tuple(
                TaskDefinition(
                    id=task.task_id,
                    name=task.name,
                    depends_on=tuple(sorted(dependencies[task.task_id])),
                    retry_policy=RetryPolicy(
                        max_attempts=task.retry_max_attempts,
                        initial_backoff_seconds=task.retry_initial_backoff_seconds,
                        backoff_multiplier=task.retry_backoff_multiplier,
                        max_backoff_seconds=task.retry_max_backoff_seconds,
                    ),
                    parameters=task.parameters or {},
                )
                for task in tasks
            ),
        )
        WorkflowDAG(workflow)
        return workflow

    async def publish(
        self,
        workflow: WorkflowDefinition,
        *,
        created_by_subject: str | None = None,
        created_by_role: str | None = None,
    ) -> WorkflowDefinition:
        """Append an immutable revision; PostgreSQL serializes same-ID publishers."""
        WorkflowDAG(workflow)
        try:
            async with self._session.begin():
                # hashtext is stable in PostgreSQL and the xact lock releases on commit.
                await self._session.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:workflow_id))"),
                    {"workflow_id": workflow.id},
                )
                current = await self._session.scalar(
                    select(func.max(WorkflowRevisionRecord.revision)).where(
                        WorkflowRevisionRecord.workflow_id == workflow.id
                    )
                )
                revision = (current or 0) + 1
                self._session.add(
                    WorkflowRevisionRecord(
                        workflow_id=workflow.id,
                        revision=revision,
                        name=workflow.name,
                        created_by_subject=created_by_subject,
                        created_by_role=created_by_role,
                    )
                )
                await self._session.flush()
                self._session.add_all(self._revision_task_rows(workflow, revision))
                await self._session.flush()
                self._session.add_all(
                    self._revision_dependency_rows(workflow, revision)
                )
            return await self.get_revision(workflow.id, revision)
        except IntegrityError as exc:
            raise PersistenceError(
                f"Failed to publish workflow revision for '{workflow.id}'."
            ) from exc

    @staticmethod
    def _revision_task_rows(
        workflow: WorkflowDefinition, revision: int
    ) -> list[WorkflowRevisionTaskRecord]:
        return [
            WorkflowRevisionTaskRecord(
                workflow_id=workflow.id,
                revision=revision,
                task_id=task.id,
                name=task.name,
                retry_max_attempts=task.retry_policy.max_attempts,
                retry_initial_backoff_seconds=(
                    task.retry_policy.initial_backoff_seconds
                ),
                retry_backoff_multiplier=task.retry_policy.backoff_multiplier,
                retry_max_backoff_seconds=task.retry_policy.max_backoff_seconds,
                parameters={
                    name: parameter.model_dump(mode="json")
                    for name, parameter in task.parameters.items()
                },
            )
            for task in workflow.tasks
        ]

    @staticmethod
    def _revision_dependency_rows(
        workflow: WorkflowDefinition, revision: int
    ) -> list[WorkflowRevisionDependencyRecord]:
        return [
            WorkflowRevisionDependencyRecord(
                workflow_id=workflow.id,
                revision=revision,
                task_id=task.id,
                depends_on_task_id=dependency,
            )
            for task in workflow.tasks
            for dependency in task.depends_on
        ]

    async def exists(self, workflow_id: str) -> bool:
        async with self._session.begin():
            return (
                await self._session.get(WorkflowDefinitionRecord, workflow_id)
                is not None
            )

    async def list(
        self,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[WorkflowDefinition, ...]:
        async with self._session.begin():
            result = await self._session.execute(
                select(WorkflowDefinitionRecord)
                .options(selectinload(WorkflowDefinitionRecord.tasks))
                .order_by(WorkflowDefinitionRecord.id)
                .limit(limit)
                .offset(offset)
            )
            records = tuple(result.scalars())
            workflow_ids = [record.id for record in records]
            dependency_result = await self._session.execute(
                select(TaskDependencyRecord).where(
                    TaskDependencyRecord.workflow_id.in_(workflow_ids)
                )
            )
            dependencies: dict[tuple[str, str], list[str]] = {
                (record.id, task.task_id): []
                for record in records
                for task in record.tasks
            }
            for dependency in dependency_result.scalars():
                dependencies[(dependency.workflow_id, dependency.task_id)].append(
                    dependency.depends_on_task_id
                )

        workflows = []
        for record in records:
            workflows.append(
                WorkflowDefinition(
                    id=record.id,
                    name=record.name,
                    revision=record.revision,
                    tasks=tuple(
                        TaskDefinition(
                            id=task.task_id,
                            name=task.name,
                            depends_on=tuple(
                                sorted(dependencies[(record.id, task.task_id)])
                            ),
                            retry_policy=RetryPolicy(
                                max_attempts=task.retry_max_attempts,
                                initial_backoff_seconds=(
                                    task.retry_initial_backoff_seconds
                                ),
                                backoff_multiplier=task.retry_backoff_multiplier,
                                max_backoff_seconds=task.retry_max_backoff_seconds,
                            ),
                            parameters=task.parameters or {},
                        )
                        for task in sorted(
                            record.tasks,
                            key=lambda item: item.task_id,
                        )
                    ),
                )
            )
        return tuple(workflows)


class WorkflowRunRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(self, workflow_run: WorkflowRun) -> None:
        try:
            async with self._session.begin():
                await self.create_in_transaction(workflow_run)
        except IntegrityError as exc:
            raise PersistenceError(
                f"Failed to persist workflow run '{workflow_run.run_id}'."
            ) from exc

    async def create_in_transaction(self, workflow_run: WorkflowRun) -> None:
        """Persist a run inside an owning transaction (schedule firing use only)."""
        if await self._session.get(WorkflowRunRecord, workflow_run.run_id):
            raise WorkflowRunAlreadyExistsError(workflow_run.run_id)
        record = WorkflowRunRecord(
            run_id=workflow_run.run_id,
            workflow_id=workflow_run.workflow_id,
            workflow_revision=workflow_run.workflow_revision,
            status=workflow_run.status.value,
            input=workflow_run.workflow_input,
            input_present=workflow_run.workflow_input_present,
            schedule_id=workflow_run.schedule_id,
            scheduled_for=workflow_run.scheduled_for,
            trigger_event_id=workflow_run.trigger_event_id,
            event_subscription_id=workflow_run.event_subscription_id,
        )
        self._session.add(record)
        await self._session.flush()
        self._session.add_all(
            [
                TaskRunRecord(
                    run_id=workflow_run.run_id,
                    workflow_id=workflow_run.workflow_id,
                    workflow_revision=workflow_run.workflow_revision,
                    task_id=task_id,
                    status=task_run.status.value,
                    next_retry_at=task_run.next_retry_at,
                    idempotency_key=task_run.idempotency_key
                    or f"{workflow_run.run_id}:{task_id}",
                    result=task_run.result,
                    result_present=task_run.result_present,
                )
                for task_id, task_run in workflow_run.task_runs.items()
            ]
        )
        events = RunEventRepository(self._session)
        events.record(
            run_id=workflow_run.run_id,
            workflow_id=workflow_run.workflow_id,
            workflow_revision=workflow_run.workflow_revision,
            event_type="run.created",
            payload={"status": workflow_run.status.value},
        )
        for task_id, task_run in workflow_run.task_runs.items():
            if task_run.status == TaskStatus.READY:
                events.record(
                    run_id=workflow_run.run_id,
                    workflow_id=workflow_run.workflow_id,
                    workflow_revision=workflow_run.workflow_revision,
                    event_type="task.ready",
                    task_id=task_id,
                    payload={"status": task_run.status.value},
                )

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
                results = {
                    task_run.task_id: task_run.result for task_run in record.task_runs
                }
                result_present = {
                    task_run.task_id: task_run.result_present
                    for task_run in record.task_runs
                }
            except ValueError as exc:
                raise RecoveryStateError(
                    run_id,
                    "persisted run contains an unknown workflow or task status.",
                ) from exc

            try:
                workflow_run = WorkflowRun.restore(
                    run_id=record.run_id,
                    workflow=workflow,
                    status=workflow_status,
                    task_statuses=task_statuses,
                    next_retry_at=next_retry_at,
                    idempotency_keys=idempotency_keys,
                    results=results,
                    result_present=result_present,
                    workflow_input=record.input,
                    workflow_input_present=record.input_present,
                    schedule_id=record.schedule_id,
                    scheduled_for=record.scheduled_for,
                    trigger_event_id=record.trigger_event_id,
                    event_subscription_id=record.event_subscription_id,
                )
                workflow_run.workflow_revision = record.workflow_revision
                return workflow_run
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
                    workflow_revision=record.workflow_revision,
                )
                for record in result.scalars()
            )

    async def list_schedulable(
        self,
        *,
        limit: int,
    ) -> tuple[IncompleteWorkflowRunRef, ...]:
        """Return a bounded, ordered set of runs with work the scheduler may act on."""
        if limit < 1:
            raise ValueError("limit must be positive.")
        now = datetime.now(UTC)
        async with self._session.begin():
            result = await self._session.execute(
                select(WorkflowRunRecord)
                .join(TaskRunRecord, TaskRunRecord.run_id == WorkflowRunRecord.run_id)
                .where(
                    WorkflowRunRecord.status.in_(
                        (WorkflowStatus.PENDING.value, WorkflowStatus.RUNNING.value)
                    ),
                    (
                        (TaskRunRecord.status == TaskStatus.READY.value)
                        | (
                            (TaskRunRecord.status == TaskStatus.RETRY_WAITING.value)
                            & (TaskRunRecord.next_retry_at <= now)
                        )
                    ),
                )
                .distinct()
                .order_by(WorkflowRunRecord.run_id)
                .limit(limit)
            )
            return tuple(
                IncompleteWorkflowRunRef(
                    run_id=record.run_id,
                    workflow_id=record.workflow_id,
                    workflow_revision=record.workflow_revision,
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

    async def get_workflow_reference(self, run_id: str) -> tuple[str, int]:
        async with self._session.begin():
            row = await self._session.get(WorkflowRunRecord, run_id)
            if row is None:
                raise WorkflowRunNotFoundError(run_id)
            return row.workflow_id, row.workflow_revision

    async def get_summary(self, run_id: str) -> WorkflowRunSummary:
        async with self._session.begin():
            record = await self._session.get(WorkflowRunRecord, run_id)
            if record is None:
                raise WorkflowRunNotFoundError(run_id)
            return WorkflowRunSummary(
                run_id=record.run_id,
                workflow_id=record.workflow_id,
                workflow_revision=record.workflow_revision,
                status=WorkflowStatus(record.status),
                created_at=record.created_at,
            )

    async def list(
        self,
        *,
        workflow_id: str | None = None,
        status: WorkflowStatus | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[WorkflowRunSummary, ...]:
        async with self._session.begin():
            query = select(WorkflowRunRecord)
            if workflow_id is not None:
                query = query.where(WorkflowRunRecord.workflow_id == workflow_id)
            if status is not None:
                query = query.where(WorkflowRunRecord.status == status.value)
            result = await self._session.execute(
                query.order_by(
                    WorkflowRunRecord.created_at.desc(),
                    WorkflowRunRecord.run_id,
                )
                .limit(limit)
                .offset(offset)
            )
            return tuple(
                WorkflowRunSummary(
                    run_id=record.run_id,
                    workflow_id=record.workflow_id,
                    workflow_revision=record.workflow_revision,
                    status=WorkflowStatus(record.status),
                    created_at=record.created_at,
                )
                for record in result.scalars()
            )

    async def save_state(
        self,
        workflow_run: WorkflowRun,
        *,
        coordinator_id: str | None = None,
        coordinator_lease_token: str | None = None,
    ) -> None:
        if (coordinator_id is None) != (coordinator_lease_token is None):
            raise ValueError(
                "Coordinator identity and token must be supplied together."
            )
        async with self._session.begin():
            query = (
                select(WorkflowRunRecord)
                .where(WorkflowRunRecord.run_id == workflow_run.run_id)
                .where(WorkflowRunRecord.workflow_id == workflow_run.workflow_id)
                .where(
                    WorkflowRunRecord.coordinator_id == coordinator_id
                    if coordinator_id is not None
                    else True
                )
                .where(
                    WorkflowRunRecord.coordinator_lease_token == coordinator_lease_token
                    if coordinator_lease_token is not None
                    else True
                )
                .where(
                    WorkflowRunRecord.coordinator_lease_expires_at > datetime.now(UTC)
                    if coordinator_id is not None
                    else True
                )
                .options(selectinload(WorkflowRunRecord.task_runs))
            )
            if coordinator_id is not None:
                query = query.with_for_update()
            result = await self._session.execute(query)
            record = result.scalar_one_or_none()
            if record is None:
                if coordinator_id is not None:
                    raise CoordinatorLeaseLostError(workflow_run.run_id)
                raise WorkflowRunNotFoundError(workflow_run.run_id)

            _record_state_events(self._session, record, record.task_runs, workflow_run)
            record.status = workflow_run.status.value
            record.input = workflow_run.workflow_input
            record.input_present = workflow_run.workflow_input_present
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
                task_records[task_id].result = task_run.result
                task_records[task_id].result_present = task_run.result_present


class DispatchOutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create_dispatch_intent(
        self,
        workflow_run: WorkflowRun,
        task_id: str,
        attempt_number: int,
        message: TaskDispatchMessage,
    ) -> tuple[TaskAttempt, DispatchOutboxEvent]:
        attempt = TaskAttempt(
            run_id=workflow_run.run_id,
            workflow_id=workflow_run.workflow_id,
            task_id=task_id,
            attempt_number=attempt_number,
            status=AttemptStatus.DISPATCHED,
        )
        event_id = str(uuid4())
        try:
            async with self._session.begin():
                task_record = await self._session.get(
                    TaskRunRecord,
                    (workflow_run.run_id, task_id),
                    with_for_update=True,
                )
                if task_record is None:
                    raise PersistenceError(
                        f"Task run '{workflow_run.run_id}:{task_id}' was not found."
                    )
                if task_record.status != TaskStatus.READY.value:
                    raise PersistenceError(
                        f"Task run '{workflow_run.run_id}:{task_id}' is not READY."
                    )
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
                record = DispatchOutboxRecord(
                    id=event_id,
                    event_type="TASK_DISPATCH",
                    payload=message.model_dump(mode="json"),
                    run_id=message.run_id,
                    workflow_id=message.workflow_id,
                    task_id=message.task_id,
                    attempt_number=message.attempt_number,
                )
                self._session.add(record)
                await self._session.flush()
                created_at = record.created_at
        except IntegrityError as exc:
            raise PersistenceError(
                f"Failed to create dispatch intent for '{message.attempt_key}'."
            ) from exc

        return attempt, DispatchOutboxEvent(
            id=event_id,
            event_type="TASK_DISPATCH",
            message=message,
            run_id=message.run_id,
            workflow_id=message.workflow_id,
            task_id=message.task_id,
            attempt_number=message.attempt_number,
            created_at=created_at,
            published_at=None,
            claimed_by=None,
            claim_token=None,
            claimed_at=None,
            claim_expires_at=None,
            discarded_at=None,
            discard_reason=None,
            publish_attempts=0,
            last_error=None,
        )

    async def list_unpublished(
        self,
        limit: int = 100,
    ) -> tuple[DispatchOutboxEvent, ...]:
        async with self._session.begin():
            result = await self._session.execute(
                select(DispatchOutboxRecord)
                .where(DispatchOutboxRecord.published_at.is_(None))
                .where(DispatchOutboxRecord.discarded_at.is_(None))
                .order_by(DispatchOutboxRecord.created_at, DispatchOutboxRecord.id)
                .limit(limit)
            )
            return tuple(self._event_from_record(record) for record in result.scalars())

    async def claim_unpublished(
        self,
        publisher_id: str,
        claim_token: str,
        now: datetime,
        claim_seconds: float,
        limit: int = 100,
    ) -> tuple[DispatchOutboxEvent, ...]:
        async with self._session.begin():
            result = await self._session.execute(
                select(DispatchOutboxRecord)
                .where(DispatchOutboxRecord.published_at.is_(None))
                .where(DispatchOutboxRecord.discarded_at.is_(None))
                .where(
                    (DispatchOutboxRecord.claim_token.is_(None))
                    | (DispatchOutboxRecord.claim_expires_at < now)
                )
                .order_by(DispatchOutboxRecord.created_at, DispatchOutboxRecord.id)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
            records = tuple(result.scalars())
            for record in records:
                record.claimed_by = publisher_id
                record.claim_token = claim_token
                record.claimed_at = now
                record.claim_expires_at = _add_seconds(now, claim_seconds)
            await self._session.flush()
            return tuple(self._event_from_record(record) for record in records)

    async def mark_published(
        self,
        event_id: str,
        published_at: datetime,
        *,
        publisher_id: str | None = None,
        claim_token: str | None = None,
    ) -> None:
        async with self._session.begin():
            record = await self._session.get(DispatchOutboxRecord, event_id)
            if record is None:
                raise PersistenceError(f"Dispatch outbox event '{event_id}' not found.")
            if (publisher_id is not None or claim_token is not None) and (
                record.claimed_by != publisher_id
                or record.claim_token != claim_token
                or record.published_at is not None
            ):
                raise PersistenceError(
                    f"Dispatch outbox event '{event_id}' claim was lost."
                )
            record.published_at = published_at
            record.claimed_by = None
            record.claim_token = None
            record.claimed_at = None
            record.claim_expires_at = None
            record.publish_attempts += 1
            record.last_error = None

    async def mark_published_batch(
        self,
        event_ids: tuple[str, ...],
        published_at: datetime,
        *,
        publisher_id: str,
        claim_token: str,
    ) -> None:
        """Acknowledge a bounded, successfully dispatched publisher batch.

        Every supplied row is locked and checked before any row is changed, so a
        stale publisher cannot partially acknowledge another publisher's work.
        """
        if not event_ids or len(set(event_ids)) != len(event_ids):
            raise PersistenceError(
                "Dispatch outbox publication batch must contain unique event IDs."
            )

        async with self._session.begin():
            result = await self._session.execute(
                select(DispatchOutboxRecord)
                .where(DispatchOutboxRecord.id.in_(event_ids))
                .with_for_update()
            )
            records = tuple(result.scalars())
            if len(records) != len(event_ids) or {
                record.id for record in records
            } != set(event_ids):
                raise PersistenceError(
                    "Dispatch outbox publication batch is incomplete."
                )

            for record in records:
                if (
                    record.claimed_by != publisher_id
                    or record.claim_token != claim_token
                    or record.published_at is not None
                ):
                    raise PersistenceError(
                        "Dispatch outbox publication batch claim was lost."
                    )

            for record in records:
                record.published_at = published_at
                record.claimed_by = None
                record.claim_token = None
                record.claimed_at = None
                record.claim_expires_at = None
                record.publish_attempts += 1
                record.last_error = None

    async def mark_discarded(
        self,
        event_id: str,
        discarded_at: datetime,
        reason: str,
        *,
        publisher_id: str | None = None,
        claim_token: str | None = None,
    ) -> None:
        async with self._session.begin():
            record = await self._session.get(DispatchOutboxRecord, event_id)
            if record is None:
                raise PersistenceError(f"Dispatch outbox event '{event_id}' not found.")
            if (publisher_id is not None or claim_token is not None) and (
                record.claimed_by != publisher_id
                or record.claim_token != claim_token
                or record.published_at is not None
            ):
                raise PersistenceError(
                    f"Dispatch outbox event '{event_id}' claim was lost."
                )
            record.discarded_at = discarded_at
            record.discard_reason = reason
            record.claimed_by = None
            record.claim_token = None
            record.claimed_at = None
            record.claim_expires_at = None

    async def is_dispatch_still_valid(self, event: DispatchOutboxEvent) -> bool:
        async with self._session.begin():
            task_record = await self._session.get(
                TaskRunRecord,
                (event.run_id, event.task_id),
            )
            attempt_record = await self._session.get(
                TaskAttemptRecord,
                (event.run_id, event.task_id, event.attempt_number),
            )
            return (
                task_record is not None
                and attempt_record is not None
                and task_record.status == TaskStatus.DISPATCHED.value
                and attempt_record.status == AttemptStatus.DISPATCHED.value
            )

    async def find_valid_dispatch_event_ids(
        self,
        event_ids: tuple[str, ...],
    ) -> frozenset[str]:
        """Return supplied outbox IDs whose canonical dispatch target is valid."""
        if not event_ids:
            return frozenset()

        async with self._session.begin():
            result = await self._session.execute(
                select(DispatchOutboxRecord.id)
                .join(
                    TaskRunRecord,
                    (TaskRunRecord.run_id == DispatchOutboxRecord.run_id)
                    & (TaskRunRecord.workflow_id == DispatchOutboxRecord.workflow_id)
                    & (TaskRunRecord.task_id == DispatchOutboxRecord.task_id),
                )
                .join(
                    TaskAttemptRecord,
                    (TaskAttemptRecord.run_id == DispatchOutboxRecord.run_id)
                    & (
                        TaskAttemptRecord.workflow_id
                        == DispatchOutboxRecord.workflow_id
                    )
                    & (TaskAttemptRecord.task_id == DispatchOutboxRecord.task_id)
                    & (
                        TaskAttemptRecord.attempt_number
                        == DispatchOutboxRecord.attempt_number
                    ),
                )
                .where(DispatchOutboxRecord.id.in_(event_ids))
                .where(TaskRunRecord.status == TaskStatus.DISPATCHED.value)
                .where(TaskAttemptRecord.status == AttemptStatus.DISPATCHED.value)
            )
            return frozenset(result.scalars())

    async def record_publish_failure(
        self,
        event_id: str,
        error: str,
        *,
        publisher_id: str | None = None,
        claim_token: str | None = None,
    ) -> None:
        async with self._session.begin():
            record = await self._session.get(DispatchOutboxRecord, event_id)
            if record is None:
                raise PersistenceError(f"Dispatch outbox event '{event_id}' not found.")
            if (publisher_id is not None or claim_token is not None) and (
                record.claimed_by != publisher_id
                or record.claim_token != claim_token
                or record.published_at is not None
            ):
                raise PersistenceError(
                    f"Dispatch outbox event '{event_id}' claim was lost."
                )
            record.publish_attempts += 1
            record.last_error = error
            record.claimed_by = None
            record.claim_token = None
            record.claimed_at = None
            record.claim_expires_at = None

    async def find_dispatched_attempts_missing_outbox(
        self,
    ) -> tuple[tuple[str, str, int], ...]:
        async with self._session.begin():
            result = await self._session.execute(
                select(
                    TaskAttemptRecord.run_id,
                    TaskAttemptRecord.task_id,
                    TaskAttemptRecord.attempt_number,
                )
                .outerjoin(
                    DispatchOutboxRecord,
                    (DispatchOutboxRecord.run_id == TaskAttemptRecord.run_id)
                    & (DispatchOutboxRecord.task_id == TaskAttemptRecord.task_id)
                    & (
                        DispatchOutboxRecord.attempt_number
                        == TaskAttemptRecord.attempt_number
                    ),
                )
                .where(TaskAttemptRecord.status == AttemptStatus.DISPATCHED.value)
                .where(DispatchOutboxRecord.id.is_(None))
                .order_by(
                    TaskAttemptRecord.run_id,
                    TaskAttemptRecord.task_id,
                    TaskAttemptRecord.attempt_number,
                )
            )
            return tuple(result.all())

    async def reconcile_stale_published(
        self,
        *,
        now: datetime,
        reconcile_after_seconds: float,
        limit: int,
    ) -> tuple[str, ...]:
        """Return safely unclaimed published dispatches to the normal publisher."""
        cutoff = now - timedelta(seconds=reconcile_after_seconds)
        reconciled: list[str] = []
        async with self._session.begin():
            result = await self._session.execute(
                select(DispatchOutboxRecord)
                .join(
                    TaskRunRecord,
                    (TaskRunRecord.run_id == DispatchOutboxRecord.run_id)
                    & (TaskRunRecord.task_id == DispatchOutboxRecord.task_id),
                )
                .join(
                    TaskAttemptRecord,
                    (TaskAttemptRecord.run_id == DispatchOutboxRecord.run_id)
                    & (TaskAttemptRecord.task_id == DispatchOutboxRecord.task_id)
                    & (
                        TaskAttemptRecord.attempt_number
                        == DispatchOutboxRecord.attempt_number
                    ),
                )
                .join(
                    WorkflowRunRecord,
                    (WorkflowRunRecord.run_id == DispatchOutboxRecord.run_id)
                    & (
                        WorkflowRunRecord.workflow_id
                        == DispatchOutboxRecord.workflow_id
                    ),
                )
                .where(DispatchOutboxRecord.published_at.is_not(None))
                .where(DispatchOutboxRecord.published_at <= cutoff)
                .where(DispatchOutboxRecord.discarded_at.is_(None))
                .where(TaskRunRecord.status == TaskStatus.DISPATCHED.value)
                .where(TaskAttemptRecord.status == AttemptStatus.DISPATCHED.value)
                .where(TaskAttemptRecord.lease_token.is_(None))
                .where(TaskAttemptRecord.lease_expires_at.is_(None))
                .where(WorkflowRunRecord.status == WorkflowStatus.RUNNING.value)
                .order_by(DispatchOutboxRecord.published_at, DispatchOutboxRecord.id)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
            for record in result.scalars():
                record.published_at = None
                record.claimed_by = None
                record.claim_token = None
                record.claimed_at = None
                record.claim_expires_at = None
                record.last_reconciled_at = now
                record.reconcile_count += 1
                reconciled.append(record.id)
        return tuple(reconciled)

    def _event_from_record(
        self,
        record: DispatchOutboxRecord,
    ) -> DispatchOutboxEvent:
        try:
            message = TaskDispatchMessage.model_validate(record.payload)
            TaskDispatchMessage.from_json(message.to_json())
        except Exception as exc:
            raise InvalidOutboxPayloadError(record.id, "payload is invalid.") from exc
        has_claim = any(
            (
                record.claimed_by,
                record.claim_token,
                record.claimed_at,
                record.claim_expires_at,
            )
        )
        has_complete_claim = all(
            (
                record.claimed_by,
                record.claim_token,
                record.claimed_at,
                record.claim_expires_at,
            )
        )
        if has_claim and not has_complete_claim:
            raise InvalidOutboxPayloadError(record.id, "claim metadata is incomplete.")
        if record.published_at is not None and has_claim:
            raise InvalidOutboxPayloadError(
                record.id,
                "published event still has claim metadata.",
            )
        if record.discarded_at is not None and has_claim:
            raise InvalidOutboxPayloadError(
                record.id,
                "discarded event still has claim metadata.",
            )
        return DispatchOutboxEvent(
            id=record.id,
            event_type=record.event_type,
            message=message,
            run_id=record.run_id,
            workflow_id=record.workflow_id,
            task_id=record.task_id,
            attempt_number=record.attempt_number,
            created_at=record.created_at,
            published_at=record.published_at,
            claimed_by=record.claimed_by,
            claim_token=record.claim_token,
            claimed_at=record.claimed_at,
            claim_expires_at=record.claim_expires_at,
            discarded_at=record.discarded_at,
            discard_reason=record.discard_reason,
            last_reconciled_at=record.last_reconciled_at,
            reconcile_count=record.reconcile_count,
            publish_attempts=record.publish_attempts,
            last_error=record.last_error,
        )

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

        _record_state_events(self._session, record, record.task_runs, workflow_run)
        if record.status != workflow_run.status.value and workflow_run.status.value in {
            "SUCCEEDED",
            "FAILED",
            "CANCELLED",
        }:
            record.completed_at = datetime.now(UTC)
        record.status = workflow_run.status.value
        record.input = workflow_run.workflow_input
        record.input_present = workflow_run.workflow_input_present
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
            task_records[task_id].result = task_run.result
            task_records[task_id].result_present = task_run.result_present


def _record_state_events(
    session: AsyncSession,
    record: WorkflowRunRecord,
    task_records: list[TaskRunRecord],
    workflow_run: WorkflowRun,
) -> None:
    """Append only canonical state changes to the current transaction."""
    events = RunEventRepository(session)
    if record.status != workflow_run.status.value:
        event_type = _RUN_EVENT_TYPES.get(workflow_run.status)
        if event_type:
            events.record(
                run_id=workflow_run.run_id,
                workflow_id=workflow_run.workflow_id,
                workflow_revision=workflow_run.workflow_revision,
                event_type=event_type,
                payload={"status": workflow_run.status.value},
            )
    persisted = {item.task_id: item for item in task_records}
    for task_id, task_run in workflow_run.task_runs.items():
        previous = persisted.get(task_id)
        if previous is None or previous.status == task_run.status.value:
            continue
        event_type = _TASK_EVENT_TYPES.get(task_run.status)
        if event_type:
            events.record(
                run_id=workflow_run.run_id,
                workflow_id=workflow_run.workflow_id,
                workflow_revision=workflow_run.workflow_revision,
                event_type=event_type,
                task_id=task_id,
                payload={
                    "status": task_run.status.value,
                    "has_result": task_run.result_present,
                },
            )


_TASK_EVENT_TYPES = {
    TaskStatus.READY: "task.ready",
    TaskStatus.DISPATCHED: "task.dispatched",
    TaskStatus.RUNNING: "task.running",
    TaskStatus.RETRY_WAITING: "task.retry_waiting",
    TaskStatus.SUCCEEDED: "task.succeeded",
    TaskStatus.FAILED: "task.failed",
    TaskStatus.INTERRUPTED: "task.interrupted",
    TaskStatus.CANCELLED: "task.cancelled",
}
_RUN_EVENT_TYPES = {
    WorkflowStatus.RUNNING: "run.started",
    WorkflowStatus.SUCCEEDED: "run.succeeded",
    WorkflowStatus.FAILED: "run.failed",
    WorkflowStatus.CANCELLED: "run.cancelled",
}


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
            RunEventRepository(self._session).record(
                run_id=attempt.run_id,
                workflow_id=attempt.workflow_id,
                workflow_revision=workflow_run.workflow_revision,
                event_type="attempt.started",
                task_id=attempt.task_id,
                attempt_number=attempt.attempt_number,
                payload={"status": AttemptStatus.RUNNING.value},
            )
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
            task_record = await self._session.get(
                TaskRunRecord,
                (attempt.run_id, attempt.task_id),
                with_for_update=True,
            )
            if task_record is None or task_record.status != TaskStatus.RUNNING.value:
                raise LeaseLostError(
                    attempt.run_id,
                    attempt.task_id,
                    "task is no longer RUNNING.",
                )
            # A distributed worker owns only this task/attempt.  Do not persist
            # its stale aggregate snapshot over concurrently claimed siblings.
            current = workflow_run.task_runs[attempt.task_id]
            task_record.status = current.status.value
            task_record.next_retry_at = current.next_retry_at
            task_record.result = current.result
            task_record.result_present = current.result_present
            record.status = status.value
            record.finished_at = finished_at
            record.error_type = error_type
            record.error_message = error_message
            record.worker_id = None
            record.lease_token = None
            record.lease_expires_at = None
            record.last_heartbeat_at = None
            await _promote_ready_tasks_from_db(
                self._session, attempt.run_id, attempt.workflow_id
            )
            task_states = tuple(
                (
                    await self._session.execute(
                        select(TaskRunRecord.status).where(
                            TaskRunRecord.run_id == attempt.run_id
                        )
                    )
                ).scalars()
            )
            run_record = await self._session.get(
                WorkflowRunRecord, attempt.run_id, with_for_update=True
            )
            if run_record is not None:
                if all(state == TaskStatus.SUCCEEDED.value for state in task_states):
                    run_record.status = WorkflowStatus.SUCCEEDED.value
                elif any(
                    state
                    in {
                        TaskStatus.FAILED.value,
                        TaskStatus.CANCELLED.value,
                        TaskStatus.INTERRUPTED.value,
                    }
                    for state in task_states
                ):
                    run_record.status = WorkflowStatus.FAILED.value
            RunEventRepository(self._session).record(
                run_id=attempt.run_id,
                workflow_id=attempt.workflow_id,
                workflow_revision=workflow_run.workflow_revision,
                event_type=(
                    "attempt.succeeded"
                    if status == AttemptStatus.SUCCEEDED
                    else "attempt.failed"
                ),
                task_id=attempt.task_id,
                attempt_number=attempt.attempt_number,
                payload={"status": status.value},
            )

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
        intervention: TaskInterventionRecord | None = None
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
            existing = await self._session.scalar(
                select(TaskInterventionRecord.id).where(
                    TaskInterventionRecord.run_id == attempt_ref.run_id,
                    TaskInterventionRecord.task_id == attempt_ref.task_id,
                    TaskInterventionRecord.interrupted_attempt_number
                    == attempt_ref.attempt_number,
                )
            )
            if existing is None:
                intervention = TaskInterventionRecord(
                    id=str(uuid4()),
                    workflow_id=attempt_ref.workflow_id,
                    run_id=attempt_ref.run_id,
                    task_id=attempt_ref.task_id,
                    interrupted_attempt_number=attempt_ref.attempt_number,
                    resolution="PENDING",
                )
                self._session.add(intervention)
            RunEventRepository(self._session).record(
                run_id=attempt_ref.run_id,
                workflow_id=attempt_ref.workflow_id,
                workflow_revision=workflow_run.workflow_revision,
                event_type="attempt.interrupted",
                task_id=attempt_ref.task_id,
                attempt_number=attempt_ref.attempt_number,
                payload={"status": AttemptStatus.INTERRUPTED.value},
            )
        if intervention is not None:
            record_task_intervention_created()
            logger.warning(
                "Task intervention required.",
                extra={
                    "event": "task.intervention.required",
                    "intervention_id": intervention.id,
                    "workflow_id": intervention.workflow_id,
                    "run_id": intervention.run_id,
                    "task_id": intervention.task_id,
                    "interrupted_attempt_number": (
                        intervention.interrupted_attempt_number
                    ),
                },
            )
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
                    created_at=record.created_at,
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
                    created_at=record.created_at,
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
        *,
        coordinator_id: str | None = None,
        coordinator_lease_token: str | None = None,
    ) -> None:
        async with self._session.begin():
            await self._save_workflow_state(
                workflow_run,
                coordinator_id=coordinator_id,
                coordinator_lease_token=coordinator_lease_token,
            )
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

    async def _save_workflow_state(
        self,
        workflow_run: WorkflowRun,
        *,
        coordinator_id: str | None = None,
        coordinator_lease_token: str | None = None,
    ) -> None:
        if (coordinator_id is None) != (coordinator_lease_token is None):
            raise ValueError(
                "Coordinator identity and token must be supplied together."
            )
        query = (
            select(WorkflowRunRecord)
            .where(WorkflowRunRecord.run_id == workflow_run.run_id)
            .where(WorkflowRunRecord.workflow_id == workflow_run.workflow_id)
        )
        if coordinator_id is not None:
            if coordinator_lease_token is None:
                raise ValueError(
                    "Coordinator identity and token must be supplied together."
                )
            query = (
                query.where(WorkflowRunRecord.coordinator_id == coordinator_id)
                .where(
                    WorkflowRunRecord.coordinator_lease_token == coordinator_lease_token
                )
                .where(
                    WorkflowRunRecord.coordinator_lease_expires_at > datetime.now(UTC)
                )
            )
            query = query.with_for_update()
        result = await self._session.execute(
            query.options(selectinload(WorkflowRunRecord.task_runs))
        )
        record = result.scalar_one_or_none()
        if record is None:
            if coordinator_id is not None:
                raise CoordinatorLeaseLostError(workflow_run.run_id)
            raise WorkflowRunNotFoundError(workflow_run.run_id)

        _record_state_events(self._session, record, record.task_runs, workflow_run)
        record.status = workflow_run.status.value
        record.input = workflow_run.workflow_input
        record.input_present = workflow_run.workflow_input_present
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
            task_records[task_id].result = task_run.result
            task_records[task_id].result_present = task_run.result_present


def _add_seconds(value: datetime, seconds: float) -> datetime:
    return value + timedelta(seconds=seconds)


async def _promote_ready_tasks_from_db(
    session: AsyncSession, run_id: str, workflow_id: str
) -> None:
    """Promote only durably unblocked siblings; never write a stale aggregate."""
    rows = tuple(
        (
            await session.execute(
                select(TaskRunRecord)
                .where(TaskRunRecord.run_id == run_id)
                .with_for_update()
            )
        ).scalars()
    )
    statuses = {row.task_id: row.status for row in rows}
    dependencies: dict[str, set[str]] = {}
    for task_id, dependency_id in await session.execute(
        select(
            TaskDependencyRecord.task_id,
            TaskDependencyRecord.depends_on_task_id,
        ).where(TaskDependencyRecord.workflow_id == workflow_id)
    ):
        dependencies.setdefault(task_id, set()).add(dependency_id)
    events = RunEventRepository(session)
    for row in rows:
        if row.status != TaskStatus.BLOCKED.value:
            continue
        if all(
            statuses.get(dependency) == TaskStatus.SUCCEEDED.value
            for dependency in dependencies.get(row.task_id, ())
        ):
            row.status = TaskStatus.READY.value
            events.record(
                run_id=run_id,
                workflow_id=workflow_id,
                event_type="task.ready",
                task_id=row.task_id,
                payload={"status": TaskStatus.READY.value},
            )
