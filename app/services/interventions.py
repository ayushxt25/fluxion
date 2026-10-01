"""Explicit operator resolution for ambiguous interrupted task execution."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.execution import (
    DispatchOutboxRecord,
    TaskAttemptRecord,
    TaskRunRecord,
    WorkflowRunRecord,
)
from app.db.models.interventions import TaskInterventionRecord
from app.dispatch.messages import TaskDispatchMessage
from app.engine.exceptions import PersistenceError
from app.engine.status import AttemptStatus, TaskStatus, WorkflowStatus
from app.observability.metrics import (
    record_task_intervention_conflict,
    record_task_intervention_resolution,
)

logger = logging.getLogger(__name__)


class InterventionConflictError(PersistenceError):
    pass


@dataclass(frozen=True)
class TaskIntervention:
    id: str
    workflow_id: str
    run_id: str
    task_id: str
    interrupted_attempt_number: int
    resolution: str
    created_at: datetime
    resolved_at: datetime | None
    resolver_subject: str | None
    resolver_role: str | None
    reason: str | None
    resulting_attempt_number: int | None


class TaskInterventionService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list(
        self, *, limit: int = 50, offset: int = 0
    ) -> tuple[TaskIntervention, ...]:
        async with self._session.begin():
            rows = await self._session.execute(
                select(TaskInterventionRecord)
                .order_by(
                    TaskInterventionRecord.created_at.desc(), TaskInterventionRecord.id
                )
                .limit(limit)
                .offset(offset)
            )
            return tuple(self._view(row) for row in rows.scalars())

    async def get(self, intervention_id: str) -> TaskIntervention:
        async with self._session.begin():
            record = await self._session.get(TaskInterventionRecord, intervention_id)
            if record is None:
                raise PersistenceError(
                    f"Task intervention '{intervention_id}' was not found."
                )
            return self._view(record)

    async def resolve(
        self,
        intervention_id: str,
        *,
        action: str,
        subject: str,
        role: str,
        reason: str | None = None,
    ) -> TaskIntervention:
        if action not in {"RETRY", "FAIL"}:
            raise ValueError("action must be RETRY or FAIL.")
        if reason is not None and len(reason) > 1024:
            raise ValueError("reason must not exceed 1024 characters.")
        metric_action = action.lower()
        async with self._session.begin():
            intervention = await self._session.get(
                TaskInterventionRecord, intervention_id, with_for_update=True
            )
            if intervention is None:
                raise PersistenceError(
                    f"Task intervention '{intervention_id}' was not found."
                )
            if intervention.resolution != "PENDING":
                self._conflict(metric_action, intervention, "already_resolved")
            attempt = await self._session.get(
                TaskAttemptRecord,
                (
                    intervention.run_id,
                    intervention.task_id,
                    intervention.interrupted_attempt_number,
                ),
                with_for_update=True,
            )
            if attempt is None or attempt.status != AttemptStatus.INTERRUPTED.value:
                self._conflict(metric_action, intervention, "attempt_not_interrupted")
            task = await self._session.get(
                TaskRunRecord,
                (intervention.run_id, intervention.task_id),
                with_for_update=True,
            )
            run = await self._session.get(
                WorkflowRunRecord, intervention.run_id, with_for_update=True
            )
            if task is None or run is None:
                self._conflict(metric_action, intervention, "execution_missing")
            now = datetime.now(UTC)
            intervention.resolution = action
            intervention.reason = reason
            intervention.resolver_subject = subject
            intervention.resolver_role = role
            intervention.resolved_at = now
            if action == "RETRY":
                newer = await self._session.scalar(
                    select(func.max(TaskAttemptRecord.attempt_number)).where(
                        TaskAttemptRecord.run_id == intervention.run_id,
                        TaskAttemptRecord.task_id == intervention.task_id,
                    )
                )
                if newer != intervention.interrupted_attempt_number:
                    self._conflict(metric_action, intervention, "newer_attempt")
                if (
                    run.status != WorkflowStatus.FAILED.value
                    or task.status != TaskStatus.INTERRUPTED.value
                ):
                    self._conflict(metric_action, intervention, "state_not_reopenable")
                next_attempt = intervention.interrupted_attempt_number + 1
                task.status = TaskStatus.DISPATCHED.value
                run.status = WorkflowStatus.RUNNING.value
                self._session.add(
                    TaskAttemptRecord(
                        run_id=intervention.run_id,
                        workflow_id=intervention.workflow_id,
                        task_id=intervention.task_id,
                        attempt_number=next_attempt,
                        status=AttemptStatus.DISPATCHED.value,
                    )
                )
                message = TaskDispatchMessage(
                    workflow_id=intervention.workflow_id,
                    workflow_revision=run.workflow_revision,
                    run_id=intervention.run_id,
                    task_id=intervention.task_id,
                    attempt_number=next_attempt,
                    attempt_key=(
                        f"{intervention.run_id}:{intervention.task_id}:{next_attempt}"
                    ),
                    idempotency_key=task.idempotency_key,
                )
                self._session.add(
                    DispatchOutboxRecord(
                        id=str(uuid4()),
                        event_type="TASK_DISPATCH",
                        payload=message.model_dump(mode="json"),
                        run_id=message.run_id,
                        workflow_id=message.workflow_id,
                        task_id=message.task_id,
                        attempt_number=message.attempt_number,
                    )
                )
                intervention.resulting_attempt_number = next_attempt
            result = self._view(intervention)
            fields = self._log_fields(
                intervention,
                f"task.intervention.{metric_action}",
            )
        record_task_intervention_resolution(action=metric_action, outcome="success")
        logger.info("Task intervention resolved.", extra=fields)
        return result

    @staticmethod
    def _conflict(
        action: str,
        intervention: TaskInterventionRecord,
        reason: str,
    ) -> None:
        record_task_intervention_conflict(action=action)
        record_task_intervention_resolution(action=action, outcome="conflict")
        logger.warning(
            "Task intervention resolution conflict.",
            extra={
                **TaskInterventionService._log_fields(
                    intervention,
                    "task.intervention.conflict",
                ),
                "action": action,
                "conflict_reason": reason,
            },
        )
        raise InterventionConflictError("Intervention cannot be resolved.")

    @staticmethod
    def _log_fields(
        intervention: TaskInterventionRecord,
        event: str,
    ) -> dict[str, str | int]:
        return {
            "event": event,
            "intervention_id": intervention.id,
            "workflow_id": intervention.workflow_id,
            "run_id": intervention.run_id,
            "task_id": intervention.task_id,
            "interrupted_attempt_number": intervention.interrupted_attempt_number,
        }

    @staticmethod
    def _view(record: TaskInterventionRecord) -> TaskIntervention:
        return TaskIntervention(
            id=record.id,
            workflow_id=record.workflow_id,
            run_id=record.run_id,
            task_id=record.task_id,
            interrupted_attempt_number=record.interrupted_attempt_number,
            resolution=record.resolution,
            created_at=record.created_at,
            resolved_at=record.resolved_at,
            resolver_subject=record.resolver_subject,
            resolver_role=record.resolver_role,
            reason=record.reason,
            resulting_attempt_number=record.resulting_attempt_number,
        )
