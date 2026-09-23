from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, exists, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings, get_settings
from app.db.models.audit import AuditEventRecord
from app.db.models.events import RunEventRecord
from app.db.models.execution import (
    DispatchOutboxRecord,
    TaskAttemptRecord,
    TaskRunRecord,
    WorkflowRunRecord,
)
from app.db.models.logs import TaskLogRecord
from app.db.models.webhooks import WebhookDeliveryRecord
from app.observability.metrics import (
    record_retention_deleted,
    record_retention_error,
    record_retention_run,
)

ACTIVE_DELIVERY_STATUSES = ("PENDING", "CLAIMED", "RETRY_WAITING")
TERMINAL_DELIVERY_STATUSES = ("DELIVERED", "DEAD")
TERMINAL_RUN_STATUSES = ("SUCCEEDED", "FAILED", "CANCELLED")
CATEGORIES = (
    "task_logs",
    "run_events",
    "webhook_deliveries",
    "dispatch_outbox",
    "audit_events",
    "workflow_runs",
)


def is_terminal_run_status(status: str) -> bool:
    return status in TERMINAL_RUN_STATUSES


def is_actionable_outbox(
    *, published_at: datetime | None, discarded_at: datetime | None
) -> bool:
    """An outbox record remains actionable until publication or explicit discard."""
    return published_at is None and discarded_at is None


@dataclass(frozen=True)
class RetentionCategorySummary:
    examined: int = 0
    eligible: int = 0
    deleted: int = 0


@dataclass(frozen=True)
class RetentionSummary:
    dry_run: bool
    categories: dict[str, RetentionCategorySummary]
    total_deleted: int
    started_at: datetime
    completed_at: datetime


class RetentionRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def delete_category(
        self, category: str, cutoff: datetime, limit: int, dry_run: bool
    ) -> RetentionCategorySummary:
        async with self._session.begin():
            ids = await self._ids(category, cutoff, limit, lock=not dry_run)
            deleted = 0
            if not dry_run and ids:
                deleted = await self._delete(category, ids)
            return RetentionCategorySummary(len(ids), len(ids), deleted)

    async def _ids(
        self, category: str, cutoff: datetime, limit: int, lock: bool
    ) -> tuple:
        if category == "task_logs":
            query = (
                select(TaskLogRecord.id)
                .where(TaskLogRecord.created_at < cutoff)
                .order_by(TaskLogRecord.created_at, TaskLogRecord.id)
            )
        elif category == "audit_events":
            query = (
                select(AuditEventRecord.id)
                .where(AuditEventRecord.occurred_at < cutoff)
                .order_by(AuditEventRecord.occurred_at, AuditEventRecord.id)
            )
        elif category == "webhook_deliveries":
            query = (
                select(WebhookDeliveryRecord.id)
                .where(
                    WebhookDeliveryRecord.status.in_(TERMINAL_DELIVERY_STATUSES),
                    WebhookDeliveryRecord.created_at < cutoff,
                )
                .order_by(WebhookDeliveryRecord.created_at, WebhookDeliveryRecord.id)
            )
        elif category == "dispatch_outbox":
            query = (
                select(DispatchOutboxRecord.id)
                .where(
                    (DispatchOutboxRecord.published_at.is_not(None))
                    | (DispatchOutboxRecord.discarded_at.is_not(None))
                )
                .where(DispatchOutboxRecord.created_at < cutoff)
                .order_by(DispatchOutboxRecord.created_at, DispatchOutboxRecord.id)
            )
        elif category == "run_events":
            referenced = exists(
                select(WebhookDeliveryRecord.id).where(
                    WebhookDeliveryRecord.run_event_id == RunEventRecord.id,
                )
            )
            query = (
                select(RunEventRecord.id)
                .where(RunEventRecord.created_at < cutoff, ~referenced)
                .order_by(RunEventRecord.created_at, RunEventRecord.id)
            )
        elif category == "workflow_runs":
            actionable_outbox = exists(
                select(DispatchOutboxRecord.id).where(
                    DispatchOutboxRecord.run_id == WorkflowRunRecord.run_id,
                    DispatchOutboxRecord.published_at.is_(None),
                    DispatchOutboxRecord.discarded_at.is_(None),
                )
            )
            query = (
                select(WorkflowRunRecord.run_id)
                .where(
                    WorkflowRunRecord.status.in_(TERMINAL_RUN_STATUSES),
                    WorkflowRunRecord.completed_at.is_not(None),
                    WorkflowRunRecord.completed_at < cutoff,
                    ~actionable_outbox,
                )
                .order_by(WorkflowRunRecord.completed_at, WorkflowRunRecord.run_id)
            )
        else:
            raise ValueError(f"Unknown retention category '{category}'.")
        if lock:
            query = query.with_for_update(skip_locked=True)
        return tuple((await self._session.execute(query.limit(limit))).scalars())

    async def _delete(self, category: str, ids: tuple) -> int:
        if category == "task_logs":
            await self._session.execute(
                delete(TaskLogRecord).where(TaskLogRecord.id.in_(ids))
            )
            return len(ids)
        elif category == "audit_events":
            await self._session.execute(
                delete(AuditEventRecord).where(AuditEventRecord.id.in_(ids))
            )
            return len(ids)
        elif category == "webhook_deliveries":
            await self._session.execute(
                delete(WebhookDeliveryRecord).where(WebhookDeliveryRecord.id.in_(ids))
            )
            return len(ids)
        elif category == "dispatch_outbox":
            await self._session.execute(
                delete(DispatchOutboxRecord).where(DispatchOutboxRecord.id.in_(ids))
            )
            return len(ids)
        elif category == "run_events":
            await self._session.execute(
                delete(RunEventRecord).where(RunEventRecord.id.in_(ids))
            )
            return len(ids)
        else:
            deleted = 0
            for run_id in ids:
                active = await self._session.scalar(
                    select(
                        exists(
                            select(WebhookDeliveryRecord.id)
                            .join(RunEventRecord)
                            .where(
                                RunEventRecord.run_id == run_id,
                                WebhookDeliveryRecord.status.in_(
                                    ACTIVE_DELIVERY_STATUSES
                                ),
                            )
                        )
                    )
                )
                if active:
                    continue
                actionable_outbox = await self._session.scalar(
                    select(
                        exists(
                            select(DispatchOutboxRecord.id).where(
                                DispatchOutboxRecord.run_id == run_id,
                                DispatchOutboxRecord.published_at.is_(None),
                                DispatchOutboxRecord.discarded_at.is_(None),
                            )
                        )
                    )
                )
                if actionable_outbox:
                    continue
                event_ids = select(RunEventRecord.id).where(
                    RunEventRecord.run_id == run_id
                )
                await self._session.execute(
                    delete(WebhookDeliveryRecord).where(
                        WebhookDeliveryRecord.run_event_id.in_(event_ids)
                    )
                )
                await self._session.execute(
                    delete(TaskLogRecord).where(TaskLogRecord.run_id == run_id)
                )
                await self._session.execute(
                    delete(DispatchOutboxRecord).where(
                        DispatchOutboxRecord.run_id == run_id
                    )
                )
                await self._session.execute(
                    delete(TaskAttemptRecord).where(TaskAttemptRecord.run_id == run_id)
                )
                await self._session.execute(
                    delete(TaskRunRecord).where(TaskRunRecord.run_id == run_id)
                )
                await self._session.execute(
                    delete(RunEventRecord).where(RunEventRecord.run_id == run_id)
                )
                await self._session.execute(
                    delete(WorkflowRunRecord).where(WorkflowRunRecord.run_id == run_id)
                )
                deleted += 1
            return deleted


class RetentionService:
    def __init__(
        self, repository: RetentionRepository, settings: Settings | None = None
    ) -> None:
        self._repository, self._settings = repository, settings or get_settings()

    async def run_retention(
        self, *, categories: tuple[str, ...] | None = None, dry_run: bool = False
    ) -> RetentionSummary:
        started = datetime.now(UTC)
        selected = categories or CATEGORIES
        if set(selected) - set(CATEGORIES):
            raise ValueError("Unknown retention category.")
        days = {
            "task_logs": self._settings.retention_task_log_days,
            "run_events": self._settings.retention_run_event_days,
            "webhook_deliveries": self._settings.retention_webhook_delivery_days,
            "dispatch_outbox": self._settings.retention_outbox_days,
            "audit_events": self._settings.retention_audit_event_days,
            "workflow_runs": self._settings.retention_completed_run_days,
        }
        result = {}
        try:
            for category in selected:
                result[category] = await self._repository.delete_category(
                    category,
                    started - timedelta(days=days[category]),
                    self._settings.retention_batch_size,
                    dry_run,
                )
                record_retention_deleted(category, result[category].deleted)
        except Exception:
            record_retention_error(category)
            record_retention_run(
                "failed", (datetime.now(UTC) - started).total_seconds()
            )
            raise
        summary = RetentionSummary(
            dry_run,
            result,
            sum(item.deleted for item in result.values()),
            started,
            datetime.now(UTC),
        )
        record_retention_run(
            "success", (summary.completed_at - started).total_seconds()
        )
        return summary

    async def preview_retention(
        self, *, categories: tuple[str, ...] | None = None
    ) -> RetentionSummary:
        return await self.run_retention(categories=categories, dry_run=True)
