# ruff: noqa: E501
import logging
from datetime import UTC, datetime, timedelta
from uuid import uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.schedules import WorkflowScheduleFiringRecord, WorkflowScheduleRecord
from app.db.models.workflow import WorkflowRevisionRecord
from app.engine.exceptions import WorkflowNotFoundError, WorkflowScheduleNotFoundError
from app.engine.execution import WorkflowRun
from app.observability.metrics import record_schedule_fire, record_schedule_misfire
from app.schemas.schedules import (
    MisfirePolicy,
    ScheduleCreate,
    ScheduleType,
    ScheduleUpdate,
    WorkflowSchedule,
)
from app.services.repositories import WorkflowRepository, WorkflowRunRepository

logger = logging.getLogger(__name__)


class ScheduleRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create(
        self, request: ScheduleCreate, *, subject: str | None, role: str | None
    ) -> WorkflowSchedule:
        workflows = WorkflowRepository(self.session)
        if request.workflow_revision is None:
            await workflows.get_latest(request.workflow_id)
        else:
            await workflows.get_revision(request.workflow_id, request.workflow_revision)
        now = datetime.now(UTC)
        next_fire = request.next_fire_at or _next_fire(request, now)
        async with self.session.begin():
            record = WorkflowScheduleRecord(
                id=str(uuid4()),
                workflow_id=request.workflow_id,
                workflow_revision=request.workflow_revision,
                schedule_type=request.schedule_type.value,
                cron_expression=request.cron_expression,
                interval_seconds=request.interval_seconds,
                timezone=request.timezone,
                misfire_policy=request.misfire_policy.value,
                enabled=True,
                next_fire_at=next_fire,
                created_by_subject=subject,
                created_by_role=role,
            )
            self.session.add(record)
        return _schedule(record)

    async def get(self, schedule_id: str) -> WorkflowSchedule:
        async with self.session.begin():
            record = await self.session.get(WorkflowScheduleRecord, schedule_id)
            if record is None:
                raise WorkflowScheduleNotFoundError(schedule_id)
            return _schedule(record)

    async def list(self, limit: int, offset: int) -> tuple[WorkflowSchedule, ...]:
        async with self.session.begin():
            rows = (
                await self.session.execute(
                    select(WorkflowScheduleRecord)
                    .order_by(WorkflowScheduleRecord.id)
                    .limit(limit)
                    .offset(offset)
                )
            ).scalars()
            return tuple(_schedule(row) for row in rows)

    async def update(
        self, schedule_id: str, update: ScheduleUpdate
    ) -> WorkflowSchedule:
        async with self.session.begin():
            record = await self.session.get(
                WorkflowScheduleRecord, schedule_id, with_for_update=True
            )
            if record is None:
                raise WorkflowScheduleNotFoundError(schedule_id)
            for field in update.model_fields_set:
                setattr(
                    record,
                    field,
                    getattr(update, field).value
                    if field == "misfire_policy" and getattr(update, field)
                    else getattr(update, field),
                )
            record.next_fire_at = _next_fire(_schedule(record), datetime.now(UTC))
        return _schedule(record)

    async def set_enabled(self, schedule_id: str, enabled: bool) -> WorkflowSchedule:
        async with self.session.begin():
            record = await self.session.get(
                WorkflowScheduleRecord, schedule_id, with_for_update=True
            )
            if record is None:
                raise WorkflowScheduleNotFoundError(schedule_id)
            record.enabled = enabled
            if enabled:
                record.next_fire_at = _next_fire(_schedule(record), datetime.now(UTC))
        return _schedule(record)

    async def delete(self, schedule_id: str) -> None:
        async with self.session.begin():
            record = await self.session.get(
                WorkflowScheduleRecord, schedule_id, with_for_update=True
            )
            if record is None:
                raise WorkflowScheduleNotFoundError(schedule_id)
            record.enabled = False


class ScheduleRunner:
    """Claims due schedules with PostgreSQL row locks and records one firing per slot."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def tick(self, now: datetime | None = None) -> int:
        now = now or datetime.now(UTC)
        fired = 0
        async with self.session.begin():
            due = (
                await self.session.execute(
                    select(WorkflowScheduleRecord)
                    .where(
                        WorkflowScheduleRecord.enabled.is_(True),
                        WorkflowScheduleRecord.next_fire_at <= now,
                    )
                    .order_by(
                        WorkflowScheduleRecord.next_fire_at, WorkflowScheduleRecord.id
                    )
                    .with_for_update(skip_locked=True)
                )
            ).scalars()
            for schedule in due:
                scheduled_for = schedule.next_fire_at
                if (
                    schedule.misfire_policy == MisfirePolicy.SKIP.value
                    and scheduled_for < now
                ):
                    schedule.next_fire_at = _next_fire(_schedule(schedule), now)
                    record_schedule_misfire(schedule.misfire_policy)
                    continue
                workflow = await _resolve_workflow(self.session, schedule)
                run = WorkflowRun.create(
                    str(uuid4()),
                    workflow,
                    schedule_id=schedule.id,
                    scheduled_for=scheduled_for,
                )
                self.session.add(
                    WorkflowScheduleFiringRecord(
                        id=str(uuid4()),
                        schedule_id=schedule.id,
                        scheduled_for=scheduled_for,
                        run_id=run.run_id,
                    )
                )
                await WorkflowRunRepository(self.session).create_in_transaction(run)
                schedule.last_fire_at = scheduled_for
                schedule.next_fire_at = _next_fire(_schedule(schedule), scheduled_for)
                record_schedule_fire("success")
                fired += 1
        return fired


async def _resolve_workflow(session: AsyncSession, schedule: WorkflowScheduleRecord):
    repository = WorkflowRepository(session)
    if schedule.workflow_revision is None:
        revision = await session.scalar(
            select(func.max(WorkflowRevisionRecord.revision)).where(
                WorkflowRevisionRecord.workflow_id == schedule.workflow_id
            )
        )
        if revision is None:
            raise WorkflowNotFoundError(schedule.workflow_id)
        return await repository._load_revision(schedule.workflow_id, revision)
    return await repository._load_revision(
        schedule.workflow_id, schedule.workflow_revision
    )


def _schedule(record: WorkflowScheduleRecord) -> WorkflowSchedule:
    return WorkflowSchedule.model_validate(
        {
            "id": record.id,
            "workflow_id": record.workflow_id,
            "workflow_revision": record.workflow_revision,
            "schedule_type": record.schedule_type,
            "cron_expression": record.cron_expression,
            "interval_seconds": record.interval_seconds,
            "timezone": record.timezone,
            "misfire_policy": record.misfire_policy,
            "enabled": record.enabled,
            "next_fire_at": record.next_fire_at,
            "last_fire_at": record.last_fire_at,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
            "created_by_subject": record.created_by_subject,
            "created_by_role": record.created_by_role,
        }
    )


def _next_fire(schedule: ScheduleCreate | WorkflowSchedule, base: datetime) -> datetime:
    base = base.astimezone(UTC)
    if (
        schedule.schedule_type is ScheduleType.INTERVAL
        or schedule.schedule_type == "INTERVAL"
    ):
        return base + timedelta(seconds=schedule.interval_seconds or 1)
    zone = ZoneInfo(schedule.timezone)
    candidate = base.astimezone(zone).replace(second=0, microsecond=0) + timedelta(
        minutes=1
    )
    fields = (schedule.cron_expression or "").split()
    for _ in range(527040):
        if _cron_matches(fields, candidate):
            return candidate.astimezone(UTC)
        candidate += timedelta(minutes=1)
    raise ValueError("cron expression has no future occurrence")


def _cron_matches(fields: list[str], value: datetime) -> bool:
    values = (value.minute, value.hour, value.day, value.month, value.weekday())
    return all(
        _field_matches(field, item) for field, item in zip(fields, values, strict=True)
    )


def _field_matches(field: str, value: int) -> bool:
    for part in field.split(","):
        base, _, step = part.partition("/")
        interval = int(step) if step else 1
        if base == "*" and value % interval == 0:
            return True
        if base.isdigit() and int(base) == value:
            return True
        if "-" in base:
            start, end = (int(piece) for piece in base.split("-", 1))
            if start <= value <= end and (value - start) % interval == 0:
                return True
    return False
