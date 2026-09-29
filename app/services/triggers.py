from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.triggers import (
    WorkflowEventFiringRecord,
    WorkflowEventSubscriptionRecord,
    WorkflowTriggerEventRecord,
)
from app.db.models.workflow import WorkflowRevisionRecord
from app.engine.exceptions import WorkflowNotFoundError, WorkflowScheduleNotFoundError
from app.engine.execution import WorkflowRun
from app.observability.metrics import (
    record_event_filter_result,
    record_event_firing,
    record_trigger_event_ingested,
)
from app.schemas.triggers import (
    EventIngestRequest,
    EventIngestResponse,
    EventSubscription,
    EventSubscriptionCreate,
    EventSubscriptionUpdate,
)
from app.services.repositories import WorkflowRepository, WorkflowRunRepository


class EventSubscriptionRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create(
        self,
        request: EventSubscriptionCreate,
        *,
        subject: str | None,
        role: str | None,
    ) -> EventSubscription:
        async with self.session.begin():
            await _resolve(self.session, request.workflow_id, request.workflow_revision)
            row = WorkflowEventSubscriptionRecord(
                id=str(uuid4()), workflow_id=request.workflow_id,
                workflow_revision=request.workflow_revision,
                event_type=request.event_type,
                filter_json=request.filter_json,
                pass_event_payload_as_input=request.pass_event_payload_as_input,
                created_by_subject=subject, created_by_role=role,
            )
            self.session.add(row)
            await self.session.flush()
            await self.session.refresh(row)
            result = _subscription(row)
        return result

    async def get(self, subscription_id: str) -> EventSubscription:
        async with self.session.begin():
            row = await self.session.get(
                WorkflowEventSubscriptionRecord, subscription_id
            )
            if row is None:
                raise WorkflowScheduleNotFoundError(subscription_id)
            return _subscription(row)

    async def list(self, limit: int, offset: int) -> tuple[EventSubscription, ...]:
        async with self.session.begin():
            result = await self.session.execute(
                select(WorkflowEventSubscriptionRecord)
                .order_by(WorkflowEventSubscriptionRecord.id)
                .limit(limit).offset(offset)
            )
            return tuple(_subscription(row) for row in result.scalars())

    async def update(
        self, subscription_id: str, update: EventSubscriptionUpdate
    ) -> EventSubscription:
        async with self.session.begin():
            row = await self.session.get(
                WorkflowEventSubscriptionRecord,
                subscription_id,
                with_for_update=True,
            )
            if row is None:
                raise WorkflowScheduleNotFoundError(subscription_id)
            for field in update.model_fields_set:
                setattr(row, field, getattr(update, field))
            await self.session.flush()
            await self.session.refresh(row)
            result = _subscription(row)
        return result

    async def set_enabled(
        self, subscription_id: str, enabled: bool
    ) -> EventSubscription:
        async with self.session.begin():
            row = await self.session.get(
                WorkflowEventSubscriptionRecord,
                subscription_id,
                with_for_update=True,
            )
            if row is None:
                raise WorkflowScheduleNotFoundError(subscription_id)
            row.enabled = enabled
            await self.session.flush()
            await self.session.refresh(row)
            result = _subscription(row)
        return result


class EventIngestionService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def ingest(self, request: EventIngestRequest) -> EventIngestResponse:
        async with self.session.begin():
            event_id = str(uuid4())
            statement = insert(WorkflowTriggerEventRecord).values(
                id=event_id, source=request.source,
                external_event_id=request.external_event_id,
                event_type=request.event_type, payload_json=request.payload,
            ).on_conflict_do_nothing(
                index_elements=("source", "external_event_id")
            ).returning(WorkflowTriggerEventRecord.id)
            inserted_id = await self.session.scalar(statement)
            if inserted_id is None:
                return await self._duplicate_response(request)
            event = await self.session.get(WorkflowTriggerEventRecord, inserted_id)
            assert event is not None
            return await self._create_firings(event, request)

    async def _duplicate_response(
        self, request: EventIngestRequest
    ) -> EventIngestResponse:
        event = await self.session.scalar(
            select(WorkflowTriggerEventRecord).where(
                WorkflowTriggerEventRecord.source == request.source,
                WorkflowTriggerEventRecord.external_event_id
                == request.external_event_id,
            )
        )
        assert event is not None
        result = await self.session.execute(
            select(WorkflowEventFiringRecord.run_id).where(
                WorkflowEventFiringRecord.trigger_event_id == event.id
            )
        )
        runs = tuple(result.scalars())
        record_trigger_event_ingested("duplicate")
        return EventIngestResponse(
            trigger_event_id=event.id,
            created=False,
            matched_subscriptions=len(runs),
            run_ids=runs,
        )

    async def _create_firings(
        self, event, request: EventIngestRequest
    ) -> EventIngestResponse:
        result = await self.session.execute(
            select(WorkflowEventSubscriptionRecord)
            .where(WorkflowEventSubscriptionRecord.enabled.is_(True))
            .where(WorkflowEventSubscriptionRecord.event_type == request.event_type)
            .with_for_update()
        )
        run_ids: list[str] = []
        for subscription in result.scalars():
            if not _matches(subscription.filter_json, request.payload):
                record_event_filter_result(False)
                continue
            record_event_filter_result(True)
            workflow = await _resolve(
                self.session,
                subscription.workflow_id,
                subscription.workflow_revision,
            )
            run = WorkflowRun.create(
                str(uuid4()), workflow,
                workflow_input=(
                    request.payload if subscription.pass_event_payload_as_input else None
                ),
                workflow_input_present=subscription.pass_event_payload_as_input,
                trigger_event_id=event.id, event_subscription_id=subscription.id,
            )
            self.session.add(WorkflowEventFiringRecord(
                id=str(uuid4()), subscription_id=subscription.id,
                trigger_event_id=event.id, run_id=run.run_id,
            ))
            await WorkflowRunRepository(self.session).create_in_transaction(run)
            record_event_firing("success")
            run_ids.append(run.run_id)
        record_trigger_event_ingested("success")
        return EventIngestResponse(
            trigger_event_id=event.id,
            created=True,
            matched_subscriptions=len(run_ids),
            run_ids=tuple(run_ids),
        )


async def _resolve(session: AsyncSession, workflow_id: str, revision: int | None):
    if revision is None:
        revision = await session.scalar(
            select(func.max(WorkflowRevisionRecord.revision)).where(
                WorkflowRevisionRecord.workflow_id == workflow_id
            )
        )
    if revision is None:
        raise WorkflowNotFoundError(workflow_id)
    return await WorkflowRepository(session)._load_revision(workflow_id, revision)


def _matches(filter_json: dict | None, payload: object) -> bool:
    if filter_json is None:
        return True
    if not isinstance(payload, dict):
        return False
    return all(
        key in payload
        and type(payload[key]) is type(value)
        and payload[key] == value
        for key, value in filter_json.items()
    )


def _subscription(row: WorkflowEventSubscriptionRecord) -> EventSubscription:
    return EventSubscription.model_validate({
        "id": row.id, "workflow_id": row.workflow_id,
        "workflow_revision": row.workflow_revision, "event_type": row.event_type,
        "filter_json": row.filter_json,
        "pass_event_payload_as_input": row.pass_event_payload_as_input,
        "enabled": row.enabled, "created_at": row.created_at,
        "updated_at": row.updated_at, "created_by_subject": row.created_by_subject,
        "created_by_role": row.created_by_role,
    })
