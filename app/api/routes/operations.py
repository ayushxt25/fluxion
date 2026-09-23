from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import (
    get_current_principal,
    get_db_session,
    get_redis_dispatcher,
    require_rate_limit,
    require_role,
)
from app.api.pagination import LimitQuery, OffsetQuery
from app.core.config import get_settings
from app.dispatch.transport import RedisTaskDispatcher
from app.schemas.api import (
    AuditEventListResponse,
    AuditEventResponse,
    LeaseReapResponse,
    OutboxPublishResponse,
    RetentionRunRequest,
    RetentionSummaryResponse,
    SchedulerTickResponse,
)
from app.security.models import Principal, Role
from app.services.audit import AuditEventRepository, AuditService
from app.services.leases import LeaseReaper
from app.services.loops import LeaseReaperLoop, SchedulerLoop
from app.services.outbox import DispatchOutboxPublisher
from app.services.repositories import (
    DispatchOutboxRepository,
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)
from app.services.retention import RetentionRepository, RetentionService
from app.services.scheduler import WorkflowScheduler

router = APIRouter(prefix="/ops", tags=["operations"])


def _retention_response(summary) -> RetentionSummaryResponse:
    return RetentionSummaryResponse(
        dry_run=summary.dry_run,
        categories={name: item.__dict__ for name, item in summary.categories.items()},
        total_deleted=summary.total_deleted,
        started_at=summary.started_at,
        completed_at=summary.completed_at,
    )


@router.get(
    "/retention/preview",
    response_model=RetentionSummaryResponse,
    dependencies=[
        Depends(require_role(Role.ADMIN)),
        Depends(require_rate_limit(ops=True)),
    ],
)
async def retention_preview(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    categories: Annotated[tuple[str, ...] | None, Query()] = None,
) -> RetentionSummaryResponse:
    try:
        summary = await RetentionService(
            RetentionRepository(session)
        ).preview_retention(categories=categories)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return _retention_response(summary)


@router.post(
    "/retention/run",
    response_model=RetentionSummaryResponse,
    dependencies=[
        Depends(require_role(Role.ADMIN)),
        Depends(require_rate_limit(ops=True)),
    ],
)
async def retention_run(
    request: Request,
    body: RetentionRunRequest,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    principal: Annotated[Principal, Depends(get_current_principal)],
) -> RetentionSummaryResponse:
    if not get_settings().retention_enabled:
        raise HTTPException(status_code=409, detail="Retention is disabled.")
    try:
        summary = await RetentionService(RetentionRepository(session)).run_retention(
            categories=body.categories
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    await AuditService(AuditEventRepository(session)).record_success(
        request_id=getattr(request.state, "request_id", ""),
        principal=principal,
        action="ops.retention.run",
        metadata={
            "categories": body.categories,
            "total_deleted": summary.total_deleted,
            "deleted": {
                name: item.deleted for name, item in summary.categories.items()
            },
        },
    )
    return _retention_response(summary)


@router.post(
    "/scheduler/tick",
    response_model=SchedulerTickResponse,
    summary="Run one scheduler tick",
    dependencies=[
        Depends(require_role(Role.ADMIN)),
        Depends(require_rate_limit(ops=True)),
    ],
)
async def scheduler_tick(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    dispatcher: Annotated[RedisTaskDispatcher, Depends(get_redis_dispatcher)],
    principal: Annotated[Principal, Depends(get_current_principal)],
) -> SchedulerTickResponse:
    run_repository = WorkflowRunRepository(session)
    scheduler = WorkflowScheduler(
        WorkflowRepository(session),
        run_repository,
        TaskAttemptRepository(session),
        dispatcher,
        outbox_repository=DispatchOutboxRepository(session),
    )
    result = await SchedulerLoop(scheduler, run_repository).tick()
    summaries = result.scheduled
    response = SchedulerTickResponse(
        scheduled_runs=len(summaries),
        dispatched_tasks=sum(len(summary.dispatched_task_ids) for summary in summaries),
        outbox_event_ids=tuple(
            event_id for summary in summaries for event_id in summary.outbox_event_ids
        ),
    )
    await AuditService(AuditEventRepository(session)).record_success(
        request_id=getattr(request.state, "request_id", ""),
        principal=principal,
        action="ops.scheduler.tick",
    )
    return response


@router.post(
    "/outbox/publish",
    response_model=OutboxPublishResponse,
    summary="Publish pending outbox dispatches",
    dependencies=[
        Depends(require_role(Role.ADMIN)),
        Depends(require_rate_limit(ops=True)),
    ],
)
async def outbox_publish(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    dispatcher: Annotated[RedisTaskDispatcher, Depends(get_redis_dispatcher)],
    principal: Annotated[Principal, Depends(get_current_principal)],
) -> OutboxPublishResponse:
    result = await DispatchOutboxPublisher(
        DispatchOutboxRepository(session),
        dispatcher,
    ).publish_pending()
    response = OutboxPublishResponse(
        attempted=result.attempted,
        published=result.published,
        failed=result.failed,
        discarded=len(result.discarded_event_ids),
        published_event_ids=result.published_event_ids,
        failed_event_ids=result.failed_event_ids,
        discarded_event_ids=result.discarded_event_ids,
    )
    await AuditService(AuditEventRepository(session)).record_success(
        request_id=getattr(request.state, "request_id", ""),
        principal=principal,
        action="ops.outbox.publish",
    )
    return response


@router.post(
    "/leases/reap",
    response_model=LeaseReapResponse,
    summary="Reclaim expired worker leases",
    dependencies=[
        Depends(require_role(Role.ADMIN)),
        Depends(require_rate_limit(ops=True)),
    ],
)
async def leases_reap(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    principal: Annotated[Principal, Depends(get_current_principal)],
) -> LeaseReapResponse:
    reaper = LeaseReaper(
        WorkflowRepository(session),
        WorkflowRunRepository(session),
        TaskAttemptRepository(session),
    )
    result = await LeaseReaperLoop(reaper).tick()
    response = LeaseReapResponse(
        reclaimed=len(result.reclaimed),
        run_ids=tuple(item.run_id for item in result.reclaimed),
    )
    await AuditService(AuditEventRepository(session)).record_success(
        request_id=getattr(request.state, "request_id", ""),
        principal=principal,
        action="ops.leases.reap",
    )
    return response


@router.get(
    "/audit",
    response_model=AuditEventListResponse,
    summary="List audit events",
    dependencies=[
        Depends(require_role(Role.ADMIN)),
        Depends(require_rate_limit(ops=True)),
    ],
)
async def list_audit_events(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    limit: LimitQuery = 50,
    offset: OffsetQuery = 0,
    action: str | None = None,
    principal_subject: str | None = None,
    outcome: str | None = None,
) -> AuditEventListResponse:
    events = await AuditEventRepository(session).list(
        limit=limit,
        offset=offset,
        action=action,
        principal_subject=principal_subject,
        outcome=outcome,
    )
    items = tuple(
        AuditEventResponse(
            id=event.id,
            occurred_at=event.occurred_at,
            request_id=event.request_id,
            principal_subject=event.principal_subject,
            principal_role=event.principal_role,
            action=event.action,
            resource_type=event.resource_type,
            resource_id=event.resource_id,
            outcome=event.outcome,
            metadata=event.metadata,
        )
        for event in events
    )
    return AuditEventListResponse(
        items=items,
        limit=limit,
        offset=offset,
        count=len(items),
    )
