from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_db_session, get_redis_dispatcher, require_role
from app.dispatch.transport import RedisTaskDispatcher
from app.schemas.api import (
    LeaseReapResponse,
    OutboxPublishResponse,
    SchedulerTickResponse,
)
from app.security.models import Role
from app.services.leases import LeaseReaper
from app.services.loops import LeaseReaperLoop, SchedulerLoop
from app.services.outbox import DispatchOutboxPublisher
from app.services.repositories import (
    DispatchOutboxRepository,
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)
from app.services.scheduler import WorkflowScheduler

router = APIRouter(prefix="/ops", tags=["operations"])


@router.post(
    "/scheduler/tick",
    response_model=SchedulerTickResponse,
    summary="Run one scheduler tick",
    dependencies=[Depends(require_role(Role.ADMIN))],
)
async def scheduler_tick(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> SchedulerTickResponse:
    run_repository = WorkflowRunRepository(session)
    scheduler = WorkflowScheduler(
        WorkflowRepository(session),
        run_repository,
        TaskAttemptRepository(session),
        outbox_repository=DispatchOutboxRepository(session),
    )
    result = await SchedulerLoop(scheduler, run_repository).tick()
    summaries = result.scheduled
    return SchedulerTickResponse(
        scheduled_runs=len(summaries),
        dispatched_tasks=sum(len(summary.dispatched_task_ids) for summary in summaries),
        outbox_event_ids=tuple(
            event_id for summary in summaries for event_id in summary.outbox_event_ids
        ),
    )


@router.post(
    "/outbox/publish",
    response_model=OutboxPublishResponse,
    summary="Publish pending outbox dispatches",
    dependencies=[Depends(require_role(Role.ADMIN))],
)
async def outbox_publish(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    dispatcher: Annotated[RedisTaskDispatcher, Depends(get_redis_dispatcher)],
) -> OutboxPublishResponse:
    result = await DispatchOutboxPublisher(
        DispatchOutboxRepository(session),
        dispatcher,
    ).publish_pending()
    return OutboxPublishResponse(
        attempted=result.attempted,
        published=result.published,
        failed=result.failed,
        discarded=len(result.discarded_event_ids),
        published_event_ids=result.published_event_ids,
        failed_event_ids=result.failed_event_ids,
        discarded_event_ids=result.discarded_event_ids,
    )


@router.post(
    "/leases/reap",
    response_model=LeaseReapResponse,
    summary="Reclaim expired worker leases",
    dependencies=[Depends(require_role(Role.ADMIN))],
)
async def leases_reap(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> LeaseReapResponse:
    reaper = LeaseReaper(
        WorkflowRepository(session),
        WorkflowRunRepository(session),
        TaskAttemptRepository(session),
    )
    result = await LeaseReaperLoop(reaper).tick()
    return LeaseReapResponse(
        reclaimed=len(result.reclaimed),
        run_ids=tuple(item.run_id for item in result.reclaimed),
    )
