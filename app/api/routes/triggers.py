from typing import Annotated

from fastapi import APIRouter, Depends, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import (
    get_current_principal,
    get_db_session,
    require_rate_limit,
    require_role,
)
from app.api.pagination import LimitQuery, OffsetQuery
from app.schemas.triggers import (
    EventIngestRequest,
    EventIngestResponse,
    EventSubscription,
    EventSubscriptionCreate,
    EventSubscriptionList,
    EventSubscriptionUpdate,
)
from app.security.models import Principal, Role
from app.services.audit import AuditEventRepository, AuditService
from app.services.triggers import EventIngestionService, EventSubscriptionRepository

router = APIRouter(prefix="/event-subscriptions", tags=["event-subscriptions"])
ingest_router = APIRouter(prefix="/events", tags=["events"])


async def _audit(
    session: AsyncSession,
    request: Request,
    principal: Principal,
    action: str,
    subscription: EventSubscription,
) -> None:
    await AuditService(AuditEventRepository(session)).record_success(
        request_id=getattr(request.state, "request_id", ""),
        principal=principal,
        action=action,
        resource_type="workflow_event_subscription",
        resource_id=subscription.id,
        metadata={
            "workflow_id": subscription.workflow_id,
            "workflow_revision": subscription.workflow_revision,
            "event_type": subscription.event_type,
        },
    )


@router.post(
    "",
    response_model=EventSubscription,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_role(Role.OPERATOR)), Depends(require_rate_limit())],
)
async def create_subscription(
    request: EventSubscriptionCreate,
    http_request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    principal: Annotated[Principal, Depends(get_current_principal)],
) -> EventSubscription:
    subscription = await EventSubscriptionRepository(session).create(
        request, subject=principal.subject, role=principal.role.value
    )
    await _audit(
        session,
        http_request,
        principal,
        "event_subscription.create",
        subscription,
    )
    return subscription


@router.get(
    "",
    response_model=EventSubscriptionList,
    dependencies=[Depends(require_role(Role.VIEWER)), Depends(require_rate_limit())],
)
async def list_subscriptions(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    limit: LimitQuery = 50,
    offset: OffsetQuery = 0,
) -> EventSubscriptionList:
    rows = await EventSubscriptionRepository(session).list(limit, offset)
    return EventSubscriptionList(
        items=rows,
        limit=limit,
        offset=offset,
        count=len(rows),
    )


@router.get(
    "/{subscription_id}",
    response_model=EventSubscription,
    dependencies=[Depends(require_role(Role.VIEWER)), Depends(require_rate_limit())],
)
async def get_subscription(
    subscription_id: str,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> EventSubscription:
    return await EventSubscriptionRepository(session).get(subscription_id)


@router.patch(
    "/{subscription_id}",
    response_model=EventSubscription,
    dependencies=[Depends(require_role(Role.OPERATOR)), Depends(require_rate_limit())],
)
async def update_subscription(
    subscription_id: str,
    request: EventSubscriptionUpdate,
    http_request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    principal: Annotated[Principal, Depends(get_current_principal)],
) -> EventSubscription:
    subscription = await EventSubscriptionRepository(session).update(
        subscription_id,
        request,
    )
    await _audit(
        session,
        http_request,
        principal,
        "event_subscription.update",
        subscription,
    )
    return subscription


async def _set_enabled(
    subscription_id: str,
    enabled: bool,
    request: Request,
    session: AsyncSession,
    principal: Principal,
) -> EventSubscription:
    subscription = await EventSubscriptionRepository(session).set_enabled(
        subscription_id,
        enabled,
    )
    action = "event_subscription.resume" if enabled else "event_subscription.pause"
    await _audit(session, request, principal, action, subscription)
    return subscription


@router.post(
    "/{subscription_id}/pause",
    response_model=EventSubscription,
    dependencies=[Depends(require_role(Role.OPERATOR)), Depends(require_rate_limit())],
)
async def pause_subscription(
    subscription_id: str,
    request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    principal: Annotated[Principal, Depends(get_current_principal)],
) -> EventSubscription:
    return await _set_enabled(subscription_id, False, request, session, principal)


@router.post(
    "/{subscription_id}/resume",
    response_model=EventSubscription,
    dependencies=[Depends(require_role(Role.OPERATOR)), Depends(require_rate_limit())],
)
async def resume_subscription(
    subscription_id: str,
    request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    principal: Annotated[Principal, Depends(get_current_principal)],
) -> EventSubscription:
    return await _set_enabled(subscription_id, True, request, session, principal)


@router.delete(
    "/{subscription_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(require_role(Role.OPERATOR)), Depends(require_rate_limit())],
)
async def delete_subscription(
    subscription_id: str,
    request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    principal: Annotated[Principal, Depends(get_current_principal)],
) -> None:
    subscription = await EventSubscriptionRepository(session).set_enabled(
        subscription_id,
        False,
    )
    await _audit(session, request, principal, "event_subscription.delete", subscription)


@ingest_router.post(
    "/ingest",
    response_model=EventIngestResponse,
    dependencies=[Depends(require_role(Role.OPERATOR)), Depends(require_rate_limit())],
)
async def ingest_event(
    request: EventIngestRequest,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> EventIngestResponse:
    return await EventIngestionService(session).ingest(request)
