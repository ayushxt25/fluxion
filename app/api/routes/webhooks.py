from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import (
    get_current_principal,
    get_db_session,
    require_rate_limit,
    require_role,
)
from app.core.config import get_settings
from app.schemas.api import (
    CreateWebhookSubscriptionRequest,
    UpdateWebhookSubscriptionRequest,
    WebhookDeliveryListResponse,
    WebhookDeliveryResponse,
    WebhookSubscriptionListResponse,
    WebhookSubscriptionResponse,
)
from app.security.models import Principal, Role
from app.services.audit import AuditEventRepository, AuditService
from app.services.webhooks import (
    WebhookRepository,
    WebhookSubscription,
    validate_webhook_target,
)

router = APIRouter(prefix="/webhooks", tags=["webhooks"])
delivery_router = APIRouter(prefix="/webhook-deliveries", tags=["webhooks"])


def _response(item: WebhookSubscription) -> WebhookSubscriptionResponse:
    return WebhookSubscriptionResponse(**item.__dict__)


def _delivery_response(item) -> WebhookDeliveryResponse:
    return WebhookDeliveryResponse(**item.__dict__)


@router.post(
    "",
    response_model=WebhookSubscriptionResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[
        Depends(require_role(Role.ADMIN)),
        Depends(require_rate_limit(ops=True)),
    ],
)
async def create_webhook(
    request: Request,
    body: CreateWebhookSubscriptionRequest,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    principal: Annotated[Principal, Depends(get_current_principal)],
) -> WebhookSubscriptionResponse:
    try:
        settings = get_settings()
        validate_webhook_target(
            body.target_url,
            settings.webhook_allow_insecure_http,
            settings.webhook_allow_private_networks,
        )
        item = await WebhookRepository(session).create_subscription(
            body.name, body.target_url, body.secret, body.event_types, body.workflow_id
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    await AuditService(AuditEventRepository(session)).record_success(
        request_id=getattr(request.state, "request_id", ""),
        principal=principal,
        action="webhook.subscription.create",
        resource_type="webhook_subscription",
        resource_id=item.id,
        metadata={
            "event_types": list(item.event_types),
            "workflow_id": item.workflow_id,
        },
    )
    return _response(item)


@router.get(
    "",
    response_model=WebhookSubscriptionListResponse,
    dependencies=[
        Depends(require_role(Role.ADMIN)),
        Depends(require_rate_limit(ops=True)),
    ],
)
async def list_webhooks(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> WebhookSubscriptionListResponse:
    items = await WebhookRepository(session).list_subscriptions()
    return WebhookSubscriptionListResponse(
        items=tuple(_response(item) for item in items), count=len(items)
    )


@router.get(
    "/{subscription_id}",
    response_model=WebhookSubscriptionResponse,
    dependencies=[
        Depends(require_role(Role.ADMIN)),
        Depends(require_rate_limit(ops=True)),
    ],
)
async def get_webhook(
    subscription_id: str, session: Annotated[AsyncSession, Depends(get_db_session)]
) -> WebhookSubscriptionResponse:
    try:
        return _response(
            await WebhookRepository(session).get_subscription(subscription_id)
        )
    except KeyError as exc:
        raise HTTPException(
            status_code=404, detail="Webhook subscription not found."
        ) from exc


@router.patch(
    "/{subscription_id}",
    response_model=WebhookSubscriptionResponse,
    dependencies=[
        Depends(require_role(Role.ADMIN)),
        Depends(require_rate_limit(ops=True)),
    ],
)
async def update_webhook(
    subscription_id: str,
    body: UpdateWebhookSubscriptionRequest,
    request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    principal: Annotated[Principal, Depends(get_current_principal)],
) -> WebhookSubscriptionResponse:
    try:
        settings = get_settings()
        if body.target_url is not None:
            validate_webhook_target(
                body.target_url,
                settings.webhook_allow_insecure_http,
                settings.webhook_allow_private_networks,
            )
        item = await WebhookRepository(session).update_subscription(
            subscription_id,
            name=body.name,
            target_url=body.target_url,
            event_types=body.event_types,
            workflow_id=body.workflow_id,
            secret=body.secret,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(
            status_code=404, detail="Webhook subscription not found."
        ) from exc
    await AuditService(AuditEventRepository(session)).record_success(
        request_id=getattr(request.state, "request_id", ""),
        principal=principal,
        action="webhook.subscription.rotate_secret"
        if body.secret is not None
        else "webhook.subscription.update",
        resource_type="webhook_subscription",
        resource_id=item.id,
        metadata={"updated_fields": sorted(body.model_fields_set - {"secret"})},
    )
    return _response(item)


@router.post(
    "/{subscription_id}/disable",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[
        Depends(require_role(Role.ADMIN)),
        Depends(require_rate_limit(ops=True)),
    ],
)
async def post_disable_webhook(
    subscription_id: str,
    request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    principal: Annotated[Principal, Depends(get_current_principal)],
) -> None:
    await disable_webhook(subscription_id, request, session, principal)


@router.get(
    "/{subscription_id}/deliveries",
    response_model=WebhookDeliveryListResponse,
    dependencies=[
        Depends(require_role(Role.ADMIN)),
        Depends(require_rate_limit(ops=True)),
    ],
)
async def list_webhook_deliveries(
    subscription_id: str, session: Annotated[AsyncSession, Depends(get_db_session)]
) -> WebhookDeliveryListResponse:
    items = await WebhookRepository(session).list_deliveries(subscription_id)
    return WebhookDeliveryListResponse(
        items=tuple(_delivery_response(item) for item in items), count=len(items)
    )


@delivery_router.get(
    "/{delivery_id}",
    response_model=WebhookDeliveryResponse,
    dependencies=[
        Depends(require_role(Role.ADMIN)),
        Depends(require_rate_limit(ops=True)),
    ],
)
async def get_webhook_delivery(
    delivery_id: str, session: Annotated[AsyncSession, Depends(get_db_session)]
) -> WebhookDeliveryResponse:
    try:
        return _delivery_response(
            await WebhookRepository(session).get_delivery(delivery_id)
        )
    except KeyError as exc:
        raise HTTPException(
            status_code=404, detail="Webhook delivery not found."
        ) from exc


@router.delete(
    "/{subscription_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[
        Depends(require_role(Role.ADMIN)),
        Depends(require_rate_limit(ops=True)),
    ],
)
async def disable_webhook(
    subscription_id: str,
    request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    principal: Annotated[Principal, Depends(get_current_principal)],
) -> None:
    try:
        await WebhookRepository(session).disable(subscription_id)
    except KeyError as exc:
        raise HTTPException(
            status_code=404, detail="Webhook subscription not found."
        ) from exc
    await AuditService(AuditEventRepository(session)).record_success(
        request_id=getattr(request.state, "request_id", ""),
        principal=principal,
        action="webhook.subscription.disable",
        resource_type="webhook_subscription",
        resource_id=subscription_id,
    )
