from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Header, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session
from app.dispatch.transport import RedisTaskDispatcher
from app.observability.context import set_principal
from app.security.auth import (
    AuthorizationError,
    RateLimitExceededError,
    authenticate_bearer_token,
)
from app.security.models import ROLE_RANK, Principal, Role
from app.security.rate_limit import RedisRateLimiter, limit_for_role
from app.services.audit import AuditEventRepository, AuditService
from app.services.repositories import (
    DispatchOutboxRepository,
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)


async def get_db_session() -> AsyncIterator[AsyncSession]:
    async for session in get_session():
        yield session


def get_workflow_repository(session: AsyncSession) -> WorkflowRepository:
    return WorkflowRepository(session)


def get_run_repository(session: AsyncSession) -> WorkflowRunRepository:
    return WorkflowRunRepository(session)


def get_attempt_repository(session: AsyncSession) -> TaskAttemptRepository:
    return TaskAttemptRepository(session)


def get_outbox_repository(session: AsyncSession) -> DispatchOutboxRepository:
    return DispatchOutboxRepository(session)


def get_redis_dispatcher(request: Request) -> RedisTaskDispatcher:
    return request.app.state.redis_dispatcher


def get_rate_limiter(request: Request) -> RedisRateLimiter:
    return request.app.state.rate_limiter


def get_current_principal(
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
) -> Principal:
    principal = authenticate_bearer_token(authorization)
    request.state.principal = principal
    set_principal(principal.subject, principal.role.value)
    return principal


def require_role(required_role: Role):
    async def dependency(
        request: Request,
        session: Annotated[AsyncSession, Depends(get_db_session)],
        principal: Annotated[Principal, Depends(get_current_principal)],
    ) -> Principal:
        if ROLE_RANK[principal.role] < ROLE_RANK[required_role]:
            await _audit_denied(
                session,
                request,
                principal,
                "authorization.denied",
                {"required_role": required_role.value},
            )
            raise AuthorizationError("Insufficient role.")
        return principal

    return dependency


def require_rate_limit(*, ops: bool = False):
    async def dependency(
        request: Request,
        session: Annotated[AsyncSession, Depends(get_db_session)],
        principal: Annotated[Principal, Depends(get_current_principal)],
        limiter: Annotated[RedisRateLimiter, Depends(get_rate_limiter)],
    ) -> None:
        from app.core.config import get_settings

        settings = get_settings()
        if not settings.rate_limit_enabled:
            return
        limit = limit_for_role(principal, ops=ops, settings=settings)
        scope = "ops" if ops else "api"
        try:
            decision = await limiter.check(principal, scope=scope, limit=limit)
        except RateLimitExceededError as exc:
            await _audit_denied(
                session,
                request,
                principal,
                "rate_limit.denied",
                {"scope": scope, "limit": limit},
            )
            request.state.rate_limit = exc
            raise
        request.state.rate_limit = decision

    return dependency


async def _audit_denied(
    session: AsyncSession,
    request: Request,
    principal: Principal,
    action: str,
    metadata: dict,
) -> None:
    await AuditService(AuditEventRepository(session)).record_denied(
        request_id=getattr(request.state, "request_id", ""),
        principal=principal,
        action=action,
        metadata=metadata,
    )
