from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Header, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session
from app.dispatch.transport import RedisTaskDispatcher
from app.security.auth import AuthorizationError, authenticate_bearer_token
from app.security.models import ROLE_RANK, Principal, Role
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


def get_current_principal(
    authorization: Annotated[str | None, Header()] = None,
) -> Principal:
    return authenticate_bearer_token(authorization)


def require_role(required_role: Role):
    def dependency(
        principal: Annotated[Principal, Depends(get_current_principal)],
    ) -> Principal:
        if ROLE_RANK[principal.role] < ROLE_RANK[required_role]:
            raise AuthorizationError("Insufficient role.")
        return principal

    return dependency
