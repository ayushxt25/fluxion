from collections.abc import AsyncIterator

from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session
from app.dispatch.transport import RedisTaskDispatcher
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
