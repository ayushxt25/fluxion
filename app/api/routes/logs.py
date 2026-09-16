from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_db_session, require_rate_limit, require_role
from app.schemas.api import TaskLogListResponse, TaskLogResponse
from app.security.models import Role
from app.services.repositories import WorkflowRunRepository
from app.services.task_logs import TaskLogRepository

router = APIRouter(prefix="/runs", tags=["task-logs"])


@router.get(
    "/{run_id}/tasks/{task_id}/attempts/{attempt_number}/logs",
    response_model=TaskLogListResponse,
    summary="List attempt diagnostic logs",
    dependencies=[Depends(require_role(Role.VIEWER)), Depends(require_rate_limit())],
)
async def get_attempt_logs(
    run_id: str,
    task_id: str,
    attempt_number: int,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    after_sequence: int | None = Query(default=None, ge=0),
    limit: int = Query(default=100, ge=1, le=500),
) -> TaskLogListResponse:
    await WorkflowRunRepository(session).get_workflow_id(run_id)
    logs = await TaskLogRepository(session).list_attempt(
        run_id,
        task_id,
        attempt_number,
        after_sequence=after_sequence,
        limit=limit,
    )
    return TaskLogListResponse(
        items=tuple(
            TaskLogResponse(
                sequence=entry.sequence_number,
                level=entry.level,
                message=entry.message,
                fields=entry.fields,
                created_at=entry.created_at,
            )
            for entry in logs
        ),
        limit=limit,
        count=len(logs),
    )
