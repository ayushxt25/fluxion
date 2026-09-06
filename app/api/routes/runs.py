from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_db_session
from app.api.pagination import LimitQuery, OffsetQuery
from app.engine.exceptions import UnknownTaskRunError
from app.engine.status import WorkflowStatus
from app.schemas.api import (
    RecoveryResponse,
    TaskAttemptListResponse,
    TaskRunResponse,
    WorkflowRunListResponse,
    WorkflowRunResponse,
)
from app.services.management import WorkflowRunManagementService
from app.services.repositories import (
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)

router = APIRouter(prefix="/runs", tags=["runs"])


def _run_service(session: AsyncSession) -> WorkflowRunManagementService:
    return WorkflowRunManagementService(
        WorkflowRepository(session),
        WorkflowRunRepository(session),
        TaskAttemptRepository(session),
    )


@router.get("", response_model=WorkflowRunListResponse, summary="List workflow runs")
async def list_runs(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    workflow_id: str | None = None,
    status: WorkflowStatus | None = None,
    limit: LimitQuery = 50,
    offset: OffsetQuery = 0,
) -> WorkflowRunListResponse:
    runs = await _run_service(session).list_runs(
        workflow_id=workflow_id,
        status=status,
        limit=limit,
        offset=offset,
    )
    return WorkflowRunListResponse(
        items=runs,
        limit=limit,
        offset=offset,
        count=len(runs),
    )


@router.get("/{run_id}", response_model=WorkflowRunResponse, summary="Get run")
async def get_run(
    run_id: str,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> WorkflowRunResponse:
    return await _run_service(session).get_run(run_id)


@router.get(
    "/{run_id}/tasks",
    response_model=tuple[TaskRunResponse, ...],
    summary="List task runs",
)
async def list_tasks(
    run_id: str,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> tuple[TaskRunResponse, ...]:
    return (await _run_service(session).get_run(run_id)).tasks


@router.get(
    "/{run_id}/tasks/{task_id}",
    response_model=TaskRunResponse,
    summary="Get task run",
)
async def get_task(
    run_id: str,
    task_id: str,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> TaskRunResponse:
    tasks = (await _run_service(session).get_run(run_id)).tasks
    for task in tasks:
        if task.task_id == task_id:
            return task
    raise UnknownTaskRunError(task_id)


@router.get(
    "/{run_id}/tasks/{task_id}/attempts",
    response_model=TaskAttemptListResponse,
    summary="List task attempts",
)
async def list_attempts(
    run_id: str,
    task_id: str,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> TaskAttemptListResponse:
    attempts = await _run_service(session).list_attempts(run_id, task_id)
    return TaskAttemptListResponse(items=attempts, count=len(attempts))


@router.post(
    "/{run_id}/cancel",
    response_model=WorkflowRunResponse,
    summary="Cancel workflow run",
)
async def cancel_run(
    run_id: str,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> WorkflowRunResponse:
    return await _run_service(session).cancel_run(run_id)


@router.post(
    "/{run_id}/recover",
    response_model=RecoveryResponse,
    summary="Recover workflow run state",
)
async def recover_run(
    run_id: str,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> RecoveryResponse:
    return await _run_service(session).recover_run(run_id)


@router.post(
    "/{run_id}/resume",
    response_model=WorkflowRunResponse,
    summary="Validate run continuation",
)
async def continue_run(
    run_id: str,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> WorkflowRunResponse:
    return await _run_service(session).continue_run(run_id)
