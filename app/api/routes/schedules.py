# ruff: noqa: E501
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
from app.schemas.schedules import (
    ScheduleCreate,
    ScheduleUpdate,
    WorkflowSchedule,
    WorkflowScheduleList,
)
from app.security.models import Principal, Role
from app.services.audit import AuditEventRepository, AuditService
from app.services.schedules import ScheduleRepository

router = APIRouter(prefix="/schedules", tags=["schedules"])


async def _audit(
    session: AsyncSession,
    request: Request,
    principal: Principal,
    action: str,
    schedule: WorkflowSchedule,
) -> None:
    await AuditService(AuditEventRepository(session)).record_success(
        request_id=getattr(request.state, "request_id", ""),
        principal=principal,
        action=action,
        resource_type="workflow_schedule",
        resource_id=schedule.id,
        metadata={
            "workflow_id": schedule.workflow_id,
            "workflow_revision": schedule.workflow_revision,
        },
    )


@router.post(
    "",
    response_model=WorkflowSchedule,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_role(Role.OPERATOR)), Depends(require_rate_limit())],
)
async def create_schedule(
    request: ScheduleCreate,
    http_request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    principal: Annotated[Principal, Depends(get_current_principal)],
) -> WorkflowSchedule:
    schedule = await ScheduleRepository(session).create(
        request, subject=principal.subject, role=principal.role.value
    )
    await _audit(session, http_request, principal, "schedule.create", schedule)
    return schedule


@router.get(
    "",
    response_model=WorkflowScheduleList,
    dependencies=[Depends(require_role(Role.VIEWER)), Depends(require_rate_limit())],
)
async def list_schedules(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    limit: LimitQuery = 50,
    offset: OffsetQuery = 0,
) -> WorkflowScheduleList:
    schedules = await ScheduleRepository(session).list(limit, offset)
    return WorkflowScheduleList(
        items=schedules, limit=limit, offset=offset, count=len(schedules)
    )


@router.get(
    "/{schedule_id}",
    response_model=WorkflowSchedule,
    dependencies=[Depends(require_role(Role.VIEWER)), Depends(require_rate_limit())],
)
async def get_schedule(
    schedule_id: str, session: Annotated[AsyncSession, Depends(get_db_session)]
) -> WorkflowSchedule:
    return await ScheduleRepository(session).get(schedule_id)


@router.patch(
    "/{schedule_id}",
    response_model=WorkflowSchedule,
    dependencies=[Depends(require_role(Role.OPERATOR)), Depends(require_rate_limit())],
)
async def update_schedule(
    schedule_id: str,
    request: ScheduleUpdate,
    http_request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    principal: Annotated[Principal, Depends(get_current_principal)],
) -> WorkflowSchedule:
    schedule = await ScheduleRepository(session).update(schedule_id, request)
    await _audit(session, http_request, principal, "schedule.update", schedule)
    return schedule


@router.post(
    "/{schedule_id}/pause",
    response_model=WorkflowSchedule,
    dependencies=[Depends(require_role(Role.OPERATOR)), Depends(require_rate_limit())],
)
async def pause_schedule(
    schedule_id: str,
    request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    principal: Annotated[Principal, Depends(get_current_principal)],
) -> WorkflowSchedule:
    schedule = await ScheduleRepository(session).set_enabled(schedule_id, False)
    await _audit(session, request, principal, "schedule.pause", schedule)
    return schedule


@router.post(
    "/{schedule_id}/resume",
    response_model=WorkflowSchedule,
    dependencies=[Depends(require_role(Role.OPERATOR)), Depends(require_rate_limit())],
)
async def resume_schedule(
    schedule_id: str,
    request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    principal: Annotated[Principal, Depends(get_current_principal)],
) -> WorkflowSchedule:
    schedule = await ScheduleRepository(session).set_enabled(schedule_id, True)
    await _audit(session, request, principal, "schedule.resume", schedule)
    return schedule


@router.delete(
    "/{schedule_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(require_role(Role.OPERATOR)), Depends(require_rate_limit())],
)
async def delete_schedule(
    schedule_id: str,
    request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    principal: Annotated[Principal, Depends(get_current_principal)],
) -> None:
    schedule = await ScheduleRepository(session).set_enabled(schedule_id, False)
    await _audit(session, request, principal, "schedule.delete", schedule)
