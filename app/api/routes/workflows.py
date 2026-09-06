from typing import Annotated

from fastapi import APIRouter, Depends, Request, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import (
    get_current_principal,
    get_db_session,
    require_rate_limit,
    require_role,
)
from app.api.pagination import LimitQuery, OffsetQuery
from app.schemas.api import (
    CreateWorkflowRunRequest,
    WorkflowListResponse,
    WorkflowRunResponse,
)
from app.schemas.workflow import WorkflowDefinition
from app.security.models import Principal, Role
from app.services.audit import AuditEventRepository, AuditService
from app.services.management import (
    WorkflowManagementService,
    WorkflowRunManagementService,
)
from app.services.repositories import (
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)

router = APIRouter(prefix="/workflows", tags=["workflows"])


def _workflow_service(session: AsyncSession) -> WorkflowManagementService:
    return WorkflowManagementService(WorkflowRepository(session))


def _run_service(session: AsyncSession) -> WorkflowRunManagementService:
    return WorkflowRunManagementService(
        WorkflowRepository(session),
        WorkflowRunRepository(session),
        TaskAttemptRepository(session),
    )


@router.post(
    "",
    response_model=WorkflowDefinition,
    status_code=status.HTTP_201_CREATED,
    summary="Create workflow definition",
    dependencies=[
        Depends(require_role(Role.OPERATOR)),
        Depends(require_rate_limit()),
    ],
)
async def create_workflow(
    workflow: WorkflowDefinition,
    request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    principal: Annotated[Principal, Depends(get_current_principal)],
) -> WorkflowDefinition:
    created = await _workflow_service(session).create(workflow)
    await AuditService(AuditEventRepository(session)).record_success(
        request_id=getattr(request.state, "request_id", ""),
        principal=principal,
        action="workflow.create",
        resource_type="workflow",
        resource_id=workflow.id,
    )
    return created


@router.get(
    "",
    response_model=WorkflowListResponse,
    summary="List workflows",
    dependencies=[Depends(require_role(Role.VIEWER)), Depends(require_rate_limit())],
)
async def list_workflows(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    limit: LimitQuery = 50,
    offset: OffsetQuery = 0,
) -> WorkflowListResponse:
    workflows = await _workflow_service(session).list(limit=limit, offset=offset)
    return WorkflowListResponse(
        items=workflows,
        limit=limit,
        offset=offset,
        count=len(workflows),
    )


@router.get(
    "/{workflow_id}",
    response_model=WorkflowDefinition,
    summary="Get workflow definition",
    dependencies=[Depends(require_role(Role.VIEWER)), Depends(require_rate_limit())],
)
async def get_workflow(
    workflow_id: str,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> WorkflowDefinition:
    return await _workflow_service(session).get(workflow_id)


@router.post(
    "/{workflow_id}/runs",
    response_model=WorkflowRunResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create durable workflow run",
    dependencies=[
        Depends(require_role(Role.OPERATOR)),
        Depends(require_rate_limit()),
    ],
)
async def create_run(
    workflow_id: str,
    request: CreateWorkflowRunRequest,
    response: Response,
    http_request: Request,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    principal: Annotated[Principal, Depends(get_current_principal)],
) -> WorkflowRunResponse:
    run = await _run_service(session).create_run(workflow_id, request.run_id)
    response.headers["Location"] = f"/api/v1/runs/{run.run_id}"
    await AuditService(AuditEventRepository(session)).record_success(
        request_id=getattr(http_request.state, "request_id", ""),
        principal=principal,
        action="run.create",
        resource_type="run",
        resource_id=run.run_id,
        metadata={"workflow_id": workflow_id},
    )
    return run
