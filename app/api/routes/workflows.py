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
    WorkflowRevisionDiff,
    WorkflowRevisionSummary,
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
    summary="Create or publish workflow definition",
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
    created = await _workflow_service(session).create(
        workflow,
        created_by_subject=principal.subject,
        created_by_role=principal.role.value,
    )
    await AuditService(AuditEventRepository(session)).record_success(
        request_id=getattr(request.state, "request_id", ""),
        principal=principal,
        action="workflow.publish",
        resource_type="workflow",
        resource_id=workflow.id,
        metadata={"workflow_id": created.id, "revision": created.revision},
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
    "/{workflow_id}/revisions",
    response_model=tuple[WorkflowRevisionSummary, ...],
    summary="List immutable workflow revisions",
    dependencies=[Depends(require_role(Role.VIEWER)), Depends(require_rate_limit())],
)
async def list_workflow_revisions(
    workflow_id: str,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> tuple[WorkflowRevisionSummary, ...]:
    revisions = await WorkflowRepository(session).list_revisions(workflow_id)
    return tuple(
        WorkflowRevisionSummary(
            workflow_id=item.id,
            revision=item.revision,
            name=item.name,
            created_at=item.created_at,
            created_by_subject=item.created_by_subject,
            created_by_role=item.created_by_role,
        )
        for item in revisions
    )


@router.get(
    "/{workflow_id}/revisions/{revision}",
    response_model=WorkflowDefinition,
    summary="Get immutable workflow revision",
    dependencies=[Depends(require_role(Role.VIEWER)), Depends(require_rate_limit())],
)
async def get_workflow_revision(
    workflow_id: str,
    revision: int,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> WorkflowDefinition:
    return await WorkflowRepository(session).get_revision(workflow_id, revision)


@router.get(
    "/{workflow_id}/revisions/{from_revision}/diff/{to_revision}",
    response_model=WorkflowRevisionDiff,
    summary="Compare immutable workflow revisions",
    dependencies=[Depends(require_role(Role.VIEWER)), Depends(require_rate_limit())],
)
async def diff_workflow_revisions(
    workflow_id: str,
    from_revision: int,
    to_revision: int,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> WorkflowRevisionDiff:
    return await _workflow_service(session).compare_revisions(
        workflow_id, from_revision, to_revision
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
    run = await _run_service(session).create_run(
        workflow_id,
        request.run_id,
        workflow_revision=request.workflow_revision,
        workflow_input=request.input,
        workflow_input_present="input" in request.model_fields_set,
    )
    response.headers["Location"] = f"/api/v1/runs/{run.run_id}"
    await AuditService(AuditEventRepository(session)).record_success(
        request_id=getattr(http_request.state, "request_id", ""),
        principal=principal,
        action="run.create",
        resource_type="run",
        resource_id=run.run_id,
        metadata={
            "workflow_id": workflow_id,
            "workflow_revision": run.workflow_revision,
        },
    )
    return run
