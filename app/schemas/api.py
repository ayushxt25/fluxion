from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from app.schemas.workflow import WorkflowDefinition


class ErrorDetail(BaseModel):
    code: str
    message: str


class ErrorResponse(BaseModel):
    error: ErrorDetail


class WorkflowListResponse(BaseModel):
    items: tuple[WorkflowDefinition, ...]
    limit: int
    offset: int
    count: int


class CreateWorkflowRunRequest(BaseModel):
    run_id: str | None = None
    input: Any = None


class TaskRunResponse(BaseModel):
    task_id: str
    status: str
    next_retry_at: datetime | None
    idempotency_key: str
    result: Any = None
    has_result: bool = False
    attempt_count: int
    latest_attempt_status: str | None
    dependencies: tuple[str, ...] = ()


class WorkflowRunResponse(BaseModel):
    run_id: str
    workflow_id: str
    status: str
    created_at: datetime
    input: Any = None
    has_input: bool = False
    tasks: tuple[TaskRunResponse, ...]


class WorkflowRunListItem(BaseModel):
    run_id: str
    workflow_id: str
    status: str
    created_at: datetime


class WorkflowRunListResponse(BaseModel):
    items: tuple[WorkflowRunListItem, ...]
    limit: int
    offset: int
    count: int


class TaskAttemptResponse(BaseModel):
    attempt_number: int
    status: str
    created_at: datetime | None = None
    started_at: datetime | None
    finished_at: datetime | None
    worker_id: str | None
    last_heartbeat_at: datetime | None
    lease_expires_at: datetime | None
    error_type: str | None
    error_message: str | None
    attempt_key: str


class TaskAttemptListResponse(BaseModel):
    items: tuple[TaskAttemptResponse, ...]
    count: int


class RecoveryResponse(BaseModel):
    run_id: str
    workflow_id: str
    previous_status: str
    recovered_status: str
    interrupted_task_ids: tuple[str, ...]
    task_statuses: dict[str, str]
    resumable: bool


class SchedulerTickResponse(BaseModel):
    scheduled_runs: int
    dispatched_tasks: int
    outbox_event_ids: tuple[str, ...]


class OutboxPublishResponse(BaseModel):
    attempted: int
    published: int
    failed: int
    discarded: int = 0
    published_event_ids: tuple[str, ...]
    failed_event_ids: tuple[str, ...]
    discarded_event_ids: tuple[str, ...] = Field(default_factory=tuple)


class LeaseReapResponse(BaseModel):
    reclaimed: int
    run_ids: tuple[str, ...]


class AuditEventResponse(BaseModel):
    id: str
    occurred_at: datetime
    request_id: str
    principal_subject: str | None
    principal_role: str | None
    action: str
    resource_type: str | None
    resource_id: str | None
    outcome: str
    metadata: dict | None


class AuditEventListResponse(BaseModel):
    items: tuple[AuditEventResponse, ...]
    limit: int
    offset: int
    count: int
