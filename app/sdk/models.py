from datetime import datetime
from typing import Annotated, Any, Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field

JSONValue: TypeAlias = dict[str, Any] | list[Any] | str | int | float | bool | None


class SDKModel(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True)


class Health(SDKModel):
    status: str
    service: str


class Readiness(SDKModel):
    status: str
    checks: dict[str, str]


class Retry(SDKModel):
    max_attempts: int = Field(default=1, ge=1)
    initial_delay_seconds: float = Field(
        default=0.0,
        ge=0,
        validation_alias="initial_backoff_seconds",
        serialization_alias="initial_backoff_seconds",
    )
    backoff_multiplier: float = Field(default=2.0, ge=1)
    max_delay_seconds: float | None = Field(
        default=None,
        ge=0,
        validation_alias="max_backoff_seconds",
        serialization_alias="max_backoff_seconds",
    )


class WorkflowInputParameter(SDKModel):
    source: Literal["workflow_input"] = "workflow_input"
    path: tuple[str | int, ...] = ()


class DependencyResultParameter(SDKModel):
    source: Literal["dependency_result"] = "dependency_result"
    task_id: str
    path: tuple[str | int, ...] = ()


class LiteralParameter(SDKModel):
    source: Literal["literal"] = "literal"
    value: JSONValue


ParameterMapping: TypeAlias = Annotated[
    WorkflowInputParameter | DependencyResultParameter | LiteralParameter,
    Field(discriminator="source"),
]


class TaskDefinition(SDKModel):
    id: str
    name: str | None = None
    depends_on: tuple[str, ...] = ()
    retry_policy: Retry = Field(default_factory=Retry)
    parameters: dict[str, ParameterMapping] = Field(default_factory=dict)


class Workflow(SDKModel):
    id: str
    name: str
    tasks: tuple[TaskDefinition, ...]


class WorkflowList(SDKModel):
    items: tuple[Workflow, ...]
    limit: int
    offset: int
    count: int


class TaskRun(SDKModel):
    task_id: str
    status: str
    next_retry_at: datetime | None
    idempotency_key: str
    result: JSONValue = None
    has_result: bool = False
    attempt_count: int
    latest_attempt_status: str | None
    dependencies: tuple[str, ...] = ()


class WorkflowRun(SDKModel):
    run_id: str
    workflow_id: str
    status: str
    created_at: datetime
    input: JSONValue = None
    has_input: bool = False
    tasks: tuple[TaskRun, ...]


class WorkflowRunListItem(SDKModel):
    run_id: str
    workflow_id: str
    status: str
    created_at: datetime


class WorkflowRunList(SDKModel):
    items: tuple[WorkflowRunListItem, ...]
    limit: int
    offset: int
    count: int


class TaskAttempt(SDKModel):
    attempt_number: int
    status: str
    created_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    worker_id: str | None = None
    last_heartbeat_at: datetime | None = None
    lease_expires_at: datetime | None = None
    error_type: str | None = None
    error_message: str | None = None
    attempt_key: str


class TaskAttemptList(SDKModel):
    items: tuple[TaskAttempt, ...]
    count: int


class RunEvent(SDKModel):
    id: int
    version: int
    event_type: str
    workflow_id: str
    run_id: str
    task_id: str | None = None
    attempt_number: int | None = None
    created_at: datetime
    payload: dict[str, Any] | None = None


class RunEventList(SDKModel):
    items: tuple[RunEvent, ...]
    limit: int
    count: int


class RecoveryResult(SDKModel):
    run_id: str
    workflow_id: str
    previous_status: str
    recovered_status: str
    interrupted_task_ids: tuple[str, ...]
    task_statuses: dict[str, str]
    resumable: bool


class SchedulerTickResult(SDKModel):
    scheduled_runs: int
    dispatched_tasks: int
    outbox_event_ids: tuple[str, ...]


class OutboxPublishResult(SDKModel):
    attempted: int
    published: int
    failed: int
    discarded: int = 0
    published_event_ids: tuple[str, ...]
    failed_event_ids: tuple[str, ...]
    discarded_event_ids: tuple[str, ...] = ()


class LeaseReapResult(SDKModel):
    reclaimed: int
    run_ids: tuple[str, ...]


class AuditEvent(SDKModel):
    id: str
    occurred_at: datetime
    request_id: str
    principal_subject: str | None = None
    principal_role: str | None = None
    action: str
    resource_type: str | None = None
    resource_id: str | None = None
    outcome: str
    metadata: dict[str, Any] | None = None


class AuditEventList(SDKModel):
    items: tuple[AuditEvent, ...]
    limit: int
    offset: int
    count: int
