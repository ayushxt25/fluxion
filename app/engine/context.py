from dataclasses import dataclass


@dataclass(frozen=True)
class TaskExecutionContext:
    workflow_id: str
    run_id: str
    task_id: str
    attempt_number: int
    attempt_key: str
    idempotency_key: str
