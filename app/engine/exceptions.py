class WorkflowValidationError(Exception):
    """Base exception for invalid workflow definitions."""


class EmptyWorkflowError(WorkflowValidationError):
    def __init__(self, workflow_id: str) -> None:
        super().__init__(f"Workflow '{workflow_id}' must define at least one task.")


class DuplicateTaskError(WorkflowValidationError):
    def __init__(self, task_id: str) -> None:
        super().__init__(f"Workflow contains duplicate task id '{task_id}'.")


class DuplicateDependencyError(WorkflowValidationError):
    def __init__(self, task_id: str, dependency_id: str) -> None:
        super().__init__(
            f"Task '{task_id}' contains duplicate dependency '{dependency_id}'."
        )


class SelfDependencyError(WorkflowValidationError):
    def __init__(self, task_id: str) -> None:
        super().__init__(f"Task '{task_id}' cannot depend on itself.")


class UnknownDependencyError(WorkflowValidationError):
    def __init__(self, task_id: str, dependency_id: str) -> None:
        super().__init__(
            f"Task '{task_id}' depends on unknown task '{dependency_id}'."
        )


class CycleDetectedError(WorkflowValidationError):
    def __init__(self, task_ids: list[str]) -> None:
        cycle_candidates = ", ".join(sorted(task_ids))
        super().__init__(
            f"Workflow contains a cycle involving task ids: {cycle_candidates}."
        )


class UnknownTaskError(WorkflowValidationError):
    def __init__(self, task_id: str) -> None:
        super().__init__(f"Workflow does not contain task '{task_id}'.")


class ExecutionStateError(Exception):
    """Base exception for invalid workflow run state operations."""


class InvalidTaskTransitionError(ExecutionStateError):
    def __init__(self, task_id: str, current_status: str, next_status: str) -> None:
        super().__init__(
            f"Task '{task_id}' cannot transition from "
            f"'{current_status}' to '{next_status}'."
        )


class UnknownTaskRunError(ExecutionStateError):
    def __init__(self, task_id: str) -> None:
        super().__init__(f"Workflow run does not contain task '{task_id}'.")


class WorkflowAlreadyTerminalError(ExecutionStateError):
    def __init__(self, run_id: str, status: str) -> None:
        super().__init__(f"Workflow run '{run_id}' is already terminal: '{status}'.")


class TaskImplementationError(Exception):
    """Base exception for task implementation registry errors."""


class MissingTaskImplementationError(TaskImplementationError):
    def __init__(self, task_id: str) -> None:
        super().__init__(f"Task '{task_id}' has no registered implementation.")


class DuplicateTaskImplementationError(TaskImplementationError):
    def __init__(self, task_id: str) -> None:
        super().__init__(f"Task '{task_id}' already has a registered implementation.")


class InvalidTaskCallableError(TaskImplementationError):
    def __init__(self, task_id: str, reason: str) -> None:
        super().__init__(f"Task '{task_id}' has an invalid callable: {reason}")


class InvalidConcurrencyLimitError(TaskImplementationError):
    def __init__(self, max_concurrency: int) -> None:
        super().__init__(
            f"Executor max_concurrency must be a positive integer or None; "
            f"got {max_concurrency}."
        )


class PersistenceError(Exception):
    """Base exception for persistence-layer failures."""


class WorkflowAlreadyExistsError(PersistenceError):
    def __init__(self, workflow_id: str) -> None:
        super().__init__(f"Workflow '{workflow_id}' already exists.")


class WorkflowNotFoundError(PersistenceError):
    def __init__(self, workflow_id: str) -> None:
        super().__init__(f"Workflow '{workflow_id}' was not found.")


class WorkflowRunAlreadyExistsError(PersistenceError):
    def __init__(self, run_id: str) -> None:
        super().__init__(f"Workflow run '{run_id}' already exists.")


class WorkflowRunNotFoundError(PersistenceError):
    def __init__(self, run_id: str) -> None:
        super().__init__(f"Workflow run '{run_id}' was not found.")


class ExecutionPersistenceError(PersistenceError):
    def __init__(self, run_id: str, operation: str) -> None:
        super().__init__(
            f"Failed to durably persist workflow run '{run_id}' during {operation}."
        )


class RecoveryStateError(Exception):
    def __init__(self, run_id: str, message: str) -> None:
        super().__init__(f"Workflow run '{run_id}' cannot be recovered: {message}")


class WorkflowRunNotResumableError(Exception):
    def __init__(self, run_id: str, reason: str) -> None:
        super().__init__(f"Workflow run '{run_id}' is not resumable: {reason}")


class DispatchError(Exception):
    """Base exception for dispatch transport and state failures."""


class InvalidDispatchMessageError(DispatchError):
    def __init__(self, reason: str) -> None:
        super().__init__(f"Invalid dispatch message: {reason}")


class DispatchStateError(DispatchError):
    def __init__(self, run_id: str, task_id: str, reason: str) -> None:
        super().__init__(
            f"Dispatch state for run '{run_id}' task '{task_id}' is invalid: "
            f"{reason}"
        )
