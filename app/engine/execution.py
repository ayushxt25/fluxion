from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType

from app.engine.dag import WorkflowDAG
from app.engine.exceptions import (
    InvalidTaskTransitionError,
    RecoveryStateError,
    UnknownTaskRunError,
    WorkflowAlreadyTerminalError,
)
from app.engine.status import AttemptStatus, TaskStatus, WorkflowStatus
from app.schemas.workflow import WorkflowDefinition

_VALID_TASK_TRANSITIONS = {
    TaskStatus.BLOCKED: {TaskStatus.READY, TaskStatus.CANCELLED},
    TaskStatus.READY: {TaskStatus.RUNNING, TaskStatus.CANCELLED},
    TaskStatus.RUNNING: {
        TaskStatus.SUCCEEDED,
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
        TaskStatus.RETRY_WAITING,
    },
    TaskStatus.RETRY_WAITING: {TaskStatus.READY, TaskStatus.CANCELLED},
}


@dataclass(frozen=True)
class TaskRun:
    task_id: str
    status: TaskStatus
    next_retry_at: datetime | None = None


@dataclass(frozen=True)
class TaskAttempt:
    run_id: str
    workflow_id: str
    task_id: str
    attempt_number: int
    status: AttemptStatus
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error_type: str | None = None
    error_message: str | None = None

    @property
    def attempt_key(self) -> str:
        return f"{self.run_id}:{self.task_id}:{self.attempt_number}"


class WorkflowRun:
    def __init__(
        self,
        run_id: str,
        workflow: WorkflowDefinition,
        dag: WorkflowDAG | None = None,
    ) -> None:
        self.run_id = run_id
        self.workflow_id = workflow.id
        self._dag = dag or WorkflowDAG(workflow)
        self._status = WorkflowStatus.PENDING
        self._task_runs = {
            task_id: TaskRun(
                task_id=task_id,
                status=TaskStatus.READY
                if task_id in self._dag.roots
                else TaskStatus.BLOCKED,
            )
            for task_id in self._dag.topological_order()
        }

    @classmethod
    def create(
        cls,
        run_id: str,
        workflow: WorkflowDefinition,
        dag: WorkflowDAG | None = None,
    ) -> "WorkflowRun":
        return cls(run_id=run_id, workflow=workflow, dag=dag)

    @classmethod
    def restore(
        cls,
        run_id: str,
        workflow: WorkflowDefinition,
        status: WorkflowStatus,
        task_statuses: dict[str, TaskStatus],
        next_retry_at: dict[str, datetime | None] | None = None,
        dag: WorkflowDAG | None = None,
    ) -> "WorkflowRun":
        workflow_dag = dag or WorkflowDAG(workflow)
        expected_task_ids = set(workflow_dag.topological_order())

        if set(task_statuses) != expected_task_ids:
            missing = sorted(expected_task_ids - set(task_statuses))
            extra = sorted(set(task_statuses) - expected_task_ids)
            raise UnknownTaskRunError(
                f"invalid restored task state; missing={missing}, extra={extra}"
            )

        workflow_run = cls(run_id=run_id, workflow=workflow, dag=workflow_dag)
        workflow_run._status = status
        workflow_run._task_runs = {
            task_id: TaskRun(
                task_id=task_id,
                status=task_statuses[task_id],
                next_retry_at=(next_retry_at or {}).get(task_id),
            )
            for task_id in workflow_dag.topological_order()
        }
        return workflow_run

    @property
    def status(self) -> WorkflowStatus:
        return self._status

    @property
    def task_runs(self) -> MappingProxyType[str, TaskRun]:
        return MappingProxyType(dict(self._task_runs))

    def get_task_status(self, task_id: str) -> TaskStatus:
        return self._get_task_run(task_id).status

    def ready_tasks(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                task_id
                for task_id, task_run in self._task_runs.items()
                if task_run.status == TaskStatus.READY
            )
        )

    def start_task(self, task_id: str) -> None:
        self._ensure_workflow_can_advance()
        self._transition_task(task_id, TaskStatus.RUNNING)
        self._status = WorkflowStatus.RUNNING

    def complete_task(self, task_id: str) -> None:
        workflow_already_failed = self._status == WorkflowStatus.FAILED
        self._ensure_task_can_finish(task_id)
        self._transition_task(task_id, TaskStatus.SUCCEEDED)
        if not workflow_already_failed:
            self._unlock_ready_dependents(task_id)
        self._refresh_terminal_status()

    def fail_task(self, task_id: str) -> None:
        self._ensure_task_can_finish(task_id)
        self._transition_task(task_id, TaskStatus.FAILED)
        self._status = WorkflowStatus.FAILED

    def schedule_retry(self, task_id: str, next_retry_at: datetime) -> None:
        self._ensure_task_can_finish(task_id)
        self._transition_task(
            task_id,
            TaskStatus.RETRY_WAITING,
            next_retry_at=next_retry_at,
        )

    def make_retry_ready(self, task_id: str) -> None:
        self._transition_task(task_id, TaskStatus.READY, next_retry_at=None)

    def cancel_task(self, task_id: str) -> None:
        self._ensure_workflow_can_advance()
        self._transition_task(task_id, TaskStatus.CANCELLED)
        self._status = WorkflowStatus.FAILED

    def cancel_workflow(self) -> None:
        if self._status == WorkflowStatus.CANCELLED:
            return

        for task_run in self._task_runs.values():
            if not task_run.status.is_terminal:
                self._task_runs[task_run.task_id] = TaskRun(
                    task_id=task_run.task_id,
                    status=TaskStatus.CANCELLED,
                )

        self._status = WorkflowStatus.CANCELLED

    def interrupt_running_tasks_for_recovery(self) -> tuple[str, ...]:
        if self._status != WorkflowStatus.RUNNING:
            return ()

        interrupted = tuple(
            sorted(
                task_id
                for task_id, task_run in self._task_runs.items()
                if task_run.status == TaskStatus.RUNNING
            )
        )
        for task_id in interrupted:
            self._task_runs[task_id] = TaskRun(
                task_id=task_id,
                status=TaskStatus.INTERRUPTED,
            )

        if interrupted:
            self._status = WorkflowStatus.FAILED

        return interrupted

    def reconcile_readiness_for_recovery(self, now: datetime | None = None) -> None:
        self._validate_recoverable_state()

        if self._status.is_terminal:
            return

        for task_id in self._dag.topological_order():
            task_run = self._task_runs[task_id]
            if task_run.status.is_terminal or task_run.status == TaskStatus.RUNNING:
                continue
            if task_run.status == TaskStatus.RETRY_WAITING:
                if task_run.next_retry_at is None:
                    raise RecoveryStateError(
                        self.run_id,
                        f"RETRY_WAITING task '{task_id}' has no next_retry_at.",
                    )
                if now is None or task_run.next_retry_at > now:
                    continue

            next_status = (
                TaskStatus.READY
                if all(
                    self.get_task_status(dependency_id) == TaskStatus.SUCCEEDED
                    for dependency_id in self._dag.dependencies_of(task_id)
                )
                else TaskStatus.BLOCKED
            )
            if task_run.status != next_status:
                self._task_runs[task_id] = TaskRun(
                    task_id=task_id,
                    status=next_status,
                    next_retry_at=None,
                )

    def _validate_recoverable_state(self) -> None:
        if self._status == WorkflowStatus.PENDING:
            invalid_tasks = [
                task_id
                for task_id, task_run in self._task_runs.items()
                if task_run.status not in {TaskStatus.READY, TaskStatus.BLOCKED}
            ]
            if invalid_tasks:
                raise RecoveryStateError(
                    self.run_id,
                    f"PENDING run has tasks that already began: "
                    f"{sorted(invalid_tasks)}.",
                )

        if self._status == WorkflowStatus.SUCCEEDED and any(
            task_run.status != TaskStatus.SUCCEEDED
            for task_run in self._task_runs.values()
        ):
            raise RecoveryStateError(
                self.run_id,
                "SUCCEEDED run contains non-SUCCEEDED tasks.",
            )

        if (
            self._status in {WorkflowStatus.PENDING, WorkflowStatus.RUNNING}
            and all(
                task_run.status == TaskStatus.SUCCEEDED
                for task_run in self._task_runs.values()
            )
        ):
            raise RecoveryStateError(
                self.run_id,
                f"{self._status} run contains only SUCCEEDED tasks.",
            )

        for task_id in self._dag.topological_order():
            if self.get_task_status(task_id) != TaskStatus.SUCCEEDED:
                continue

            invalid_dependencies = [
                dependency_id
                for dependency_id in self._dag.dependencies_of(task_id)
                if self.get_task_status(dependency_id) != TaskStatus.SUCCEEDED
            ]
            if invalid_dependencies:
                raise RecoveryStateError(
                    self.run_id,
                    f"SUCCEEDED task '{task_id}' has non-SUCCEEDED dependencies: "
                    f"{sorted(invalid_dependencies)}.",
                )

    def _transition_task(
        self,
        task_id: str,
        next_status: TaskStatus,
        next_retry_at: datetime | None = None,
    ) -> None:
        task_run = self._get_task_run(task_id)
        allowed = _VALID_TASK_TRANSITIONS.get(task_run.status, set())

        if next_status not in allowed:
            raise InvalidTaskTransitionError(task_id, task_run.status, next_status)

        self._task_runs[task_id] = TaskRun(
            task_id=task_id,
            status=next_status,
            next_retry_at=next_retry_at,
        )

    def _unlock_ready_dependents(self, task_id: str) -> None:
        for dependent_id in self._dag.dependents_of(task_id):
            dependent = self._get_task_run(dependent_id)
            if dependent.status != TaskStatus.BLOCKED:
                continue

            if all(
                self.get_task_status(dependency_id) == TaskStatus.SUCCEEDED
                for dependency_id in self._dag.dependencies_of(dependent_id)
            ):
                self._transition_task(dependent_id, TaskStatus.READY)

    def _refresh_terminal_status(self) -> None:
        if self._status == WorkflowStatus.FAILED:
            return

        if all(
            task_run.status == TaskStatus.SUCCEEDED
            for task_run in self._task_runs.values()
        ):
            self._status = WorkflowStatus.SUCCEEDED

    def _ensure_workflow_can_advance(self) -> None:
        if self._status.is_terminal:
            raise WorkflowAlreadyTerminalError(self.run_id, self._status)

    def _ensure_task_can_finish(self, task_id: str) -> None:
        task_run = self._get_task_run(task_id)
        if (
            self._status == WorkflowStatus.FAILED
            and task_run.status == TaskStatus.RUNNING
        ):
            return

        self._ensure_workflow_can_advance()

    def _get_task_run(self, task_id: str) -> TaskRun:
        try:
            return self._task_runs[task_id]
        except KeyError as exc:
            raise UnknownTaskRunError(task_id) from exc
