from dataclasses import dataclass
from types import MappingProxyType

from app.engine.dag import WorkflowDAG
from app.engine.exceptions import (
    InvalidTaskTransitionError,
    UnknownTaskRunError,
    WorkflowAlreadyTerminalError,
)
from app.engine.status import TaskStatus, WorkflowStatus
from app.schemas.workflow import WorkflowDefinition

_VALID_TASK_TRANSITIONS = {
    TaskStatus.BLOCKED: {TaskStatus.READY, TaskStatus.CANCELLED},
    TaskStatus.READY: {TaskStatus.RUNNING, TaskStatus.CANCELLED},
    TaskStatus.RUNNING: {
        TaskStatus.SUCCEEDED,
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
    },
}


@dataclass(frozen=True)
class TaskRun:
    task_id: str
    status: TaskStatus


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

    def _transition_task(self, task_id: str, next_status: TaskStatus) -> None:
        task_run = self._get_task_run(task_id)
        allowed = _VALID_TASK_TRANSITIONS.get(task_run.status, set())

        if next_status not in allowed:
            raise InvalidTaskTransitionError(task_id, task_run.status, next_status)

        self._task_runs[task_id] = TaskRun(
            task_id=task_id,
            status=next_status,
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
