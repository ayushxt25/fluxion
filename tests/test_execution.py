import pytest

from app.engine.exceptions import (
    InvalidTaskTransitionError,
    UnknownTaskRunError,
    WorkflowAlreadyTerminalError,
)
from app.engine.execution import WorkflowRun
from app.engine.status import TaskStatus, WorkflowStatus
from app.schemas.workflow import TaskDefinition, WorkflowDefinition


def task(task_id: str, depends_on: tuple[str, ...] = ()) -> TaskDefinition:
    return TaskDefinition(id=task_id, depends_on=depends_on)


def workflow(*tasks: TaskDefinition) -> WorkflowDefinition:
    return WorkflowDefinition(id="workflow", name="Workflow", tasks=tasks)


def run(*tasks: TaskDefinition) -> WorkflowRun:
    return WorkflowRun.create("run-1", workflow(*tasks))


def test_single_task_initialization() -> None:
    workflow_run = run(task("a"))

    assert workflow_run.status == WorkflowStatus.PENDING
    assert workflow_run.get_task_status("a") == TaskStatus.READY
    assert workflow_run.ready_tasks() == ("a",)


def test_roots_ready_and_non_roots_blocked() -> None:
    workflow_run = run(task("a"), task("b"), task("c", ("a",)))

    assert workflow_run.get_task_status("a") == TaskStatus.READY
    assert workflow_run.get_task_status("b") == TaskStatus.READY
    assert workflow_run.get_task_status("c") == TaskStatus.BLOCKED


def test_ready_tasks_order_is_deterministic() -> None:
    workflow_run = run(task("c"), task("a"), task("b"))

    assert workflow_run.ready_tasks() == ("a", "b", "c")


def test_ready_to_running_transition() -> None:
    workflow_run = run(task("a"))

    workflow_run.start_task("a")

    assert workflow_run.get_task_status("a") == TaskStatus.RUNNING
    assert workflow_run.status == WorkflowStatus.RUNNING


def test_running_to_succeeded_transition() -> None:
    workflow_run = run(task("a"))

    workflow_run.start_task("a")
    workflow_run.complete_task("a")

    assert workflow_run.get_task_status("a") == TaskStatus.SUCCEEDED
    assert workflow_run.status == WorkflowStatus.SUCCEEDED


def test_running_to_failed_transition() -> None:
    workflow_run = run(task("a"))

    workflow_run.start_task("a")
    workflow_run.fail_task("a")

    assert workflow_run.get_task_status("a") == TaskStatus.FAILED
    assert workflow_run.status == WorkflowStatus.FAILED


def test_blocked_task_cannot_start() -> None:
    workflow_run = run(task("a"), task("b", ("a",)))

    with pytest.raises(InvalidTaskTransitionError, match="'BLOCKED' to 'RUNNING'"):
        workflow_run.start_task("b")


def test_ready_task_cannot_complete_without_running() -> None:
    workflow_run = run(task("a"))

    with pytest.raises(InvalidTaskTransitionError, match="'READY' to 'SUCCEEDED'"):
        workflow_run.complete_task("a")


def test_terminal_task_cannot_transition() -> None:
    workflow_run = run(task("a"), task("b"))

    workflow_run.start_task("a")
    workflow_run.complete_task("a")

    with pytest.raises(InvalidTaskTransitionError, match="'SUCCEEDED' to 'RUNNING'"):
        workflow_run.start_task("a")


def test_completing_root_unlocks_direct_dependent() -> None:
    workflow_run = run(task("a"), task("b", ("a",)))

    workflow_run.start_task("a")
    workflow_run.complete_task("a")

    assert workflow_run.get_task_status("b") == TaskStatus.READY
    assert workflow_run.ready_tasks() == ("b",)


def test_diamond_join_unlocks_after_both_parents_succeed() -> None:
    workflow_run = run(
        task("a"),
        task("b", ("a",)),
        task("c", ("a",)),
        task("d", ("b", "c")),
    )

    workflow_run.start_task("a")
    workflow_run.complete_task("a")
    workflow_run.start_task("b")
    workflow_run.complete_task("b")
    assert workflow_run.get_task_status("d") == TaskStatus.BLOCKED

    workflow_run.start_task("c")
    workflow_run.complete_task("c")
    assert workflow_run.get_task_status("d") == TaskStatus.READY


def test_running_parent_keeps_join_blocked() -> None:
    workflow_run = run(task("a"), task("b"), task("d", ("a", "b")))

    workflow_run.start_task("a")
    workflow_run.start_task("b")
    workflow_run.complete_task("a")

    assert workflow_run.get_task_status("d") == TaskStatus.BLOCKED


def test_failed_dependency_does_not_unlock_downstream() -> None:
    workflow_run = run(task("a"), task("b", ("a",)))

    workflow_run.start_task("a")
    workflow_run.fail_task("a")

    assert workflow_run.get_task_status("b") == TaskStatus.BLOCKED


def test_cancelled_dependency_does_not_unlock_downstream() -> None:
    workflow_run = run(task("a"), task("b", ("a",)))

    workflow_run.cancel_task("a")

    assert workflow_run.status == WorkflowStatus.FAILED
    assert workflow_run.get_task_status("b") == TaskStatus.BLOCKED


def test_disconnected_component_remains_independently_executable() -> None:
    workflow_run = run(task("a"), task("b", ("a",)), task("c"), task("d", ("c",)))

    workflow_run.start_task("a")
    workflow_run.complete_task("a")

    assert workflow_run.get_task_status("b") == TaskStatus.READY
    assert workflow_run.get_task_status("c") == TaskStatus.READY
    assert workflow_run.ready_tasks() == ("b", "c")


def test_unknown_task_lookup_is_rejected() -> None:
    workflow_run = run(task("a"))

    with pytest.raises(UnknownTaskRunError, match="does not contain task 'missing'"):
        workflow_run.get_task_status("missing")


def test_workflow_succeeds_when_all_tasks_succeed() -> None:
    workflow_run = run(task("a"), task("b", ("a",)))

    workflow_run.start_task("a")
    workflow_run.complete_task("a")
    workflow_run.start_task("b")
    workflow_run.complete_task("b")

    assert workflow_run.status == WorkflowStatus.SUCCEEDED


def test_workflow_fails_when_a_task_fails() -> None:
    workflow_run = run(task("a"), task("b", ("a",)))

    workflow_run.start_task("a")
    workflow_run.fail_task("a")

    assert workflow_run.status == WorkflowStatus.FAILED
    assert workflow_run.get_task_status("b") == TaskStatus.BLOCKED


def test_workflow_cancellation() -> None:
    workflow_run = run(task("a"))

    workflow_run.cancel_workflow()

    assert workflow_run.status == WorkflowStatus.CANCELLED
    assert workflow_run.get_task_status("a") == TaskStatus.CANCELLED


def test_cancelling_workflow_preserves_succeeded_tasks() -> None:
    workflow_run = run(task("a"), task("b"))

    workflow_run.start_task("a")
    workflow_run.complete_task("a")
    workflow_run.cancel_workflow()

    assert workflow_run.get_task_status("a") == TaskStatus.SUCCEEDED


def test_cancelling_workflow_cancels_non_terminal_tasks() -> None:
    workflow_run = run(task("a"), task("b"), task("c", ("a",)))

    workflow_run.start_task("a")
    workflow_run.cancel_workflow()

    assert workflow_run.get_task_status("a") == TaskStatus.CANCELLED
    assert workflow_run.get_task_status("b") == TaskStatus.CANCELLED
    assert workflow_run.get_task_status("c") == TaskStatus.CANCELLED


def test_repeated_workflow_cancellation_is_idempotent() -> None:
    workflow_run = run(task("a"))

    workflow_run.cancel_workflow()
    workflow_run.cancel_workflow()

    assert workflow_run.status == WorkflowStatus.CANCELLED
    assert workflow_run.get_task_status("a") == TaskStatus.CANCELLED


def test_multiple_roots() -> None:
    workflow_run = run(task("a"), task("b"), task("c", ("a", "b")))

    assert workflow_run.ready_tasks() == ("a", "b")


def test_readiness_after_successive_completions() -> None:
    workflow_run = run(task("a"), task("b", ("a",)), task("c", ("b",)))

    workflow_run.start_task("a")
    workflow_run.complete_task("a")
    assert workflow_run.ready_tasks() == ("b",)

    workflow_run.start_task("b")
    workflow_run.complete_task("b")
    assert workflow_run.ready_tasks() == ("c",)


def test_separate_workflow_runs_are_independent() -> None:
    definition = workflow(task("a"), task("b", ("a",)))
    first_run = WorkflowRun.create("run-1", definition)
    second_run = WorkflowRun.create("run-2", definition)

    first_run.start_task("a")
    first_run.complete_task("a")

    assert first_run.get_task_status("b") == TaskStatus.READY
    assert second_run.get_task_status("a") == TaskStatus.READY
    assert second_run.get_task_status("b") == TaskStatus.BLOCKED


def test_operations_after_terminal_workflow_fail() -> None:
    workflow_run = run(task("a"))

    workflow_run.start_task("a")
    workflow_run.fail_task("a")

    with pytest.raises(WorkflowAlreadyTerminalError, match="already terminal"):
        workflow_run.start_task("a")
