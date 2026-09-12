import pytest

from app.engine.execution import WorkflowRun
from app.schemas.workflow import TaskDefinition, WorkflowDefinition

pytestmark = pytest.mark.stress


def test_wide_dag_preserves_all_ready_tasks() -> None:
    workflow = WorkflowDefinition(
        id="stress-wide",
        name="Wide",
        tasks=tuple(TaskDefinition(id=f"task-{index:04d}") for index in range(500)),
    )

    run = WorkflowRun.create("run-wide", workflow)

    assert len(run.ready_tasks()) == 500


def test_deep_dag_unlocks_in_deterministic_order() -> None:
    tasks = [TaskDefinition(id="task-0000")]
    tasks.extend(
        TaskDefinition(id=f"task-{index:04d}", depends_on=(f"task-{index - 1:04d}",))
        for index in range(1, 250)
    )
    workflow = WorkflowDefinition(id="stress-deep", name="Deep", tasks=tuple(tasks))
    run = WorkflowRun.create("run-deep", workflow)

    for index in range(250):
        task_id = f"task-{index:04d}"
        assert run.ready_tasks() == (task_id,)
        run.start_task(task_id)
        run.complete_task(task_id)

    assert run.status.value == "SUCCEEDED"


def test_fan_out_and_fan_in_requires_every_child() -> None:
    children = tuple(f"child-{index:03d}" for index in range(200))
    workflow = WorkflowDefinition(
        id="stress-fanout",
        name="Fan out",
        tasks=(
            TaskDefinition(id="root"),
            *(TaskDefinition(id=task_id, depends_on=("root",)) for task_id in children),
            TaskDefinition(id="join", depends_on=children),
        ),
    )
    run = WorkflowRun.create("run-fanout", workflow)

    run.start_task("root")
    run.complete_task("root")
    assert run.ready_tasks() == children
    for task_id in children:
        run.start_task(task_id)
        run.complete_task(task_id)

    assert run.ready_tasks() == ("join",)
    run.start_task("join")
    run.complete_task("join")
    assert run.status.value == "SUCCEEDED"


def test_multiple_wide_workflows_keep_independent_task_state() -> None:
    for workflow_index in range(20):
        workflow = WorkflowDefinition(
            id=f"stress-workflow-{workflow_index:02d}",
            name="Independent",
            tasks=tuple(
                TaskDefinition(id=f"task-{task_index:02d}")
                for task_index in range(50)
            ),
        )
        run = WorkflowRun.create(f"run-{workflow_index:02d}", workflow)

        assert len(run.ready_tasks()) == 50
