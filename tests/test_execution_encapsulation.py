from dataclasses import FrozenInstanceError

import pytest

from app.engine.execution import WorkflowRun
from app.engine.status import TaskStatus
from app.schemas.workflow import TaskDefinition, WorkflowDefinition


def test_task_run_snapshots_cannot_mutate_workflow_run_state() -> None:
    definition = WorkflowDefinition(
        id="workflow",
        name="Workflow",
        tasks=(TaskDefinition(id="a"),),
    )
    workflow_run = WorkflowRun.create("run-1", definition)
    task_run = workflow_run.task_runs["a"]

    with pytest.raises(FrozenInstanceError):
        task_run.status = TaskStatus.SUCCEEDED

    assert workflow_run.get_task_status("a") == TaskStatus.READY
