"""Deterministic structural comparison for immutable workflow revisions."""

from typing import Any

from app.schemas.api import TaskRevisionDiff, WorkflowRevisionDiff
from app.schemas.workflow import TaskDefinition, WorkflowDefinition


def compare_workflow_revisions(
    before: WorkflowDefinition,
    after: WorkflowDefinition,
) -> WorkflowRevisionDiff:
    """Compare execution-relevant definition fields without object-identity leaks."""
    before_tasks = {task.id: task for task in before.tasks}
    after_tasks = {task.id: task for task in after.tasks}
    shared_ids = sorted(before_tasks.keys() & after_tasks.keys())
    modified = tuple(
        diff
        for task_id in shared_ids
        if (diff := _task_diff(before_tasks[task_id], after_tasks[task_id])) is not None
    )
    workflow_changes: dict[str, dict[str, Any]] = {}
    if before.name != after.name:
        workflow_changes["name"] = {"before": before.name, "after": after.name}
    return WorkflowRevisionDiff(
        workflow_id=before.id,
        from_revision=before.revision,
        to_revision=after.revision,
        workflow_changes=workflow_changes,
        added_tasks=tuple(sorted(after_tasks.keys() - before_tasks.keys())),
        removed_tasks=tuple(sorted(before_tasks.keys() - after_tasks.keys())),
        modified_tasks=modified,
    )


def _task_diff(
    before: TaskDefinition, after: TaskDefinition
) -> TaskRevisionDiff | None:
    before_values = _task_values(before)
    after_values = _task_values(after)
    changed_fields = tuple(
        field
        for field in sorted(before_values)
        if before_values[field] != after_values[field]
    )
    if not changed_fields:
        return None
    return TaskRevisionDiff(
        task_id=before.id,
        changed_fields=changed_fields,
        before={field: before_values[field] for field in changed_fields},
        after={field: after_values[field] for field in changed_fields},
    )


def _task_values(task: TaskDefinition) -> dict[str, Any]:
    return {
        "dependencies": tuple(sorted(task.depends_on)),
        "name": task.name,
        "parameters": _canonical(task.parameters),
        "retry_policy": _canonical(task.retry_policy.model_dump(mode="json")),
    }


def _canonical(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return _canonical(value.model_dump(mode="json"))
    if isinstance(value, dict):
        return {key: _canonical(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return tuple(_canonical(item) for item in value)
    return value
