from app.schemas.workflow import (
    LiteralParameter,
    RetryPolicy,
    TaskDefinition,
    WorkflowDefinition,
)
from app.services.revision_diff import compare_workflow_revisions


def workflow(
    *, name: str = "Workflow", tasks: tuple[TaskDefinition, ...]
) -> WorkflowDefinition:
    return WorkflowDefinition(id="wf", name=name, tasks=tasks)


def test_revision_diff_is_deterministic_and_semantic() -> None:
    before = workflow(
        tasks=(
            TaskDefinition(id="b"),
            TaskDefinition(
                id="a",
                retry_policy=RetryPolicy(max_attempts=2),
                parameters={"z": LiteralParameter(source="literal", value=1)},
            ),
        )
    ).model_copy(update={"revision": 1})
    after = workflow(
        name="Workflow v2",
        tasks=(
            TaskDefinition(id="c"),
            TaskDefinition(
                id="a",
                name="A v2",
                depends_on=("c",),
                retry_policy=RetryPolicy(max_attempts=3),
                parameters={
                    "a": LiteralParameter(source="literal", value={"mode": "safe"}),
                    "z": LiteralParameter(source="literal", value=2),
                },
            ),
        ),
    ).model_copy(update={"revision": 2})

    diff = compare_workflow_revisions(before, after)

    assert diff.workflow_changes == {
        "name": {"before": "Workflow", "after": "Workflow v2"}
    }
    assert diff.added_tasks == ("c",)
    assert diff.removed_tasks == ("b",)
    assert diff.modified_tasks[0].task_id == "a"
    assert diff.modified_tasks[0].changed_fields == (
        "dependencies",
        "name",
        "parameters",
        "retry_policy",
    )


def test_identical_revision_diff_is_empty_and_reverse_is_directional() -> None:
    original = workflow(tasks=(TaskDefinition(id="a"),)).model_copy(
        update={"revision": 1}
    )
    renamed = original.model_copy(update={"revision": 2, "name": "Renamed"})

    empty = compare_workflow_revisions(original, original)
    reverse = compare_workflow_revisions(renamed, original)

    assert not empty.workflow_changes
    assert empty.added_tasks == empty.removed_tasks == empty.modified_tasks == ()
    assert reverse.workflow_changes["name"] == {
        "before": "Renamed",
        "after": "Workflow",
    }
