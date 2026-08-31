import pytest

from app.engine.dag import WorkflowDAG
from app.engine.exceptions import (
    CycleDetectedError,
    DuplicateDependencyError,
    DuplicateTaskError,
    EmptyWorkflowError,
    SelfDependencyError,
    UnknownDependencyError,
    UnknownTaskError,
)
from app.schemas.workflow import TaskDefinition, WorkflowDefinition


def workflow(*tasks: TaskDefinition) -> WorkflowDefinition:
    return WorkflowDefinition(id="workflow", name="Test workflow", tasks=tasks)


def task(task_id: str, depends_on: tuple[str, ...] = ()) -> TaskDefinition:
    return TaskDefinition(id=task_id, depends_on=depends_on)


def test_single_task_workflow() -> None:
    dag = WorkflowDAG(workflow(task("a")))

    assert dag.task_count == 1
    assert len(dag) == 1
    assert "a" in dag
    assert dag.roots == ("a",)
    assert dag.leaves == ("a",)
    assert dag.topological_order() == ("a",)


def test_linear_dag() -> None:
    dag = WorkflowDAG(workflow(task("a"), task("b", ("a",)), task("c", ("b",))))

    assert dag.roots == ("a",)
    assert dag.leaves == ("c",)
    assert dag.topological_order() == ("a", "b", "c")


def test_branching_dag() -> None:
    dag = WorkflowDAG(
        workflow(task("fetch"), task("notify", ("fetch",)), task("store", ("fetch",)))
    )

    assert dag.roots == ("fetch",)
    assert dag.leaves == ("notify", "store")
    assert dag.dependents_of("fetch") == ("notify", "store")
    assert dag.topological_order() == ("fetch", "notify", "store")


def test_diamond_dag() -> None:
    dag = WorkflowDAG(
        workflow(
            task("a"),
            task("b", ("a",)),
            task("c", ("a",)),
            task("d", ("b", "c")),
        )
    )

    assert dag.roots == ("a",)
    assert dag.leaves == ("d",)
    assert dag.dependencies_of("d") == ("b", "c")
    assert dag.topological_order() == ("a", "b", "c", "d")


def test_disconnected_components_are_valid() -> None:
    dag = WorkflowDAG(
        workflow(task("a"), task("b", ("a",)), task("c"), task("d", ("c",)))
    )

    assert dag.roots == ("a", "c")
    assert dag.leaves == ("b", "d")
    assert dag.topological_order() == ("a", "b", "c", "d")


def test_duplicate_task_ids_are_rejected() -> None:
    with pytest.raises(DuplicateTaskError, match="duplicate task id 'a'"):
        WorkflowDAG(workflow(task("a"), task("a")))


def test_unknown_dependencies_are_rejected() -> None:
    with pytest.raises(
        UnknownDependencyError,
        match="Task 'b' depends on unknown task 'missing'",
    ):
        WorkflowDAG(workflow(task("a"), task("b", ("missing",))))


def test_self_dependencies_are_rejected() -> None:
    with pytest.raises(SelfDependencyError, match="Task 'a' cannot depend on itself"):
        WorkflowDAG(workflow(task("a", ("a",))))


def test_duplicate_dependencies_are_rejected() -> None:
    with pytest.raises(
        DuplicateDependencyError,
        match="Task 'b' contains duplicate dependency 'a'",
    ):
        WorkflowDAG(workflow(task("a"), task("b", ("a", "a"))))


def test_simple_two_node_cycle_is_rejected() -> None:
    with pytest.raises(CycleDetectedError, match="a, b"):
        WorkflowDAG(workflow(task("a", ("b",)), task("b", ("a",))))


def test_longer_cycle_is_rejected() -> None:
    with pytest.raises(CycleDetectedError, match="a, b, c"):
        WorkflowDAG(
            workflow(task("a", ("c",)), task("b", ("a",)), task("c", ("b",)))
        )


def test_roots_detection() -> None:
    dag = WorkflowDAG(workflow(task("a"), task("b"), task("c", ("a",))))

    assert dag.roots == ("a", "b")


def test_leaves_detection() -> None:
    dag = WorkflowDAG(workflow(task("a"), task("b", ("a",)), task("c", ("a",))))

    assert dag.leaves == ("b", "c")


def test_dependencies_of() -> None:
    dag = WorkflowDAG(workflow(task("a"), task("b"), task("c", ("b", "a"))))

    assert dag.dependencies_of("c") == ("a", "b")


def test_dependents_of() -> None:
    dag = WorkflowDAG(workflow(task("a"), task("b", ("a",)), task("c", ("a",))))

    assert dag.dependents_of("a") == ("b", "c")


def test_topological_order_is_deterministic() -> None:
    dag = WorkflowDAG(
        workflow(task("d", ("b",)), task("c", ("a",)), task("b"), task("a"))
    )

    assert dag.topological_order() == ("a", "b", "c", "d")


def test_relationship_lookup_for_unknown_task_is_rejected() -> None:
    dag = WorkflowDAG(workflow(task("a")))

    with pytest.raises(UnknownTaskError, match="does not contain task 'missing'"):
        dag.dependencies_of("missing")

    with pytest.raises(UnknownTaskError, match="does not contain task 'missing'"):
        dag.dependents_of("missing")


def test_empty_workflow_is_rejected() -> None:
    empty_workflow = WorkflowDefinition(id="empty", name="Empty workflow", tasks=())

    with pytest.raises(EmptyWorkflowError, match="must define at least one task"):
        WorkflowDAG(empty_workflow)
