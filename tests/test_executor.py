import asyncio
from dataclasses import FrozenInstanceError
from types import MappingProxyType

import pytest

from app.engine.dag import WorkflowDAG
from app.engine.exceptions import (
    DuplicateTaskImplementationError,
    InvalidConcurrencyLimitError,
    MissingTaskImplementationError,
)
from app.engine.executor import WorkflowExecutionResult, WorkflowExecutor
from app.engine.registry import TaskRegistry
from app.engine.status import TaskStatus, WorkflowStatus
from app.schemas.workflow import TaskDefinition, WorkflowDefinition


def task(task_id: str, depends_on: tuple[str, ...] = ()) -> TaskDefinition:
    return TaskDefinition(id=task_id, depends_on=depends_on)


def workflow(*tasks: TaskDefinition) -> WorkflowDefinition:
    return WorkflowDefinition(id="workflow", name="Workflow", tasks=tasks)


def execute(
    definition: WorkflowDefinition,
    implementations: dict[str, object],
    *,
    max_concurrency: int | None = None,
):
    return asyncio.run(
        WorkflowExecutor(
            definition,
            implementations,
            max_concurrency=max_concurrency,
            run_id="run-1",
        ).run()
    )


def recorders(calls: list[str], task_ids: str) -> dict[str, object]:
    return {
        task_id: lambda task_id=task_id: calls.append(task_id)
        for task_id in task_ids
    }


def test_single_task_executes_successfully() -> None:
    calls: list[str] = []
    definition = workflow(task("a"))

    result = execute(definition, {"a": lambda: calls.append("a")})

    assert calls == ["a"]
    assert result.status == WorkflowStatus.SUCCEEDED
    assert result.task_statuses["a"] == TaskStatus.SUCCEEDED


def test_linear_dag_executes_in_dependency_order() -> None:
    calls: list[str] = []
    definition = workflow(task("a"), task("b", ("a",)), task("c", ("b",)))

    result = execute(definition, recorders(calls, "abc"))

    assert calls == ["a", "b", "c"]
    assert result.status == WorkflowStatus.SUCCEEDED


def test_branching_tasks_execute_after_parent_success() -> None:
    calls: list[str] = []
    definition = workflow(task("a"), task("b", ("a",)), task("c", ("a",)))

    result = execute(definition, recorders(calls, "abc"))

    assert calls[0] == "a"
    assert set(calls[1:]) == {"b", "c"}
    assert result.status == WorkflowStatus.SUCCEEDED


def test_diamond_join_executes_after_both_parents() -> None:
    calls: list[str] = []
    definition = workflow(
        task("a"), task("b", ("a",)), task("c", ("a",)), task("d", ("b", "c"))
    )

    result = execute(definition, recorders(calls, "abcd"))

    assert calls[0] == "a"
    assert calls[-1] == "d"
    assert result.status == WorkflowStatus.SUCCEEDED


def test_disconnected_components_execute_correctly() -> None:
    calls: list[str] = []
    definition = workflow(task("a"), task("b", ("a",)), task("c"), task("d", ("c",)))

    result = execute(definition, recorders(calls, "abcd"))

    assert calls == ["a", "c", "b", "d"]
    assert result.status == WorkflowStatus.SUCCEEDED


def test_multiple_roots_execute() -> None:
    calls: list[str] = []
    definition = workflow(task("a"), task("b"), task("c"))

    result = execute(definition, recorders(calls, "abc"))

    assert calls == ["a", "b", "c"]
    assert result.status == WorkflowStatus.SUCCEEDED


def test_async_task_callable_support() -> None:
    calls: list[str] = []

    async def task_a() -> None:
        calls.append("a")

    result = execute(workflow(task("a")), {"a": task_a})

    assert calls == ["a"]
    assert result.status == WorkflowStatus.SUCCEEDED


def test_sync_task_callable_support() -> None:
    calls: list[str] = []

    def task_a() -> None:
        calls.append("a")

    result = execute(workflow(task("a")), {"a": task_a})

    assert calls == ["a"]
    assert result.status == WorkflowStatus.SUCCEEDED


def test_all_workflow_tasks_require_implementations() -> None:
    definition = workflow(task("a"), task("b"))

    with pytest.raises(MissingTaskImplementationError, match="'b'"):
        asyncio.run(WorkflowExecutor(definition, {"a": lambda: None}).run())


def test_missing_implementation_is_detected_before_any_task_executes() -> None:
    calls: list[str] = []
    definition = workflow(task("a"), task("b"))

    with pytest.raises(MissingTaskImplementationError):
        asyncio.run(
            WorkflowExecutor(definition, {"a": lambda: calls.append("a")}).run()
        )

    assert calls == []


def test_duplicate_registration_is_rejected() -> None:
    registry = TaskRegistry({"a": lambda: None})

    with pytest.raises(DuplicateTaskImplementationError, match="'a'"):
        registry.register("a", lambda: None)


def test_task_exception_marks_task_failed() -> None:
    def fail() -> None:
        raise RuntimeError("boom")

    result = execute(workflow(task("a")), {"a": fail})

    assert result.status == WorkflowStatus.FAILED
    assert result.task_statuses["a"] == TaskStatus.FAILED
    assert result.errors == {"a": "RuntimeError: boom"}


def test_execution_result_snapshot_is_immutable() -> None:
    result = execute(workflow(task("a")), {"a": lambda: None})

    with pytest.raises(FrozenInstanceError):
        result.status = WorkflowStatus.FAILED

    with pytest.raises(TypeError):
        result.task_statuses["a"] = TaskStatus.FAILED

    with pytest.raises(TypeError):
        result.errors["a"] = "RuntimeError: boom"


def test_execution_result_uses_defensive_mapping_snapshots() -> None:
    task_statuses = {"a": TaskStatus.SUCCEEDED}
    errors = {"a": "RuntimeError: original"}
    result = WorkflowExecutionResult(
        run_id="run-1",
        workflow_id="workflow",
        status=WorkflowStatus.SUCCEEDED,
        task_statuses=MappingProxyType(dict(task_statuses)),
        errors=MappingProxyType(dict(errors)),
    )

    task_statuses["a"] = TaskStatus.FAILED
    errors["a"] = "RuntimeError: changed"

    assert result.task_statuses["a"] == TaskStatus.SUCCEEDED
    assert result.errors["a"] == "RuntimeError: original"


def test_failed_dependency_prevents_dependent_execution() -> None:
    calls: list[str] = []

    def fail() -> None:
        raise RuntimeError("boom")

    result = execute(
        workflow(task("a"), task("b", ("a",))),
        {"a": fail, "b": lambda: calls.append("b")},
    )

    assert calls == []
    assert result.task_statuses["b"] == TaskStatus.BLOCKED


def test_workflow_final_status_succeeded() -> None:
    result = execute(workflow(task("a")), {"a": lambda: None})

    assert result.status == WorkflowStatus.SUCCEEDED


def test_workflow_final_status_failed() -> None:
    def fail() -> None:
        raise RuntimeError("x")

    result = execute(workflow(task("a")), {"a": fail})

    assert result.status == WorkflowStatus.FAILED


def test_independent_ready_tasks_execute_concurrently() -> None:
    async def scenario():
        both_started = asyncio.Event()
        active = 0
        max_seen = 0

        async def make_task():
            nonlocal active, max_seen
            active += 1
            max_seen = max(max_seen, active)
            if active == 2:
                both_started.set()
            await both_started.wait()
            active -= 1

        definition = workflow(task("a"), task("b"))
        result = await WorkflowExecutor(
            definition,
            {"a": make_task, "b": make_task},
        ).run()
        return result, max_seen

    result, max_seen = asyncio.run(scenario())

    assert max_seen == 2
    assert result.status == WorkflowStatus.SUCCEEDED


def test_concurrency_limit_one_serializes_independent_tasks() -> None:
    async def scenario():
        active = 0
        max_seen = 0

        async def make_task():
            nonlocal active, max_seen
            active += 1
            max_seen = max(max_seen, active)
            await asyncio.sleep(0)
            active -= 1

        definition = workflow(task("a"), task("b"))
        result = await WorkflowExecutor(
            definition,
            {"a": make_task, "b": make_task},
            max_concurrency=1,
        ).run()
        return result, max_seen

    result, max_seen = asyncio.run(scenario())

    assert max_seen == 1
    assert result.status == WorkflowStatus.SUCCEEDED


def test_concurrency_limit_one_admits_only_one_task_at_a_time() -> None:
    async def scenario():
        first_started = asyncio.Event()
        release_first = asyncio.Event()
        calls: list[str] = []

        async def first_task() -> None:
            calls.append("a")
            first_started.set()
            await release_first.wait()

        async def second_task() -> None:
            calls.append("b")

        definition = workflow(task("a"), task("b"))
        executor_task = asyncio.create_task(
            WorkflowExecutor(
                definition,
                {"a": first_task, "b": second_task},
                max_concurrency=1,
            ).run()
        )
        await first_started.wait()
        assert calls == ["a"]

        release_first.set()
        result = await executor_task
        return result, calls

    result, calls = asyncio.run(scenario())

    assert calls == ["a", "b"]
    assert result.status == WorkflowStatus.SUCCEEDED


def test_concurrency_limit_two_never_exceeds_two() -> None:
    async def scenario():
        active = 0
        max_seen = 0

        async def make_task():
            nonlocal active, max_seen
            active += 1
            max_seen = max(max_seen, active)
            await asyncio.sleep(0)
            active -= 1

        definition = workflow(task("a"), task("b"), task("c"))
        result = await WorkflowExecutor(
            definition,
            {task_id: make_task for task_id in "abc"},
            max_concurrency=2,
        ).run()
        return result, max_seen

    result, max_seen = asyncio.run(scenario())

    assert max_seen <= 2
    assert result.status == WorkflowStatus.SUCCEEDED


def test_invalid_concurrency_limit_rejected() -> None:
    with pytest.raises(InvalidConcurrencyLimitError):
        WorkflowExecutor(workflow(task("a")), {"a": lambda: None}, max_concurrency=0)


def test_deterministic_execution_scheduling_for_serial_roots() -> None:
    calls: list[str] = []
    definition = workflow(task("c"), task("a"), task("b"))

    execute(
        definition,
        {task_id: lambda task_id=task_id: calls.append(task_id) for task_id in "abc"},
        max_concurrency=1,
    )

    assert calls == ["a", "b", "c"]


def test_separate_executor_runs_remain_isolated() -> None:
    definition = workflow(task("a"))
    first = execute(definition, {"a": lambda: None}, max_concurrency=1)
    second = execute(definition, {"a": lambda: None}, max_concurrency=1)

    assert first.run_id == "run-1"
    assert second.run_id == "run-1"
    assert first.task_statuses is not second.task_statuses


def test_executor_does_not_mutate_workflow_definition() -> None:
    definition = workflow(task("a"))
    before = definition.model_dump()

    execute(definition, {"a": lambda: None})

    assert definition.model_dump() == before


def test_executor_does_not_mutate_dag() -> None:
    definition = workflow(task("a"), task("b", ("a",)))
    dag = WorkflowDAG(definition)
    before = (dag.roots, dag.leaves, dag.topological_order())

    asyncio.run(
        WorkflowExecutor(
            definition,
            {"a": lambda: None, "b": lambda: None},
            dag=dag,
        ).run()
    )

    assert (dag.roots, dag.leaves, dag.topological_order()) == before


def test_callable_return_values_are_not_dependency_inputs() -> None:
    seen_arguments: list[object] = []

    def task_a() -> str:
        return "ignored"

    def task_b() -> None:
        seen_arguments.append("called without inputs")

    result = execute(workflow(task("a"), task("b", ("a",))), {"a": task_a, "b": task_b})

    assert seen_arguments == ["called without inputs"]
    assert result.status == WorkflowStatus.SUCCEEDED


def test_running_independent_tasks_may_finish_after_concurrent_failure() -> None:
    async def scenario():
        both_started = asyncio.Event()
        active = 0
        calls: list[str] = []

        async def fail_fast() -> None:
            nonlocal active
            calls.append("a")
            active += 1
            if active == 2:
                both_started.set()
            await both_started.wait()
            raise RuntimeError("boom")

        async def finish_after_failure() -> None:
            nonlocal active
            calls.append("b")
            active += 1
            if active == 2:
                both_started.set()
            await both_started.wait()

        definition = workflow(task("a"), task("b"))
        result = await WorkflowExecutor(
            definition,
            {"a": fail_fast, "b": finish_after_failure},
        ).run()
        return result, calls

    result, calls = asyncio.run(scenario())

    assert calls == ["a", "b"]
    assert result.status == WorkflowStatus.FAILED
    assert result.task_statuses["a"] == TaskStatus.FAILED
    assert result.task_statuses["b"] == TaskStatus.SUCCEEDED


def test_success_after_concurrent_failure_does_not_launch_downstream_task() -> None:
    async def scenario():
        both_started = asyncio.Event()
        active = 0
        calls: list[str] = []

        async def fail_root() -> None:
            nonlocal active
            calls.append("b")
            active += 1
            if active == 2:
                both_started.set()
            await both_started.wait()
            raise RuntimeError("boom")

        async def succeed_root() -> None:
            nonlocal active
            calls.append("c")
            active += 1
            if active == 2:
                both_started.set()
            await both_started.wait()

        definition = workflow(task("b"), task("c"), task("d", ("c",)))
        result = await WorkflowExecutor(
            definition,
            {"b": fail_root, "c": succeed_root, "d": lambda: calls.append("d")},
        ).run()
        return result, calls

    result, calls = asyncio.run(scenario())

    assert calls == ["b", "c"]
    assert result.status == WorkflowStatus.FAILED
    assert result.task_statuses["b"] == TaskStatus.FAILED
    assert result.task_statuses["c"] == TaskStatus.SUCCEEDED
    assert result.task_statuses["d"] == TaskStatus.BLOCKED


def test_no_new_tasks_launch_once_failure_is_known() -> None:
    calls: list[str] = []

    def fail() -> None:
        calls.append("a")
        raise RuntimeError("boom")

    result = execute(
        workflow(task("a"), task("b"), task("c", ("b",))),
        {"a": fail, "b": lambda: calls.append("b"), "c": lambda: calls.append("c")},
        max_concurrency=1,
    )

    assert calls == ["a"]
    assert result.status == WorkflowStatus.FAILED
    assert result.task_statuses["b"] == TaskStatus.READY
    assert result.task_statuses["c"] == TaskStatus.BLOCKED


def test_each_task_executes_at_most_once() -> None:
    counts = {"a": 0, "b": 0, "c": 0}

    def increment(task_id: str) -> None:
        counts[task_id] += 1

    result = execute(
        workflow(task("a"), task("b", ("a",)), task("c", ("a",))),
        {task_id: lambda task_id=task_id: increment(task_id) for task_id in counts},
    )

    assert result.status == WorkflowStatus.SUCCEEDED
    assert counts == {"a": 1, "b": 1, "c": 1}
