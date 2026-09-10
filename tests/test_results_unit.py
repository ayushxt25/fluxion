import asyncio
from dataclasses import FrozenInstanceError

import pytest

from app.engine.context import TaskExecutionContext
from app.engine.exceptions import TaskResultValidationError
from app.engine.results import normalize_task_result
from app.engine.status import WorkflowStatus
from app.schemas.workflow import RetryPolicy, TaskDefinition, WorkflowDefinition
from app.services.execution import (
    PersistentWorkflowExecutor,
    _InMemoryTaskAttemptRepository,
)


class FakeWorkflowRepository:
    async def exists(self, workflow_id: str) -> bool:
        return True


class RecordingRunRepository:
    async def create(self, workflow_run) -> None:
        self.workflow_run = workflow_run

    async def save_state(self, workflow_run) -> None:
        self.workflow_run = workflow_run


def task(
    task_id: str,
    depends_on: tuple[str, ...] = (),
    retry_policy: RetryPolicy | None = None,
) -> TaskDefinition:
    return TaskDefinition(
        id=task_id,
        depends_on=depends_on,
        retry_policy=retry_policy or RetryPolicy(),
    )


def workflow(*tasks: TaskDefinition) -> WorkflowDefinition:
    return WorkflowDefinition(id="workflow", name="Workflow", tasks=tasks)


def execute(definition, implementations):
    run_repository = RecordingRunRepository()
    result = asyncio.run(
        PersistentWorkflowExecutor(
            definition,
            implementations,
            FakeWorkflowRepository(),
            run_repository,
            _InMemoryTaskAttemptRepository(run_repository),
            run_id="run-1",
        ).run()
    )
    return result, run_repository.workflow_run


def test_root_context_has_empty_dependency_results() -> None:
    observed = []

    def root(context: TaskExecutionContext) -> dict[str, int]:
        observed.append(context.dependency_results)
        return {"value": 1}

    result, _ = execute(workflow(task("a")), {"a": root})

    assert result.status == WorkflowStatus.SUCCEEDED
    assert dict(observed[0]) == {}


def test_simple_upstream_result_reaches_downstream() -> None:
    observed = []

    def upstream() -> dict[str, int]:
        return {"value": 21}

    def downstream(context: TaskExecutionContext) -> dict[str, int]:
        observed.append(context.dependency_results["a"])
        return {"value": context.dependency_results["a"]["value"] * 2}

    result, run = execute(
        workflow(task("a"), task("b", ("a",))),
        {"a": upstream, "b": downstream},
    )

    assert result.status == WorkflowStatus.SUCCEEDED
    assert observed == [{"value": 21}]
    assert run.task_runs["b"].result == {"value": 42}


def test_multiple_dependency_results_reach_join_task() -> None:
    observed = {}

    def join(context: TaskExecutionContext) -> list[int]:
        observed.update(context.dependency_results)
        return [
            context.dependency_results["a"],
            context.dependency_results["b"],
        ]

    result, _ = execute(
        workflow(task("a"), task("b"), task("c", ("a", "b"))),
        {"a": lambda: 1, "b": lambda: 2, "c": join},
    )

    assert result.status == WorkflowStatus.SUCCEEDED
    assert observed == {"a": 1, "b": 2}


def test_dependency_results_mapping_is_immutable() -> None:
    context = TaskExecutionContext(
        workflow_id="workflow",
        run_id="run-1",
        task_id="b",
        attempt_number=1,
        attempt_key="run-1:b:1",
        idempotency_key="run-1:b",
        dependency_results={"a": {"value": 1}},
    )

    with pytest.raises(TypeError):
        context.dependency_results["x"] = 2
    with pytest.raises(FrozenInstanceError):
        context.dependency_results = {}


def test_json_result_validation_accepts_supported_values() -> None:
    values = (
        {"a": [1, "x", True, None]},
        [1, 2],
        "value",
        3,
        1.5,
        True,
        None,
    )

    assert [normalize_task_result(value, 1024) for value in values] == list(values)


def test_non_json_result_is_rejected_safely() -> None:
    with pytest.raises(TaskResultValidationError):
        normalize_task_result({"bad": object()}, 1024)


def test_oversized_result_is_rejected() -> None:
    with pytest.raises(TaskResultValidationError):
        normalize_task_result("x" * 20, 8)


def test_none_result_is_distinguished_from_missing_result() -> None:
    result, run = execute(workflow(task("a")), {"a": lambda: None})

    assert result.status == WorkflowStatus.SUCCEEDED
    assert run.task_runs["a"].result is None
    assert run.task_runs["a"].result_present is True


def test_final_successful_retry_writes_canonical_result() -> None:
    calls = 0

    def flaky() -> dict[str, int]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("boom")
        return {"value": 2}

    result, run = execute(
        workflow(task("a", retry_policy=RetryPolicy(max_attempts=2))),
        {"a": flaky},
    )

    assert result.status == WorkflowStatus.SUCCEEDED
    assert run.task_runs["a"].result == {"value": 2}
    assert run.task_runs["a"].result_present is True
