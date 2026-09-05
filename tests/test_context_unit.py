import asyncio
from dataclasses import FrozenInstanceError
from functools import partial

import pytest

from app.engine.context import TaskExecutionContext
from app.engine.exceptions import InvalidTaskCallableError
from app.engine.status import WorkflowStatus
from app.schemas.workflow import RetryPolicy, TaskDefinition, WorkflowDefinition
from app.services.execution import (
    PersistentWorkflowExecutor,
    _InMemoryTaskAttemptRepository,
)


def task(
    task_id: str,
    retry_policy: RetryPolicy | None = None,
) -> TaskDefinition:
    return TaskDefinition(
        id=task_id,
        retry_policy=retry_policy or RetryPolicy(),
    )


def workflow(*tasks: TaskDefinition) -> WorkflowDefinition:
    return WorkflowDefinition(id="workflow", name="Workflow", tasks=tasks)


class FakeWorkflowRepository:
    async def exists(self, workflow_id: str) -> bool:
        return True


class RecordingRunRepository:
    async def create(self, workflow_run) -> None:
        self.workflow_run = workflow_run

    async def save_state(self, workflow_run) -> None:
        self.workflow_run = workflow_run


def execute(definition, implementations, attempts=None):
    return asyncio.run(
        PersistentWorkflowExecutor(
            definition,
            implementations,
            FakeWorkflowRepository(),
            RecordingRunRepository(),
            attempts or _InMemoryTaskAttemptRepository(),
            run_id="run-1",
        ).run()
    )


def test_task_execution_context_is_immutable() -> None:
    context = TaskExecutionContext(
        workflow_id="workflow",
        run_id="run-1",
        task_id="a",
        attempt_number=1,
        attempt_key="run-1:a:1",
        idempotency_key="run-1:a",
    )

    with pytest.raises(FrozenInstanceError):
        context.run_id = "other"


def test_context_async_task_receives_deterministic_identity() -> None:
    observed = []

    async def task_a(context: TaskExecutionContext) -> None:
        observed.append(context)

    result = execute(workflow(task("a")), {"a": task_a})

    assert result.status == WorkflowStatus.SUCCEEDED
    assert observed[0] == TaskExecutionContext(
        workflow_id="workflow",
        run_id="run-1",
        task_id="a",
        attempt_number=1,
        attempt_key="run-1:a:1",
        idempotency_key="run-1:a",
    )


def test_context_sync_task_receives_deterministic_identity() -> None:
    observed = []

    def task_a(context: TaskExecutionContext) -> None:
        observed.append(context.idempotency_key)

    execute(workflow(task("a")), {"a": task_a})

    assert observed == ["run-1:a"]


def test_lambda_captured_default_executes_as_zero_argument_task() -> None:
    calls = []
    implementations = {
        task_id: (lambda task_id=task_id: calls.append(task_id))
        for task_id in ("a", "b")
    }

    execute(workflow(task("a"), task("b")), implementations)

    assert sorted(calls) == ["a", "b"]


def test_optional_positional_parameter_executes_with_zero_arguments() -> None:
    observed = []

    def task_a(context=None) -> None:
        observed.append(context)

    execute(workflow(task("a")), {"a": task_a})

    assert observed == [None]


def test_context_exposes_no_database_or_repository_handles() -> None:
    context = TaskExecutionContext(
        workflow_id="workflow",
        run_id="run-1",
        task_id="a",
        attempt_number=1,
        attempt_key="run-1:a:1",
        idempotency_key="run-1:a",
    )

    assert not hasattr(context, "session")
    assert not hasattr(context, "repository")


def test_zero_argument_async_and_sync_tasks_still_work() -> None:
    calls = []

    async def async_task() -> None:
        calls.append("async")

    def sync_task() -> None:
        calls.append("sync")

    execute(workflow(task("a"), task("b")), {"a": async_task, "b": sync_task})

    assert sorted(calls) == ["async", "sync"]


def test_retry_context_changes_attempt_key_but_not_idempotency_key() -> None:
    observed = []
    calls = 0

    def flaky(context: TaskExecutionContext) -> None:
        nonlocal calls
        calls += 1
        observed.append(
            (
                context.attempt_number,
                context.attempt_key,
                context.idempotency_key,
            )
        )
        if calls == 1:
            raise RuntimeError("boom")

    execute(
        workflow(task("a", retry_policy=RetryPolicy(max_attempts=2))),
        {"a": flaky},
    )

    assert observed == [
        (1, "run-1:a:1", "run-1:a"),
        (2, "run-1:a:2", "run-1:a"),
    ]


def test_invalid_two_required_args_callable_rejected_before_invocation() -> None:
    calls = []

    def invalid(a, b) -> None:
        calls.append((a, b))

    with pytest.raises(InvalidTaskCallableError):
        execute(workflow(task("a")), {"a": invalid})

    assert calls == []


def test_invalid_required_keyword_only_callable_rejected_before_invocation() -> None:
    calls = []

    def invalid(*, context) -> None:
        calls.append(context)

    with pytest.raises(InvalidTaskCallableError):
        execute(workflow(task("a")), {"a": invalid})

    assert calls == []


def test_varargs_callable_rejected_before_invocation() -> None:
    calls = []

    def invalid(*args) -> None:
        calls.append(args)

    with pytest.raises(InvalidTaskCallableError):
        execute(workflow(task("a")), {"a": invalid})

    assert calls == []


def test_varkwargs_callable_rejected_before_invocation() -> None:
    calls = []

    def invalid(**kwargs) -> None:
        calls.append(kwargs)

    with pytest.raises(InvalidTaskCallableError):
        execute(workflow(task("a")), {"a": invalid})

    assert calls == []


def test_bound_method_and_partial_follow_supported_signature_contract() -> None:
    class Handler:
        def run(self, context: TaskExecutionContext) -> None:
            seen.append(context.task_id)

    def sync_task(value: str) -> None:
        seen.append(value)

    seen = []
    execute(
        workflow(task("a"), task("b")),
        {"a": Handler().run, "b": partial(sync_task, "partial")},
    )

    assert sorted(seen) == ["a", "partial"]
