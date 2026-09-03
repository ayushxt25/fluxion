import asyncio

import pytest
from pydantic import ValidationError

from app.engine.status import AttemptStatus, TaskStatus, WorkflowStatus
from app.schemas.workflow import RetryPolicy, TaskDefinition, WorkflowDefinition
from app.services.execution import (
    PersistentWorkflowExecutor,
    _InMemoryTaskAttemptRepository,
)


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


class FakeWorkflowRepository:
    async def exists(self, workflow_id: str) -> bool:
        return True


class RecordingRunRepository:
    def __init__(self) -> None:
        self.saved_snapshots: list[dict[str, TaskStatus]] = []

    async def create(self, workflow_run) -> None:
        self._record(workflow_run)

    async def save_state(self, workflow_run) -> None:
        self._record(workflow_run)

    def _record(self, workflow_run) -> None:
        self.saved_snapshots.append(
            {
                task_id: task_run.status
                for task_id, task_run in workflow_run.task_runs.items()
            }
        )


def execute(definition, implementations, attempt_repository):
    return asyncio.run(
        PersistentWorkflowExecutor(
            definition,
            implementations,
            FakeWorkflowRepository(),
            RecordingRunRepository(),
            attempt_repository,
            run_id="run-1",
        ).run()
    )


def test_default_retry_policy_allows_one_attempt() -> None:
    assert RetryPolicy().max_attempts == 1


def test_retry_policy_validation() -> None:
    with pytest.raises(ValidationError):
        RetryPolicy(max_attempts=0)
    with pytest.raises(ValidationError):
        RetryPolicy(initial_backoff_seconds=-1)
    with pytest.raises(ValidationError):
        RetryPolicy(backoff_multiplier=0.5)
    with pytest.raises(ValidationError):
        RetryPolicy(max_backoff_seconds=-1)


def test_backoff_calculation_and_cap() -> None:
    policy = RetryPolicy(
        max_attempts=4,
        initial_backoff_seconds=1,
        backoff_multiplier=2,
        max_backoff_seconds=3,
    )

    assert policy.delay_after_failure(1) == 1
    assert policy.delay_after_failure(2) == 2
    assert policy.delay_after_failure(3) == 3


def test_fail_once_then_succeed_records_two_attempts() -> None:
    attempts = _InMemoryTaskAttemptRepository()
    calls = 0

    def flaky() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("boom")

    result = execute(
        workflow(task("a", retry_policy=RetryPolicy(max_attempts=2))),
        {"a": flaky},
        attempts,
    )
    task_attempts = asyncio.run(attempts.list_attempts("run-1", "a"))

    assert result.status == WorkflowStatus.SUCCEEDED
    assert calls == 2
    assert [attempt.attempt_number for attempt in task_attempts] == [1, 2]
    assert [attempt.status for attempt in task_attempts] == [
        AttemptStatus.FAILED,
        AttemptStatus.SUCCEEDED,
    ]
    assert task_attempts[0].attempt_key == "run-1:a:1"


def test_attempts_exhausted_marks_task_and_workflow_failed() -> None:
    attempts = _InMemoryTaskAttemptRepository()

    def fail() -> None:
        raise RuntimeError("boom")

    result = execute(
        workflow(task("a", retry_policy=RetryPolicy(max_attempts=2))),
        {"a": fail},
        attempts,
    )

    assert result.status == WorkflowStatus.FAILED
    assert result.task_statuses["a"] == TaskStatus.FAILED
    assert result.errors == {"a": "RuntimeError: boom"}


def test_dependent_waits_during_retry_and_unlocks_after_success() -> None:
    attempts = _InMemoryTaskAttemptRepository()
    calls: list[str] = []

    def flaky() -> None:
        calls.append("a")
        if calls.count("a") == 1:
            raise RuntimeError("boom")

    result = execute(
        workflow(
            task("a", retry_policy=RetryPolicy(max_attempts=2)),
            task("b", ("a",)),
        ),
        {"a": flaky, "b": lambda: calls.append("b")},
        attempts,
    )

    assert result.status == WorkflowStatus.SUCCEEDED
    assert calls == ["a", "a", "b"]


def test_retry_waiting_does_not_occupy_concurrency_slot() -> None:
    async def scenario():
        attempts = _InMemoryTaskAttemptRepository()
        calls: list[str] = []

        def fail_then_succeed() -> None:
            calls.append("a")
            if calls.count("a") == 1:
                raise RuntimeError("boom")

        result = await PersistentWorkflowExecutor(
            workflow(
                task("a", retry_policy=RetryPolicy(max_attempts=2)),
                task("b"),
            ),
            {"a": fail_then_succeed, "b": lambda: calls.append("b")},
            FakeWorkflowRepository(),
            RecordingRunRepository(),
            attempts,
            run_id="run-1",
            max_concurrency=1,
        ).run()
        return result, calls

    result, calls = asyncio.run(scenario())

    assert result.status == WorkflowStatus.SUCCEEDED
    assert calls[0] == "a"
    assert "b" in calls
