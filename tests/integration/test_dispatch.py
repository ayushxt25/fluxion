import os
from urllib.parse import urlparse

import pytest

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
if not TEST_DATABASE_URL:
    pytest.skip("TEST_DATABASE_URL is not set", allow_module_level=True)

test_database_name = urlparse(TEST_DATABASE_URL).path.rsplit("/", maxsplit=1)[-1]
if not test_database_name.endswith("_test"):
    pytest.skip(
        "TEST_DATABASE_URL must point to a *_test database",
        allow_module_level=True,
    )

# ruff: noqa: E402
import asyncio

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db import models  # noqa: F401
from app.db.base import Base
from app.dispatch.messages import TaskDispatchMessage
from app.dispatch.transport import InMemoryTaskDispatcher
from app.engine.context import TaskExecutionContext
from app.engine.exceptions import DispatchError, DispatchStateError
from app.engine.execution import WorkflowRun
from app.engine.status import AttemptStatus, TaskStatus, WorkflowStatus
from app.schemas.workflow import RetryPolicy, TaskDefinition, WorkflowDefinition
from app.services.recovery import WorkflowRecoveryService
from app.services.repositories import (
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)
from app.services.scheduler import WorkflowScheduler
from app.services.worker import TaskWorker


class FailingTaskDispatcher:
    async def dispatch(self, message: TaskDispatchMessage) -> None:
        raise DispatchError("publish failed")

    async def receive(self, timeout: float | None = None) -> TaskDispatchMessage | None:
        return None


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


def workflow(workflow_id: str, *tasks: TaskDefinition) -> WorkflowDefinition:
    return WorkflowDefinition(id=workflow_id, name="Workflow", tasks=tasks)


async def reset_schema(engine) -> None:
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)


def run_in_db(test_body):
    async def scenario():
        engine = create_async_engine(TEST_DATABASE_URL)
        await reset_schema(engine)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with session_factory() as session:
                await test_body(session)
        finally:
            await engine.dispose()

    asyncio.run(scenario())


async def persist_run(session, definition, run_id="run-1"):
    await WorkflowRepository(session).save(definition)
    run = WorkflowRun.create(run_id, definition)
    await WorkflowRunRepository(session).create(run)
    return run


def test_scheduler_dispatches_ready_task_and_persists_identity() -> None:
    async def body(session):
        definition = workflow("wf-dispatch", task("a"), task("b", ("a",)))
        await persist_run(session, definition)
        dispatcher = InMemoryTaskDispatcher()

        summary = await WorkflowScheduler(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            dispatcher,
        ).dispatch_ready("run-1")
        loaded = await WorkflowRunRepository(session).get("run-1", definition)
        attempts = await TaskAttemptRepository(session).list_attempts("run-1", "a")
        message = await dispatcher.receive(timeout=0.1)

        assert summary.dispatched_task_ids == ("a",)
        assert loaded.get_task_status("a") == TaskStatus.DISPATCHED
        assert loaded.get_task_status("b") == TaskStatus.BLOCKED
        assert attempts[0].status == AttemptStatus.DISPATCHED
        assert message.attempt_key == attempts[0].attempt_key
        assert message.idempotency_key == "run-1:a"

    run_in_db(body)


def test_worker_success_persists_and_unlocks_dependent_without_dispatching_it() -> None:
    async def body(session):
        definition = workflow("wf-worker", task("a"), task("b", ("a",)))
        await persist_run(session, definition)
        dispatcher = InMemoryTaskDispatcher()
        scheduler = WorkflowScheduler(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            dispatcher,
        )
        await scheduler.dispatch_ready("run-1")
        observed = []

        def task_a(context: TaskExecutionContext) -> None:
            observed.append((context.attempt_key, context.idempotency_key))

        result = await TaskWorker(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            dispatcher,
            {"a": task_a},
        ).run_once(timeout=0.1)
        loaded = await WorkflowRunRepository(session).get("run-1", definition)

        assert result.attempt_status == AttemptStatus.SUCCEEDED
        assert loaded.get_task_status("a") == TaskStatus.SUCCEEDED
        assert loaded.get_task_status("b") == TaskStatus.READY
        assert observed == [("run-1:a:1", "run-1:a")]
        assert await dispatcher.receive(timeout=0.01) is None

    run_in_db(body)


def test_worker_failure_with_retry_persists_retry_waiting_without_sleeping() -> None:
    async def body(session):
        definition = workflow(
            "wf-retry-dispatch",
            task(
                "a",
                retry_policy=RetryPolicy(
                    max_attempts=2,
                    initial_backoff_seconds=5,
                ),
            ),
        )
        await persist_run(session, definition)
        dispatcher = InMemoryTaskDispatcher()
        await WorkflowScheduler(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            dispatcher,
        ).dispatch_ready("run-1")

        def fail() -> None:
            raise RuntimeError("boom")

        result = await TaskWorker(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            dispatcher,
            {"a": fail},
        ).run_once(timeout=0.1)
        loaded = await WorkflowRunRepository(session).get("run-1", definition)

        assert result.workflow_status == WorkflowStatus.RUNNING
        assert loaded.get_task_status("a") == TaskStatus.RETRY_WAITING
        assert loaded.task_runs["a"].next_retry_at is not None

    run_in_db(body)


def test_duplicate_message_does_not_rerun_successful_attempt() -> None:
    async def body(session):
        definition = workflow("wf-duplicate-message", task("a"))
        await persist_run(session, definition)
        dispatcher = InMemoryTaskDispatcher()
        await WorkflowScheduler(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            dispatcher,
        ).dispatch_ready("run-1")
        message = await dispatcher.receive(timeout=0.1)
        calls = []
        worker = TaskWorker(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            dispatcher,
            {"a": lambda: calls.append("a")},
        )

        await worker.process_message(message)
        with pytest.raises(DispatchStateError):
            await worker.process_message(message)

        assert calls == ["a"]

    run_in_db(body)


def test_publish_failure_leaves_durable_dispatched_state() -> None:
    async def body(session):
        definition = workflow("wf-publish-failure", task("a"))
        await persist_run(session, definition)

        with pytest.raises(DispatchError):
            await WorkflowScheduler(
                WorkflowRepository(session),
                WorkflowRunRepository(session),
                TaskAttemptRepository(session),
                FailingTaskDispatcher(),
            ).dispatch_ready("run-1")
        loaded = await WorkflowRunRepository(session).get("run-1", definition)

        assert loaded.get_task_status("a") == TaskStatus.DISPATCHED

    run_in_db(body)


def test_recovery_keeps_dispatched_state_not_resumable() -> None:
    async def body(session):
        definition = workflow("wf-dispatched-recovery", task("a"))
        await persist_run(session, definition)
        await WorkflowScheduler(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            InMemoryTaskDispatcher(),
        ).dispatch_ready("run-1")

        result = await WorkflowRecoveryService(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
        ).recover_run("run-1", definition.id)

        assert result.task_statuses["a"] == TaskStatus.DISPATCHED
        assert result.resumable is False

    run_in_db(body)
