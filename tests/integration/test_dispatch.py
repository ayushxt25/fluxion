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
from datetime import UTC, datetime, timedelta

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db import models  # noqa: F401
from app.db.base import Base
from app.dispatch.messages import TaskDispatchMessage
from app.dispatch.transport import InMemoryTaskDispatcher
from app.engine.context import TaskExecutionContext
from app.engine.exceptions import DispatchError, DispatchStateError, LeaseLostError
from app.engine.execution import WorkflowRun
from app.engine.status import AttemptStatus, TaskStatus, WorkflowStatus
from app.schemas.workflow import RetryPolicy, TaskDefinition, WorkflowDefinition
from app.services.leases import LeaseReaper
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


def test_worker_claim_persists_lease_before_callable_starts() -> None:
    async def body(session):
        definition = workflow("wf-lease-claim", task("a"))
        await persist_run(session, definition)
        dispatcher = InMemoryTaskDispatcher()
        await WorkflowScheduler(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            dispatcher,
        ).dispatch_ready("run-1")
        observed = []

        async def task_a() -> None:
            attempts = await TaskAttemptRepository(session).list_attempts("run-1", "a")
            observed.append(attempts[0])

        await TaskWorker(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            dispatcher,
            {"a": task_a},
            worker_id="worker-1",
            lease_seconds=1,
            heartbeat_seconds=0.1,
        ).run_once(timeout=0.1)

        assert observed[0].status == AttemptStatus.RUNNING
        assert observed[0].worker_id == "worker-1"
        assert observed[0].lease_token
        assert observed[0].lease_expires_at is not None
        assert observed[0].last_heartbeat_at is not None

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


def test_heartbeat_extends_lease_and_wrong_token_is_rejected() -> None:
    async def body(session):
        definition = workflow("wf-heartbeat", task("a"))
        await persist_run(session, definition)
        dispatcher = InMemoryTaskDispatcher()
        summary = await WorkflowScheduler(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            dispatcher,
        ).dispatch_ready("run-1")
        attempt_repo = TaskAttemptRepository(session)
        run = await WorkflowRunRepository(session).get("run-1", definition)
        attempt = (await attempt_repo.list_attempts("run-1", "a"))[0]
        run.start_dispatched_task("a")
        claimed = await attempt_repo.claim_dispatched_attempt(
            run,
            attempt,
            "worker-1",
            "token-1",
            datetime.now(UTC),
            1,
        )

        before = claimed.lease_expires_at
        heartbeat_at = datetime.now(UTC) + timedelta(seconds=1)
        await attempt_repo.heartbeat(
            "run-1",
            "a",
            1,
            "worker-1",
            "token-1",
            heartbeat_at,
            5,
        )
        after = (await attempt_repo.list_attempts("run-1", "a"))[0]

        assert summary.dispatched_task_ids == ("a",)
        assert after.lease_expires_at > before
        assert after.last_heartbeat_at == heartbeat_at
        with pytest.raises(LeaseLostError):
            await attempt_repo.heartbeat(
                "run-1",
                "a",
                1,
                "worker-1",
                "wrong",
                datetime.now(UTC),
                5,
            )

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


def test_expired_lease_reclaim_interrupts_and_fences_stale_worker() -> None:
    async def body(session):
        definition = workflow("wf-reclaim", task("a"))
        await persist_run(session, definition)
        dispatcher = InMemoryTaskDispatcher()
        await WorkflowScheduler(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            dispatcher,
        ).dispatch_ready("run-1")
        attempt_repo = TaskAttemptRepository(session)
        run_repo = WorkflowRunRepository(session)
        run = await run_repo.get("run-1", definition)
        attempt = (await attempt_repo.list_attempts("run-1", "a"))[0]
        run.start_dispatched_task("a")
        claimed = await attempt_repo.claim_dispatched_attempt(
            run,
            attempt,
            "worker-a",
            "token-a",
            datetime.now(UTC) - timedelta(seconds=10),
            1,
        )

        reclaimed = await LeaseReaper(
            WorkflowRepository(session),
            run_repo,
            attempt_repo,
        ).reclaim_expired()
        stale_run = WorkflowRun.restore(
            run_id="run-1",
            workflow=definition,
            status=WorkflowStatus.RUNNING,
            task_statuses={"a": TaskStatus.RUNNING},
        )
        stale_run.complete_task("a")

        with pytest.raises(LeaseLostError):
            await attempt_repo.finish_leased_attempt(
                stale_run,
                claimed,
                AttemptStatus.SUCCEEDED,
                datetime.now(UTC),
            )
        loaded = await run_repo.get("run-1", definition)
        attempts = await attempt_repo.list_attempts("run-1", "a")

        assert reclaimed[0].task_status == TaskStatus.INTERRUPTED
        assert loaded.status == WorkflowStatus.FAILED
        assert loaded.get_task_status("a") == TaskStatus.INTERRUPTED
        assert attempts[0].status == AttemptStatus.INTERRUPTED

    run_in_db(body)


def test_active_lease_is_not_reclaimed() -> None:
    async def body(session):
        definition = workflow("wf-active-lease", task("a"))
        await persist_run(session, definition)
        dispatcher = InMemoryTaskDispatcher()
        await WorkflowScheduler(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            dispatcher,
        ).dispatch_ready("run-1")
        attempt_repo = TaskAttemptRepository(session)
        run = await WorkflowRunRepository(session).get("run-1", definition)
        attempt = (await attempt_repo.list_attempts("run-1", "a"))[0]
        run.start_dispatched_task("a")
        await attempt_repo.claim_dispatched_attempt(
            run,
            attempt,
            "worker-a",
            "token-a",
            datetime.now(UTC),
            60,
        )

        reclaimed = await LeaseReaper(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            attempt_repo,
        ).reclaim_expired()

        assert reclaimed == ()

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
