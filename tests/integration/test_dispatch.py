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
from app.engine.exceptions import (
    DispatchError,
    DispatchStateError,
    LeaseLostError,
    PersistenceError,
)
from app.engine.execution import WorkflowRun
from app.engine.status import AttemptStatus, TaskStatus, WorkflowStatus
from app.schemas.workflow import RetryPolicy, TaskDefinition, WorkflowDefinition
from app.services.leases import LeaseReaper
from app.services.management import WorkflowRunManagementService
from app.services.outbox import (
    DispatchOutboxPublisher,
    DispatchReconciler,
    DispatchReconciliationService,
)
from app.services.recovery import WorkflowRecoveryService
from app.services.repositories import (
    DispatchOutboxRepository,
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

    async def queue_depth(self) -> int:
        return 0


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


async def schedule_and_publish(session, dispatcher, run_id="run-1"):
    summary = await WorkflowScheduler(
        WorkflowRepository(session),
        WorkflowRunRepository(session),
        TaskAttemptRepository(session),
        dispatcher,
    ).dispatch_ready(run_id)
    await DispatchOutboxPublisher(
        DispatchOutboxRepository(session),
        dispatcher,
    ).publish_pending()
    return summary


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
        events = await DispatchOutboxRepository(session).list_unpublished()

        assert summary.dispatched_task_ids == ("a",)
        assert summary.outbox_event_ids == (events[0].id,)
        assert loaded.get_task_status("a") == TaskStatus.DISPATCHED
        assert loaded.get_task_status("b") == TaskStatus.BLOCKED
        assert attempts[0].status == AttemptStatus.DISPATCHED
        assert events[0].message.attempt_key == attempts[0].attempt_key
        assert events[0].message.idempotency_key == "run-1:a"
        assert await dispatcher.receive(timeout=0.01) is None

    run_in_db(body)


def test_outbox_publisher_publishes_and_marks_event() -> None:
    async def body(session):
        definition = workflow("wf-outbox-publish", task("a"))
        await persist_run(session, definition)
        dispatcher = InMemoryTaskDispatcher()
        summary = await WorkflowScheduler(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            dispatcher,
        ).dispatch_ready("run-1")

        result = await DispatchOutboxPublisher(
            DispatchOutboxRepository(session),
            dispatcher,
        ).publish_pending()
        message = await dispatcher.receive(timeout=0.1)
        unpublished = await DispatchOutboxRepository(session).list_unpublished()

        assert result.attempted == 1
        assert result.published == 1
        assert result.published_event_ids == summary.outbox_event_ids
        assert message == summary.messages[0]
        assert unpublished == ()

    run_in_db(body)


def test_stale_published_dispatch_is_repaired_without_new_attempt() -> None:
    async def body(session):
        definition = workflow("wf-reconcile", task("a"))
        await persist_run(session, definition)
        dispatcher = InMemoryTaskDispatcher()
        summary = await WorkflowScheduler(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            dispatcher,
        ).dispatch_ready("run-1")
        event_id = summary.outbox_event_ids[0]
        old = datetime.now(UTC) - timedelta(minutes=10)
        await DispatchOutboxRepository(session).mark_published(event_id, old)

        result = await DispatchReconciler(
            DispatchOutboxRepository(session),
            reconcile_after_seconds=60,
            batch_size=10,
        ).reconcile_once(datetime.now(UTC))

        unpublished = await DispatchOutboxRepository(session).list_unpublished()
        attempts = await TaskAttemptRepository(session).list_attempts("run-1", "a")
        reissued = unpublished[0]

        assert result.reconciled_event_ids == (event_id,)
        assert reissued.id == event_id
        assert reissued.published_at is None
        assert reissued.reconcile_count == 1
        assert reissued.last_reconciled_at is not None
        assert reissued.run_id == "run-1"
        assert reissued.task_id == "a"
        assert reissued.attempt_number == 1
        assert reissued.message.idempotency_key == "run-1:a"
        assert reissued.message.attempt_key == "run-1:a:1"
        assert len(attempts) == 1
        assert attempts[0].attempt_number == 1
        assert attempts[0].status is AttemptStatus.DISPATCHED

        published = await DispatchOutboxPublisher(
            DispatchOutboxRepository(session),
            dispatcher,
        ).publish_pending()
        assert published.published_event_ids == (event_id,)

    run_in_db(body)


def test_too_new_or_discarded_published_dispatch_is_never_reconciled() -> None:
    async def body(session):
        definition = workflow("wf-reconcile-safety", task("a"), task("b"))
        await persist_run(session, definition)
        scheduler = WorkflowScheduler(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            InMemoryTaskDispatcher(),
        )
        summary = await scheduler.dispatch_ready("run-1", max_dispatch=2)
        outbox = DispatchOutboxRepository(session)
        now = datetime.now(UTC)
        await outbox.mark_published(summary.outbox_event_ids[0], now)
        await outbox.mark_published(
            summary.outbox_event_ids[1],
            now - timedelta(minutes=10),
        )
        await outbox.mark_discarded(
            summary.outbox_event_ids[1],
            now,
            "cancelled before worker claim",
        )

        result = await DispatchReconciler(
            outbox,
            reconcile_after_seconds=60,
            batch_size=10,
        ).reconcile_once(now)
        events = {item.id: item for item in await outbox.list_unpublished()}

        assert result.reconciled == 0
        assert summary.outbox_event_ids[0] not in events
        assert summary.outbox_event_ids[1] not in events

    run_in_db(body)


def test_running_leased_dispatch_is_never_reconciled() -> None:
    async def body(session):
        definition = workflow("wf-reconcile-lease", task("a"))
        await persist_run(session, definition)
        dispatcher = InMemoryTaskDispatcher()
        summary = await schedule_and_publish(session, dispatcher)
        outbox = DispatchOutboxRepository(session)
        now = datetime.now(UTC)
        await outbox.mark_published(
            summary.outbox_event_ids[0], now - timedelta(minutes=10)
        )
        run = await WorkflowRunRepository(session).get("run-1", definition)
        attempt = (await TaskAttemptRepository(session).list_attempts("run-1", "a"))[0]
        run.start_dispatched_task("a")
        claimed = await TaskAttemptRepository(session).claim_dispatched_attempt(
            run, attempt, "worker", "lease-token", now, 60
        )

        result = await DispatchReconciler(
            outbox, reconcile_after_seconds=60, batch_size=10
        ).reconcile_once(now)
        attempts = await TaskAttemptRepository(session).list_attempts("run-1", "a")

        assert result.reconciled == 0
        assert len(attempts) == 1
        assert attempts[0].status is AttemptStatus.RUNNING
        assert attempts[0].lease_token == claimed.lease_token

    run_in_db(body)


def test_cancelled_published_dispatch_is_never_reconciled() -> None:
    async def body(session):
        definition = workflow("wf-reconcile-cancel", task("a"))
        await persist_run(session, definition)
        summary = await schedule_and_publish(session, InMemoryTaskDispatcher())
        outbox = DispatchOutboxRepository(session)
        now = datetime.now(UTC)
        await outbox.mark_published(
            summary.outbox_event_ids[0], now - timedelta(minutes=10)
        )
        await WorkflowRunManagementService(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
        ).cancel_run("run-1")

        result = await DispatchReconciler(
            outbox, reconcile_after_seconds=60, batch_size=10
        ).reconcile_once(now)
        attempts = await TaskAttemptRepository(session).list_attempts("run-1", "a")

        assert result.reconciled == 0
        assert len(attempts) == 1
        assert attempts[0].status is AttemptStatus.DISPATCHED

    run_in_db(body)


def test_succeeded_published_dispatch_is_never_reconciled() -> None:
    async def body(session):
        definition = workflow("wf-reconcile-success", task("a"))
        await persist_run(session, definition)
        dispatcher = InMemoryTaskDispatcher()
        summary = await schedule_and_publish(session, dispatcher)
        await TaskWorker(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            dispatcher,
            {"a": lambda: None},
        ).run_once(timeout=0.1)
        now = datetime.now(UTC)
        outbox = DispatchOutboxRepository(session)
        await outbox.mark_published(
            summary.outbox_event_ids[0], now - timedelta(minutes=10)
        )

        result = await DispatchReconciler(
            outbox, reconcile_after_seconds=60, batch_size=10
        ).reconcile_once(now)
        loaded = await WorkflowRunRepository(session).get("run-1", definition)

        assert result.reconciled == 0
        assert loaded.status is WorkflowStatus.SUCCEEDED
        assert (
            len(await TaskAttemptRepository(session).list_attempts("run-1", "a")) == 1
        )

    run_in_db(body)


def test_interrupted_published_dispatch_is_never_reconciled() -> None:
    async def body(session):
        definition = workflow("wf-reconcile-interrupted", task("a"))
        await persist_run(session, definition)
        dispatcher = InMemoryTaskDispatcher()
        summary = await schedule_and_publish(session, dispatcher)
        now = datetime.now(UTC)
        outbox = DispatchOutboxRepository(session)
        await outbox.mark_published(
            summary.outbox_event_ids[0], now - timedelta(minutes=10)
        )
        run_repo = WorkflowRunRepository(session)
        attempt_repo = TaskAttemptRepository(session)
        run = await run_repo.get("run-1", definition)
        attempt = (await attempt_repo.list_attempts("run-1", "a"))[0]
        run.start_dispatched_task("a")
        await attempt_repo.claim_dispatched_attempt(
            run, attempt, "worker", "lease-token", now - timedelta(minutes=5), 1
        )
        await LeaseReaper(
            WorkflowRepository(session), run_repo, attempt_repo
        ).reclaim_expired()

        result = await DispatchReconciler(
            outbox, reconcile_after_seconds=60, batch_size=10
        ).reconcile_once(now)
        attempts = await attempt_repo.list_attempts("run-1", "a")

        assert result.reconciled == 0
        assert len(attempts) == 1
        assert attempts[0].status is AttemptStatus.INTERRUPTED

    run_in_db(body)


def test_concurrent_reconcilers_lock_distinct_stale_dispatches() -> None:
    async def scenario() -> None:
        engine = create_async_engine(TEST_DATABASE_URL)
        await reset_schema(engine)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        definition = workflow("wf-reconcile-race", task("a"))
        now = datetime.now(UTC)
        event_ids: list[str] = []
        try:
            async with session_factory() as setup:
                await WorkflowRepository(setup).save(definition)
                for index in range(3):
                    run = WorkflowRun.create(f"run-race-{index}", definition)
                    await WorkflowRunRepository(setup).create(run)
                    summary = await WorkflowScheduler(
                        WorkflowRepository(setup),
                        WorkflowRunRepository(setup),
                        TaskAttemptRepository(setup),
                        InMemoryTaskDispatcher(),
                    ).dispatch_ready(run.run_id)
                    event_id = summary.outbox_event_ids[0]
                    event_ids.append(event_id)
                    await DispatchOutboxRepository(setup).mark_published(
                        event_id,
                        now - timedelta(minutes=10),
                    )

            async def reconcile_once() -> tuple[str, ...]:
                async with session_factory() as session:
                    return (
                        await DispatchReconciler(
                            DispatchOutboxRepository(session),
                            reconcile_after_seconds=60,
                            batch_size=1,
                        ).reconcile_once(now)
                    ).reconciled_event_ids

            first, second = await asyncio.gather(reconcile_once(), reconcile_once())
            assert set(first).isdisjoint(second)
            assert len(first) + len(second) == 2

            async with session_factory() as cleanup:
                last = await DispatchReconciler(
                    DispatchOutboxRepository(cleanup),
                    reconcile_after_seconds=60,
                    batch_size=1,
                ).reconcile_once(now)
                assert len(last.reconciled_event_ids) == 1
                events = await DispatchOutboxRepository(cleanup).list_unpublished()
                assert {event.id for event in events} == set(event_ids)
                assert all(event.reconcile_count == 1 for event in events)
                for index in range(3):
                    attempts = await TaskAttemptRepository(cleanup).list_attempts(
                        f"run-race-{index}",
                        "a",
                    )
                    assert len(attempts) == 1
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_outbox_claim_prevents_second_live_claim_and_expires() -> None:
    async def body(session):
        definition = workflow("wf-outbox-claim", task("a"))
        await persist_run(session, definition)
        await WorkflowScheduler(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            InMemoryTaskDispatcher(),
        ).dispatch_ready("run-1")
        outbox = DispatchOutboxRepository(session)
        now = datetime.now(UTC)

        first = await outbox.claim_unpublished("publisher-a", "token-a", now, 60, 1)
        second = await outbox.claim_unpublished(
            "publisher-b",
            "token-b",
            now,
            60,
            1,
        )
        reclaimed = await outbox.claim_unpublished(
            "publisher-b",
            "token-b",
            now + timedelta(seconds=61),
            60,
            1,
        )

        assert first[0].claimed_by == "publisher-a"
        assert first[0].claim_token == "token-a"
        assert second == ()
        assert reclaimed[0].claimed_by == "publisher-b"
        assert reclaimed[0].claim_token == "token-b"

    run_in_db(body)


def test_stale_outbox_claim_cannot_mark_published_after_reclaim() -> None:
    async def body(session):
        definition = workflow("wf-outbox-fencing", task("a"))
        await persist_run(session, definition)
        await WorkflowScheduler(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            InMemoryTaskDispatcher(),
        ).dispatch_ready("run-1")
        outbox = DispatchOutboxRepository(session)
        now = datetime.now(UTC)
        first = await outbox.claim_unpublished("publisher-a", "token-a", now, 1, 1)
        second = await outbox.claim_unpublished(
            "publisher-b",
            "token-b",
            now + timedelta(seconds=2),
            60,
            1,
        )

        with pytest.raises(PersistenceError):
            await outbox.mark_published(
                first[0].id,
                datetime.now(UTC),
                publisher_id="publisher-a",
                claim_token="token-a",
            )
        await outbox.mark_published(
            second[0].id,
            datetime.now(UTC),
            publisher_id="publisher-b",
            claim_token="token-b",
        )

        assert await outbox.list_unpublished() == ()

    run_in_db(body)


def test_outbox_batch_acknowledgment_is_atomic_and_fenced() -> None:
    async def body(session):
        definition = workflow("wf-outbox-batch", task("a"), task("b"))
        await persist_run(session, definition)
        summary = await WorkflowScheduler(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            InMemoryTaskDispatcher(),
        ).dispatch_ready("run-1", max_dispatch=2)
        outbox = DispatchOutboxRepository(session)
        now = datetime.now(UTC)
        claimed = await outbox.claim_unpublished("publisher-a", "token-a", now, 60, 2)
        event_ids = tuple(event.id for event in claimed)

        with pytest.raises(PersistenceError):
            await outbox.mark_published_batch(
                event_ids,
                now,
                publisher_id="publisher-b",
                claim_token="token-a",
            )
        with pytest.raises(PersistenceError):
            await outbox.mark_published_batch(
                event_ids,
                now,
                publisher_id="publisher-a",
                claim_token="token-b",
            )
        with pytest.raises(PersistenceError):
            await outbox.mark_published_batch(
                (*event_ids, "missing-event"),
                now,
                publisher_id="publisher-a",
                claim_token="token-a",
            )

        after_failed_batches = await outbox.list_unpublished()
        assert {event.id for event in after_failed_batches} == set(event_ids)
        assert all(event.published_at is None for event in after_failed_batches)
        assert all(event.claimed_by == "publisher-a" for event in after_failed_batches)
        assert all(event.claim_token == "token-a" for event in after_failed_batches)

        await outbox.mark_published_batch(
            event_ids,
            now,
            publisher_id="publisher-a",
            claim_token="token-a",
        )
        records = [
            await session.get(models.DispatchOutboxRecord, event_id)
            for event_id in event_ids
        ]

        assert summary.outbox_event_ids == event_ids
        assert all(record is not None for record in records)
        assert all(record.published_at == now for record in records)
        assert all(record.claimed_by is None for record in records)
        assert all(record.claim_token is None for record in records)
        assert all(record.claimed_at is None for record in records)
        assert all(record.claim_expires_at is None for record in records)
        assert all(record.publish_attempts == 1 for record in records)
        assert all(record.last_error is None for record in records)

    run_in_db(body)


def test_outbox_batch_acknowledgment_rejects_published_row_without_partial_update() -> (
    None
):
    async def body(session):
        definition = workflow("wf-outbox-batch-published", task("a"), task("b"))
        await persist_run(session, definition)
        summary = await WorkflowScheduler(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            InMemoryTaskDispatcher(),
        ).dispatch_ready("run-1", max_dispatch=2)
        outbox = DispatchOutboxRepository(session)
        now = datetime.now(UTC)
        claimed = await outbox.claim_unpublished("publisher-a", "token-a", now, 60, 2)
        event_ids = tuple(event.id for event in claimed)
        await outbox.mark_published(
            event_ids[0],
            now,
            publisher_id="publisher-a",
            claim_token="token-a",
        )

        with pytest.raises(PersistenceError):
            await outbox.mark_published_batch(
                event_ids,
                now + timedelta(seconds=1),
                publisher_id="publisher-a",
                claim_token="token-a",
            )

        first = await session.get(models.DispatchOutboxRecord, event_ids[0])
        second = await session.get(models.DispatchOutboxRecord, event_ids[1])
        assert summary.outbox_event_ids == event_ids
        assert first is not None and first.published_at == now
        assert first.claimed_by is None
        assert first.claim_token is None
        assert first.claimed_at is None
        assert first.claim_expires_at is None
        assert first.publish_attempts == 1
        assert first.last_error is None
        assert second is not None and second.published_at is None
        assert second.claimed_by == "publisher-a"
        assert second.claim_token == "token-a"
        assert second.publish_attempts == 0

    run_in_db(body)


def test_outbox_batch_validity_discards_stale_or_missing_attempt() -> None:
    async def body(session):
        definition = workflow(
            "wf-outbox-batch-validity",
            task("a"),
            task("b"),
            task("c"),
        )
        await persist_run(session, definition)
        dispatcher = InMemoryTaskDispatcher()
        summary = await WorkflowScheduler(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            dispatcher,
        ).dispatch_ready("run-1", max_dispatch=3)

        async with session.begin():
            stale_task = await session.get(models.TaskRunRecord, ("run-1", "b"))
            missing_attempt = await session.get(
                models.TaskAttemptRecord,
                ("run-1", "c", 1),
            )
            assert stale_task is not None
            assert missing_attempt is not None
            stale_task.status = TaskStatus.READY.value
            await session.delete(missing_attempt)

        result = await DispatchOutboxPublisher(
            DispatchOutboxRepository(session),
            dispatcher,
        ).publish_pending()

        assert result.published_event_ids == (summary.outbox_event_ids[0],)
        assert result.discarded_event_ids == summary.outbox_event_ids[1:]
        assert await dispatcher.receive(timeout=0.1) == summary.messages[0]
        assert await dispatcher.receive(timeout=0.01) is None
        assert await DispatchOutboxRepository(session).list_unpublished() == ()

    run_in_db(body)


def test_worker_success_persists_and_unlocks_dependent_without_dispatching_it() -> None:
    async def body(session):
        definition = workflow("wf-worker", task("a"), task("b", ("a",)))
        await persist_run(session, definition)
        dispatcher = InMemoryTaskDispatcher()
        await schedule_and_publish(session, dispatcher)
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
        await schedule_and_publish(session, dispatcher)
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
        await schedule_and_publish(session, dispatcher)

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
        summary = await schedule_and_publish(session, dispatcher)
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
        await schedule_and_publish(session, dispatcher)
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
        await schedule_and_publish(session, dispatcher)
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
        await schedule_and_publish(session, dispatcher)
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

        summary = await WorkflowScheduler(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            FailingTaskDispatcher(),
        ).dispatch_ready("run-1")
        result = await DispatchOutboxPublisher(
            DispatchOutboxRepository(session),
            FailingTaskDispatcher(),
        ).publish_pending()
        loaded = await WorkflowRunRepository(session).get("run-1", definition)
        events = await DispatchOutboxRepository(session).list_unpublished()

        assert summary.dispatched_task_ids == ("a",)
        assert result.failed == 1
        assert loaded.get_task_status("a") == TaskStatus.DISPATCHED
        assert events[0].published_at is None
        assert events[0].publish_attempts == 1
        assert events[0].last_error is not None

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


def test_retry_scheduling_creates_new_outbox_intent() -> None:
    async def body(session):
        definition = workflow(
            "wf-retry-outbox",
            task(
                "a",
                retry_policy=RetryPolicy(
                    max_attempts=2,
                    initial_backoff_seconds=0,
                ),
            ),
        )
        await persist_run(session, definition)
        dispatcher = InMemoryTaskDispatcher()
        await schedule_and_publish(session, dispatcher)

        def fail_once() -> None:
            raise RuntimeError("try again")

        await TaskWorker(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            dispatcher,
            {"a": fail_once},
        ).run_once(timeout=0.1)

        summary = await WorkflowScheduler(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            dispatcher,
        ).dispatch_ready("run-1")
        attempts = await TaskAttemptRepository(session).list_attempts("run-1", "a")
        events = await DispatchOutboxRepository(session).list_unpublished()

        assert summary.dispatched_task_ids == ("a",)
        assert attempts[-1].attempt_number == 2
        assert attempts[-1].status == AttemptStatus.DISPATCHED
        assert events[0].message.attempt_key == "run-1:a:2"

    run_in_db(body)


def test_reconciliation_detects_legacy_dispatched_without_outbox() -> None:
    async def body(session):
        definition = workflow("wf-legacy-dispatch", task("a"))
        await persist_run(session, definition)
        run = await WorkflowRunRepository(session).get("run-1", definition)
        run.dispatch_task("a")
        await TaskAttemptRepository(session).create_dispatched_attempt(run, "a", 1)

        result = await DispatchReconciliationService(
            DispatchOutboxRepository(session)
        ).inspect()

        assert result.dispatched_attempts_missing_outbox == (("run-1", "a", 1),)

    run_in_db(body)
