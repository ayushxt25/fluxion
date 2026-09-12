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
from app.dispatch.transport import InMemoryTaskDispatcher
from app.engine.exceptions import DispatchError, LeaseLostError, PersistenceError
from app.engine.execution import WorkflowRun
from app.engine.status import AttemptStatus, TaskStatus, WorkflowStatus
from app.observability.metrics import render_prometheus, reset_metrics_for_tests
from app.runtime.worker import run_worker_loop
from app.schemas.workflow import TaskDefinition, WorkflowDefinition
from app.services.outbox import DispatchOutboxPublisher
from app.services.repositories import (
    DispatchOutboxRepository,
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)
from app.services.scheduler import WorkflowScheduler
from app.services.worker import TaskWorker
from tests.support.faults import FaultPlan, InjectedFault

pytestmark = [pytest.mark.integration, pytest.mark.chaos]


async def _reset_schema(engine) -> None:
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)


def test_stale_worker_completion_cannot_overwrite_reclaimed_result() -> None:
    async def scenario() -> None:
        engine = create_async_engine(TEST_DATABASE_URL)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        await _reset_schema(engine)
        dispatcher = InMemoryTaskDispatcher()
        definition = WorkflowDefinition(
            id="chaos-stale-worker",
            name="Chaos stale worker",
            tasks=(TaskDefinition(id="a"),),
        )
        entered = asyncio.Event()
        release = asyncio.Event()

        async def delayed_task() -> dict[str, int]:
            entered.set()
            await release.wait()
            return {"value": 1}

        try:
            async with session_factory() as setup_session:
                await WorkflowRepository(setup_session).save(definition)
                await WorkflowRunRepository(setup_session).create(
                    WorkflowRun.create("run-1", definition)
                )
                await WorkflowScheduler(
                    WorkflowRepository(setup_session),
                    WorkflowRunRepository(setup_session),
                    TaskAttemptRepository(setup_session),
                    dispatcher,
                    DispatchOutboxRepository(setup_session),
                ).dispatch_ready("run-1")
                events = await DispatchOutboxRepository(
                    setup_session
                ).list_unpublished()
                event = events[0]
                await dispatcher.dispatch(event.message)

            message = await dispatcher.receive(timeout=0.1)
            async with session_factory() as worker_session:
                worker = TaskWorker(
                    WorkflowRepository(worker_session),
                    WorkflowRunRepository(worker_session),
                    TaskAttemptRepository(worker_session),
                    dispatcher,
                    {"a": delayed_task},
                    worker_id="worker-a",
                    lease_seconds=60,
                    heartbeat_seconds=1,
                )
                completion = asyncio.create_task(worker.process_message(message))
                await entered.wait()

                async with session_factory() as reaper_session:
                    attempts = TaskAttemptRepository(reaper_session)
                    expired = await attempts.list_expired_running_attempts(
                        datetime.now(UTC) + timedelta(seconds=61)
                    )
                    assert len(expired) == 1
                    run_repository = WorkflowRunRepository(reaper_session)
                    run = await run_repository.get("run-1", definition)
                    run.interrupt_tasks_for_recovery(("a",))
                    assert await attempts.reclaim_expired_attempt(
                        run,
                        expired[0],
                        datetime.now(UTC) + timedelta(seconds=61),
                    )

                release.set()
                with pytest.raises(LeaseLostError):
                    await completion

            async with session_factory() as verify_session:
                loaded = await WorkflowRunRepository(verify_session).get(
                    "run-1", definition
                )
                attempts = await TaskAttemptRepository(verify_session).list_attempts(
                    "run-1", "a"
                )

            assert loaded.status == WorkflowStatus.FAILED
            assert loaded.get_task_status("a") == TaskStatus.INTERRUPTED
            assert loaded.task_runs["a"].result_present is False
            assert attempts[0].status == AttemptStatus.INTERRUPTED
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_scheduler_fault_leaves_no_dispatched_task_or_outbox_intent() -> None:
    async def scenario() -> None:
        engine = create_async_engine(TEST_DATABASE_URL)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        await _reset_schema(engine)
        definition = WorkflowDefinition(
            id="chaos-scheduler-rollback",
            name="Chaos scheduler rollback",
            tasks=(TaskDefinition(id="a"),),
        )
        plan = FaultPlan()
        plan.fail_next("before.scheduler.dispatch_intent")

        class FaultingOutboxRepository:
            def __init__(self, delegate: DispatchOutboxRepository) -> None:
                self._delegate = delegate

            async def create_dispatch_intent(self, *args, **kwargs):
                await plan.checkpoint("before.scheduler.dispatch_intent")
                return await self._delegate.create_dispatch_intent(*args, **kwargs)

        try:
            async with session_factory() as session:
                await WorkflowRepository(session).save(definition)
                await WorkflowRunRepository(session).create(
                    WorkflowRun.create("run-1", definition)
                )
                scheduler = WorkflowScheduler(
                    WorkflowRepository(session),
                    WorkflowRunRepository(session),
                    TaskAttemptRepository(session),
                    InMemoryTaskDispatcher(),
                    FaultingOutboxRepository(DispatchOutboxRepository(session)),
                )
                with pytest.raises(InjectedFault):
                    await scheduler.dispatch_ready("run-1")

            async with session_factory() as verify_session:
                run = await WorkflowRunRepository(verify_session).get(
                    "run-1", definition
                )
                attempts = await TaskAttemptRepository(verify_session).list_attempts(
                    "run-1", "a"
                )
                events = await DispatchOutboxRepository(
                    verify_session
                ).list_unpublished()

            assert run.get_task_status("a") == TaskStatus.READY
            assert attempts == ()
            assert events == ()
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_two_schedulers_create_one_canonical_dispatch_intent() -> None:
    async def scenario() -> None:
        engine = create_async_engine(TEST_DATABASE_URL)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        await _reset_schema(engine)
        definition = WorkflowDefinition(
            id="chaos-scheduler-race",
            name="Chaos scheduler race",
            tasks=(TaskDefinition(id="a"),),
        )
        try:
            async with factory() as session:
                await WorkflowRepository(session).save(definition)
                await WorkflowRunRepository(session).create(
                    WorkflowRun.create("run-1", definition)
                )

            async def dispatch_once():
                async with factory() as session:
                    return await WorkflowScheduler(
                        WorkflowRepository(session),
                        WorkflowRunRepository(session),
                        TaskAttemptRepository(session),
                        InMemoryTaskDispatcher(),
                        DispatchOutboxRepository(session),
                    ).dispatch_ready("run-1")

            outcomes = await asyncio.gather(
                dispatch_once(),
                dispatch_once(),
                return_exceptions=True,
            )
            dispatched = [
                item
                for item in outcomes
                if not isinstance(item, Exception) and item.dispatched_task_ids
            ]
            assert len(dispatched) == 1
            assert dispatched[0].dispatched_task_ids == ("a",)
            assert all(
                not isinstance(item, Exception) or isinstance(item, PersistenceError)
                for item in outcomes
            )

            async with factory() as session:
                attempts = await TaskAttemptRepository(session).list_attempts(
                    "run-1", "a"
                )
                events = await DispatchOutboxRepository(session).list_unpublished()
            assert [attempt.attempt_number for attempt in attempts] == [1]
            assert len(events) == 1
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_two_publishers_claim_one_event_and_publish_once() -> None:
    async def scenario() -> None:
        engine = create_async_engine(TEST_DATABASE_URL)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        await _reset_schema(engine)
        dispatcher = InMemoryTaskDispatcher()
        definition = WorkflowDefinition(
            id="chaos-publisher-race",
            name="Chaos publisher race",
            tasks=(TaskDefinition(id="a"),),
        )
        try:
            async with factory() as session:
                await WorkflowRepository(session).save(definition)
                await WorkflowRunRepository(session).create(
                    WorkflowRun.create("run-1", definition)
                )
                await WorkflowScheduler(
                    WorkflowRepository(session),
                    WorkflowRunRepository(session),
                    TaskAttemptRepository(session),
                    dispatcher,
                    DispatchOutboxRepository(session),
                ).dispatch_ready("run-1")

            async def publish_once(publisher_id: str):
                async with factory() as session:
                    return await DispatchOutboxPublisher(
                        DispatchOutboxRepository(session),
                        dispatcher,
                        publisher_id=publisher_id,
                    ).publish_pending()

            first, second = await asyncio.gather(
                publish_once("publisher-a"),
                publish_once("publisher-b"),
            )
            assert first.published + second.published == 1
            assert (await dispatcher.receive(timeout=0.1)).task_id == "a"
            assert await dispatcher.receive(timeout=0.01) is None
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_queue_depth_failure_preserves_ready_state() -> None:
    class FailingDispatcher(InMemoryTaskDispatcher):
        async def queue_depth(self) -> int:
            raise DispatchError("queue depth unavailable")

    async def scenario() -> None:
        engine = create_async_engine(TEST_DATABASE_URL)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        await _reset_schema(engine)
        definition = WorkflowDefinition(
            id="chaos-queue-depth",
            name="Chaos queue depth",
            tasks=(TaskDefinition(id="a"),),
        )
        try:
            async with factory() as session:
                await WorkflowRepository(session).save(definition)
                await WorkflowRunRepository(session).create(
                    WorkflowRun.create("run-1", definition)
                )
                with pytest.raises(DispatchError):
                    await WorkflowScheduler(
                        WorkflowRepository(session),
                        WorkflowRunRepository(session),
                        TaskAttemptRepository(session),
                        FailingDispatcher(),
                        DispatchOutboxRepository(session),
                    ).dispatch_ready("run-1")

            async with factory() as session:
                run = await WorkflowRunRepository(session).get("run-1", definition)
                events = await DispatchOutboxRepository(session).list_unpublished()
            assert run.get_task_status("a") == TaskStatus.READY
            assert events == ()
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_backpressure_pauses_new_intents_but_not_existing_outbox_publish() -> None:
    async def scenario() -> None:
        engine = create_async_engine(TEST_DATABASE_URL)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        await _reset_schema(engine)
        dispatcher = InMemoryTaskDispatcher()
        first = WorkflowDefinition(
            id="chaos-backpressure-first",
            name="First",
            tasks=(TaskDefinition(id="a"),),
        )
        second = WorkflowDefinition(
            id="chaos-backpressure-second",
            name="Second",
            tasks=(TaskDefinition(id="b"),),
        )
        try:
            async with factory() as session:
                workflows = WorkflowRepository(session)
                runs = WorkflowRunRepository(session)
                await workflows.save(first)
                await workflows.save(second)
                await runs.create(WorkflowRun.create("run-first", first))
                await runs.create(WorkflowRun.create("run-second", second))
                first_scheduler = WorkflowScheduler(
                    workflows,
                    runs,
                    TaskAttemptRepository(session),
                    dispatcher,
                    DispatchOutboxRepository(session),
                    queue_high_watermark=1,
                )
                assert (
                    await first_scheduler.dispatch_ready("run-first")
                ).dispatched_task_ids == ("a",)

                events = await DispatchOutboxRepository(session).list_unpublished()
                await dispatcher.dispatch(events[0].message)
                blocked = await first_scheduler.dispatch_ready("run-second")
                assert blocked.dispatched_task_ids == ()

                published = await DispatchOutboxPublisher(
                    DispatchOutboxRepository(session), dispatcher
                ).publish_pending()
                assert published.published == 1

            assert await dispatcher.receive(timeout=0.1) is not None
            assert await dispatcher.receive(timeout=0.1) is not None

            async with factory() as session:
                resumed = await WorkflowScheduler(
                    WorkflowRepository(session),
                    WorkflowRunRepository(session),
                    TaskAttemptRepository(session),
                    dispatcher,
                    DispatchOutboxRepository(session),
                    queue_high_watermark=1,
                ).dispatch_ready("run-second")
            assert resumed.dispatched_task_ids == ("b",)
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_bounded_worker_shutdown_keeps_overdue_work_lease_reclaimable() -> None:
    async def scenario() -> None:
        engine = create_async_engine(TEST_DATABASE_URL)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        await _reset_schema(engine)
        reset_metrics_for_tests()
        dispatcher = InMemoryTaskDispatcher()
        definition = WorkflowDefinition(
            id="chaos-shutdown",
            name="Chaos shutdown",
            tasks=(
                TaskDefinition(id="a-fast"),
                TaskDefinition(id="b-slow"),
                TaskDefinition(id="c-queued"),
            ),
        )
        fast_started = asyncio.Event()
        slow_started = asyncio.Event()
        release_fast = asyncio.Event()
        release_slow = asyncio.Event()
        stop_event = asyncio.Event()

        async def fast() -> dict[str, str]:
            fast_started.set()
            await release_fast.wait()
            return {"state": "fast"}

        async def slow() -> dict[str, str]:
            slow_started.set()
            await release_slow.wait()
            return {"state": "slow"}

        try:
            async with factory() as session:
                await WorkflowRepository(session).save(definition)
                await WorkflowRunRepository(session).create(
                    WorkflowRun.create("run-1", definition)
                )
                await WorkflowScheduler(
                    WorkflowRepository(session),
                    WorkflowRunRepository(session),
                    TaskAttemptRepository(session),
                    dispatcher,
                    DispatchOutboxRepository(session),
                    max_dispatch_per_run=3,
                ).dispatch_ready("run-1")
                await DispatchOutboxPublisher(
                    DispatchOutboxRepository(session), dispatcher
                ).publish_pending(limit=3)

            runtime = asyncio.create_task(
                run_worker_loop(
                    factory,
                    dispatcher,
                    {"a-fast": fast, "b-slow": slow, "c-queued": fast},
                    stop_event,
                    concurrency=2,
                    shutdown_grace_seconds=0.02,
                    worker_id="shutdown-worker",
                )
            )
            await fast_started.wait()
            await slow_started.wait()
            stop_event.set()
            release_fast.set()
            assert await asyncio.wait_for(runtime, timeout=1) == "shutdown-worker"

            async with factory() as session:
                run = await WorkflowRunRepository(session).get("run-1", definition)
                slow_attempts = await TaskAttemptRepository(session).list_attempts(
                    "run-1", "b-slow"
                )
                queued_attempts = await TaskAttemptRepository(session).list_attempts(
                    "run-1", "c-queued"
                )

            assert run.get_task_status("a-fast") == TaskStatus.SUCCEEDED
            assert run.get_task_status("b-slow") == TaskStatus.RUNNING
            assert run.get_task_status("c-queued") == TaskStatus.DISPATCHED
            assert slow_attempts[0].status == AttemptStatus.RUNNING
            assert queued_attempts[0].status == AttemptStatus.DISPATCHED
            assert "fluxion_worker_active_tasks 0" in render_prometheus()

            async with factory() as session:
                attempts = TaskAttemptRepository(session)
                expired = await attempts.list_expired_running_attempts(
                    datetime.now(UTC) + timedelta(seconds=31)
                )
                run = await WorkflowRunRepository(session).get("run-1", definition)
                run.interrupt_tasks_for_recovery(("b-slow",))
                assert await attempts.reclaim_expired_attempt(
                    run,
                    expired[0],
                    datetime.now(UTC) + timedelta(seconds=31),
                )
        finally:
            release_slow.set()
            await engine.dispose()

    asyncio.run(scenario())
