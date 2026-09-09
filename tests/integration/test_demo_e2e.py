import os
from collections import Counter
from urllib.parse import urlparse
from uuid import uuid4

import pytest

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
REDIS_URL = os.getenv("REDIS_URL")
if not TEST_DATABASE_URL:
    pytest.skip("TEST_DATABASE_URL is not set", allow_module_level=True)
if not REDIS_URL:
    pytest.skip("REDIS_URL is not set", allow_module_level=True)

test_database_name = urlparse(TEST_DATABASE_URL).path.rsplit("/", maxsplit=1)[-1]
if not test_database_name.endswith("_test"):
    pytest.skip(
        "TEST_DATABASE_URL must point to a *_test database",
        allow_module_level=True,
    )

# ruff: noqa: E402
import asyncio

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db import models  # noqa: F401
from app.db.base import Base
from app.dispatch.transport import RedisTaskDispatcher
from app.engine.context import TaskExecutionContext
from app.engine.execution import WorkflowRun
from app.engine.status import AttemptStatus, TaskStatus, WorkflowStatus
from app.services.outbox import DispatchOutboxPublisher
from app.services.repositories import (
    DispatchOutboxRepository,
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)
from app.services.scheduler import WorkflowScheduler
from app.services.worker import TaskWorker
from app.tasks.demo import DEMO_TASK_IDS, build_demo_workflow


async def reset_schema(engine) -> None:
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)


async def load_run(session_factory, run_id, workflow):
    async with session_factory() as session:
        return await WorkflowRunRepository(session).get(run_id, workflow)


async def load_attempts(session_factory, run_id):
    async with session_factory() as session:
        repository = TaskAttemptRepository(session)
        attempts = []
        for task_id in DEMO_TASK_IDS:
            attempts.extend(await repository.list_attempts(run_id, task_id))
        return tuple(attempts)


async def schedule_and_publish(session_factory, dispatcher, run_id):
    async with session_factory() as session:
        summary = await WorkflowScheduler(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
        ).dispatch_ready(run_id)
        publish = await DispatchOutboxPublisher(
            DispatchOutboxRepository(session),
            dispatcher,
            claim_seconds=1,
        ).publish_pending()
    return summary, publish


async def run_one_worker(session_factory, dispatcher, registry, *, wait_for_heartbeat):
    async with session_factory() as session:
        worker = TaskWorker(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            dispatcher,
            registry,
            worker_id="demo-worker",
            lease_seconds=1,
            heartbeat_seconds=0.01,
        )
        if not wait_for_heartbeat:
            return await worker.run_once(timeout=1)
        return await worker.run_once(timeout=1)


def test_demo_distributed_e2e_happy_path() -> None:
    async def scenario() -> None:
        queue_name = f"fluxion:test:demo:{uuid4()}"
        dispatcher = RedisTaskDispatcher(REDIS_URL, queue_name)
        engine = create_async_engine(TEST_DATABASE_URL)
        await reset_schema(engine)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)

        workflow_id = f"wf-demo-{uuid4().hex}"
        run_id = f"run-demo-{uuid4().hex}"
        workflow = build_demo_workflow(workflow_id)
        calls: list[str] = []
        contexts: list[TaskExecutionContext] = []
        prepare_started = asyncio.Event()
        prepare_release = asyncio.Event()

        async def prepare(context: TaskExecutionContext) -> None:
            calls.append("demo.prepare")
            contexts.append(context)
            prepare_started.set()
            await prepare_release.wait()

        def process() -> None:
            calls.append("demo.process")

        async def finalize(context: TaskExecutionContext) -> None:
            calls.append("demo.finalize")
            contexts.append(context)

        registry = {
            "demo.prepare": prepare,
            "demo.process": process,
            "demo.finalize": finalize,
        }

        try:
            try:
                await dispatcher.ping()
            except Exception as exc:
                pytest.skip(f"Redis is unavailable: {exc}")

            async with session_factory() as session:
                await WorkflowRepository(session).save(workflow)
                await WorkflowRunRepository(session).create(
                    WorkflowRun.create(run_id, workflow)
                )

            initial = await load_run(session_factory, run_id, workflow)
            assert initial.status == WorkflowStatus.PENDING
            assert initial.get_task_status("demo.prepare") == TaskStatus.READY
            assert initial.get_task_status("demo.process") == TaskStatus.BLOCKED

            summary, publish = await schedule_and_publish(
                session_factory,
                dispatcher,
                run_id,
            )
            dispatched = await load_run(session_factory, run_id, workflow)

            assert summary.dispatched_task_ids == ("demo.prepare",)
            assert publish.published == 1
            assert dispatched.get_task_status("demo.prepare") == TaskStatus.DISPATCHED
            assert dispatched.get_task_status("demo.process") == TaskStatus.BLOCKED

            worker_task = asyncio.create_task(
                run_one_worker(
                    session_factory,
                    dispatcher,
                    registry,
                    wait_for_heartbeat=True,
                )
            )
            await asyncio.wait_for(prepare_started.wait(), timeout=1)

            async def heartbeat_seen() -> bool:
                attempts = await load_attempts(session_factory, run_id)
                prepare_attempt = next(
                    attempt
                    for attempt in attempts
                    if attempt.task_id == "demo.prepare"
                )
                return (
                    prepare_attempt.status == AttemptStatus.RUNNING
                    and prepare_attempt.worker_id == "demo-worker"
                    and prepare_attempt.lease_token is not None
                    and prepare_attempt.started_at is not None
                    and prepare_attempt.last_heartbeat_at is not None
                    and prepare_attempt.last_heartbeat_at > prepare_attempt.started_at
                )

            for _ in range(100):
                if await heartbeat_seen():
                    break
                await asyncio.sleep(0.01)
            else:
                raise AssertionError("worker heartbeat was not persisted")

            prepare_release.set()
            prepare_result = await asyncio.wait_for(worker_task, timeout=1)
            assert prepare_result.attempt_status == AttemptStatus.SUCCEEDED

            after_prepare = await load_run(session_factory, run_id, workflow)
            assert after_prepare.get_task_status("demo.prepare") == TaskStatus.SUCCEEDED
            assert after_prepare.get_task_status("demo.process") == TaskStatus.READY
            assert after_prepare.get_task_status("demo.finalize") == TaskStatus.BLOCKED

            summary, publish = await schedule_and_publish(
                session_factory,
                dispatcher,
                run_id,
            )
            assert summary.dispatched_task_ids == ("demo.process",)
            assert publish.published == 1
            process_result = await run_one_worker(
                session_factory,
                dispatcher,
                registry,
                wait_for_heartbeat=False,
            )
            assert process_result.attempt_status == AttemptStatus.SUCCEEDED

            after_process = await load_run(session_factory, run_id, workflow)
            assert after_process.get_task_status("demo.finalize") == TaskStatus.READY

            summary, publish = await schedule_and_publish(
                session_factory,
                dispatcher,
                run_id,
            )
            assert summary.dispatched_task_ids == ("demo.finalize",)
            assert publish.published == 1
            finalize_result = await run_one_worker(
                session_factory,
                dispatcher,
                registry,
                wait_for_heartbeat=False,
            )
            assert finalize_result.workflow_status == WorkflowStatus.SUCCEEDED

            final_run = await load_run(session_factory, run_id, workflow)
            attempts = await load_attempts(session_factory, run_id)

            assert final_run.status == WorkflowStatus.SUCCEEDED
            assert all(
                final_run.get_task_status(task_id) == TaskStatus.SUCCEEDED
                for task_id in DEMO_TASK_IDS
            )
            assert Counter(calls) == Counter(
                {
                    "demo.prepare": 1,
                    "demo.process": 1,
                    "demo.finalize": 1,
                }
            )
            assert [attempt.task_id for attempt in attempts] == list(DEMO_TASK_IDS)
            assert all(
                attempt.status == AttemptStatus.SUCCEEDED for attempt in attempts
            )
            assert all(attempt.attempt_number == 1 for attempt in attempts)
            assert contexts[0].attempt_key == f"{run_id}:demo.prepare:1"
            assert contexts[0].idempotency_key == f"{run_id}:demo.prepare"
        finally:
            try:
                client = Redis.from_url(REDIS_URL, decode_responses=True)
                await client.delete(queue_name)
                await client.aclose()
            finally:
                await dispatcher.aclose()
                await engine.dispose()

    asyncio.run(scenario())
