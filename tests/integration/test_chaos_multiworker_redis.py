import os
from urllib.parse import urlparse
from uuid import uuid4

import pytest

REDIS_URL = os.getenv("REDIS_URL")
TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
if not REDIS_URL or not TEST_DATABASE_URL:
    pytest.skip("REDIS_URL and TEST_DATABASE_URL are required", allow_module_level=True)

test_database_name = urlparse(TEST_DATABASE_URL).path.rsplit("/", maxsplit=1)[-1]
if not test_database_name.endswith("_test"):
    pytest.skip(
        "TEST_DATABASE_URL must point to a *_test database",
        allow_module_level=True,
    )

# ruff: noqa: E402
import asyncio
from contextlib import suppress

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db import models  # noqa: F401
from app.db.base import Base
from app.dispatch.transport import RedisTaskDispatcher
from app.engine.context import TaskExecutionContext
from app.engine.exceptions import DispatchStateError
from app.engine.execution import WorkflowRun
from app.engine.status import AttemptStatus, TaskStatus, WorkflowStatus
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

pytestmark = [pytest.mark.integration, pytest.mark.chaos]


async def _reset_schema(engine) -> None:
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)


def test_two_workers_consume_redis_dispatches_without_duplicate_success() -> None:
    async def scenario() -> None:
        engine = create_async_engine(TEST_DATABASE_URL)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        queue_name = f"fluxion:test:chaos:workers:{uuid4()}"
        dispatcher = RedisTaskDispatcher(REDIS_URL, queue_name)
        redis = Redis.from_url(REDIS_URL, decode_responses=True)
        task_ids = tuple(f"task-{index:02d}" for index in range(12))
        definition = WorkflowDefinition(
            id="chaos-multiworker",
            name="Chaos multiworker",
            tasks=tuple(TaskDefinition(id=task_id) for task_id in task_ids),
        )
        calls: list[tuple[str, str]] = []

        async def worker_a_task(context: TaskExecutionContext) -> dict[str, str]:
            calls.append(("worker-a", context.task_id))
            return {"worker": "worker-a"}

        async def worker_b_task(context: TaskExecutionContext) -> dict[str, str]:
            calls.append(("worker-b", context.task_id))
            return {"worker": "worker-b"}

        try:
            await _reset_schema(engine)
            async with factory() as session:
                await WorkflowRepository(session).save(definition)
                await WorkflowRunRepository(session).create(
                    WorkflowRun.create("run-1", definition)
                )
                summary = await WorkflowScheduler(
                    WorkflowRepository(session),
                    WorkflowRunRepository(session),
                    TaskAttemptRepository(session),
                    dispatcher,
                    DispatchOutboxRepository(session),
                    max_dispatch_per_run=len(task_ids),
                    queue_high_watermark=100,
                ).dispatch_ready("run-1")
                await DispatchOutboxPublisher(
                    DispatchOutboxRepository(session), dispatcher
                ).publish_pending(limit=len(task_ids))

            async with factory() as first_session, factory() as second_session:
                worker_a = TaskWorker(
                    WorkflowRepository(first_session),
                    WorkflowRunRepository(first_session),
                    TaskAttemptRepository(first_session),
                    dispatcher,
                    {task_id: worker_a_task for task_id in task_ids},
                    worker_id="worker-a",
                )
                worker_b = TaskWorker(
                    WorkflowRepository(second_session),
                    WorkflowRunRepository(second_session),
                    TaskAttemptRepository(second_session),
                    dispatcher,
                    {task_id: worker_b_task for task_id in task_ids},
                    worker_id="worker-b",
                )
                for _ in range(len(task_ids) // 2):
                    assert await worker_a.run_once(timeout=1) is not None
                    assert await worker_b.run_once(timeout=1) is not None

                await dispatcher.dispatch(summary.messages[0])
                with pytest.raises(DispatchStateError):
                    await worker_a.run_once(timeout=1)

            async with factory() as session:
                run = await WorkflowRunRepository(session).get("run-1", definition)
                attempts = {
                    task_id: await TaskAttemptRepository(session).list_attempts(
                        "run-1", task_id
                    )
                    for task_id in task_ids
                }

            assert run.status == WorkflowStatus.SUCCEEDED
            assert {worker_id for worker_id, _ in calls} == {"worker-a", "worker-b"}
            assert len(calls) == len(task_ids)
            assert len({task_id for _, task_id in calls}) == len(task_ids)
            assert all(
                len(attempts[task_id]) == 1
                and attempts[task_id][0].status == AttemptStatus.SUCCEEDED
                and run.task_runs[task_id].status == TaskStatus.SUCCEEDED
                and run.task_runs[task_id].result_present
                for task_id in task_ids
            )
        finally:
            with suppress(Exception):
                await redis.delete(queue_name)
            with suppress(Exception):
                await redis.aclose()
            with suppress(Exception):
                await dispatcher.aclose()
            with suppress(Exception):
                await engine.dispose()

    asyncio.run(scenario())
