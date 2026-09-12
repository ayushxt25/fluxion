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
from app.dispatch.transport import InMemoryTaskDispatcher
from app.engine.context import TaskExecutionContext
from app.engine.execution import WorkflowRun
from app.engine.status import AttemptStatus, TaskStatus, WorkflowStatus
from app.schemas.workflow import RetryPolicy, TaskDefinition, WorkflowDefinition
from app.services.outbox import DispatchOutboxPublisher
from app.services.repositories import (
    DispatchOutboxRepository,
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)
from app.services.scheduler import WorkflowScheduler
from app.services.worker import TaskWorker

pytestmark = [pytest.mark.integration, pytest.mark.stress]


async def _reset_schema(engine) -> None:
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)


def test_retry_storm_preserves_three_canonical_attempts_per_task() -> None:
    async def scenario() -> None:
        engine = create_async_engine(TEST_DATABASE_URL)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        dispatcher = InMemoryTaskDispatcher()
        task_ids = tuple(f"task-{index:02d}" for index in range(50))
        definition = WorkflowDefinition(
            id="stress-retry-storm",
            name="Retry storm",
            tasks=tuple(
                TaskDefinition(
                    id=task_id,
                    retry_policy=RetryPolicy(max_attempts=3, initial_backoff_seconds=0),
                )
                for task_id in task_ids
            ),
        )
        calls = {task_id: 0 for task_id in task_ids}

        async def fail_twice(context: TaskExecutionContext) -> dict[str, int]:
            calls[context.task_id] += 1
            if calls[context.task_id] < 3:
                raise RuntimeError("planned retry")
            return {"attempt": calls[context.task_id]}

        try:
            await _reset_schema(engine)
            async with factory() as session:
                await WorkflowRepository(session).save(definition)
                await WorkflowRunRepository(session).create(
                    WorkflowRun.create("run-1", definition)
                )

            for _ in range(3):
                async with factory() as session:
                    scheduler = WorkflowScheduler(
                        WorkflowRepository(session),
                        WorkflowRunRepository(session),
                        TaskAttemptRepository(session),
                        dispatcher,
                        DispatchOutboxRepository(session),
                        max_dispatch_per_run=50,
                        queue_high_watermark=1000,
                    )
                    await scheduler.dispatch_ready("run-1")
                    await DispatchOutboxPublisher(
                        DispatchOutboxRepository(session), dispatcher
                    ).publish_pending(limit=50)

                async with factory() as session:
                    worker = TaskWorker(
                        WorkflowRepository(session),
                        WorkflowRunRepository(session),
                        TaskAttemptRepository(session),
                        dispatcher,
                        {task_id: fail_twice for task_id in task_ids},
                        worker_id="storm-worker",
                    )
                    for _ in task_ids:
                        assert await worker.run_once(timeout=0.1) is not None

            async with factory() as session:
                run = await WorkflowRunRepository(session).get("run-1", definition)
                attempt_repository = TaskAttemptRepository(session)
                attempts = {
                    task_id: await attempt_repository.list_attempts("run-1", task_id)
                    for task_id in task_ids
                }

            assert run.status == WorkflowStatus.SUCCEEDED
            assert all(calls[task_id] == 3 for task_id in task_ids)
            assert all(
                [attempt.attempt_number for attempt in attempts[task_id]] == [1, 2, 3]
                for task_id in task_ids
            )
            assert all(
                attempts[task_id][-1].status == AttemptStatus.SUCCEEDED
                and run.task_runs[task_id].status == TaskStatus.SUCCEEDED
                and run.task_runs[task_id].result == {"attempt": 3}
                for task_id in task_ids
            )
        finally:
            await engine.dispose()

    asyncio.run(scenario())
