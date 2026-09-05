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

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db import models  # noqa: F401
from app.db.base import Base
from app.db.models.execution import TaskAttemptRecord, TaskRunRecord
from app.engine.context import TaskExecutionContext
from app.engine.execution import WorkflowRun
from app.engine.status import AttemptStatus, TaskStatus, WorkflowStatus
from app.schemas.workflow import RetryPolicy, TaskDefinition, WorkflowDefinition
from app.services.execution import PersistentWorkflowExecutor
from app.services.repositories import (
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)
from app.services.resume import WorkflowResumeService


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


def test_retry_attempt_rows_persist_and_final_success_matches_db() -> None:
    async def body(session):
        definition = workflow(
            "wf-retry-success",
            task("a", retry_policy=RetryPolicy(max_attempts=2)),
        )
        await WorkflowRepository(session).save(definition)
        calls = 0

        def flaky() -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("boom")

        result = await PersistentWorkflowExecutor(
            definition,
            {"a": flaky},
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            run_id="run-1",
        ).run()
        attempts = await TaskAttemptRepository(session).list_attempts("run-1", "a")
        loaded = await WorkflowRunRepository(session).get("run-1", definition)
        task_run = loaded.task_runs["a"]

        assert result.status == WorkflowStatus.SUCCEEDED
        assert loaded.status == WorkflowStatus.SUCCEEDED
        assert task_run.idempotency_key == "run-1:a"
        assert [attempt.status for attempt in attempts] == [
            AttemptStatus.FAILED,
            AttemptStatus.SUCCEEDED,
        ]
        assert attempts[0].error_type == "RuntimeError"
        assert attempts[0].error_message == "boom"

    run_in_db(body)


def test_context_identity_persists_across_retries() -> None:
    async def body(session):
        definition = workflow(
            "wf-context-retry",
            task("a", retry_policy=RetryPolicy(max_attempts=2)),
        )
        await WorkflowRepository(session).save(definition)
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

        await PersistentWorkflowExecutor(
            definition,
            {"a": flaky},
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            run_id="run-1",
        ).run()
        loaded = await WorkflowRunRepository(session).get("run-1", definition)

        assert observed == [
            (1, "run-1:a:1", "run-1:a"),
            (2, "run-1:a:2", "run-1:a"),
        ]
        assert loaded.task_runs["a"].idempotency_key == "run-1:a"

    run_in_db(body)


def test_separate_tasks_and_runs_get_unique_idempotency_keys() -> None:
    async def body(session):
        definition = workflow("wf-keys", task("a"), task("b"))
        await WorkflowRepository(session).save(definition)
        await WorkflowRunRepository(session).create(
            WorkflowRun.create("run-1", definition)
        )
        await WorkflowRunRepository(session).create(
            WorkflowRun.create("run-2", definition)
        )

        result = await session.execute(
            select(TaskRunRecord.idempotency_key).order_by(
                TaskRunRecord.run_id,
                TaskRunRecord.task_id,
            )
        )

        assert tuple(result.scalars()) == (
            "run-1:a",
            "run-1:b",
            "run-2:a",
            "run-2:b",
        )

    run_in_db(body)


def test_duplicate_idempotency_key_is_prevented() -> None:
    async def body(session):
        definition = workflow("wf-duplicate-key", task("a"), task("b"))
        await WorkflowRepository(session).save(definition)
        await WorkflowRunRepository(session).create(
            WorkflowRun.create("run-1", definition)
        )

        with pytest.raises(IntegrityError):
            async with session.begin():
                record = await session.get(TaskRunRecord, ("run-1", "b"))
                record.idempotency_key = "run-1:a"

    run_in_db(body)


def test_attempts_exhausted_persist_failed_task_and_workflow() -> None:
    async def body(session):
        definition = workflow(
            "wf-retry-failed",
            task("a", retry_policy=RetryPolicy(max_attempts=2)),
            task("b", ("a",)),
        )
        await WorkflowRepository(session).save(definition)

        def fail() -> None:
            raise RuntimeError("boom")

        result = await PersistentWorkflowExecutor(
            definition,
            {"a": fail, "b": lambda: None},
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            run_id="run-1",
        ).run()
        loaded = await WorkflowRunRepository(session).get("run-1", definition)

        assert result.status == WorkflowStatus.FAILED
        assert loaded.status == WorkflowStatus.FAILED
        assert loaded.get_task_status("a") == TaskStatus.FAILED
        assert loaded.get_task_status("b") == TaskStatus.BLOCKED

    run_in_db(body)


def test_overdue_retry_waiting_resumes_and_preserves_attempt_history() -> None:
    async def body(session):
        definition = workflow(
            "wf-retry-resume",
            task("a", retry_policy=RetryPolicy(max_attempts=2)),
        )
        await WorkflowRepository(session).save(definition)
        workflow_run = WorkflowRun.restore(
            run_id="run-1",
            workflow=definition,
            status=WorkflowStatus.RUNNING,
            task_statuses={"a": TaskStatus.RETRY_WAITING},
            next_retry_at={"a": datetime.now(UTC) - timedelta(seconds=1)},
        )
        await WorkflowRunRepository(session).create(workflow_run)
        async with session.begin():
            session.add(
                TaskAttemptRecord(
                    run_id="run-1",
                    workflow_id=definition.id,
                    task_id="a",
                    attempt_number=1,
                    status=AttemptStatus.FAILED.value,
                    started_at=datetime.now(UTC) - timedelta(seconds=2),
                    finished_at=datetime.now(UTC) - timedelta(seconds=1),
                    error_type="RuntimeError",
                    error_message="boom",
                )
            )

        result = await WorkflowResumeService(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
        ).resume_run("run-1", {"a": lambda: None})
        attempts = await TaskAttemptRepository(session).list_attempts("run-1", "a")

        assert result.status == WorkflowStatus.SUCCEEDED
        assert [attempt.attempt_number for attempt in attempts] == [1, 2]
        assert attempts[-1].status == AttemptStatus.SUCCEEDED

    run_in_db(body)


def test_duplicate_attempt_number_is_prevented() -> None:
    async def body(session):
        definition = workflow("wf-duplicate-attempt", task("a"))
        await WorkflowRepository(session).save(definition)
        await WorkflowRunRepository(session).create(
            WorkflowRun.create("run-1", definition)
        )
        attempt = TaskAttemptRecord(
            run_id="run-1",
            workflow_id=definition.id,
            task_id="a",
            attempt_number=1,
            status=AttemptStatus.FAILED.value,
        )
        duplicate = TaskAttemptRecord(
            run_id="run-1",
            workflow_id=definition.id,
            task_id="a",
            attempt_number=1,
            status=AttemptStatus.FAILED.value,
        )

        with pytest.raises(IntegrityError):
            async with session.begin():
                session.add_all([attempt, duplicate])

    run_in_db(body)
