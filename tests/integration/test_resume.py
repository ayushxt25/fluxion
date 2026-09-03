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

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db import models  # noqa: F401
from app.db.base import Base
from app.db.models.execution import (
    TaskAttemptRecord,
    TaskRunRecord,
    WorkflowRunRecord,
)
from app.engine.exceptions import (
    MissingTaskImplementationError,
    WorkflowRunNotResumableError,
)
from app.engine.execution import WorkflowRun
from app.engine.status import AttemptStatus, TaskStatus, WorkflowStatus
from app.schemas.workflow import TaskDefinition, WorkflowDefinition
from app.services.repositories import WorkflowRepository, WorkflowRunRepository
from app.services.resume import WorkflowResumeService


def task(task_id: str, depends_on: tuple[str, ...] = ()) -> TaskDefinition:
    return TaskDefinition(id=task_id, depends_on=depends_on)


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


async def save_run(session, definition, workflow_run) -> None:
    await WorkflowRepository(session).save(definition)
    await WorkflowRunRepository(session).create(workflow_run)


async def add_attempt(
    session,
    definition,
    run_id: str,
    task_id: str,
    status: AttemptStatus,
) -> None:
    started_at = datetime.now(UTC) - timedelta(seconds=1)
    finished_at = None if status == AttemptStatus.RUNNING else datetime.now(UTC)
    async with session.begin():
        session.add(
            TaskAttemptRecord(
                run_id=run_id,
                workflow_id=definition.id,
                task_id=task_id,
                attempt_number=1,
                status=status.value,
                started_at=started_at,
                finished_at=finished_at,
            )
        )


async def row_count(session, model) -> int:
    async with session.begin():
        return await session.scalar(select(func.count()).select_from(model))


def test_resume_pending_single_task_run_persists_success() -> None:
    async def body(session):
        definition = workflow("wf-resume-single", task("a"))
        await save_run(session, definition, WorkflowRun.create("run-1", definition))

        result = await WorkflowResumeService(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
        ).resume_run("run-1", {"a": lambda: None})
        loaded = await WorkflowRunRepository(session).get("run-1", definition)

        assert result.run_id == "run-1"
        assert result.status == WorkflowStatus.SUCCEEDED
        assert loaded.status == WorkflowStatus.SUCCEEDED
        assert loaded.get_task_status("a") == TaskStatus.SUCCEEDED

    run_in_db(body)


def test_resume_running_ready_task_does_not_rerun_succeeded_task() -> None:
    async def body(session):
        definition = workflow("wf-resume-ready", task("a"), task("b", ("a",)))
        workflow_run = WorkflowRun.restore(
            run_id="run-1",
            workflow=definition,
            status=WorkflowStatus.RUNNING,
            task_statuses={"a": TaskStatus.SUCCEEDED, "b": TaskStatus.READY},
        )
        await save_run(session, definition, workflow_run)
        await add_attempt(session, definition, "run-1", "a", AttemptStatus.SUCCEEDED)
        calls = []

        result = await WorkflowResumeService(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
        ).resume_run("run-1", {"b": lambda: calls.append("b")})

        assert result.status == WorkflowStatus.SUCCEEDED
        assert calls == ["b"]

    run_in_db(body)


def test_resume_diamond_and_disconnected_components() -> None:
    async def body(session):
        definition = workflow(
            "wf-resume-shapes",
            task("a"),
            task("b", ("a",)),
            task("c", ("a",)),
            task("d", ("b", "c")),
            task("x"),
            task("y", ("x",)),
        )
        workflow_run = WorkflowRun.restore(
            run_id="run-1",
            workflow=definition,
            status=WorkflowStatus.RUNNING,
            task_statuses={
                "a": TaskStatus.SUCCEEDED,
                "b": TaskStatus.READY,
                "c": TaskStatus.READY,
                "d": TaskStatus.BLOCKED,
                "x": TaskStatus.READY,
                "y": TaskStatus.BLOCKED,
            },
        )
        await save_run(session, definition, workflow_run)
        await add_attempt(session, definition, "run-1", "a", AttemptStatus.SUCCEEDED)
        implementations = {
            task_id: lambda: None for task_id in ("b", "c", "d", "x", "y")
        }

        result = await WorkflowResumeService(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
        ).resume_run("run-1", implementations, max_concurrency=2)
        loaded = await WorkflowRunRepository(session).get("run-1", definition)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert all(
            status == TaskStatus.SUCCEEDED
            for status in result.task_statuses.values()
        )
        assert loaded.status == WorkflowStatus.SUCCEEDED

    run_in_db(body)


def test_resume_does_not_create_duplicate_run_or_task_rows() -> None:
    async def body(session):
        definition = workflow("wf-resume-counts", task("a"), task("b", ("a",)))
        await save_run(session, definition, WorkflowRun.create("run-1", definition))

        await WorkflowResumeService(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
        ).resume_run("run-1", {"a": lambda: None, "b": lambda: None})

        assert await row_count(session, WorkflowRunRecord) == 1
        assert await row_count(session, TaskRunRecord) == 2

    run_in_db(body)


def test_resume_rejects_terminal_and_interrupted_runs() -> None:
    async def body(session):
        succeeded_def = workflow("wf-succeeded", task("a"))
        succeeded_run = WorkflowRun.restore(
            run_id="succeeded",
            workflow=succeeded_def,
            status=WorkflowStatus.SUCCEEDED,
            task_statuses={"a": TaskStatus.SUCCEEDED},
        )
        interrupted_def = workflow("wf-interrupted", task("a"))
        interrupted_run = WorkflowRun.restore(
            run_id="interrupted",
            workflow=interrupted_def,
            status=WorkflowStatus.FAILED,
            task_statuses={"a": TaskStatus.INTERRUPTED},
        )
        await save_run(session, succeeded_def, succeeded_run)
        await save_run(session, interrupted_def, interrupted_run)
        await add_attempt(
            session,
            succeeded_def,
            "succeeded",
            "a",
            AttemptStatus.SUCCEEDED,
        )
        await add_attempt(
            session,
            interrupted_def,
            "interrupted",
            "a",
            AttemptStatus.INTERRUPTED,
        )
        service = WorkflowResumeService(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
        )

        with pytest.raises(WorkflowRunNotResumableError):
            await service.resume_run("succeeded", {})
        with pytest.raises(WorkflowRunNotResumableError):
            await service.resume_run("interrupted", {"a": lambda: None})

    run_in_db(body)


def test_stale_running_task_is_recovered_then_resume_rejected() -> None:
    async def body(session):
        definition = workflow("wf-stale-resume", task("a"))
        workflow_run = WorkflowRun.restore(
            run_id="run-1",
            workflow=definition,
            status=WorkflowStatus.RUNNING,
            task_statuses={"a": TaskStatus.RUNNING},
        )
        await save_run(session, definition, workflow_run)
        await add_attempt(session, definition, "run-1", "a", AttemptStatus.RUNNING)
        calls = []

        with pytest.raises(WorkflowRunNotResumableError):
            await WorkflowResumeService(
                WorkflowRepository(session),
                WorkflowRunRepository(session),
            ).resume_run("run-1", {"a": lambda: calls.append("a")})
        loaded = await WorkflowRunRepository(session).get("run-1", definition)

        assert calls == []
        assert loaded.status == WorkflowStatus.FAILED
        assert loaded.get_task_status("a") == TaskStatus.INTERRUPTED

    run_in_db(body)


def test_missing_implementation_rejected_before_callable_runs() -> None:
    async def body(session):
        definition = workflow("wf-missing-impl", task("a"), task("b"))
        await save_run(session, definition, WorkflowRun.create("run-1", definition))
        calls = []

        with pytest.raises(MissingTaskImplementationError):
            await WorkflowResumeService(
                WorkflowRepository(session),
                WorkflowRunRepository(session),
            ).resume_run("run-1", {"a": lambda: calls.append("a")})

        assert calls == []

    run_in_db(body)


def test_callable_failure_during_resume_persists_failed_state() -> None:
    async def body(session):
        definition = workflow("wf-resume-fail", task("a"), task("b", ("a",)))
        await save_run(session, definition, WorkflowRun.create("run-1", definition))

        def fail() -> None:
            raise RuntimeError("boom")

        result = await WorkflowResumeService(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
        ).resume_run("run-1", {"a": fail, "b": lambda: None})
        loaded = await WorkflowRunRepository(session).get("run-1", definition)

        assert result.status == WorkflowStatus.FAILED
        assert loaded.status == WorkflowStatus.FAILED
        assert loaded.get_task_status("a") == TaskStatus.FAILED
        assert loaded.get_task_status("b") == TaskStatus.BLOCKED

    run_in_db(body)
