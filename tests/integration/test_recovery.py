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

from sqlalchemy import delete, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db import models  # noqa: F401
from app.db.base import Base
from app.db.models.execution import (
    TaskAttemptRecord,
    TaskRunRecord,
    WorkflowRunRecord,
)
from app.engine.exceptions import RecoveryStateError
from app.engine.execution import WorkflowRun
from app.engine.status import AttemptStatus, TaskStatus, WorkflowStatus
from app.schemas.workflow import TaskDefinition, WorkflowDefinition
from app.services.recovery import WorkflowRecoveryService
from app.services.repositories import (
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)


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


def test_pending_run_remains_resumable_and_persisted() -> None:
    async def body(session):
        definition = workflow("wf-pending", task("a"), task("b", ("a",)))
        workflow_run = WorkflowRun.create("run-1", definition)
        await save_run(session, definition, workflow_run)

        result = await WorkflowRecoveryService(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
        ).recover_run("run-1", "wf-pending")
        loaded = await WorkflowRunRepository(session).get("run-1", definition)

        assert result.resumable is True
        assert result.recovered_status == WorkflowStatus.PENDING
        assert loaded.status == WorkflowStatus.PENDING
        assert loaded.get_task_status("a") == TaskStatus.READY
        assert loaded.get_task_status("b") == TaskStatus.BLOCKED

    run_in_db(body)


def test_stale_running_task_is_interrupted_and_saved_atomically() -> None:
    async def body(session):
        definition = workflow("wf-stale", task("a"), task("b", ("a",)))
        workflow_run = WorkflowRun.restore(
            run_id="run-1",
            workflow=definition,
            status=WorkflowStatus.RUNNING,
            task_statuses={"a": TaskStatus.SUCCEEDED, "b": TaskStatus.RUNNING},
        )
        await save_run(session, definition, workflow_run)
        await add_attempt(session, definition, "run-1", "a", AttemptStatus.SUCCEEDED)
        await add_attempt(session, definition, "run-1", "b", AttemptStatus.RUNNING)

        result = await WorkflowRecoveryService(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
        ).recover_run("run-1", "wf-stale")
        loaded = await WorkflowRunRepository(session).get("run-1", definition)

        assert result.interrupted_task_ids == ("b",)
        assert result.recovered_status == WorkflowStatus.FAILED
        assert loaded.status == WorkflowStatus.FAILED
        assert loaded.get_task_status("a") == TaskStatus.SUCCEEDED
        assert loaded.get_task_status("b") == TaskStatus.INTERRUPTED
        attempts = await TaskAttemptRepository(session).list_attempts("run-1", "b")
        assert attempts[-1].status == AttemptStatus.INTERRUPTED

    run_in_db(body)


def test_multiple_running_tasks_and_disconnected_graph_recover() -> None:
    async def body(session):
        definition = workflow(
            "wf-disconnected-recovery",
            task("a"),
            task("b"),
            task("c", ("b",)),
        )
        workflow_run = WorkflowRun.restore(
            run_id="run-1",
            workflow=definition,
            status=WorkflowStatus.RUNNING,
            task_statuses={
                "a": TaskStatus.RUNNING,
                "b": TaskStatus.RUNNING,
                "c": TaskStatus.BLOCKED,
            },
        )
        await save_run(session, definition, workflow_run)
        await add_attempt(session, definition, "run-1", "a", AttemptStatus.RUNNING)
        await add_attempt(session, definition, "run-1", "b", AttemptStatus.RUNNING)

        result = await WorkflowRecoveryService(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
        ).recover_run("run-1", "wf-disconnected-recovery")

        assert result.interrupted_task_ids == ("a", "b")
        assert result.task_statuses["c"] == TaskStatus.BLOCKED
        a_attempts = await TaskAttemptRepository(session).list_attempts("run-1", "a")
        b_attempts = await TaskAttemptRepository(session).list_attempts("run-1", "b")
        assert a_attempts[-1].status == AttemptStatus.INTERRUPTED
        assert b_attempts[-1].status == AttemptStatus.INTERRUPTED

    run_in_db(body)


def test_running_workflow_without_running_tasks_is_resumable() -> None:
    async def body(session):
        definition = workflow("wf-resumable", task("a"), task("b", ("a",)))
        workflow_run = WorkflowRun.restore(
            run_id="run-1",
            workflow=definition,
            status=WorkflowStatus.RUNNING,
            task_statuses={"a": TaskStatus.SUCCEEDED, "b": TaskStatus.READY},
        )
        await save_run(session, definition, workflow_run)
        await add_attempt(session, definition, "run-1", "a", AttemptStatus.SUCCEEDED)

        result = await WorkflowRecoveryService(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
        ).recover_run("run-1", "wf-resumable")

        assert result.resumable is True
        assert result.recovered_status == WorkflowStatus.RUNNING
        assert result.interrupted_task_ids == ()

    run_in_db(body)


def test_running_task_without_running_attempt_is_rejected() -> None:
    async def body(session):
        definition = workflow("wf-missing-running-attempt", task("a"))
        workflow_run = WorkflowRun.restore(
            run_id="run-1",
            workflow=definition,
            status=WorkflowStatus.RUNNING,
            task_statuses={"a": TaskStatus.RUNNING},
        )
        await save_run(session, definition, workflow_run)

        with pytest.raises(RecoveryStateError):
            await WorkflowRecoveryService(
                WorkflowRepository(session),
                WorkflowRunRepository(session),
            ).recover_run("run-1", "wf-missing-running-attempt")

    run_in_db(body)


def test_list_incomplete_is_deterministic_and_excludes_terminal_runs() -> None:
    async def body(session):
        definition = workflow("wf-list", task("a"))
        await WorkflowRepository(session).save(definition)
        pending = WorkflowRun.create("b-run", definition)
        running = WorkflowRun.restore(
            run_id="a-run",
            workflow=definition,
            status=WorkflowStatus.RUNNING,
            task_statuses={"a": TaskStatus.READY},
        )
        succeeded = WorkflowRun.restore(
            run_id="c-run",
            workflow=definition,
            status=WorkflowStatus.SUCCEEDED,
            task_statuses={"a": TaskStatus.SUCCEEDED},
        )
        repository = WorkflowRunRepository(session)
        await repository.create(pending)
        await repository.create(running)
        await repository.create(succeeded)
        await add_attempt(session, definition, "c-run", "a", AttemptStatus.SUCCEEDED)

        refs = await repository.list_incomplete()

        assert [ref.run_id for ref in refs] == ["a-run", "b-run"]

    run_in_db(body)


def test_recovery_is_idempotent_after_interruption() -> None:
    async def body(session):
        definition = workflow("wf-idempotent", task("a"))
        workflow_run = WorkflowRun.restore(
            run_id="run-1",
            workflow=definition,
            status=WorkflowStatus.RUNNING,
            task_statuses={"a": TaskStatus.RUNNING},
        )
        await save_run(session, definition, workflow_run)
        await add_attempt(session, definition, "run-1", "a", AttemptStatus.RUNNING)
        service = WorkflowRecoveryService(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
        )

        first = await service.recover_incomplete_runs()
        second = await service.recover_incomplete_runs()

        assert len(first) == 1
        assert first[0].recovered_status == WorkflowStatus.FAILED
        assert second == ()

    run_in_db(body)


def test_missing_task_state_is_rejected() -> None:
    async def body(session):
        definition = workflow("wf-missing-state", task("a"), task("b"))
        await save_run(session, definition, WorkflowRun.create("run-1", definition))

        async with session.begin():
            await session.execute(
                delete(TaskRunRecord).where(TaskRunRecord.task_id == "b")
            )

        with pytest.raises(RecoveryStateError):
            await WorkflowRecoveryService(
                WorkflowRepository(session),
                WorkflowRunRepository(session),
            ).recover_run("run-1", "wf-missing-state")

    run_in_db(body)


def test_unknown_task_status_is_rejected() -> None:
    async def body(session):
        definition = workflow("wf-unknown-status", task("a"))
        await save_run(session, definition, WorkflowRun.create("run-1", definition))

        async with session.begin():
            await session.execute(
                update(TaskRunRecord)
                .where(TaskRunRecord.run_id == "run-1")
                .values(status="BOGUS")
            )

        with pytest.raises(RecoveryStateError):
            await WorkflowRecoveryService(
                WorkflowRepository(session),
                WorkflowRunRepository(session),
            ).recover_run("run-1", "wf-unknown-status")

    run_in_db(body)


def test_impossible_workflow_task_status_combination_is_rejected() -> None:
    async def body(session):
        definition = workflow("wf-impossible", task("a"))
        await save_run(session, definition, WorkflowRun.create("run-1", definition))

        async with session.begin():
            await session.execute(
                update(WorkflowRunRecord)
                .where(WorkflowRunRecord.run_id == "run-1")
                .values(status=WorkflowStatus.SUCCEEDED.value)
            )

        with pytest.raises(RecoveryStateError):
            await WorkflowRecoveryService(
                WorkflowRepository(session),
                WorkflowRunRepository(session),
            ).recover_run("run-1", "wf-impossible")

    run_in_db(body)


def test_separate_runs_recover_independently() -> None:
    async def body(session):
        definition = workflow("wf-independent-recovery", task("a"))
        await WorkflowRepository(session).save(definition)
        first = WorkflowRun.restore(
            run_id="run-1",
            workflow=definition,
            status=WorkflowStatus.RUNNING,
            task_statuses={"a": TaskStatus.RUNNING},
        )
        second = WorkflowRun.create("run-2", definition)
        repository = WorkflowRunRepository(session)
        await repository.create(first)
        await repository.create(second)
        await add_attempt(session, definition, "run-1", "a", AttemptStatus.RUNNING)

        results = await WorkflowRecoveryService(
            WorkflowRepository(session),
            repository,
        ).recover_incomplete_runs()
        loaded_first = await repository.get("run-1", definition)
        loaded_second = await repository.get("run-2", definition)

        assert [result.run_id for result in results] == ["run-1", "run-2"]
        assert loaded_first.status == WorkflowStatus.FAILED
        assert loaded_first.get_task_status("a") == TaskStatus.INTERRUPTED
        assert loaded_second.status == WorkflowStatus.PENDING
        assert loaded_second.get_task_status("a") == TaskStatus.READY

    run_in_db(body)
