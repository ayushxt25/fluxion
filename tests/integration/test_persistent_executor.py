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
from app.engine.exceptions import (
    ExecutionPersistenceError,
    WorkflowNotFoundError,
    WorkflowRunAlreadyExistsError,
)
from app.engine.execution import WorkflowRun
from app.engine.status import TaskStatus, WorkflowStatus
from app.schemas.workflow import TaskDefinition, WorkflowDefinition
from app.services.execution import PersistentWorkflowExecutor
from app.services.repositories import WorkflowRepository, WorkflowRunRepository


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


async def execute_persisted(session, definition, implementations, **kwargs):
    await WorkflowRepository(session).save(definition)
    executor = PersistentWorkflowExecutor(
        definition,
        implementations,
        WorkflowRepository(session),
        WorkflowRunRepository(session),
        **kwargs,
    )
    return await executor.run()


def test_initial_run_persisted_before_any_task_begins() -> None:
    async def body(session):
        definition = workflow("wf-initial", task("a"), task("b", ("a",)))
        await WorkflowRepository(session).save(definition)
        observed = {}

        async def task_a() -> None:
            loaded = await WorkflowRunRepository(session).get("run-1", definition)
            observed["workflow"] = loaded.status
            observed["a"] = loaded.get_task_status("a")
            observed["b"] = loaded.get_task_status("b")

        executor = PersistentWorkflowExecutor(
            definition,
            {"a": task_a, "b": lambda: None},
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            run_id="run-1",
            max_concurrency=1,
        )
        await executor.run()

        assert observed == {
            "workflow": WorkflowStatus.RUNNING,
            "a": TaskStatus.RUNNING,
            "b": TaskStatus.BLOCKED,
        }

    run_in_db(body)


def test_successful_linear_execution_persists_unlocks_and_final_state() -> None:
    async def body(session):
        definition = workflow("wf-linear", task("a"), task("b", ("a",)))
        result = await execute_persisted(
            session,
            definition,
            {"a": lambda: None, "b": lambda: None},
            run_id="run-1",
            max_concurrency=1,
        )
        loaded = await WorkflowRunRepository(session).get("run-1", definition)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert loaded.status == result.status
        assert loaded.get_task_status("a") == TaskStatus.SUCCEEDED
        assert loaded.get_task_status("b") == TaskStatus.SUCCEEDED

    run_in_db(body)


def test_newly_unlocked_ready_state_is_durable() -> None:
    async def body(session):
        definition = workflow("wf-ready", task("a"), task("b", ("a",)))
        await WorkflowRepository(session).save(definition)
        observed = {}

        async def task_b() -> None:
            loaded = await WorkflowRunRepository(session).get("run-1", definition)
            observed["b"] = loaded.get_task_status("b")

        await PersistentWorkflowExecutor(
            definition,
            {"a": lambda: None, "b": task_b},
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            run_id="run-1",
            max_concurrency=1,
        ).run()

        assert observed["b"] == TaskStatus.RUNNING

    run_in_db(body)


def test_task_failure_persists_failed_workflow_and_blocks_downstream() -> None:
    async def body(session):
        definition = workflow("wf-fail", task("a"), task("b", ("a",)))

        def fail() -> None:
            raise RuntimeError("boom")

        result = await execute_persisted(
            session,
            definition,
            {"a": fail, "b": lambda: None},
            run_id="run-1",
        )
        loaded = await WorkflowRunRepository(session).get("run-1", definition)

        assert result.status == WorkflowStatus.FAILED
        assert result.errors == {"a": "RuntimeError: boom"}
        assert loaded.status == WorkflowStatus.FAILED
        assert loaded.get_task_status("a") == TaskStatus.FAILED
        assert loaded.get_task_status("b") == TaskStatus.BLOCKED

    run_in_db(body)


def test_multiple_roots_diamond_and_disconnected_components_persist() -> None:
    async def body(session):
        definition = workflow(
            "wf-shapes",
            task("a"),
            task("b", ("a",)),
            task("c", ("a",)),
            task("d", ("b", "c")),
            task("x"),
            task("y", ("x",)),
        )
        result = await execute_persisted(
            session,
            definition,
            {task_id: lambda: None for task_id in ("a", "b", "c", "d", "x", "y")},
            run_id="run-1",
            max_concurrency=2,
        )
        loaded = await WorkflowRunRepository(session).get("run-1", definition)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert all(
            status == TaskStatus.SUCCEEDED for status in result.task_statuses.values()
        )
        assert loaded.status == WorkflowStatus.SUCCEEDED

    run_in_db(body)


def test_concurrent_completions_do_not_lose_updates() -> None:
    async def body(session):
        definition = workflow("wf-concurrent", task("a"), task("b"))
        await WorkflowRepository(session).save(definition)
        both_started = asyncio.Event()
        active = 0

        async def concurrent_task() -> None:
            nonlocal active
            active += 1
            if active == 2:
                both_started.set()
            await both_started.wait()

        result = await PersistentWorkflowExecutor(
            definition,
            {"a": concurrent_task, "b": concurrent_task},
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            run_id="run-1",
        ).run()
        loaded = await WorkflowRunRepository(session).get("run-1", definition)

        assert result.task_statuses == {
            "a": TaskStatus.SUCCEEDED,
            "b": TaskStatus.SUCCEEDED,
        }
        assert loaded.get_task_status("a") == TaskStatus.SUCCEEDED
        assert loaded.get_task_status("b") == TaskStatus.SUCCEEDED

    run_in_db(body)


def test_duplicate_run_id_rejected_before_task_execution() -> None:
    async def body(session):
        definition = workflow("wf-dup-run", task("a"))
        await WorkflowRepository(session).save(definition)
        repository = WorkflowRunRepository(session)
        await repository.create(WorkflowRun.create("run-1", definition))
        calls = []

        with pytest.raises(WorkflowRunAlreadyExistsError):
            await PersistentWorkflowExecutor(
                definition,
                {"a": lambda: calls.append("a")},
                WorkflowRepository(session),
                WorkflowRunRepository(session),
                run_id="run-1",
            ).run()

        assert calls == []

    run_in_db(body)


def test_non_persisted_workflow_rejected_before_task_execution() -> None:
    async def body(session):
        definition = workflow("wf-missing", task("a"))
        calls = []

        with pytest.raises(WorkflowNotFoundError):
            await PersistentWorkflowExecutor(
                definition,
                {"a": lambda: calls.append("a")},
                WorkflowRepository(session),
                WorkflowRunRepository(session),
            ).run()

        assert calls == []

    run_in_db(body)


class FakeRunRepository:
    def __init__(self, fail_on_save_number: int | None = None) -> None:
        self.fail_on_save_number = fail_on_save_number
        self.created = False
        self.save_count = 0

    async def create(self, workflow_run):
        self.created = True

    async def save_state(self, workflow_run):
        self.save_count += 1
        if self.save_count == self.fail_on_save_number:
            raise RuntimeError("db down")


class ExistingWorkflowRepository:
    async def exists(self, workflow_id: str) -> bool:
        return True


def test_persistence_failure_before_task_start_prevents_callable_invocation() -> None:
    async def scenario():
        calls = []
        with pytest.raises(ExecutionPersistenceError):
            await PersistentWorkflowExecutor(
                workflow("wf", task("a")),
                {"a": lambda: calls.append("a")},
                ExistingWorkflowRepository(),
                FakeRunRepository(fail_on_save_number=1),
            ).run()
        return calls

    assert asyncio.run(scenario()) == []


def test_persistence_failure_after_task_success_is_infrastructure_failure() -> None:
    async def scenario():
        calls = []
        repository = FakeRunRepository(fail_on_save_number=2)
        with pytest.raises(ExecutionPersistenceError):
            await PersistentWorkflowExecutor(
                workflow("wf", task("a")),
                {"a": lambda: calls.append("a")},
                ExistingWorkflowRepository(),
                repository,
            ).run()
        return calls

    assert asyncio.run(scenario()) == ["a"]
