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
from app.engine.context import TaskExecutionContext
from app.engine.execution import WorkflowRun
from app.engine.status import TaskStatus, WorkflowStatus
from app.schemas.workflow import RetryPolicy, TaskDefinition, WorkflowDefinition
from app.services.execution import PersistentWorkflowExecutor
from app.services.repositories import WorkflowRepository, WorkflowRunRepository


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


def run_in_db(test_body) -> None:
    async def scenario() -> None:
        engine = create_async_engine(TEST_DATABASE_URL)
        await reset_schema(engine)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with session_factory() as session:
                await test_body(session)
        finally:
            await engine.dispose()

    asyncio.run(scenario())


async def execute(session, definition, implementations, **kwargs):
    await WorkflowRepository(session).save(definition)
    return await PersistentWorkflowExecutor(
        definition,
        implementations,
        WorkflowRepository(session),
        WorkflowRunRepository(session),
        **kwargs,
    ).run()


def test_result_persists_across_reload_and_reaches_downstream() -> None:
    async def body(session) -> None:
        definition = workflow(
            "wf-results",
            task("a"),
            task("b", ("a",)),
        )
        observed = {}

        def task_a() -> dict[str, int]:
            return {"value": 21}

        async def task_b(context: TaskExecutionContext) -> dict[str, int]:
            loaded = await WorkflowRunRepository(session).get("run-1", definition)
            observed["persisted_before_b"] = loaded.task_runs["a"].result
            observed["dependency"] = context.dependency_results["a"]
            return {"value": context.dependency_results["a"]["value"] * 2}

        result = await execute(
            session,
            definition,
            {"a": task_a, "b": task_b},
            run_id="run-1",
            max_concurrency=1,
        )
        loaded = await WorkflowRunRepository(session).get("run-1", definition)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert observed == {
            "persisted_before_b": {"value": 21},
            "dependency": {"value": 21},
        }
        assert loaded.task_runs["a"].result == {"value": 21}
        assert loaded.task_runs["a"].result_present is True
        assert loaded.task_runs["b"].result == {"value": 42}
        assert loaded.task_runs["b"].result_present is True

    run_in_db(body)


def test_failed_attempts_do_not_create_task_result_and_retry_success_does() -> None:
    async def body(session) -> None:
        definition = workflow(
            "wf-retry-result",
            task("a", retry_policy=RetryPolicy(max_attempts=2)),
        )
        calls = 0

        def flaky() -> dict[str, int]:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("boom")
            return {"value": calls}

        result = await execute(
            session,
            definition,
            {"a": flaky},
            run_id="run-1",
        )
        loaded = await WorkflowRunRepository(session).get("run-1", definition)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert loaded.task_runs["a"].result == {"value": 2}
        assert loaded.task_runs["a"].result_present is True

    run_in_db(body)


def test_unsupported_result_fails_task_without_persisting_result() -> None:
    async def body(session) -> None:
        definition = workflow("wf-bad-result", task("a"))

        result = await execute(
            session,
            definition,
            {"a": lambda: {"bad": object()}},
            run_id="run-1",
        )
        loaded = await WorkflowRunRepository(session).get("run-1", definition)

        assert result.status == WorkflowStatus.FAILED
        assert loaded.get_task_status("a") == TaskStatus.FAILED
        assert loaded.task_runs["a"].result is None
        assert loaded.task_runs["a"].result_present is False

    run_in_db(body)


def test_cancellation_preserves_prior_successful_result() -> None:
    async def body(session) -> None:
        definition = workflow("wf-cancel-result", task("a"), task("b", ("a",)))
        await WorkflowRepository(session).save(definition)
        run = WorkflowRun.create("run-1", definition)
        await WorkflowRunRepository(session).create(run)
        run.start_task("a")
        run.complete_task("a", result={"value": 1})
        await WorkflowRunRepository(session).save_state(run)
        run.cancel_workflow()
        await WorkflowRunRepository(session).save_state(run)
        loaded = await WorkflowRunRepository(session).get("run-1", definition)

        assert loaded.status == WorkflowStatus.CANCELLED
        assert loaded.task_runs["a"].result == {"value": 1}
        assert loaded.task_runs["a"].result_present is True

    run_in_db(body)
