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
from app.engine.status import WorkflowStatus
from app.schemas.workflow import (
    DependencyResultParameter,
    TaskDefinition,
    WorkflowDefinition,
    WorkflowInputParameter,
)
from app.services.execution import PersistentWorkflowExecutor
from app.services.repositories import WorkflowRepository, WorkflowRunRepository


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


def test_workflow_input_persists_reloads_and_feeds_parameters() -> None:
    async def body(session) -> None:
        observed = []
        definition = WorkflowDefinition(
            id="wf-input",
            name="Workflow",
            tasks=(
                TaskDefinition(
                    id="prepare",
                    parameters={
                        "seed": WorkflowInputParameter(
                            source="workflow_input",
                            path=("seed",),
                        ),
                    },
                ),
                TaskDefinition(
                    id="process",
                    depends_on=("prepare",),
                    parameters={
                        "value": DependencyResultParameter(
                            source="dependency_result",
                            task_id="prepare",
                            path=("value",),
                        ),
                    },
                ),
            ),
        )

        def prepare(*, seed: int) -> dict[str, int]:
            return {"value": seed}

        def process(context: TaskExecutionContext, *, value: int) -> dict[str, int]:
            observed.append((context.workflow_input, context.workflow_input_present))
            return {"value": value * 2}

        await WorkflowRepository(session).save(definition)
        result = await PersistentWorkflowExecutor(
            definition,
            {"prepare": prepare, "process": process},
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            run_id="run-input",
            workflow_input={"seed": 21},
            workflow_input_present=True,
        ).run()

        loaded = await WorkflowRunRepository(session).get("run-input", definition)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert loaded.workflow_input == {"seed": 21}
        assert loaded.workflow_input_present is True
        assert loaded.task_runs["process"].result == {"value": 42}
        assert observed == [({"seed": 21}, True)]

    run_in_db(body)


def test_explicit_null_input_is_distinct_from_missing_input() -> None:
    async def body(session) -> None:
        definition = WorkflowDefinition(
            id="wf-null-input",
            name="Workflow",
            tasks=(TaskDefinition(id="a"),),
        )
        await WorkflowRepository(session).save(definition)

        await PersistentWorkflowExecutor(
            definition,
            {"a": lambda: None},
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            run_id="run-null",
            workflow_input=None,
            workflow_input_present=True,
        ).run()

        loaded = await WorkflowRunRepository(session).get("run-null", definition)

        assert loaded.workflow_input is None
        assert loaded.workflow_input_present is True

    run_in_db(body)
