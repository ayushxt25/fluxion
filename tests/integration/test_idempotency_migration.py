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

from alembic.config import Config
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from alembic import command
from app.core.config import get_settings
from app.db.models.execution import TaskRunRecord
from app.schemas.workflow import TaskDefinition, WorkflowDefinition
from app.services.repositories import WorkflowRunRepository


def alembic_config() -> Config:
    os.environ["DATABASE_URL"] = TEST_DATABASE_URL
    get_settings.cache_clear()
    return Config("alembic.ini")


async def insert_phase8_run(engine) -> None:
    async with engine.begin() as connection:
        await connection.execute(
            text(
                """
                INSERT INTO workflow_definitions (id, name)
                VALUES ('wf-migrated', 'Migrated')
                """
            )
        )
        await connection.execute(
            text(
                """
                INSERT INTO task_definitions (
                    workflow_id,
                    task_id,
                    name,
                    retry_max_attempts,
                    retry_initial_backoff_seconds,
                    retry_backoff_multiplier,
                    retry_max_backoff_seconds
                )
                VALUES
                    ('wf-migrated', 'a', NULL, 1, 0, 2, NULL),
                    ('wf-migrated', 'b', NULL, 1, 0, 2, NULL)
                """
            )
        )
        await connection.execute(
            text(
                """
                INSERT INTO workflow_runs (run_id, workflow_id, status)
                VALUES ('run-migrated', 'wf-migrated', 'PENDING')
                """
            )
        )
        await connection.execute(
            text(
                """
                INSERT INTO task_runs (
                    run_id,
                    workflow_id,
                    task_id,
                    status,
                    next_retry_at
                )
                VALUES
                    ('run-migrated', 'wf-migrated', 'a', 'READY', NULL),
                    ('run-migrated', 'wf-migrated', 'b', 'READY', NULL)
                """
            )
        )


def test_phase9_migration_backfills_task_run_idempotency_keys() -> None:
    config = alembic_config()
    command.downgrade(config, "base")
    command.upgrade(config, "20260903_0002")

    async def scenario() -> None:
        engine = create_async_engine(TEST_DATABASE_URL)
        try:
            await insert_phase8_run(engine)
        finally:
            await engine.dispose()

    asyncio.run(scenario())
    command.upgrade(config, "head")

    async def verify() -> None:
        engine = create_async_engine(TEST_DATABASE_URL)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with session_factory() as session:
                result = await session.execute(
                    select(TaskRunRecord.task_id, TaskRunRecord.idempotency_key)
                    .where(TaskRunRecord.run_id == "run-migrated")
                    .order_by(TaskRunRecord.task_id)
                )
                assert tuple(result.all()) == (
                    ("a", "run-migrated:a"),
                    ("b", "run-migrated:b"),
                )

            async with session_factory() as session:
                workflow = WorkflowDefinition(
                    id="wf-migrated",
                    name="Migrated",
                    tasks=(TaskDefinition(id="a"), TaskDefinition(id="b")),
                )
                loaded = await WorkflowRunRepository(session).get(
                    "run-migrated",
                    workflow,
                )
                assert loaded.task_runs["a"].idempotency_key == "run-migrated:a"

            async with session_factory() as session:
                with pytest.raises(IntegrityError):
                    async with session.begin():
                        record = await session.get(
                            TaskRunRecord,
                            ("run-migrated", "b"),
                        )
                        record.idempotency_key = "run-migrated:a"
        finally:
            await engine.dispose()

    asyncio.run(verify())
