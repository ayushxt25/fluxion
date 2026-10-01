import asyncio
import os
from urllib.parse import urlparse

import pytest

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
if not TEST_DATABASE_URL:
    pytest.skip("TEST_DATABASE_URL is not set", allow_module_level=True)

database_name = urlparse(TEST_DATABASE_URL).path.rsplit("/", maxsplit=1)[-1]
if not database_name.endswith("_test"):
    pytest.skip(
        "TEST_DATABASE_URL must point to a *_test database",
        allow_module_level=True,
    )

# ruff: noqa: E402
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from alembic import command
from app.core.config import get_settings


def _config() -> Config:
    os.environ["DATABASE_URL"] = TEST_DATABASE_URL
    get_settings.cache_clear()
    return Config("alembic.ini")


async def _reset() -> None:
    engine = create_async_engine(TEST_DATABASE_URL)
    try:
        async with engine.begin() as connection:
            await connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
            await connection.execute(text("CREATE SCHEMA public"))
            await connection.execute(text("GRANT ALL ON SCHEMA public TO public"))
    finally:
        await engine.dispose()


def test_run_coordinator_migration_round_trip() -> None:
    config = _config()
    asyncio.run(_reset())
    try:
        command.upgrade(config, "20260929_0022")
        command.upgrade(config, "20260930_0023")

        async def verify_upgraded() -> None:
            engine = create_async_engine(TEST_DATABASE_URL)
            try:
                async with engine.connect() as connection:
                    columns = await connection.execute(
                        text(
                            "SELECT column_name, is_nullable FROM "
                            "information_schema.columns WHERE "
                            "table_name = 'workflow_runs'"
                        )
                    )
                    values = dict(columns.all())
                    assert values["coordinator_id"] == "YES"
                    assert values["coordinator_lease_token"] == "YES"
                    assert values["coordinator_lease_expires_at"] == "YES"
                    assert values["coordinator_last_heartbeat_at"] == "YES"
                    indexes = await connection.execute(
                        text(
                            "SELECT indexdef FROM pg_indexes WHERE "
                            "indexname = 'ix_workflow_runs_coordinator_claimable'"
                        )
                    )
                    assert "status" in indexes.scalar_one()
            finally:
                await engine.dispose()

        asyncio.run(verify_upgraded())
        command.downgrade(config, "20260929_0022")
        command.upgrade(config, "20260930_0023")
    finally:
        asyncio.run(_reset())
