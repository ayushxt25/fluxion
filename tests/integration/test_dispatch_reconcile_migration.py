"""Alembic round-trip coverage for dispatch reconciliation metadata."""

import asyncio
import os

import pytest
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from alembic import command

URL = os.environ.get("TEST_DATABASE_URL")
if not URL:
    pytest.skip("TEST_DATABASE_URL is required", allow_module_level=True)


async def _columns() -> set[str]:
    engine = create_async_engine(URL)
    try:
        async with engine.connect() as connection:
            result = await connection.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'dispatch_outbox'"
                )
            )
            return set(result.scalars())
    finally:
        await engine.dispose()


def test_dispatch_reconciliation_migration_round_trip() -> None:
    config = Config("alembic.ini")
    command.upgrade(config, "20260928_0021")
    command.upgrade(config, "20260929_0022")
    assert {"last_reconciled_at", "reconcile_count"} <= asyncio.run(_columns())

    command.downgrade(config, "20260928_0021")
    assert not {"last_reconciled_at", "reconcile_count"} & asyncio.run(_columns())

    command.upgrade(config, "20260929_0022")
    assert {"last_reconciled_at", "reconcile_count"} <= asyncio.run(_columns())
