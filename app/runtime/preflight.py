"""Bounded, non-destructive startup checks for service runtimes."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from app.core.config import Settings
from app.dispatch.transport import RedisTaskDispatcher
from app.version import __version__

logger = logging.getLogger(__name__)


class StartupPreflightError(RuntimeError):
    """Raised when a role cannot safely start against its dependencies."""


def expected_schema_revision() -> str:
    root = Path(__file__).resolve().parents[2]
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "alembic"))
    heads = ScriptDirectory.from_config(config).get_heads()
    if len(heads) != 1:
        raise StartupPreflightError("Migration configuration has multiple heads.")
    return heads[0]


async def check_database(engine: AsyncEngine, timeout_seconds: float) -> None:
    try:
        async with engine.connect() as connection:
            await asyncio.wait_for(
                connection.execute(text("SELECT 1")), timeout=timeout_seconds
            )
            revision = await asyncio.wait_for(
                connection.scalar(text("SELECT version_num FROM alembic_version")),
                timeout=timeout_seconds,
            )
    except (SQLAlchemyError, TimeoutError):
        raise StartupPreflightError(
            "PostgreSQL is unavailable or migrations have not been applied."
        ) from None
    if revision != expected_schema_revision():
        raise StartupPreflightError(
            "Database schema revision is incompatible; run the migration job first."
        )


async def check_redis(dispatcher: RedisTaskDispatcher, timeout_seconds: float) -> None:
    try:
        await asyncio.wait_for(dispatcher.ping(), timeout=timeout_seconds)
    except Exception:
        raise StartupPreflightError("Redis is unavailable.") from None


async def preflight(
    *,
    role: str,
    settings: Settings,
    engine: AsyncEngine,
    dispatcher: RedisTaskDispatcher | None = None,
    require_redis: bool = False,
) -> None:
    logger.info(
        "Fluxion service starting.",
        extra={
            "event": "service.starting",
            "service": role,
            "version": __version__,
            "environment": settings.app_env,
        },
    )
    try:
        await check_database(engine, settings.readiness_timeout_seconds)
        if require_redis:
            if dispatcher is None:
                raise StartupPreflightError(
                    "Redis dispatcher is required for this role."
                )
            await check_redis(dispatcher, settings.readiness_timeout_seconds)
    except StartupPreflightError:
        logger.error(
            "Fluxion service startup failed.",
            extra={"event": "service.startup_failed", "service": role},
        )
        raise
    logger.info(
        "Fluxion service ready.",
        extra={"event": "service.ready", "service": role, "version": __version__},
    )


def log_stopping(role: str) -> None:
    logger.info(
        "Fluxion service stopping.",
        extra={"event": "service.stopping", "service": role},
    )


def log_stopped(role: str) -> None:
    logger.info(
        "Fluxion service stopped.", extra={"event": "service.stopped", "service": role}
    )
