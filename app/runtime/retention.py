import asyncio
import logging
from contextlib import suppress

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import Settings, get_settings
from app.observability.logging import configure_logging
from app.runtime.bootstrap import (
    install_shutdown_handlers,
    parse_runtime_arguments,
    run_async,
)
from app.services.retention import RetentionRepository, RetentionService

logger = logging.getLogger(__name__)


async def run_once(
    sessions: async_sessionmaker[AsyncSession], settings: Settings
) -> None:
    """Execute one configured, bounded retention pass using PostgreSQL only."""
    async with sessions() as session:
        summary = await RetentionService(
            RetentionRepository(session), settings
        ).run_retention()
    logger.info("retention_cycle_completed", extra={"deleted": summary.total_deleted})


async def run(
    stop_event: asyncio.Event | None = None, settings: Settings | None = None
) -> None:
    install_signals = stop_event is None
    stop_event = stop_event or asyncio.Event()
    settings = settings or get_settings()
    configure_logging(settings.log_level, settings.log_format)
    if not settings.retention_enabled:
        logger.info("retention_runtime_disabled")
        return
    if install_signals:
        install_shutdown_handlers(stop_event)

    engine = create_async_engine(settings.database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    try:
        while not stop_event.is_set():
            try:
                await run_once(sessions, settings)
            except Exception:
                logger.exception("retention_cycle_failed")
            with suppress(TimeoutError):
                await asyncio.wait_for(
                    stop_event.wait(), timeout=settings.retention_poll_interval_seconds
                )
    finally:
        await engine.dispose()


def main() -> None:
    parse_runtime_arguments("fluxion-retention", "Run the Fluxion retention service.")
    run_async(run)
