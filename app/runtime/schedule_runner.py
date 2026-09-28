import asyncio
import logging
from contextlib import suppress

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import get_settings
from app.observability.metrics import record_schedule_runner_error
from app.runtime.bootstrap import (
    install_shutdown_handlers,
    parse_runtime_arguments,
    run_async,
)
from app.services.schedules import ScheduleRunner

logger = logging.getLogger(__name__)


async def run() -> None:
    settings = get_settings()
    stop_event = asyncio.Event()
    install_shutdown_handlers(stop_event)
    engine = create_async_engine(settings.database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    try:
        while not stop_event.is_set():
            try:
                async with sessions() as session:
                    await ScheduleRunner(session).tick()
            except Exception:
                record_schedule_runner_error()
                logger.exception("schedule_runner_tick_failed")
            with suppress(TimeoutError):
                await asyncio.wait_for(
                    stop_event.wait(), timeout=settings.schedule_runner_poll_seconds
                )
    finally:
        await engine.dispose()


def main() -> None:
    parse_runtime_arguments(
        "fluxion-schedule-runner", "Run durable workflow schedules."
    )
    run_async(run)
