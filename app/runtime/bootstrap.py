import argparse
import asyncio
import signal
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import Settings, get_settings
from app.dispatch.transport import RedisTaskDispatcher
from app.observability.logging import configure_logging
from app.security.rate_limit import RedisRateLimiter


@dataclass
class RuntimeResources:
    settings: Settings
    session_factory: async_sessionmaker[AsyncSession]
    dispatcher: RedisTaskDispatcher
    rate_limiter: RedisRateLimiter
    _engine: object

    async def aclose(self) -> None:
        await self.dispatcher.aclose()
        await self.rate_limiter.aclose()
        await self._engine.dispose()


@asynccontextmanager
async def runtime_resources(
    settings: Settings | None = None,
) -> AsyncIterator[RuntimeResources]:
    settings = settings or get_settings()
    configure_logging(settings.log_level, settings.log_format)
    engine = create_async_engine(settings.database_url)
    resources = RuntimeResources(
        settings=settings,
        session_factory=async_sessionmaker(
            engine,
            expire_on_commit=False,
            class_=AsyncSession,
        ),
        dispatcher=RedisTaskDispatcher(
            settings.redis_url,
            settings.dispatch_queue_name,
        ),
        rate_limiter=RedisRateLimiter(settings.redis_url),
        _engine=engine,
    )
    try:
        yield resources
    finally:
        await resources.aclose()


def configure_runtime() -> Settings:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)
    return settings


def install_shutdown_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            signal.signal(sig, _signal_handler(stop_event))


def _signal_handler(stop_event: asyncio.Event) -> Callable[[int, object], None]:
    def handler(signum: int, frame: object) -> None:
        stop_event.set()

    return handler


def run_async(entrypoint: Callable[[], object]) -> None:
    configure_runtime()
    asyncio.run(entrypoint())


def parse_runtime_arguments(program: str, description: str) -> None:
    """Provide consistent help without initializing runtime resources."""
    parser = argparse.ArgumentParser(prog=program, description=description)
    parser.parse_args()
