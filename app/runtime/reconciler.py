import asyncio

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import get_settings
from app.observability.logging import configure_logging
from app.runtime.bootstrap import (
    install_shutdown_handlers,
    parse_runtime_arguments,
    run_async,
)
from app.runtime.preflight import preflight
from app.services.loops import DispatchReconcilerLoop
from app.services.outbox import DispatchReconciler
from app.services.repositories import DispatchOutboxRepository


async def run(stop_event: asyncio.Event | None = None) -> None:
    install_signals = stop_event is None
    stop_event = stop_event or asyncio.Event()
    if install_signals:
        install_shutdown_handlers(stop_event)
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)
    engine = create_async_engine(settings.database_url)
    session_factory = async_sessionmaker(
        engine, expire_on_commit=False, class_=AsyncSession
    )
    try:
        await preflight(role="reconciler", settings=settings, engine=engine)
        async with session_factory() as session:
            reconciler = DispatchReconciler(
                DispatchOutboxRepository(session),
                reconcile_after_seconds=settings.dispatch_reconcile_after_seconds,
                batch_size=settings.dispatch_reconcile_batch_size,
            )
            await DispatchReconcilerLoop(reconciler).run(stop_event)
    finally:
        await engine.dispose()


def main() -> None:
    parse_runtime_arguments(
        "fluxion-reconciler",
        "Run Fluxion dispatch reconciliation.",
    )
    run_async(run)
