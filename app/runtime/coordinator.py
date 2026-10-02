import asyncio
import uuid

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import get_settings
from app.observability.logging import configure_logging
from app.runtime.bootstrap import (
    install_shutdown_handlers,
    parse_runtime_arguments,
    run_async,
)
from app.runtime.preflight import preflight
from app.services.coordinator import RunCoordinator, RunCoordinatorRepository
from app.services.loops import RunCoordinatorLoop
from app.services.recovery import WorkflowRecoveryService
from app.services.repositories import (
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)


async def run(stop_event: asyncio.Event | None = None) -> None:
    install_signals = stop_event is None
    stop_event = stop_event or asyncio.Event()
    if install_signals:
        install_shutdown_handlers(stop_event)
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)
    engine = create_async_engine(settings.database_url)
    session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )
    try:
        await preflight(role="coordinator", settings=settings, engine=engine)
        async with session_factory() as session:
            workflow_repository = WorkflowRepository(session)
            run_repository = WorkflowRunRepository(session)
            coordinator = RunCoordinator(
                RunCoordinatorRepository(session),
                coordinator_id=str(uuid.uuid4()),
                lease_seconds=settings.run_coordinator_lease_seconds,
                batch_size=settings.run_coordinator_batch_size,
                recovery_service=WorkflowRecoveryService(
                    workflow_repository,
                    run_repository,
                    TaskAttemptRepository(session),
                ),
            )
            await RunCoordinatorLoop(coordinator).run(stop_event)
    finally:
        await engine.dispose()


def main() -> None:
    parse_runtime_arguments(
        "fluxion-coordinator",
        "Run Fluxion distributed run coordination.",
    )
    run_async(run)
