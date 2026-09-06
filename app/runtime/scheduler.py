import asyncio

from app.runtime.bootstrap import (
    install_shutdown_handlers,
    run_async,
    runtime_resources,
)
from app.services.loops import SchedulerLoop
from app.services.repositories import (
    DispatchOutboxRepository,
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)
from app.services.scheduler import WorkflowScheduler


async def run(stop_event: asyncio.Event | None = None) -> None:
    install_signals = stop_event is None
    stop_event = stop_event or asyncio.Event()
    if install_signals:
        install_shutdown_handlers(stop_event)
    async with (
        runtime_resources() as resources,
        resources.session_factory() as session,
    ):
        run_repository = WorkflowRunRepository(session)
        scheduler = WorkflowScheduler(
            WorkflowRepository(session),
            run_repository,
            TaskAttemptRepository(session),
            outbox_repository=DispatchOutboxRepository(session),
        )
        await SchedulerLoop(scheduler, run_repository).run(stop_event)


def main() -> None:
    run_async(run)
