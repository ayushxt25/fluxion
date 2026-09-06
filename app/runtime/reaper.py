import asyncio

from app.runtime.bootstrap import (
    install_shutdown_handlers,
    run_async,
    runtime_resources,
)
from app.services.leases import LeaseReaper
from app.services.loops import LeaseReaperLoop
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
    async with (
        runtime_resources() as resources,
        resources.session_factory() as session,
    ):
        reaper = LeaseReaper(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
        )
        await LeaseReaperLoop(reaper).run(stop_event)


def main() -> None:
    run_async(run)
