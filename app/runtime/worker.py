import asyncio
import logging

from app.runtime.bootstrap import (
    install_shutdown_handlers,
    run_async,
    runtime_resources,
)
from app.services.repositories import (
    TaskAttemptRepository,
    WorkflowRepository,
    WorkflowRunRepository,
)
from app.services.worker import TaskWorker
from app.tasks.registry import build_task_registry

logger = logging.getLogger(__name__)


async def run(stop_event: asyncio.Event | None = None) -> None:
    install_signals = stop_event is None
    stop_event = stop_event or asyncio.Event()
    if install_signals:
        install_shutdown_handlers(stop_event)
    registry = build_task_registry()
    async with (
        runtime_resources() as resources,
        resources.session_factory() as session,
    ):
        worker = TaskWorker(
            WorkflowRepository(session),
            WorkflowRunRepository(session),
            TaskAttemptRepository(session),
            resources.dispatcher,
            registry,
        )
        logger.info(
            "Worker runtime started.",
            extra={"worker_id": worker.worker_id},
        )
        await run_worker_loop(worker, stop_event)
        logger.info(
            "Worker runtime stopped.",
            extra={"worker_id": worker.worker_id},
        )


async def run_worker_loop(worker: TaskWorker, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        await worker.run_once(timeout=1)


def main() -> None:
    run_async(run)
