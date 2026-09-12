import asyncio
import logging
from contextlib import suppress
from uuid import uuid4

from app.core.config import get_settings
from app.dispatch.messages import TaskDispatchMessage
from app.observability.metrics import (
    record_worker_capacity,
    record_worker_dispatch_accepted,
    record_worker_dispatch_rejected,
)
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
    settings = get_settings()
    stop_event = stop_event or asyncio.Event()
    if install_signals:
        install_shutdown_handlers(stop_event)
    registry = build_task_registry()
    async with runtime_resources() as resources:
        worker_id = str(uuid4())
        record_worker_capacity(settings.worker_concurrency)
        logger.info(
            "Worker runtime started.",
            extra={
                "worker_id": worker_id,
                "configured_concurrency": settings.worker_concurrency,
            },
        )
        worker_id = await run_worker_loop(
            resources.session_factory,
            resources.dispatcher,
            registry,
            stop_event,
            concurrency=settings.worker_concurrency,
            shutdown_grace_seconds=settings.worker_shutdown_grace_seconds,
            worker_id=worker_id,
        )
        logger.info(
            "Worker runtime stopped.",
            extra={"worker_id": worker_id},
        )


async def run_worker_loop(
    session_factory,
    dispatcher,
    registry,
    stop_event: asyncio.Event,
    *,
    concurrency: int,
    shutdown_grace_seconds: float,
    worker_id: str | None = None,
) -> str:
    if concurrency < 1:
        raise ValueError("concurrency must be at least 1.")
    if shutdown_grace_seconds <= 0:
        raise ValueError("shutdown_grace_seconds must be positive.")

    worker_id = worker_id or str(uuid4())
    active: set[asyncio.Task[None]] = set()

    async def execute(message: TaskDispatchMessage) -> None:
        nonlocal worker_id
        async with session_factory() as session:
            worker = TaskWorker(
                WorkflowRepository(session),
                WorkflowRunRepository(session),
                TaskAttemptRepository(session),
                dispatcher,
                registry,
                worker_id=worker_id,
            )
            try:
                await worker.process_message(message)
                record_worker_dispatch_accepted()
            except Exception:
                record_worker_dispatch_rejected()
                logger.exception(
                    "Worker failed to process dispatch message.",
                    extra={"worker_id": worker.worker_id},
                )

    while not stop_event.is_set():
        if len(active) >= concurrency:
            stop_waiter = asyncio.create_task(stop_event.wait())
            done, _ = await asyncio.wait(
                {*active, stop_waiter},
                return_when=asyncio.FIRST_COMPLETED,
            )
            stop_waiter.cancel()
            with suppress(asyncio.CancelledError):
                await stop_waiter
            if stop_event.is_set():
                break
            for task in done:
                if task is not stop_waiter:
                    active.remove(task)
                    task.result()
            continue

        receive_task = asyncio.create_task(dispatcher.receive(timeout=1))
        stop_waiter = asyncio.create_task(stop_event.wait())
        done, _ = await asyncio.wait(
            {receive_task, stop_waiter},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_waiter in done:
            receive_task.cancel()
            with suppress(asyncio.CancelledError):
                await receive_task
            break
        stop_waiter.cancel()
        with suppress(asyncio.CancelledError):
            await stop_waiter
        message = receive_task.result()
        if message is None:
            active = {task for task in active if not task.done()}
            continue
        active.add(asyncio.create_task(execute(message)))

    if active:
        done, pending = await asyncio.wait(active, timeout=shutdown_grace_seconds)
        for task in done:
            task.result()
        if pending:
            logger.warning(
                "Worker shutdown grace expired with active executions.",
                extra={"active_count": len(pending), "worker_id": worker_id},
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
    return worker_id


def main() -> None:
    run_async(run)
